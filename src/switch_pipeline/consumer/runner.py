"""Kafka consume loop: one batch -> one database transaction -> commit offsets.

Offsets are committed only after the transaction commits, so a crash replays
at most the last uncommitted batch, which the idempotent writes absorb.
"""

import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import psycopg
from confluent_kafka import (
    OFFSET_BEGINNING,
    OFFSET_STORED,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from psycopg_pool import ConnectionPool, PoolTimeout

from switch_pipeline.consumer.decoding import InboundMessage
from switch_pipeline.consumer.processor import EventProcessor, Outcome
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.kafka import consumer_config
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.observability import get_logger
from switch_pipeline.retry import Backoff
from switch_pipeline.settings import ConsumerSettings, KafkaSettings

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class CatchUpReport:
    outcomes: Counter[Outcome]
    end_offsets: dict[int, int]
    complete: bool


class ConsumerRunner:
    def __init__(
        self,
        *,
        kafka: KafkaSettings,
        settings: ConsumerSettings,
        pool: ConnectionPool,
        processor: EventProcessor,
        shutdown: Shutdown,
        heartbeat: Heartbeat,
        client_id: str,
    ) -> None:
        self._kafka = kafka
        self._settings = settings
        self._pool = pool
        self._processor = processor
        self._shutdown = shutdown
        self._heartbeat = heartbeat
        self._client_id = client_id
        self._backoff = Backoff(
            settings.db_backoff_initial_seconds, settings.db_backoff_max_seconds
        )

    def run(self) -> None:
        """Consume as a member of the consumer group until shutdown."""
        consumer = Consumer(consumer_config(self._kafka, client_id=self._client_id))
        consumer.subscribe(
            [self._kafka.topic],
            on_assign=lambda _consumer, partitions: log.info(
                "partitions_assigned", partitions=sorted(p.partition for p in partitions)
            ),
            on_revoke=lambda _consumer, partitions: log.info(
                "partitions_revoked", partitions=sorted(p.partition for p in partitions)
            ),
        )
        log.info("consumer_started", topic=self._kafka.topic, group=self._kafka.consumer_group)
        try:
            while not self._shutdown.requested():
                self._heartbeat.beat()
                self._consume_batch(consumer)
        finally:
            consumer.close()
        log.info("consumer_stopped")

    def end_offsets(self) -> dict[int, int]:
        """Current end offset of every non-empty partition."""
        consumer = Consumer(consumer_config(self._kafka, client_id=self._client_id))
        try:
            return self._end_offsets(consumer, self._partitions(consumer))
        finally:
            consumer.close()

    def catch_up(
        self, *, from_beginning: bool, until: Mapping[int, int] | None = None
    ) -> CatchUpReport:
        """Process records up to ``until`` (default: the end offsets now), then return.

        Records at or past the bound are neither processed nor committed.
        ``from_beginning=True`` replays the topic from offset 0. Partitions are
        assigned manually, so the consumer group must have no active members.
        """
        consumer = Consumer(consumer_config(self._kafka, client_id=self._client_id))
        try:
            partitions = self._partitions(consumer)
            end_offsets = (
                dict(until) if until is not None else self._end_offsets(consumer, partitions)
            )
            pending = {p: end for p, end in end_offsets.items() if p in partitions and end > 0}
            if not from_beginning:
                # position() stays unset for partitions with nothing left to read,
                # so drop the ones whose committed offset already reached the end.
                committed = consumer.committed(
                    [TopicPartition(self._kafka.topic, p) for p in pending],
                    timeout=self._kafka.admin_timeout_seconds,
                )
                for partition in committed:
                    if partition.offset >= pending[partition.partition]:
                        del pending[partition.partition]
            start = OFFSET_BEGINNING if from_beginning else OFFSET_STORED
            consumer.assign([TopicPartition(self._kafka.topic, p, start) for p in partitions])
            totals: Counter[Outcome] = Counter()
            while pending and not self._shutdown.requested():
                self._heartbeat.beat()
                totals.update(self._consume_batch(consumer, bounds=end_offsets))
                positions = consumer.position(
                    [TopicPartition(self._kafka.topic, p) for p in pending]
                )
                for position in positions:
                    if position.offset >= 0 and position.offset >= pending[position.partition]:
                        del pending[position.partition]
            return CatchUpReport(outcomes=totals, end_offsets=end_offsets, complete=not pending)
        finally:
            consumer.close()

    def _consume_batch(
        self, consumer: Consumer, *, bounds: Mapping[int, int] | None = None
    ) -> Counter[Outcome]:
        records = consumer.consume(
            num_messages=self._settings.batch_size, timeout=self._settings.poll_timeout_seconds
        )
        messages: list[InboundMessage] = []
        for record in records:
            error = record.error()
            if error is None:
                message = InboundMessage.from_kafka(record)
                if bounds is None or message.offset < bounds.get(message.partition, 0):
                    messages.append(message)
            elif error.fatal():
                raise FatalPipelineError(f"fatal consumer error: {error.str()}")
            else:
                log.warning("consumer_error", error=error.str(), code=error.name())
        if not messages:
            return Counter()
        started = time.perf_counter()
        outcomes = self._process_with_retry(messages)
        self._commit(consumer, messages)
        log.info(
            "batch_processed",
            records=len(messages),
            duration_ms=round((time.perf_counter() - started) * 1000, 1),
            adapter_batches=sorted(
                {m.headers["batch_id"] for m in messages if "batch_id" in m.headers}
            ),
            **{outcome.value: count for outcome, count in outcomes.items()},
        )
        return outcomes

    def _process_with_retry(self, messages: Sequence[InboundMessage]) -> Counter[Outcome]:
        attempt = 0
        while True:
            attempt += 1
            try:
                with self._pool.connection() as conn, conn.transaction():
                    return self._processor.process_batch(conn, messages)
            except (psycopg.OperationalError, PoolTimeout) as exc:
                if attempt >= self._settings.db_max_attempts or self._shutdown.requested():
                    raise
                delay = self._backoff.delay(attempt)
                log.warning(
                    "sink_write_retry_scheduled",
                    attempt=attempt,
                    max_attempts=self._settings.db_max_attempts,
                    retry_in_seconds=round(delay, 2),
                    error=str(exc).strip(),
                )
                self._shutdown.sleep(delay)

    def _commit(self, consumer: Consumer, messages: Sequence[InboundMessage]) -> None:
        next_offsets: dict[int, int] = {}
        for message in messages:
            next_offsets[message.partition] = max(
                next_offsets.get(message.partition, 0), message.offset + 1
            )
        offsets = [TopicPartition(self._kafka.topic, p, o) for p, o in sorted(next_offsets.items())]
        try:
            consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as exc:
            error: KafkaError = exc.args[0]
            if error.fatal():
                raise FatalPipelineError(f"fatal error committing offsets: {error.str()}") from exc
            # The records are already in the database; if they are redelivered
            # (e.g. after a rebalance) they are skipped as duplicates.
            log.warning("offset_commit_failed", error=error.str())

    def _partitions(self, consumer: Consumer) -> list[int]:
        metadata = consumer.list_topics(
            self._kafka.topic, timeout=self._kafka.admin_timeout_seconds
        )
        topic = metadata.topics.get(self._kafka.topic)
        if topic is None or topic.error is not None:
            raise FatalPipelineError(f"topic {self._kafka.topic!r} is not available")
        return sorted(topic.partitions)

    def _end_offsets(self, consumer: Consumer, partitions: list[int]) -> dict[int, int]:
        ends: dict[int, int] = {}
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(self._kafka.topic, partition),
                timeout=self._kafka.admin_timeout_seconds,
            )
            if high > low:
                ends[partition] = high
        return ends
