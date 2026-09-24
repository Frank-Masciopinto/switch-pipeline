"""Kafka consumer side: the record streams the consumer reads.

Offsets are committed explicitly, and only for records the caller reports as
processed, so a crash replays at most the uncommitted tail.
"""

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager

from confluent_kafka import (
    OFFSET_BEGINNING,
    OFFSET_STORED,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)

from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.observability import get_logger
from switch_pipeline.settings import KafkaSettings
from switch_pipeline.transport.codec import InboundMessage, decode_headers
from switch_pipeline.transport.config import consumer_config

log = get_logger(__name__)


class KafkaRecordStream:
    """Implements the consumer's RecordStream port over one Kafka consumer."""

    def __init__(self, consumer: Consumer, topic: str) -> None:
        self._consumer = consumer
        self._topic = topic

    def poll(self, max_records: int, timeout_seconds: float) -> list[InboundMessage]:
        messages: list[InboundMessage] = []
        for record in self._consumer.consume(num_messages=max_records, timeout=timeout_seconds):
            error = record.error()
            if error is None:
                messages.append(to_inbound(record))
            elif error.fatal():
                raise FatalPipelineError(f"fatal consumer error: {error.str()}")
            else:
                # librdkafka recovers from these on its own (e.g. a broker restart).
                log.warning("consumer_error", error=error.str(), code=error.name())
        return messages

    def commit(self, records: Sequence[InboundMessage]) -> None:
        next_offsets: dict[int, int] = {}
        for record in records:
            next_offsets[record.partition] = max(
                next_offsets.get(record.partition, 0), record.offset + 1
            )
        if not next_offsets:
            return
        offsets = [TopicPartition(self._topic, p, o) for p, o in sorted(next_offsets.items())]
        try:
            self._consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as exc:
            error: KafkaError = exc.args[0]
            if error.fatal():
                raise FatalPipelineError(f"fatal error committing offsets: {error.str()}") from exc
            # The records are already in the sink; if they are redelivered (e.g.
            # after a rebalance) the consumer skips them as duplicates.
            log.warning("offset_commit_failed", error=error.str())


class BoundedKafkaStream(KafkaRecordStream):
    """Reads manually assigned partitions up to fixed end offsets."""

    def __init__(
        self, consumer: Consumer, topic: str, *, bounds: Mapping[int, int], pending: set[int]
    ) -> None:
        super().__init__(consumer, topic)
        self._bounds = dict(bounds)
        self._pending = pending  # partitions not yet read up to their bound

    def poll(self, max_records: int, timeout_seconds: float) -> list[InboundMessage]:
        messages = [
            message
            for message in super().poll(max_records, timeout_seconds)
            if message.offset < self._bounds.get(message.partition, 0)
        ]
        if self._pending:
            positions = self._consumer.position(
                [TopicPartition(self._topic, p) for p in self._pending]
            )
            for position in positions:
                # Unfetched partitions report a negative position, below any bound.
                if position.offset >= self._bounds[position.partition]:
                    self._pending.discard(position.partition)
        return messages

    def finished(self) -> bool:
        return not self._pending


class KafkaStreams:
    """Opens record streams on the pipeline topic."""

    def __init__(self, settings: KafkaSettings, *, client_id: str) -> None:
        self._settings = settings
        self._client_id = client_id

    @contextmanager
    def live(self) -> Iterator[KafkaRecordStream]:
        """Read as a member of the consumer group, from its committed offsets."""
        consumer = self._consumer()
        try:
            consumer.subscribe(
                [self._settings.topic],
                on_assign=lambda _consumer, partitions: log.info(
                    "partitions_assigned", partitions=sorted(p.partition for p in partitions)
                ),
                on_revoke=lambda _consumer, partitions: log.info(
                    "partitions_revoked", partitions=sorted(p.partition for p in partitions)
                ),
            )
            yield KafkaRecordStream(consumer, self._settings.topic)
        finally:
            consumer.close()

    @contextmanager
    def bounded(
        self, *, from_beginning: bool, until: Mapping[int, int] | None = None
    ) -> Iterator[BoundedKafkaStream]:
        """Read up to ``until`` (default: the end offsets now), from offset 0 or
        from the group's committed offsets.

        Partitions are assigned manually, so the consumer group must have no
        active members.
        """
        topic = self._settings.topic
        consumer = self._consumer()
        try:
            partitions = self._partitions(consumer)
            bounds = dict(until) if until is not None else self._end_offsets(consumer, partitions)
            pending = {p for p, end in bounds.items() if p in partitions and end > 0}
            if pending and not from_beginning:
                committed = consumer.committed(
                    [TopicPartition(topic, p) for p in pending],
                    timeout=self._settings.admin_timeout_seconds,
                )
                pending -= {c.partition for c in committed if c.offset >= bounds[c.partition]}
            start = OFFSET_BEGINNING if from_beginning else OFFSET_STORED
            consumer.assign([TopicPartition(topic, p, start) for p in partitions])
            yield BoundedKafkaStream(consumer, topic, bounds=bounds, pending=pending)
        finally:
            consumer.close()

    def end_offsets(self) -> dict[int, int]:
        """Current end offset of every non-empty partition."""
        consumer = self._consumer()
        try:
            return self._end_offsets(consumer, self._partitions(consumer))
        finally:
            consumer.close()

    def _consumer(self) -> Consumer:
        return Consumer(consumer_config(self._settings, client_id=self._client_id))

    def _partitions(self, consumer: Consumer) -> list[int]:
        metadata = consumer.list_topics(
            self._settings.topic, timeout=self._settings.admin_timeout_seconds
        )
        topic = metadata.topics.get(self._settings.topic)
        if topic is None or topic.error is not None:
            raise FatalPipelineError(f"topic {self._settings.topic!r} is not available")
        return sorted(topic.partitions)

    def _end_offsets(self, consumer: Consumer, partitions: list[int]) -> dict[int, int]:
        ends: dict[int, int] = {}
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(self._settings.topic, partition),
                timeout=self._settings.admin_timeout_seconds,
            )
            if high > low:
                ends[partition] = high
        return ends


def to_inbound(record: Message) -> InboundMessage:
    topic, partition, offset = record.topic(), record.partition(), record.offset()
    if topic is None or partition is None or offset is None:
        raise ValueError("consumed record has no topic/partition/offset")
    return InboundMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        key=record.key(),
        value=record.value(),
        headers=decode_headers(record.headers()),
    )
