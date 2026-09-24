"""Materialization loop: one batch of records -> one sink transaction -> commit offsets.

Offsets are committed only after the transaction commits, so a crash replays
at most the last uncommitted batch, which the idempotent writes absorb.
"""

import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

from switch_pipeline.consumer.ports import BoundedStream, RecordStream, Sink
from switch_pipeline.consumer.processor import EventProcessor, Outcome
from switch_pipeline.errors import RetryableError
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.observability import get_logger
from switch_pipeline.retry import Backoff
from switch_pipeline.settings import ConsumerSettings
from switch_pipeline.transport.codec import InboundMessage

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class CatchUpReport:
    outcomes: Counter[Outcome]
    complete: bool


class ConsumerRunner:
    def __init__(
        self,
        *,
        sink: Sink,
        processor: EventProcessor,
        settings: ConsumerSettings,
        shutdown: Shutdown,
        heartbeat: Heartbeat,
    ) -> None:
        self._sink = sink
        self._processor = processor
        self._settings = settings
        self._shutdown = shutdown
        self._heartbeat = heartbeat
        self._backoff = Backoff(
            settings.db_backoff_initial_seconds, settings.db_backoff_max_seconds
        )

    def run(self, stream: RecordStream) -> None:
        """Materialize records until shutdown."""
        while not self._shutdown.requested():
            self._heartbeat.beat()
            self._consume_batch(stream)

    def catch_up(self, stream: BoundedStream) -> CatchUpReport:
        """Materialize records until the stream ends (or shutdown), then return."""
        totals: Counter[Outcome] = Counter()
        while not stream.finished() and not self._shutdown.requested():
            self._heartbeat.beat()
            totals.update(self._consume_batch(stream))
        return CatchUpReport(outcomes=totals, complete=stream.finished())

    def _consume_batch(self, stream: RecordStream) -> Counter[Outcome]:
        messages = stream.poll(self._settings.batch_size, self._settings.poll_timeout_seconds)
        if not messages:
            return Counter()
        started = time.perf_counter()
        outcomes = self._process_with_retry(messages)
        stream.commit(messages)
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
                with self._sink.transaction() as writer:
                    return self._processor.process_batch(writer, messages)
            except RetryableError as exc:
                if attempt >= self._settings.db_max_attempts or self._shutdown.requested():
                    raise
                delay = self._backoff.delay(attempt)
                log.warning(
                    "sink_write_retry_scheduled",
                    attempt=attempt,
                    max_attempts=self._settings.db_max_attempts,
                    retry_in_seconds=round(delay, 2),
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                self._shutdown.sleep(delay)
