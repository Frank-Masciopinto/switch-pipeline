"""Incremental capture loop: fetch -> map -> publish (confirmed) -> commit watermark.

Delivery guarantee: at-least-once. The watermark advances only after the broker
confirmed every event of a batch, so no change is lost; a crash between the
confirmation and the watermark commit re-emits that batch on restart with the
same deterministic event ids, which consumers deduplicate.
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from switch_pipeline.adapter.cursor import SyncCursor, advance_cursor
from switch_pipeline.adapter.mapper import EventMapper
from switch_pipeline.adapter.source import ChangeSource, SourceRow, SourceUnavailableError
from switch_pipeline.adapter.state import SyncMode, SyncStateStore
from switch_pipeline.domain.envelope import ChangeEvent, EventType
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.lifecycle import Heartbeat, Shutdown, idle
from switch_pipeline.observability import bound_contextvars, get_logger
from switch_pipeline.retry import Backoff

log = get_logger(__name__)


class EventPublisher(Protocol):
    def publish(self, events: Sequence[ChangeEvent]) -> None:
        """Return once every event is durably accepted by the transport, or raise."""
        ...


class OwnershipGuard(Protocol):
    def check(self) -> None: ...


@dataclass(frozen=True, slots=True)
class CycleResult:
    mode: SyncMode
    batches: int
    rows: int
    caught_up: bool


def utc_now() -> datetime:
    return datetime.now(UTC)


class SyncService:
    def __init__(
        self,
        *,
        source_id: str,
        source: ChangeSource,
        state_store: SyncStateStore,
        publisher: EventPublisher,
        mapper: EventMapper,
        batch_size: int,
        settle_seconds: int,
        guard: OwnershipGuard | None = None,
        clock: Callable[[], datetime] = utc_now,
        stop_requested: Callable[[], bool] = lambda: False,
        on_progress: Callable[[], None] = lambda: None,
    ) -> None:
        self._source_id = source_id
        self._source = source
        self._store = state_store
        self._publisher = publisher
        self._mapper = mapper
        self._batch_size = batch_size
        self._settle_seconds = settle_seconds
        self._guard = guard
        self._clock = clock
        self._stop_requested = stop_requested
        self._on_progress = on_progress

    def run_cycle(self) -> CycleResult:
        """Publish everything changed up to the settle boundary, batch by batch."""
        if self._guard is not None:
            self._guard.check()
        state = self._store.load(self._source_id)
        mode, cursor = state.mode, state.cursor
        # Fixed for the whole cycle so pagination runs over a stable window.
        upper_bound = self._source.upper_bound(self._settle_seconds)
        batches = rows = 0
        caught_up = False
        while not self._stop_requested():
            changes = self._source.fetch_changes(cursor, upper_bound, self._batch_size)
            if not changes:
                caught_up = True
                break
            cursor = self._publish_batch(changes, mode=mode, cursor=cursor, upper_bound=upper_bound)
            batches += 1
            rows += len(changes)
            self._on_progress()
            if len(changes) < self._batch_size:
                caught_up = True
                break
        if caught_up and mode is SyncMode.FULL:
            self._store.mark_initial_sync_completed(self._source_id)
            log.info("initial_sync_completed", source_id=self._source_id)
        return CycleResult(mode=mode, batches=batches, rows=rows, caught_up=caught_up)

    def run_forever(
        self,
        *,
        poll_interval_seconds: float,
        backoff: Backoff,
        shutdown: Shutdown,
        heartbeat: Heartbeat,
    ) -> None:
        consecutive_failures = 0
        while not shutdown.requested():
            heartbeat.beat()
            try:
                result = self.run_cycle()
            except FatalPipelineError:
                raise
            except SourceUnavailableError as exc:
                consecutive_failures += 1
                delay = backoff.delay(consecutive_failures)
                log.warning("source_unavailable", error=str(exc), retry_in_seconds=round(delay, 2))
            except Exception:
                if shutdown.requested():  # e.g. a retry wait cut short by SIGTERM
                    log.info("sync_cycle_interrupted_by_shutdown")
                    return
                consecutive_failures += 1
                delay = backoff.delay(consecutive_failures)
                log.exception(
                    "sync_cycle_failed",
                    consecutive_failures=consecutive_failures,
                    retry_in_seconds=round(delay, 2),
                )
            else:
                consecutive_failures = 0
                delay = poll_interval_seconds if result.caught_up else 0.0
                log.log(
                    logging.INFO if result.rows else logging.DEBUG,
                    "sync_cycle_completed",
                    mode=result.mode.value,
                    batches=result.batches,
                    rows=result.rows,
                )
            idle(delay, shutdown=shutdown, heartbeat=heartbeat)

    def _publish_batch(
        self,
        changes: Sequence[SourceRow],
        *,
        mode: SyncMode,
        cursor: SyncCursor | None,
        upper_bound: datetime,
    ) -> SyncCursor:
        cursor_end = advance_cursor(cursor, [row.position for row in changes])
        batch_id = uuid4()
        with bound_contextvars(batch_id=str(batch_id)):
            self._store.begin_batch(
                batch_id=batch_id,
                source_id=self._source_id,
                mode=mode,
                cursor_start=cursor,
                upper_bound=upper_bound,
            )
            try:
                captured_at = self._clock()
                events = [
                    self._mapper.to_event(row, batch_id=batch_id, captured_at=captured_at)
                    for row in changes
                ]
                self._publisher.publish(events)
            except BaseException as exc:
                self._record_failure(batch_id, exc)
                raise
            self._store.commit_batch(
                batch_id=batch_id,
                source_id=self._source_id,
                cursor_end=cursor_end,
                row_count=len(events),
            )
            inserts = sum(1 for event in events if event.event_type is EventType.INSERT)
            log.info(
                "batch_committed",
                mode=mode.value,
                rows=len(events),
                inserts=inserts,
                updates=len(events) - inserts,
                cursor=cursor_end.to_json(),
                first_event_id=str(events[0].event_id),
                last_event_id=str(events[-1].event_id),
            )
        return cursor_end

    def _record_failure(self, batch_id: UUID, exc: BaseException) -> None:
        try:
            self._store.fail_batch(batch_id=batch_id, error=f"{type(exc).__name__}: {exc}")
        except Exception:
            # Batches left 'running' are closed out on the next start, so this is
            # bookkeeping only; the watermark was never advanced.
            log.exception("batch_failure_not_recorded")
