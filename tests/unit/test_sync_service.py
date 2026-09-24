"""SyncService behaviour against in-memory implementations of its ports.

The fakes honour the same contracts as the real adapters (keyset semantics of
the Snowflake query, a transactional state store); the real adapters are
exercised against emulated Snowflake, PostgreSQL and Redpanda in tests/integration.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from structlog.testing import capture_logs

from switch_pipeline.adapter.cursor import CursorRegressionError, SyncCursor
from switch_pipeline.adapter.mapper import EventMapper
from switch_pipeline.adapter.service import SyncService
from switch_pipeline.adapter.source import SourceRow, SourceUnavailableError
from switch_pipeline.adapter.state import SyncMode, SyncState
from switch_pipeline.domain.envelope import ChangeEvent, EventType
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.retry import Backoff
from switch_pipeline.transport.producer import PublishError
from tests.helpers import SOURCE, T0

SOURCE_ID = "snowflake:TEST"
BULK_LOADED_AT = T0 - timedelta(minutes=1)
FAST_RETRIES = Backoff(initial_seconds=0.001, max_seconds=0.002)


class FakeSource:
    def __init__(self) -> None:
        self.now = T0
        self.rows: dict[int, SourceRow] = {}

    def write(self, key: int, *, at: datetime | None = None, **values: Any) -> None:
        previous = self.rows.get(key)
        version = previous.version + 1 if previous else 1
        self.rows[key] = SourceRow(
            key=key,
            version=version,
            updated_at=at or self.now,
            values={"O_ORDERKEY": key, "ROW_VERSION": version, **values},
        )

    def upper_bound(self, settle_seconds: int) -> datetime:
        return self.now - timedelta(seconds=settle_seconds)

    def fetch_changes(
        self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
    ) -> list[SourceRow]:
        ordered = sorted(self.rows.values(), key=lambda row: (row.updated_at, row.key))
        after = [
            row
            for row in ordered
            if row.updated_at <= upper_bound and (cursor is None or row.position.is_after(cursor))
        ]
        return after[:limit]


@dataclass
class _Batch:
    status: str
    cursor_end: SyncCursor | None = None


class MemoryStateStore:
    def __init__(self) -> None:
        self.cursor: SyncCursor | None = None
        self.completed_at: datetime | None = None
        self.last_batch_id: UUID | None = None
        self.batches: dict[UUID, _Batch] = {}

    def load(self, source_id: str) -> SyncState:
        return SyncState(
            source_id=source_id,
            cursor=self.cursor,
            initial_sync_completed_at=self.completed_at,
            last_batch_id=self.last_batch_id,
        )

    def begin_batch(self, *, batch_id: UUID, **_: object) -> None:
        self.batches[batch_id] = _Batch(status="running")

    def commit_batch(self, *, batch_id: UUID, cursor_end: SyncCursor, **_: object) -> None:
        batch = self.batches[batch_id]
        assert batch.status == "running"
        batch.status, batch.cursor_end = "committed", cursor_end
        self.cursor, self.last_batch_id = cursor_end, batch_id

    def fail_batch(self, *, batch_id: UUID, error: str) -> None:
        self.batches[batch_id].status = "failed"

    def mark_initial_sync_completed(self, source_id: str) -> None:
        self.completed_at = self.completed_at or T0

    def statuses(self) -> list[str]:
        return [batch.status for batch in self.batches.values()]


class BrokerLog:
    """What the topic holds. Can fail after part of a batch was already written."""

    def __init__(self) -> None:
        self.events: list[ChangeEvent] = []
        self.failures_to_inject = 0

    def publish(self, events: Sequence[ChangeEvent]) -> None:
        if self.failures_to_inject:
            self.failures_to_inject -= 1
            self.events.extend(events[: len(events) // 2])
            raise PublishError("broker unavailable")
        self.events.extend(events)

    def keys(self) -> list[str]:
        return [event.entity_key for event in self.events]


def make_service(
    source: FakeSource,
    store: MemoryStateStore,
    broker: BrokerLog,
    *,
    batch_size: int = 3,
    settle_seconds: int = 0,
    stop_after_batches: int | None = None,
) -> SyncService:
    batches_done = 0

    def on_progress() -> None:
        nonlocal batches_done
        batches_done += 1

    return SyncService(
        source_id=SOURCE_ID,
        source=source,
        state_store=store,
        publisher=broker,
        mapper=EventMapper(source=SOURCE, entity_type="order"),
        batch_size=batch_size,
        settle_seconds=settle_seconds,
        clock=lambda: source.now,
        stop_requested=lambda: (
            stop_after_batches is not None and batches_done >= stop_after_batches
        ),
        on_progress=on_progress,
    )


@pytest.fixture
def source() -> FakeSource:
    bulk_loaded = FakeSource()
    for key in range(1, 8):  # one bulk load: seven rows share a timestamp
        bulk_loaded.write(key, at=BULK_LOADED_AT)
    return bulk_loaded


@pytest.fixture
def store() -> MemoryStateStore:
    return MemoryStateStore()


@pytest.fixture
def broker() -> BrokerLog:
    return BrokerLog()


def test_initial_sync_publishes_each_row_once_across_batches_of_tied_timestamps(
    source: FakeSource, store: MemoryStateStore, broker: BrokerLog
) -> None:
    result = make_service(source, store, broker).run_cycle()
    assert (result.mode, result.batches, result.rows, result.caught_up) == (
        SyncMode.FULL,
        3,
        7,
        True,
    )
    assert broker.keys() == [str(key) for key in range(1, 8)]
    assert store.cursor == SyncCursor(updated_at=BULK_LOADED_AT, key=7)
    assert store.load(SOURCE_ID).mode is SyncMode.INCREMENTAL


def test_rerunning_without_source_changes_emits_nothing(
    source: FakeSource, store: MemoryStateStore, broker: BrokerLog
) -> None:
    service = make_service(source, store, broker)
    service.run_cycle()
    result = service.run_cycle()
    assert (result.mode, result.rows) == (SyncMode.INCREMENTAL, 0)
    assert len(broker.events) == 7


def test_a_restarted_adapter_resumes_from_the_persisted_watermark(
    source: FakeSource, store: MemoryStateStore, broker: BrokerLog
) -> None:
    make_service(source, store, broker).run_cycle()
    source.now = T0 + timedelta(minutes=5)
    source.write(3, O_ORDERSTATUS="F")
    source.write(99)
    broker.events.clear()

    restarted = make_service(source, store, broker)  # fresh process, same durable state
    assert restarted.run_cycle().rows == 2
    assert [(e.entity_key, e.event_type, e.entity_version) for e in broker.events] == [
        ("3", EventType.UPDATE, 2),
        ("99", EventType.INSERT, 1),
    ]


def test_rows_inside_the_settle_window_wait_for_the_next_cycle(
    source: FakeSource, store: MemoryStateStore, broker: BrokerLog
) -> None:
    service = make_service(source, store, broker, settle_seconds=10)
    service.run_cycle()
    broker.events.clear()
    source.write(50, at=source.now - timedelta(seconds=5))  # its transaction may still be open
    assert service.run_cycle().rows == 0
    source.now += timedelta(seconds=10)
    assert service.run_cycle().rows == 1
    assert broker.keys() == ["50"]


def test_a_failed_publish_keeps_the_watermark_and_the_retry_re_emits_identical_ids(
    source: FakeSource, store: MemoryStateStore, broker: BrokerLog
) -> None:
    broker.failures_to_inject = 1
    service = make_service(source, store, broker)
    with pytest.raises(PublishError):
        service.run_cycle()
    assert store.cursor is None
    assert store.statuses() == ["failed"]

    service.run_cycle()
    event_ids = [event.event_id for event in broker.events]
    assert len(set(event_ids)) == 7, "no change lost"
    assert len(event_ids) == 8, "the event delivered before the failure came twice"
    assert store.statuses() == ["failed", "committed", "committed", "committed"]


def test_a_source_returning_unordered_rows_is_stopped_before_publishing(
    store: MemoryStateStore, broker: BrokerLog
) -> None:
    class UnorderedSource(FakeSource):
        def fetch_changes(
            self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
        ) -> list[SourceRow]:
            return list(reversed(super().fetch_changes(cursor, upper_bound, limit)))

    broken = UnorderedSource()
    broken.write(1, at=BULK_LOADED_AT)
    broken.write(2, at=BULK_LOADED_AT)
    with pytest.raises(CursorRegressionError):
        make_service(broken, store, broker).run_cycle()
    assert broker.events == []
    assert store.cursor is None


def test_shutdown_between_batches_leaves_a_resumable_state(
    source: FakeSource, store: MemoryStateStore, broker: BrokerLog
) -> None:
    result = make_service(source, store, broker, stop_after_batches=1).run_cycle()
    assert (result.batches, result.caught_up) == (1, False)
    assert store.cursor == SyncCursor(updated_at=BULK_LOADED_AT, key=3)
    assert store.load(SOURCE_ID).mode is SyncMode.FULL

    make_service(source, store, broker).run_cycle()
    assert broker.keys() == [str(key) for key in range(1, 8)]


def test_an_empty_source_completes_the_initial_sync(
    store: MemoryStateStore, broker: BrokerLog
) -> None:
    result = make_service(FakeSource(), store, broker).run_cycle()
    assert (result.rows, result.caught_up) == (0, True)
    assert store.load(SOURCE_ID).mode is SyncMode.INCREMENTAL


class StopAfter(BrokerLog):
    """Requests shutdown once the broker holds every expected key."""

    def __init__(self, shutdown: Shutdown, expected_keys: int) -> None:
        super().__init__()
        self._shutdown = shutdown
        self._expected = expected_keys

    def publish(self, events: Sequence[ChangeEvent]) -> None:
        super().publish(events)
        if len(set(self.keys())) >= self._expected:
            self._shutdown.request()


def run_forever(service: SyncService, shutdown: Shutdown, heartbeat_path: Path | None) -> None:
    service.run_forever(
        poll_interval_seconds=0.01,
        backoff=FAST_RETRIES,
        shutdown=shutdown,
        heartbeat=Heartbeat(heartbeat_path),
    )


def test_run_forever_waits_for_a_missing_source_table_then_syncs(
    store: MemoryStateStore, tmp_path: Path
) -> None:
    class NotSeededYet(FakeSource):
        missing_fetches = 2

        def fetch_changes(
            self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
        ) -> list[SourceRow]:
            if self.missing_fetches:
                self.missing_fetches -= 1
                raise SourceUnavailableError("CUSTOMER_ORDERS does not exist")
            return super().fetch_changes(cursor, upper_bound, limit)

    source = NotSeededYet()
    for key in (1, 2, 3):
        source.write(key, at=BULK_LOADED_AT)
    shutdown = Shutdown()
    broker = StopAfter(shutdown, expected_keys=3)
    with capture_logs() as logs:
        run_forever(make_service(source, store, broker), shutdown, tmp_path / "heartbeat")
    assert broker.keys() == ["1", "2", "3"]
    events = [entry["event"] for entry in logs]
    assert events.count("source_unavailable") == 2
    assert "sync_cycle_failed" not in events, "a missing table is expected, not an error"
    assert (tmp_path / "heartbeat").exists()


def test_run_forever_retries_transient_failures_until_the_cycle_succeeds(
    source: FakeSource, store: MemoryStateStore
) -> None:
    shutdown = Shutdown()
    broker = StopAfter(shutdown, expected_keys=7)
    broker.failures_to_inject = 2
    with capture_logs() as logs:
        run_forever(make_service(source, store, broker), shutdown, None)
    assert set(broker.keys()) == {str(key) for key in range(1, 8)}
    assert [entry["event"] for entry in logs].count("sync_cycle_failed") == 2
    assert store.cursor == SyncCursor(updated_at=BULK_LOADED_AT, key=7)


def test_run_forever_stops_on_errors_that_retrying_cannot_fix(
    store: MemoryStateStore, broker: BrokerLog
) -> None:
    class UnorderedSource(FakeSource):
        def fetch_changes(
            self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
        ) -> list[SourceRow]:
            return list(reversed(super().fetch_changes(cursor, upper_bound, limit)))

    broken = UnorderedSource()
    broken.write(1, at=BULK_LOADED_AT)
    broken.write(2, at=BULK_LOADED_AT)
    with pytest.raises(CursorRegressionError):
        run_forever(make_service(broken, store, broker), Shutdown(), None)


def test_a_shutdown_during_a_failing_cycle_exits_without_an_error(
    source: FakeSource, store: MemoryStateStore
) -> None:
    shutdown = Shutdown()

    class BrokerDownDuringShutdown(BrokerLog):
        def publish(self, events: Sequence[ChangeEvent]) -> None:
            shutdown.request()
            raise PublishError("shutdown requested while retrying delivery")

    with capture_logs() as logs:
        run_forever(make_service(source, store, BrokerDownDuringShutdown()), shutdown, None)
    events = [entry["event"] for entry in logs]
    assert "sync_cycle_interrupted_by_shutdown" in events
    assert "sync_cycle_failed" not in events
    assert store.cursor is None
