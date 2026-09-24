"""SyncService behaviour against in-memory implementations of its ports.

The fakes (tests/fakes.py) pass the same contract tests as the real Snowflake
source and PostgreSQL store; the real adapters also run end to end against
emulated Snowflake, PostgreSQL and Redpanda in tests/integration.
"""

from collections.abc import MutableMapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from structlog.testing import capture_logs

from switch_pipeline.adapter.cursor import CursorRegressionError, SyncCursor
from switch_pipeline.adapter.mapper import EventMapper
from switch_pipeline.adapter.ports import SourceRow, SyncMode
from switch_pipeline.adapter.service import SyncService
from switch_pipeline.domain.envelope import ChangeEvent, EventType
from switch_pipeline.errors import BrokerUnavailableError, SourceUnavailableError
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.retry import Backoff
from tests.fakes import BrokerLog, FakeSource, MemoryStateStore
from tests.helpers import SOURCE, T0

SOURCE_ID = "snowflake:TEST"
BULK_LOADED_AT = T0 - timedelta(minutes=1)
FAST_RETRIES = Backoff(initial_seconds=0.001, max_seconds=0.002)


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
    assert store.load(SOURCE_ID).cursor == SyncCursor(updated_at=BULK_LOADED_AT, key=7)
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
    with pytest.raises(BrokerUnavailableError):
        service.run_cycle()
    assert store.load(SOURCE_ID).cursor is None
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
    assert store.load(SOURCE_ID).cursor is None


def test_shutdown_between_batches_leaves_a_resumable_state(
    source: FakeSource, store: MemoryStateStore, broker: BrokerLog
) -> None:
    result = make_service(source, store, broker, stop_after_batches=1).run_cycle()
    assert (result.batches, result.caught_up) == (1, False)
    assert store.load(SOURCE_ID).cursor == SyncCursor(updated_at=BULK_LOADED_AT, key=3)
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


def retried(logs: list[MutableMapping[str, Any]]) -> list[str]:
    """The error type of every retried cycle, in order."""
    return [entry["error_type"] for entry in logs if entry["event"] == "sync_cycle_retrying"]


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
    assert retried(logs) == ["SourceUnavailableError", "SourceUnavailableError"]
    assert "error" not in {entry["log_level"] for entry in logs}, "expected, not an error"
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
    assert retried(logs) == ["BrokerUnavailableError", "BrokerUnavailableError"]
    assert store.load(SOURCE_ID).cursor == SyncCursor(updated_at=BULK_LOADED_AT, key=7)


def test_run_forever_does_not_retry_a_bug(store: MemoryStateStore, broker: BrokerLog) -> None:
    class BrokenSource(FakeSource):
        def fetch_changes(
            self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
        ) -> list[SourceRow]:
            raise KeyError("O_ORDERKEY")

    with capture_logs() as logs, pytest.raises(KeyError):
        run_forever(make_service(BrokenSource(), store, broker), Shutdown(), None)
    assert retried(logs) == [], "retrying would hide the bug behind a warning loop"
    assert broker.events == []


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
            raise BrokerUnavailableError("shutdown requested while retrying delivery")

    with capture_logs() as logs:
        run_forever(make_service(source, store, BrokerDownDuringShutdown()), shutdown, None)
    assert "sync_cycle_interrupted_by_shutdown" in [entry["event"] for entry in logs]
    assert retried(logs) == []
    assert store.load(SOURCE_ID).cursor is None
