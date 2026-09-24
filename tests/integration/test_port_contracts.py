"""The adapter's ports, each scenario run against the in-memory fake the unit
tests use and against the real implementation (Snowflake through the emulator,
PostgreSQL), so the fakes cannot drift from the behaviour they stand in for."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4

import pytest

from switch_pipeline.adapter.cursor import SyncCursor
from switch_pipeline.adapter.ports import ChangeSource, SourceRow, SyncMode, SyncStateStore
from switch_pipeline.adapter.snowflake import (
    SnowflakeChangeSource,
    SnowflakeConnectionFactory,
    SourceTable,
    to_source_timestamp,
)
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.settings import PostgresSettings, SnowflakeSettings
from switch_pipeline.sink.sync_state import PostgresSyncStateStore
from tests.fakes import FakeSource, MemoryStateStore
from tests.helpers import T0

SOURCE_ID = "snowflake:DB.S.T"


class SourceHarness(Protocol):
    source: ChangeSource

    def now(self) -> datetime: ...

    def write(self, rows: list[tuple[int, datetime]]) -> None: ...


class FakeSourceHarness:
    def __init__(self) -> None:
        self.fake = FakeSource()
        self.source: ChangeSource = self.fake

    def now(self) -> datetime:
        return self.fake.now

    def write(self, rows: list[tuple[int, datetime]]) -> None:
        for key, at in rows:
            self.fake.write(key, at=at)


class SnowflakeSourceHarness:
    """A table holding only the contract columns, in a fresh emulator database."""

    def __init__(self, settings: SnowflakeSettings) -> None:
        self._factory = SnowflakeConnectionFactory(settings, query_tag="tests")
        self._table = SourceTable(
            database=settings.database,
            schema="RAW",
            table="CONTRACT",
            key_column="O_ORDERKEY",
            version_column="ROW_VERSION",
            updated_at_column="UPDATED_AT",
        )
        with self._factory.session() as conn:
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {settings.database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.database}.RAW")
            cursor.execute(
                f"CREATE TABLE {self._table.qualified_name} "
                "(O_ORDERKEY NUMBER(38, 0), ROW_VERSION NUMBER(38, 0), UPDATED_AT TIMESTAMP_NTZ(6))"
            )
        self.snowflake = SnowflakeChangeSource(self._factory, self._table)
        self.source: ChangeSource = self.snowflake

    def now(self) -> datetime:
        return datetime.now(UTC)

    def write(self, rows: list[tuple[int, datetime]]) -> None:
        with self._factory.session() as conn:
            cursor = conn.cursor()
            for key, at in rows:
                cursor.execute(
                    f"INSERT INTO {self._table.qualified_name} VALUES (%s, 1, %s)",
                    (key, to_source_timestamp(at)),
                )


@pytest.fixture(params=["fake", "snowflake"])
def harness(request: pytest.FixtureRequest) -> Iterator[SourceHarness]:
    if request.param == "fake":
        yield FakeSourceHarness()
        return
    real = SnowflakeSourceHarness(request.getfixturevalue("snowflake_settings"))
    yield real
    real.snowflake.close()


@pytest.fixture(params=["memory", "postgres"])
def store(request: pytest.FixtureRequest) -> Iterator[SyncStateStore]:
    if request.param == "memory":
        yield MemoryStateStore()
        return
    request.getfixturevalue("db")  # empty tables
    postgres: PostgresSettings = request.getfixturevalue("postgres_settings")
    real = PostgresSyncStateStore.open(postgres, application_name="tests")
    yield real
    real.close()


def drain(source: ChangeSource, upper_bound: datetime, *, limit: int) -> list[SourceRow]:
    rows: list[SourceRow] = []
    cursor: SyncCursor | None = None
    while page := source.fetch_changes(cursor, upper_bound, limit):
        rows += page
        cursor = page[-1].position
    return rows


def test_rows_sharing_a_timestamp_are_each_read_once_across_pages(harness: SourceHarness) -> None:
    at = harness.now() - timedelta(hours=1)
    harness.write([(key, at) for key in range(1, 8)])
    rows = drain(harness.source, harness.source.upper_bound(0), limit=3)
    assert [row.key for row in rows] == list(range(1, 8))


def test_rows_come_in_timestamp_then_key_order(harness: SourceHarness) -> None:
    base = harness.now() - timedelta(hours=1)
    harness.write([(3, base), (1, base + timedelta(seconds=1)), (2, base)])
    rows = harness.source.fetch_changes(None, harness.source.upper_bound(0), 10)
    assert [row.key for row in rows] == [2, 3, 1]


def test_reading_resumes_strictly_after_the_cursor(harness: SourceHarness) -> None:
    base = harness.now() - timedelta(hours=1)
    harness.write([(1, base), (2, base), (3, base + timedelta(seconds=1))])
    cursor = SyncCursor(updated_at=base, key=1)
    rows = harness.source.fetch_changes(cursor, harness.source.upper_bound(0), 10)
    assert [row.key for row in rows] == [2, 3]


def test_rows_newer_than_the_upper_bound_wait_for_a_later_cycle(harness: SourceHarness) -> None:
    harness.write([(1, harness.now() - timedelta(minutes=30))])
    assert harness.source.fetch_changes(None, harness.source.upper_bound(3600), 10) == []
    rows = harness.source.fetch_changes(None, harness.source.upper_bound(0), 10)
    assert [row.key for row in rows] == [1]


def test_timestamps_come_back_exact_and_in_utc(harness: SourceHarness) -> None:
    at = (harness.now() - timedelta(hours=1)).replace(microsecond=123456)
    harness.write([(1, at)])
    [row] = harness.source.fetch_changes(None, harness.source.upper_bound(0), 10)
    assert (row.updated_at, row.updated_at.tzinfo) == (at, UTC)


def begin(store: SyncStateStore, batch_id: UUID) -> None:
    store.begin_batch(
        batch_id=batch_id,
        source_id=SOURCE_ID,
        mode=SyncMode.FULL,
        cursor_start=None,
        upper_bound=T0,
    )


def commit(store: SyncStateStore, batch_id: UUID, cursor: SyncCursor) -> None:
    store.commit_batch(batch_id=batch_id, source_id=SOURCE_ID, cursor_end=cursor, row_count=1)


def test_a_new_source_has_no_watermark_and_needs_a_full_sync(store: SyncStateStore) -> None:
    state = store.load(SOURCE_ID)
    assert (state.cursor, state.last_batch_id, state.mode) == (None, None, SyncMode.FULL)


def test_committing_a_batch_moves_the_watermark(store: SyncStateStore) -> None:
    batch_id, cursor = uuid4(), SyncCursor(updated_at=T0, key=7)
    begin(store, batch_id)
    commit(store, batch_id, cursor)
    state = store.load(SOURCE_ID)
    assert (state.cursor, state.last_batch_id) == (cursor, batch_id)


def test_a_failed_batch_leaves_the_watermark_where_it_was(store: SyncStateStore) -> None:
    first, cursor = uuid4(), SyncCursor(updated_at=T0, key=1)
    begin(store, first)
    commit(store, first, cursor)
    failed = uuid4()
    begin(store, failed)
    store.fail_batch(batch_id=failed, error="BrokerUnavailableError: broker unavailable")
    state = store.load(SOURCE_ID)
    assert (state.cursor, state.last_batch_id) == (cursor, first)


def test_only_a_running_batch_can_be_committed(store: SyncStateStore) -> None:
    cursor = SyncCursor(updated_at=T0, key=1)
    committed, failed = uuid4(), uuid4()
    begin(store, committed)
    commit(store, committed, cursor)
    begin(store, failed)
    store.fail_batch(batch_id=failed, error="lost")
    for batch_id in (committed, failed, uuid4()):
        with pytest.raises(FatalPipelineError, match="not running"):
            commit(store, batch_id, cursor)


def test_completing_the_initial_sync_is_permanent(store: SyncStateStore) -> None:
    store.mark_initial_sync_completed(SOURCE_ID)
    completed_at = store.load(SOURCE_ID).initial_sync_completed_at
    store.mark_initial_sync_completed(SOURCE_ID)
    state = store.load(SOURCE_ID)
    assert (state.mode, state.initial_sync_completed_at) == (SyncMode.INCREMENTAL, completed_at)


def test_sources_keep_separate_state(store: SyncStateStore) -> None:
    batch_id = uuid4()
    begin(store, batch_id)
    commit(store, batch_id, SyncCursor(updated_at=T0, key=1))
    store.mark_initial_sync_completed(SOURCE_ID)
    other = store.load("snowflake:OTHER.S.T")
    assert (other.cursor, other.mode) == (None, SyncMode.FULL)
