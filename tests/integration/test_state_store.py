"""PostgreSQL-specific behaviour of the sync state and schema. What every
SyncStateStore must do is in test_port_contracts.py."""

from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest

from switch_pipeline.adapter.cursor import SyncCursor
from switch_pipeline.adapter.ports import SyncMode
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.settings import PostgresSettings
from switch_pipeline.sink.migrate import apply_migrations
from switch_pipeline.sink.sync_state import PostgresSyncStateStore, SourceLock, SourceLockedError
from tests.helpers import T0

SOURCE_ID = "snowflake:DB.S.T"


def open_store(postgres: PostgresSettings) -> PostgresSyncStateStore:
    return PostgresSyncStateStore.open(postgres, application_name="tests")


def lock(postgres: PostgresSettings, source_id: str = SOURCE_ID) -> SourceLock:
    return SourceLock(postgres, source_id=source_id, application_name="tests")


@pytest.fixture
def store(db: str, postgres_settings: PostgresSettings) -> Iterator[PostgresSyncStateStore]:
    state_store = open_store(postgres_settings)
    yield state_store
    state_store.close()


def committed_batch(store: PostgresSyncStateStore, cursor: SyncCursor) -> None:
    batch_id = uuid4()
    store.begin_batch(
        batch_id=batch_id,
        source_id=SOURCE_ID,
        mode=SyncMode.FULL,
        cursor_start=None,
        upper_bound=T0,
    )
    store.commit_batch(batch_id=batch_id, source_id=SOURCE_ID, cursor_end=cursor, row_count=1)


@pytest.mark.parametrize("key", [42, "ORD-42"])
def test_the_watermark_survives_a_restart(
    postgres_settings: PostgresSettings, store: PostgresSyncStateStore, key: int | str
) -> None:
    cursor = SyncCursor(updated_at=T0.replace(microsecond=654321), key=key)
    committed_batch(store, cursor)
    store.close()

    restarted = open_store(postgres_settings)
    try:
        assert restarted.load(SOURCE_ID).cursor == cursor
    finally:
        restarted.close()


def test_a_failed_batch_is_kept_in_the_ledger_with_its_error(
    db: str, store: PostgresSyncStateStore
) -> None:
    batch_id = uuid4()
    store.begin_batch(
        batch_id=batch_id,
        source_id=SOURCE_ID,
        mode=SyncMode.INCREMENTAL,
        cursor_start=SyncCursor(updated_at=T0, key=1),
        upper_bound=T0,
    )
    store.fail_batch(batch_id=batch_id, error="BrokerUnavailableError: broker unavailable")
    with psycopg.connect(db) as conn:
        status, error = conn.execute(
            "SELECT status, error FROM sync_batch WHERE batch_id = %s", (batch_id,)
        ).fetchone()  # type: ignore[misc]
    assert (status, error) == ("failed", "BrokerUnavailableError: broker unavailable")


def test_batches_interrupted_by_a_crash_are_closed_on_restart(
    store: PostgresSyncStateStore,
) -> None:
    store.begin_batch(
        batch_id=uuid4(), source_id=SOURCE_ID, mode=SyncMode.FULL, cursor_start=None, upper_bound=T0
    )
    assert store.recover_interrupted_batches(SOURCE_ID) == 1
    assert store.recover_interrupted_batches(SOURCE_ID) == 0


def test_only_one_adapter_can_sync_a_source_at_a_time(
    db: str, postgres_settings: PostgresSettings
) -> None:
    with lock(postgres_settings) as held:
        held.check()
        with pytest.raises(SourceLockedError), lock(postgres_settings):
            pass
        with lock(postgres_settings, "snowflake:OTHER.S.T"):  # other sources are independent
            pass
    with lock(postgres_settings):  # released when its holder exits
        pass


def test_migrations_are_idempotent_and_edits_are_refused(
    migrated: str, postgres_settings: PostgresSettings
) -> None:
    assert apply_migrations(postgres_settings, application_name="tests") == []
    with psycopg.connect(migrated, autocommit=True) as conn:
        original = conn.execute(
            "SELECT checksum FROM schema_migrations WHERE version = '0001_initial'"
        ).fetchone()
        conn.execute(
            "UPDATE schema_migrations SET checksum = 'edited' WHERE version = '0001_initial'"
        )
        try:
            with pytest.raises(FatalPipelineError, match="changed after it was applied"):
                apply_migrations(postgres_settings, application_name="tests")
        finally:
            conn.execute(
                "UPDATE schema_migrations SET checksum = %s WHERE version = '0001_initial'",
                (original[0],),  # type: ignore[index]
            )


def test_the_event_log_is_append_only(db: str) -> None:
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO event_log (event_id, event_type, schema_version, source, entity_type,
                entity_key, entity_version, payload, occurred_at, captured_at, batch_id,
                fingerprint, kafka_topic, kafka_partition, kafka_offset)
            VALUES (gen_random_uuid(), 'insert', 1, '{}', 'order', '1', 1, '{}', now(), now(),
                gen_random_uuid(), 'x', 't', 0, 0)
            """
        )
        for statement in ("UPDATE event_log SET entity_key = '2'", "DELETE FROM event_log"):
            with pytest.raises(psycopg.errors.RestrictViolation, match="append-only"):
                conn.execute(statement)
