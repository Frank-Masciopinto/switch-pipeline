"""The adapter's Snowflake SQL, run through the real connector against fakesnow."""

from datetime import UTC

import pytest

from switch_pipeline.adapter.cursor import SyncCursor, advance_cursor
from switch_pipeline.adapter.source import (
    SnowflakeChangeSource,
    SnowflakeConnectionFactory,
    SourceRow,
    SourceTable,
    SourceUnavailableError,
)
from switch_pipeline.settings import SnowflakeSettings
from switch_pipeline.tools.seed import seed_source
from switch_pipeline.tools.simulate import simulate_changes
from tests.integration.conftest import SOURCE_SETTINGS, synthetic_seed


def change_source(settings: SnowflakeSettings) -> SnowflakeChangeSource:
    return SnowflakeChangeSource(
        SnowflakeConnectionFactory(settings, query_tag="tests"),
        SourceTable.from_settings(settings, SOURCE_SETTINGS),
    )


def drain(
    source: SnowflakeChangeSource, cursor: SyncCursor | None, *, batch_size: int
) -> tuple[list[SourceRow], SyncCursor | None]:
    upper_bound = source.upper_bound(0)
    rows: list[SourceRow] = []
    while batch := source.fetch_changes(cursor, upper_bound, batch_size):
        cursor = advance_cursor(cursor, [row.position for row in batch])
        rows += batch
    return rows, cursor


def test_seeding_is_idempotent(snowflake_settings: SnowflakeSettings) -> None:
    first = seed_source(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(120), force=False)
    again = seed_source(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(999), force=False)
    assert (first.created, first.rows) == (True, 120)
    assert (again.created, again.rows) == (False, 120)


def test_keyset_pagination_reads_each_row_once_although_all_share_a_timestamp(
    snowflake_settings: SnowflakeSettings,
) -> None:
    seed_source(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(250), force=False)
    source = change_source(snowflake_settings)
    try:
        rows, cursor = drain(source, None, batch_size=60)
        assert [row.key for row in rows] == list(range(1, 251))
        assert len({row.updated_at for row in rows}) == 1
        assert all(row.updated_at.tzinfo is UTC for row in rows)
        assert cursor == rows[-1].position
        assert source.fetch_changes(cursor, source.upper_bound(0), 60) == []
    finally:
        source.close()


def test_changes_after_the_watermark_are_captured_with_their_new_versions(
    snowflake_settings: SnowflakeSettings,
) -> None:
    seed_source(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(50), force=False)
    source = change_source(snowflake_settings)
    try:
        _, cursor = drain(source, None, batch_size=100)
        report = simulate_changes(
            snowflake_settings, SOURCE_SETTINGS, inserts=3, updates=4, invalid_rows=3
        )
        changed, _ = drain(source, cursor, batch_size=100)
    finally:
        source.close()
    versions = {row.key: row.version for row in changed}
    invalid = {item["key"]: item["kind"] for item in report.invalid}
    assert set(versions) == set(report.inserted) | set(report.updated) | set(invalid)
    assert all(versions[key] == 1 for key in report.inserted)
    assert all(versions[key] == 2 for key in report.updated)
    status_update = next(key for key, kind in invalid.items() if kind == "unknown_order_status")
    assert versions[status_update] == 2


def test_a_missing_table_is_reported_as_unavailable_with_a_hint(
    snowflake_settings: SnowflakeSettings,
) -> None:
    source = change_source(snowflake_settings)
    try:
        with pytest.raises(SourceUnavailableError, match="make seed"):
            source.fetch_changes(None, source.upper_bound(0), 10)
    finally:
        source.close()


def test_rows_younger_than_the_settle_window_are_left_for_later(
    snowflake_settings: SnowflakeSettings,
) -> None:
    seed_source(snowflake_settings, SOURCE_SETTINGS, synthetic_seed(10), force=False)
    source = change_source(snowflake_settings)
    try:
        assert source.fetch_changes(None, source.upper_bound(3600), 100) == []
        assert len(source.fetch_changes(None, source.upper_bound(0), 100)) == 10
    finally:
        source.close()
