from datetime import UTC, datetime, timedelta, timezone

import pytest

from switch_pipeline.adapter.cursor import CursorRegressionError, SyncCursor, advance_cursor
from switch_pipeline.adapter.source import SourceTable, build_changes_query
from tests.helpers import T0

TABLE = SourceTable(
    database="SWITCH_DEMO",
    schema="RAW",
    table="CUSTOMER_ORDERS",
    key_column="O_ORDERKEY",
    version_column="ROW_VERSION",
    updated_at_column="UPDATED_AT",
)


def at(seconds: float, key: int | str) -> SyncCursor:
    return SyncCursor(updated_at=T0 + timedelta(seconds=seconds), key=key)


class TestSyncCursor:
    def test_orders_by_timestamp_then_key(self) -> None:
        assert at(1, 1).is_after(at(0, 99))
        assert at(0, 2).is_after(at(0, 1))
        assert not at(0, 1).is_after(at(0, 1))
        assert not at(0, 1).is_after(at(0, 2))
        assert not at(0, 99).is_after(at(1, 1))

    def test_normalizes_timestamps_to_utc(self) -> None:
        rome = timezone(timedelta(hours=2))
        cursor = SyncCursor(updated_at=datetime(2026, 1, 1, 14, tzinfo=rome), key=1)
        assert cursor.updated_at == datetime(2026, 1, 1, 12, tzinfo=UTC)
        assert cursor.updated_at.utcoffset() == timedelta(0)

    def test_rejects_naive_timestamps(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            SyncCursor(updated_at=datetime(2026, 1, 1), key=1)

    def test_rejects_non_scalar_keys(self) -> None:
        with pytest.raises(TypeError):
            SyncCursor(updated_at=T0, key=True)

    @pytest.mark.parametrize("key", [42, "ORD-42"])
    def test_survives_a_json_round_trip_with_microseconds(self, key: int | str) -> None:
        cursor = SyncCursor(updated_at=T0.replace(microsecond=123456), key=key)
        assert SyncCursor.from_json(cursor.to_json()) == cursor

    def test_a_change_of_key_type_is_a_regression(self) -> None:
        with pytest.raises(CursorRegressionError):
            at(0, "1").is_after(at(0, 1))


class TestAdvanceCursor:
    def test_first_batch_moves_the_watermark_to_its_last_row(self) -> None:
        assert advance_cursor(None, [at(0, 1), at(0, 2), at(5, 1)]) == at(5, 1)

    def test_rows_sharing_the_watermark_timestamp_continue_by_key(self) -> None:
        assert advance_cursor(at(0, 3), [at(0, 4), at(0, 5), at(1, 1)]) == at(1, 1)

    def test_a_row_at_the_watermark_would_be_a_re_emission(self) -> None:
        with pytest.raises(CursorRegressionError):
            advance_cursor(at(0, 5), [at(0, 5)])

    def test_a_row_before_the_watermark_is_rejected(self) -> None:
        with pytest.raises(CursorRegressionError):
            advance_cursor(at(10, 1), [at(5, 9)])

    def test_an_unsorted_batch_is_rejected(self) -> None:
        with pytest.raises(CursorRegressionError):
            advance_cursor(None, [at(0, 2), at(0, 1)])

    def test_an_empty_batch_cannot_advance(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            advance_cursor(at(0, 1), [])


class TestChangesQuery:
    def test_initial_sync_reads_everything_up_to_the_settle_bound(self) -> None:
        query, params = build_changes_query(TABLE, None, T0, 500)
        assert query == (
            "SELECT * FROM SWITCH_DEMO.RAW.CUSTOMER_ORDERS WHERE UPDATED_AT <= %(upper_bound)s "
            "ORDER BY UPDATED_AT, O_ORDERKEY LIMIT %(limit)s"
        )
        # TIMESTAMP_NTZ holds UTC wall time, so bound values are naive UTC.
        assert params == {"upper_bound": datetime(2026, 1, 1, 12), "limit": 500}

    def test_resume_reads_strictly_after_the_composite_watermark(self) -> None:
        query, params = build_changes_query(TABLE, at(0, 42), T0 + timedelta(minutes=1), 10)
        assert (
            "UPDATED_AT >= %(cursor_ts)s AND "
            "(UPDATED_AT > %(cursor_ts)s OR O_ORDERKEY > %(cursor_key)s)"
        ) in query
        assert params["cursor_key"] == 42
        assert params["cursor_ts"] == datetime(2026, 1, 1, 12)

    @pytest.mark.parametrize(
        "field", ["database", "schema", "table", "key_column", "version_column"]
    )
    def test_identifiers_that_could_inject_sql_are_refused(self, field: str) -> None:
        values = {
            "database": "D",
            "schema": "S",
            "table": "T",
            "key_column": "K",
            "version_column": "V",
            "updated_at_column": "U",
        }
        values[field] = "X; DROP TABLE Y"
        with pytest.raises(ValueError, match="invalid Snowflake identifier"):
            SourceTable(**values)
