"""Read-only SQL behind the API (connections run with default_transaction_read_only)."""

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, LiteralString
from uuid import UUID

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from switch_pipeline.domain.envelope import EventType
from switch_pipeline.sink.queries import SINK_CHECKSUMS

Row = dict[str, Any]

_EVENT_COLUMNS = sql.SQL(
    "log_seq, event_id, event_type, schema_version, source, entity_type, entity_key, "
    "entity_version, payload, occurred_at, captured_at, processed_at, batch_id, "
    "quality_warnings, kafka_topic, kafka_partition, kafka_offset"
)
_QUARANTINE_COLUMNS = sql.SQL(
    "quarantine_seq, quarantine_id, reason, details, event_id, entity_type, entity_key, "
    "batch_id, ruleset_fingerprint, raw_value, kafka_topic, kafka_partition, kafka_offset, "
    "quarantined_at"
)

# Lag split into its two legs: source write -> adapter read (poll interval +
# settle window) and adapter read -> sink (broker + consumer).
_LAG_SQL: LiteralString = """
WITH recent AS (
    SELECT extract(epoch FROM processed_at - occurred_at)::float8 AS end_to_end,
           extract(epoch FROM captured_at - occurred_at)::float8 AS capture,
           extract(epoch FROM processed_at - captured_at)::float8 AS delivery
    FROM event_log
    ORDER BY log_seq DESC
    LIMIT %(sample)s
)
SELECT count(*) AS sample_size,
       avg(end_to_end) AS end_to_end_avg,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY end_to_end) AS end_to_end_p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY end_to_end) AS end_to_end_p95,
       max(end_to_end) AS end_to_end_max,
       avg(capture) AS capture_avg,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY capture) AS capture_p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY capture) AS capture_p95,
       max(capture) AS capture_max,
       avg(delivery) AS delivery_avg,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY delivery) AS delivery_p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY delivery) AS delivery_p95,
       max(delivery) AS delivery_max
FROM recent
"""


class InvalidCursorError(ValueError):
    pass


def encode_cursor(sequence: int) -> str:
    return base64.urlsafe_b64encode(json.dumps({"before": sequence}).encode()).decode()


def decode_cursor(token: str) -> int:
    try:
        value = json.loads(base64.urlsafe_b64decode(token.encode()))["before"]
    except (binascii.Error, ValueError, KeyError, TypeError) as exc:
        raise InvalidCursorError("malformed pagination cursor") from exc
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidCursorError("malformed pagination cursor")
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class EventFilters:
    entity_key: str | None = None
    entity_type: str | None = None
    event_type: EventType | None = None
    batch_id: UUID | None = None
    occurred_after: datetime | None = None
    occurred_before: datetime | None = None


async def list_events(
    conn: AsyncConnection[Any], filters: EventFilters, *, limit: int, before: int | None
) -> tuple[list[Row], int | None]:
    """Newest first, keyset-paginated on the insertion sequence (stable under inserts)."""
    conditions: list[sql.Composable] = []
    params: dict[str, object] = {"limit": limit + 1}
    candidates: list[tuple[LiteralString, str, object]] = [
        ("entity_key = %(entity_key)s", "entity_key", filters.entity_key),
        ("entity_type = %(entity_type)s", "entity_type", filters.entity_type),
        (
            "event_type = %(event_type)s",
            "event_type",
            filters.event_type.value if filters.event_type else None,
        ),
        ("batch_id = %(batch_id)s", "batch_id", filters.batch_id),
        ("occurred_at >= %(occurred_after)s", "occurred_after", filters.occurred_after),
        ("occurred_at < %(occurred_before)s", "occurred_before", filters.occurred_before),
        ("log_seq < %(before)s", "before", before),
    ]
    for condition, name, value in candidates:
        if value is not None:
            conditions.append(sql.SQL(condition))
            params[name] = value
    query = sql.SQL("SELECT {columns} FROM event_log {where} ORDER BY log_seq DESC LIMIT %(limit)s")
    rows = await _fetch(
        conn, query.format(columns=_EVENT_COLUMNS, where=_where(conditions)), params
    )
    return _page(rows, limit, "log_seq")


async def get_event(conn: AsyncConnection[Any], event_id: UUID) -> Row | None:
    query = sql.SQL("SELECT {columns} FROM event_log WHERE event_id = %(event_id)s")
    rows = await _fetch(conn, query.format(columns=_EVENT_COLUMNS), {"event_id": event_id})
    return rows[0] if rows else None


async def entity_types_for_key(conn: AsyncConnection[Any], key: str) -> list[str]:
    rows = await _fetch(
        conn,
        sql.SQL(
            "SELECT entity_type FROM entity_current_state WHERE entity_key = %(key)s "
            "UNION SELECT entity_type FROM event_log WHERE entity_key = %(key)s "
            "ORDER BY entity_type"
        ),
        {"key": key},
    )
    return [row["entity_type"] for row in rows]


async def get_current_state(conn: AsyncConnection[Any], entity_type: str, key: str) -> Row | None:
    rows = await _fetch(
        conn,
        sql.SQL(
            "SELECT entity_type, entity_key, entity_version, payload, source, last_event_id, "
            "last_event_type, occurred_at, updated_at FROM entity_current_state "
            "WHERE entity_type = %(entity_type)s AND entity_key = %(key)s"
        ),
        {"entity_type": entity_type, "key": key},
    )
    return rows[0] if rows else None


async def entity_history(
    conn: AsyncConnection[Any], entity_type: str, key: str, *, limit: int
) -> tuple[list[Row], bool]:
    """The latest ``limit`` accepted events, returned oldest first."""
    query = sql.SQL(
        "SELECT {columns} FROM event_log WHERE entity_type = %(entity_type)s "
        "AND entity_key = %(key)s ORDER BY log_seq DESC LIMIT %(limit)s"
    )
    rows = await _fetch(
        conn,
        query.format(columns=_EVENT_COLUMNS),
        {"entity_type": entity_type, "key": key, "limit": limit + 1},
    )
    return list(reversed(rows[:limit])), len(rows) > limit


async def list_quarantine(
    conn: AsyncConnection[Any],
    *,
    reason: str | None,
    entity_key: str | None,
    limit: int,
    before: int | None,
) -> tuple[list[Row], int | None]:
    conditions: list[sql.Composable] = []
    params: dict[str, object] = {"limit": limit + 1}
    candidates: list[tuple[LiteralString, str, object]] = [
        ("reason = %(reason)s", "reason", reason),
        ("entity_key = %(entity_key)s", "entity_key", entity_key),
        ("quarantine_seq < %(before)s", "before", before),
    ]
    for condition, name, value in candidates:
        if value is not None:
            conditions.append(sql.SQL(condition))
            params[name] = value
    query = sql.SQL(
        "SELECT {columns} FROM quarantine {where} ORDER BY quarantine_seq DESC LIMIT %(limit)s"
    )
    rows = await _fetch(
        conn, query.format(columns=_QUARANTINE_COLUMNS, where=_where(conditions)), params
    )
    return _page(rows, limit, "quarantine_seq")


async def sink_stats(
    conn: AsyncConnection[Any], *, lag_sample_size: int, include_checksums: bool
) -> dict[str, Any]:
    by_type = await _fetch(
        conn, sql.SQL("SELECT event_type, count(*) AS n FROM event_log GROUP BY event_type")
    )
    by_reason = await _fetch(
        conn, sql.SQL("SELECT reason, count(*) AS n FROM quarantine GROUP BY reason")
    )
    totals = await _fetch(
        conn,
        sql.SQL(
            "SELECT "
            "(SELECT count(*) FROM event_log WHERE quality_warnings <> '[]'::jsonb) AS warned, "
            "(SELECT count(*) FROM entity_current_state) AS entities, "
            "(SELECT coalesce(max(value), 0) FROM consumer_counter "
            " WHERE name = 'duplicates_skipped') AS duplicates, "
            "(SELECT max(occurred_at) FROM event_log) AS latest_occurred, "
            "(SELECT max(processed_at) FROM event_log) AS latest_processed"
        ),
    )
    lag = await _fetch(conn, sql.SQL(_LAG_SQL), {"sample": lag_sample_size})
    watermarks = await _fetch(
        conn,
        sql.SQL(
            "SELECT source_id, cursor_updated_at, cursor_key, initial_sync_completed_at, "
            "last_batch_id, updated_at FROM sync_state ORDER BY source_id"
        ),
    )
    batches = await _fetch(
        conn,
        sql.SQL(
            "SELECT batch_id, source_id, sync_mode, status, row_count, started_at, finished_at, "
            "error FROM sync_batch ORDER BY started_at DESC LIMIT 5"
        ),
    )
    checksums = await _fetch(conn, sql.SQL(SINK_CHECKSUMS)) if include_checksums else [None]
    return {
        "by_type": {row["event_type"]: row["n"] for row in by_type},
        "by_reason": {row["reason"]: row["n"] for row in by_reason},
        "totals": totals[0],
        "lag": lag[0],
        "watermarks": watermarks,
        "batches": batches,
        "checksums": checksums[0],
    }


def _where(conditions: list[sql.Composable]) -> sql.Composable:
    if not conditions:
        return sql.SQL("")
    return sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)


def _page(rows: list[Row], limit: int, sequence_column: str) -> tuple[list[Row], int | None]:
    if len(rows) <= limit:
        return rows, None
    page = rows[:limit]
    return page, int(page[-1][sequence_column])


async def _fetch(
    conn: AsyncConnection[Any],
    query: sql.SQL | sql.Composed,
    params: dict[str, object] | None = None,
) -> list[Row]:
    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(query, params)
        return await cursor.fetchall()
