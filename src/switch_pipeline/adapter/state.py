"""Durable sync state (watermark + batch ledger) in PostgreSQL.

The watermark only moves inside the same transaction that marks a batch
committed, and a batch is only committed after the broker acknowledged every
event in it; a crash at any point therefore resumes from the last fully
published batch.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from switch_pipeline.adapter.cursor import SyncCursor
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.observability import get_logger

log = get_logger(__name__)


class SyncMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"


class SourceLockedError(FatalPipelineError):
    """Another adapter instance is already syncing this source."""


class LockLostError(FatalPipelineError):
    """The connection holding the source lock dropped; another instance may take over."""


@dataclass(frozen=True, slots=True, kw_only=True)
class SyncState:
    source_id: str
    cursor: SyncCursor | None
    initial_sync_completed_at: datetime | None
    last_batch_id: UUID | None

    @property
    def mode(self) -> SyncMode:
        return SyncMode.FULL if self.initial_sync_completed_at is None else SyncMode.INCREMENTAL


class SyncStateStore(Protocol):
    def load(self, source_id: str) -> SyncState: ...

    def begin_batch(
        self,
        *,
        batch_id: UUID,
        source_id: str,
        mode: SyncMode,
        cursor_start: SyncCursor | None,
        upper_bound: datetime,
    ) -> None: ...

    def commit_batch(
        self, *, batch_id: UUID, source_id: str, cursor_end: SyncCursor, row_count: int
    ) -> None: ...

    def fail_batch(self, *, batch_id: UUID, error: str) -> None: ...

    def mark_initial_sync_completed(self, source_id: str) -> None: ...


class PostgresSyncStateStore:
    def __init__(self, conninfo: str) -> None:
        self._pool = ConnectionPool(
            conninfo,
            min_size=1,
            max_size=2,
            open=False,
            check=ConnectionPool.check_connection,
            name="sync-state",
        )

    def open(self, *, timeout: float) -> None:
        self._pool.open(wait=True, timeout=timeout)

    def close(self) -> None:
        self._pool.close()

    def load(self, source_id: str) -> SyncState:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                "INSERT INTO sync_state (source_id) VALUES (%s) ON CONFLICT (source_id) DO NOTHING",
                (source_id,),
            )
            row = conn.execute(
                """
                SELECT cursor_updated_at, cursor_key, initial_sync_completed_at, last_batch_id
                FROM sync_state WHERE source_id = %s
                """,
                (source_id,),
            ).fetchone()
        assert row is not None
        cursor_updated_at, cursor_key, completed_at, last_batch_id = row
        cursor = (
            None
            if cursor_updated_at is None
            else SyncCursor(updated_at=cursor_updated_at, key=cursor_key)
        )
        return SyncState(
            source_id=source_id,
            cursor=cursor,
            initial_sync_completed_at=completed_at,
            last_batch_id=last_batch_id,
        )

    def recover_interrupted_batches(self, source_id: str) -> int:
        """Batches left 'running' by a crash never advanced the watermark; close them out."""
        with self._pool.connection() as conn, conn.transaction():
            cursor = conn.execute(
                """
                UPDATE sync_batch
                SET status = 'failed', finished_at = clock_timestamp(),
                    error = 'interrupted: adapter stopped before the batch committed'
                WHERE source_id = %s AND status = 'running'
                """,
                (source_id,),
            )
            return cursor.rowcount

    def begin_batch(
        self,
        *,
        batch_id: UUID,
        source_id: str,
        mode: SyncMode,
        cursor_start: SyncCursor | None,
        upper_bound: datetime,
    ) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                """
                INSERT INTO sync_batch
                    (batch_id, source_id, sync_mode, status, cursor_start, upper_bound)
                VALUES (%s, %s, %s, 'running', %s, %s)
                """,
                (batch_id, source_id, mode.value, _cursor_json(cursor_start), upper_bound),
            )

    def commit_batch(
        self, *, batch_id: UUID, source_id: str, cursor_end: SyncCursor, row_count: int
    ) -> None:
        with self._pool.connection() as conn, conn.transaction():
            marked = conn.execute(
                """
                UPDATE sync_batch
                SET status = 'committed', cursor_end = %s, row_count = %s,
                    finished_at = clock_timestamp()
                WHERE batch_id = %s AND status = 'running'
                """,
                (_cursor_json(cursor_end), row_count, batch_id),
            ).rowcount
            if marked != 1:
                raise FatalPipelineError(f"batch {batch_id} is not running; refusing to commit it")
            conn.execute(
                """
                INSERT INTO sync_state (source_id, cursor_updated_at, cursor_key, last_batch_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_id) DO UPDATE SET
                    cursor_updated_at = EXCLUDED.cursor_updated_at,
                    cursor_key = EXCLUDED.cursor_key,
                    last_batch_id = EXCLUDED.last_batch_id,
                    updated_at = clock_timestamp()
                """,
                (source_id, cursor_end.updated_at, Jsonb(cursor_end.key), batch_id),
            )

    def fail_batch(self, *, batch_id: UUID, error: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                """
                UPDATE sync_batch SET status = 'failed', error = %s, finished_at = clock_timestamp()
                WHERE batch_id = %s AND status = 'running'
                """,
                (error[:4000], batch_id),
            )

    def mark_initial_sync_completed(self, source_id: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                """
                INSERT INTO sync_state (source_id, initial_sync_completed_at)
                VALUES (%s, clock_timestamp())
                ON CONFLICT (source_id) DO UPDATE
                SET initial_sync_completed_at = clock_timestamp(), updated_at = clock_timestamp()
                WHERE sync_state.initial_sync_completed_at IS NULL
                """,
                (source_id,),
            )


class SourceLock:
    """Session-level advisory lock: at most one adapter instance per source.

    Two writers advancing the same watermark would race; the lock lives as long
    as its dedicated connection, so a crashed holder releases it automatically.
    """

    def __init__(self, conninfo: str, source_id: str) -> None:
        self._conninfo = conninfo
        self._source_id = source_id
        self._conn: psycopg.Connection[tuple[object, ...]] | None = None

    def __enter__(self) -> Self:
        conn = psycopg.connect(self._conninfo, autocommit=True)
        row = conn.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (self._lock_name,)
        ).fetchone()
        if row is None or row[0] is not True:
            conn.close()
            raise SourceLockedError(f"another adapter is already syncing {self._source_id}")
        self._conn = conn
        log.info("source_lock_acquired", source_id=self._source_id)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def check(self) -> None:
        if self._conn is None:
            raise LockLostError("source lock is not held")
        try:
            self._conn.execute("SELECT 1")
        except psycopg.Error as exc:
            raise LockLostError(f"lost the source lock for {self._source_id}: {exc}") from exc

    @property
    def _lock_name(self) -> str:
        return f"switch-adapter:{self._source_id}"


def _cursor_json(cursor: SyncCursor | None) -> Jsonb | None:
    return None if cursor is None else Jsonb(cursor.to_json())
