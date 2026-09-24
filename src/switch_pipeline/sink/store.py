"""The consumer's PostgreSQL: one transaction per batch, plus the operator
queries a replay needs (convergence checksums, truncation for a rebuild)."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.errors import DatabaseUnavailableError
from switch_pipeline.settings import PostgresSettings
from switch_pipeline.sink.queries import LATEST_EVENT, SINK_CHECKSUMS, TRUNCATE_SINK
from switch_pipeline.sink.writer import PostgresSinkWriter


class PostgresSink:
    """Implements the consumer's Sink port."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    @classmethod
    def open(cls, postgres: PostgresSettings, *, application_name: str) -> "PostgresSink":
        pool = ConnectionPool(
            postgres.conninfo(application_name=application_name),
            min_size=1,
            max_size=1,
            open=False,
            check=ConnectionPool.check_connection,
            name="sink",
        )
        try:
            pool.open(wait=True, timeout=float(postgres.connect_timeout_seconds))
        except psycopg.OperationalError as exc:
            pool.close()
            raise DatabaseUnavailableError(f"cannot connect to PostgreSQL: {exc}") from exc
        return cls(pool)

    @contextmanager
    def transaction(self) -> Iterator[PostgresSinkWriter]:
        with self._connection() as conn, conn.transaction():
            yield PostgresSinkWriter(conn)

    def checksums(self) -> dict[str, Any]:
        """Content fingerprints of the sink; equal before and after a replay."""
        with self._connection() as conn:
            [row] = conn.cursor(row_factory=dict_row).execute(SINK_CHECKSUMS).fetchall()
        return row

    def truncate(self) -> None:
        with self._connection() as conn, conn.transaction():
            conn.execute(TRUNCATE_SINK)

    def latest_event(self) -> ChangeEvent | None:
        with self._connection() as conn:
            row = conn.cursor(row_factory=dict_row).execute(LATEST_EVENT).fetchone()
        return None if row is None else ChangeEvent.model_validate(row)

    def close(self) -> None:
        self._pool.close()

    @contextmanager
    def _connection(self) -> Iterator[psycopg.Connection[Any]]:
        try:
            with self._pool.connection() as conn:
                yield conn
        except psycopg.OperationalError as exc:  # includes PoolTimeout
            raise DatabaseUnavailableError(f"PostgreSQL unavailable: {str(exc).strip()}") from exc
