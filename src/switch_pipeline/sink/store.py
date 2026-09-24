"""The consumer's PostgreSQL: one transaction per batch, plus the operator
queries a replay needs (convergence checksums, truncation for a rebuild)."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.settings import PostgresSettings
from switch_pipeline.sink.connection import open_pool, pooled
from switch_pipeline.sink.queries import LATEST_EVENT, SINK_CHECKSUMS, TRUNCATE_SINK
from switch_pipeline.sink.writer import PostgresSinkWriter


class PostgresSink:
    """Implements the consumer's Sink port."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    @classmethod
    def open(cls, postgres: PostgresSettings, *, application_name: str) -> "PostgresSink":
        return cls(open_pool(postgres, application_name=application_name, name="sink", max_size=1))

    @contextmanager
    def transaction(self) -> Iterator[PostgresSinkWriter]:
        with pooled(self._pool) as conn, conn.transaction():
            yield PostgresSinkWriter(conn)

    def checksums(self) -> dict[str, Any]:
        """Content fingerprints of the sink; equal before and after a replay."""
        with pooled(self._pool) as conn:
            [row] = conn.cursor(row_factory=dict_row).execute(SINK_CHECKSUMS).fetchall()
        return row

    def truncate(self) -> None:
        with pooled(self._pool) as conn, conn.transaction():
            conn.execute(TRUNCATE_SINK)

    def latest_event(self) -> ChangeEvent | None:
        with pooled(self._pool) as conn:
            row = conn.cursor(row_factory=dict_row).execute(LATEST_EVENT).fetchone()
        return None if row is None else ChangeEvent.model_validate(row)

    def close(self) -> None:
        self._pool.close()
