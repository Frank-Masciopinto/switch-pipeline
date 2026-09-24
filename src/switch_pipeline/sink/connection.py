"""Connections to PostgreSQL, with driver failures reported as DatabaseUnavailableError."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool

from switch_pipeline.errors import DatabaseUnavailableError
from switch_pipeline.settings import PostgresSettings


def conninfo(postgres: PostgresSettings, *, application_name: str, read_only: bool = False) -> str:
    options = {"options": "-c default_transaction_read_only=on"} if read_only else {}
    return make_conninfo(
        host=postgres.host,
        port=postgres.port,
        dbname=postgres.db,
        user=postgres.user,
        password=postgres.password.get_secret_value(),
        connect_timeout=postgres.connect_timeout_seconds,
        application_name=application_name,
        **options,
    )


def connect(postgres: PostgresSettings, *, application_name: str) -> psycopg.Connection[Any]:
    """A dedicated autocommit session, for session-scoped advisory locks."""
    try:
        return psycopg.connect(
            conninfo(postgres, application_name=application_name), autocommit=True
        )
    except psycopg.OperationalError as exc:
        raise unavailable(exc) from exc


def open_pool(
    postgres: PostgresSettings, *, application_name: str, name: str, max_size: int
) -> ConnectionPool:
    """A pool with at least one live connection; callers wait at most the connect timeout."""
    timeout = float(postgres.connect_timeout_seconds)
    pool = ConnectionPool(
        conninfo(postgres, application_name=application_name),
        min_size=1,
        max_size=max_size,
        timeout=timeout,
        open=False,
        check=ConnectionPool.check_connection,
        name=name,
    )
    try:
        pool.open(wait=True, timeout=timeout)
    except psycopg.OperationalError as exc:
        pool.close()
        raise unavailable(exc) from exc
    return pool


@contextmanager
def pooled(pool: ConnectionPool) -> Iterator[psycopg.Connection[Any]]:
    try:
        with pool.connection() as conn:
            yield conn
    except psycopg.OperationalError as exc:  # includes PoolTimeout
        raise unavailable(exc) from exc


def unavailable(exc: psycopg.OperationalError) -> DatabaseUnavailableError:
    return DatabaseUnavailableError(f"PostgreSQL unavailable: {str(exc).strip()}")
