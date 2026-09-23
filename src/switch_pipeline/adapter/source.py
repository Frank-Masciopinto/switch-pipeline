"""Snowflake source: reads changed rows of one table in watermark order."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection

from switch_pipeline.adapter.cursor import SyncCursor
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.observability import get_logger
from switch_pipeline.settings import SNOWFLAKE_IDENTIFIER, SnowflakeSettings, SourceSettings

log = get_logger(__name__)

_IDENTIFIER = re.compile(SNOWFLAKE_IDENTIFIER)


class SourceContractError(FatalPipelineError):
    """A row violates the source contract (e.g. NULL key, version or timestamp)."""


@dataclass(frozen=True, slots=True)
class SourceTable:
    """Validated identifiers. They are interpolated into SQL (identifiers cannot be
    bound parameters), which is only safe because each one matched the pattern."""

    database: str
    schema: str
    table: str
    key_column: str
    version_column: str
    updated_at_column: str

    def __post_init__(self) -> None:
        for name in (self.database, self.schema, self.table, *self.contract_columns):
            if not _IDENTIFIER.fullmatch(name):
                raise ValueError(f"invalid Snowflake identifier: {name!r}")

    @classmethod
    def from_settings(cls, snowflake: SnowflakeSettings, source: SourceSettings) -> "SourceTable":
        return cls(
            database=snowflake.database,
            schema=snowflake.schema_name,
            table=source.table,
            key_column=source.key_column,
            version_column=source.version_column,
            updated_at_column=source.updated_at_column,
        )

    @property
    def qualified_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}".upper()

    @property
    def contract_columns(self) -> tuple[str, str, str]:
        return (self.key_column, self.version_column, self.updated_at_column)


@dataclass(frozen=True, slots=True)
class SourceRow:
    key: int | str
    version: int
    updated_at: datetime  # timezone-aware UTC
    values: Mapping[str, Any]  # the full row, column name -> Python value

    @property
    def position(self) -> SyncCursor:
        return SyncCursor(updated_at=self.updated_at, key=self.key)


class ChangeSource(Protocol):
    def upper_bound(self, settle_seconds: int) -> datetime:
        """The newest ``updated_at`` this cycle may read (source clock, UTC)."""
        ...

    def fetch_changes(
        self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
    ) -> list[SourceRow]:
        """Rows strictly after ``cursor`` and at or before ``upper_bound``, in watermark order."""
        ...


def build_changes_query(
    table: SourceTable, cursor: SyncCursor | None, upper_bound: datetime, limit: int
) -> tuple[str, dict[str, object]]:
    """Keyset query for the next batch.

    ``ts >= :cursor_ts AND (ts > :cursor_ts OR key > :cursor_key)`` is the tuple
    comparison ``(ts, key) > (cursor_ts, cursor_key)`` written so that Snowflake
    can prune micro-partitions on the timestamp.
    """
    ts, key = table.updated_at_column, table.key_column
    predicates = [f"{ts} <= %(upper_bound)s"]
    params: dict[str, object] = {"upper_bound": to_source_timestamp(upper_bound), "limit": limit}
    if cursor is not None:
        predicates.append(
            f"{ts} >= %(cursor_ts)s AND ({ts} > %(cursor_ts)s OR {key} > %(cursor_key)s)"
        )
        params["cursor_ts"] = to_source_timestamp(cursor.updated_at)
        params["cursor_key"] = cursor.key
    query = (
        f"SELECT * FROM {table.qualified_name} "  # noqa: S608 - identifiers validated by SourceTable
        f"WHERE {' AND '.join(predicates)} "
        f"ORDER BY {ts}, {key} LIMIT %(limit)s"
    )
    return query, params


def to_source_timestamp(moment: datetime) -> datetime:
    """UTC wall time without tzinfo: the representation of a TIMESTAMP_NTZ column."""
    return moment.astimezone(UTC).replace(tzinfo=None)


def from_source_timestamp(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise SourceContractError(f"updated_at must be a timestamp, got {value!r}")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class SnowflakeConnectionFactory:
    def __init__(self, settings: SnowflakeSettings, *, query_tag: str) -> None:
        self._settings = settings
        self._query_tag = query_tag

    def connect(self, *, autocommit: bool = True) -> SnowflakeConnection:
        settings = self._settings
        params: dict[str, Any] = {
            "account": settings.account,
            "user": settings.user,
            "role": settings.role,
            "warehouse": settings.warehouse,
            "database": settings.database,
            "schema": settings.schema_name,
            "login_timeout": settings.login_timeout_seconds,
            "network_timeout": settings.network_timeout_seconds,
            "autocommit": autocommit,
            "application": "switch-pipeline",
            # The watermark column is UTC by contract; pin the session so that
            # SYSDATE()/timestamp conversions never depend on account defaults.
            "session_parameters": {"TIMEZONE": "UTC", "QUERY_TAG": self._query_tag},
        }
        if settings.private_key_path is not None:
            params["authenticator"] = "SNOWFLAKE_JWT"
            params["private_key_file"] = str(settings.private_key_path)
            if settings.private_key_passphrase is not None:
                params["private_key_file_pwd"] = settings.private_key_passphrase.get_secret_value()
        elif settings.password is not None:
            params["password"] = settings.password.get_secret_value()
        if settings.host is not None:
            params["host"] = settings.host
            if settings.port is not None:
                params["port"] = settings.port
            if settings.protocol is not None:
                params["protocol"] = settings.protocol
        return snowflake.connector.connect(**params)


class SnowflakeChangeSource:
    """Keeps one session open across cycles and drops it after any error, so the
    next cycle reconnects cleanly (expired sessions, network blips)."""

    def __init__(self, factory: SnowflakeConnectionFactory, table: SourceTable) -> None:
        self._factory = factory
        self._table = table
        self._connection: SnowflakeConnection | None = None

    def upper_bound(self, settle_seconds: int) -> datetime:
        rows = self._query(
            "SELECT DATEADD(second, -%(settle)s, SYSDATE()) AS UPPER_BOUND",
            {"settle": settle_seconds},
        )
        return from_source_timestamp(rows[0]["UPPER_BOUND"])

    def fetch_changes(
        self, cursor: SyncCursor | None, upper_bound: datetime, limit: int
    ) -> list[SourceRow]:
        query, params = build_changes_query(self._table, cursor, upper_bound, limit)
        return [self._to_row(record) for record in self._query(query, params)]

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def _query(self, query: str, params: dict[str, object]) -> list[dict[str, Any]]:
        if self._connection is None:
            self._connection = self._factory.connect()
            log.info("snowflake_connected", table=self._table.qualified_name)
        try:
            with self._connection.cursor(DictCursor) as cursor:
                cursor.execute(query, params)
                rows: list[dict[str, Any]] = cursor.fetchall()
                return rows
        except Exception:
            self.close()
            raise

    def _to_row(self, record: Mapping[str, Any]) -> SourceRow:
        values = {name.upper(): value for name, value in record.items()}
        key = values.get(self._table.key_column.upper())
        version = values.get(self._table.version_column.upper())
        updated_at = values.get(self._table.updated_at_column.upper())
        if key is None or isinstance(key, bool) or not isinstance(key, int | str):
            raise SourceContractError(f"{self._table.key_column} must be a non-null int or str")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise SourceContractError(
                f"{self._table.version_column} must be a positive integer (key={key!r})"
            )
        return SourceRow(
            key=key, version=version, updated_at=from_source_timestamp(updated_at), values=values
        )
