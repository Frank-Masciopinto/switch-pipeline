"""Creates and fills the demo source table (a no-op if it already has rows, unless forced)."""

from contextlib import closing
from dataclasses import asdict, dataclass
from typing import Any

from snowflake.connector import DictCursor

from switch_pipeline.adapter.snowflake import SnowflakeConnectionFactory, SourceTable
from switch_pipeline.observability import get_logger
from switch_pipeline.settings import SeedSettings, SnowflakeSettings, SourceSettings
from switch_pipeline.tools.source_table import (
    create_table_sql,
    sample_share_insert_sql,
    synthetic_insert_sql,
)

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SeedReport:
    table: str
    strategy: str
    rows: int
    created: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def seed_source(
    snowflake: SnowflakeSettings, source: SourceSettings, seed: SeedSettings, *, force: bool
) -> SeedReport:
    table = SourceTable.from_settings(snowflake, source)
    factory = SnowflakeConnectionFactory(snowflake, query_tag="switch-seed")
    with factory.session() as conn, closing(conn.cursor(DictCursor)) as cursor:
        # The setup script creates the database for a least-privileged role;
        # creating it here only happens where that is allowed (e.g. the emulator).
        if not _exists(cursor, "SHOW DATABASES LIKE %(name)s", table.database):
            cursor.execute(f"CREATE DATABASE {table.database}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {table.database}.{table.schema}")
        if _exists(
            cursor,
            f"SHOW TABLES LIKE %(name)s IN SCHEMA {table.database}.{table.schema}",
            table.table,
        ):
            existing = _count(cursor, table)
            if existing and not force:
                log.info("seed_skipped", table=table.qualified_name, rows=existing)
                return SeedReport(table.qualified_name, seed.strategy, existing, created=False)
        cursor.execute(create_table_sql(table))
        if seed.strategy == "sample_share":
            insert = sample_share_insert_sql(
                table, sample_schema=seed.sample_schema, row_count=seed.row_count
            )
        else:
            insert = synthetic_insert_sql(table, key_offset=0, row_count=seed.row_count)
        cursor.execute(insert)
        rows = _count(cursor, table)
    log.info("seed_completed", table=table.qualified_name, strategy=seed.strategy, rows=rows)
    return SeedReport(table.qualified_name, seed.strategy, rows, created=True)


def _exists(cursor: DictCursor, show_statement: str, name: str) -> bool:
    cursor.execute(show_statement, {"name": name})
    return any(str(row["name"]).upper() == name.upper() for row in cursor.fetchall())


def _count(cursor: DictCursor, table: SourceTable) -> int:
    cursor.execute(f"SELECT COUNT(*) AS N FROM {table.qualified_name}")  # noqa: S608 - validated
    row = cursor.fetchone()
    return int(row["N"]) if row else 0
