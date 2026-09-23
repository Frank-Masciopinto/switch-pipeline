"""Simulates source activity: inserts, updates, and rows that break quality rules.

Writers must honour the source contract: bump the version and set the UTC
modified timestamp on every write. All changes land in one transaction.
"""

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Final

from switch_pipeline.adapter.source import SnowflakeConnectionFactory, SourceTable
from switch_pipeline.observability import get_logger
from switch_pipeline.settings import SnowflakeSettings, SourceSettings
from switch_pipeline.tools.source_table import synthetic_insert_sql

log = get_logger(__name__)

INVALID_KINDS: Final = ("negative_total_price", "missing_total_price", "unknown_order_status")


@dataclass(slots=True)
class SimulationReport:
    table: str
    inserted: list[int] = field(default_factory=list)
    updated: list[int] = field(default_factory=list)
    invalid: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def simulate_changes(
    snowflake: SnowflakeSettings,
    source: SourceSettings,
    *,
    inserts: int,
    updates: int,
    invalid_rows: int,
) -> SimulationReport:
    table = SourceTable.from_settings(snowflake, source)
    name, key, version = table.qualified_name, table.key_column, table.version_column
    bump = f"{version} = {version} + 1, {table.updated_at_column} = SYSDATE()"
    kinds = [INVALID_KINDS[i % len(INVALID_KINDS)] for i in range(invalid_rows)]
    report = SimulationReport(table=name)

    # autocommit=False + commit() rather than BEGIN/COMMIT statements: the same
    # code then runs on Snowflake and on the fakesnow emulator.
    conn = SnowflakeConnectionFactory(snowflake, query_tag="switch-simulate").connect(
        autocommit=False
    )
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COALESCE(MAX({key}), 0) FROM {name}")  # noqa: S608 - validated
        row = cursor.fetchone()
        max_key = int(row[0]) if row else 0
        sampled: list[int] = []
        wanted = updates + kinds.count("unknown_order_status")
        if wanted:
            cursor.execute(
                f"SELECT {key} FROM {name} ORDER BY RANDOM() LIMIT %(n)s",  # noqa: S608
                {"n": wanted},
            )
            sampled = [int(row[0]) for row in cursor.fetchall()]
        update_keys, status_keys = sampled[:updates], sampled[updates:]

        if inserts:
            cursor.execute(synthetic_insert_sql(table, key_offset=max_key, row_count=inserts))
            report.inserted = list(range(max_key + 1, max_key + inserts + 1))

        if update_keys:
            placeholders = ", ".join(["%s"] * len(update_keys))
            cursor.execute(
                f"UPDATE {name} SET "  # noqa: S608 - validated identifiers
                "O_ORDERSTATUS = DECODE(O_ORDERSTATUS, 'O', 'F', 'F', 'P', 'O'), "
                "O_TOTALPRICE = ROUND(O_TOTALPRICE * 1.05, 2), O_ORDERPRIORITY = '1-URGENT', "
                f"{bump} WHERE {key} IN ({placeholders})",
                tuple(update_keys),
            )
            report.updated = update_keys

        next_key = max_key + inserts
        for kind in kinds:
            if kind == "unknown_order_status":
                if not status_keys:
                    continue
                target = status_keys.pop()
                cursor.execute(
                    f"UPDATE {name} SET O_ORDERSTATUS = 'X', {bump} WHERE {key} = %s",  # noqa: S608
                    (target,),
                )
            else:
                # A regular new row whose price is corrupted before the commit, so
                # the adapter only ever sees the invalid version.
                target = next_key = next_key + 1
                cursor.execute(synthetic_insert_sql(table, key_offset=target - 1, row_count=1))
                price = Decimal("-42.00") if kind == "negative_total_price" else None
                cursor.execute(
                    f"UPDATE {name} SET O_TOTALPRICE = %s WHERE {key} = %s",  # noqa: S608
                    (price, target),
                )
            report.invalid.append({"key": target, "kind": kind})
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info(
        "changes_simulated",
        table=name,
        inserted=len(report.inserted),
        updated=len(report.updated),
        invalid=len(report.invalid),
    )
    return report
