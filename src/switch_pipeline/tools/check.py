"""Diagnoses the Snowflake setup before the first sync: key, sign-in, grants, objects.

Each check reports what it found and, when it fails, the most likely fix, so a
first run against a new account does not end in a bare connector stack trace.
"""

import os
from contextlib import closing
from dataclasses import dataclass

from snowflake.connector import DictCursor
from snowflake.connector.errors import Error as SnowflakeError

from switch_pipeline.adapter.source import SnowflakeConnectionFactory, SourceTable
from switch_pipeline.settings import SeedSettings, SnowflakeSettings, SourceSettings

# Needles are matched case-insensitively against the connector's error message.
_CONNECT_HINTS: tuple[tuple[str, str], ...] = (
    (
        "private key",
        "the key file could not be loaded: check the file and SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    ),
    (
        "jwt",
        "the user's registered public key does not match SNOWFLAKE_PRIVATE_KEY_PATH: set "
        "RSA_PUBLIC_KEY from secrets/snowflake_rsa_key.pub (snowflake/setup.sql)",
    ),
    (
        "multi-factor",
        "password sign-in needs MFA for this user: use the key pair of a TYPE = SERVICE user "
        "(snowflake/setup.sql) or a programmatic access token",
    ),
    (
        "incorrect username or password",
        "check SNOWFLAKE_USER and SNOWFLAKE_PASSWORD; service users cannot use passwords, use "
        "a key pair or a programmatic access token",
    ),
    (
        "role",
        "grant the role to the user or fix SNOWFLAKE_ROLE (snowflake/setup.sql)",
    ),
    (
        "warehouse",
        "grant USAGE on the warehouse to the role or fix SNOWFLAKE_WAREHOUSE",
    ),
)
_ACCOUNT_HINT = (
    "check SNOWFLAKE_ACCOUNT: it must be <orgname>-<account_name>, as printed at the end of "
    "snowflake/setup.sql"
)


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    ok: bool
    detail: str
    hint: str | None = None


def check_snowflake(
    snowflake: SnowflakeSettings, source: SourceSettings, seed: SeedSettings
) -> list[CheckResult]:
    table = SourceTable.from_settings(snowflake, source)
    results: list[CheckResult] = []
    key_path = snowflake.private_key_path
    if key_path is not None:
        if not os.access(key_path, os.R_OK):
            results.append(
                CheckResult(
                    "private_key",
                    ok=False,
                    detail=f"{key_path} is missing or unreadable",
                    hint="run `make snowflake-keypair`; ./secrets is mounted at /run/secrets",
                )
            )
            return results
        results.append(CheckResult("private_key", ok=True, detail=f"{key_path} is readable"))

    try:
        conn = SnowflakeConnectionFactory(snowflake, query_tag="switch-check").connect()
    except Exception as exc:  # the connector raises many unrelated types for sign-in failures
        message = _first_line(exc)
        results.append(CheckResult("sign_in", ok=False, detail=message, hint=sign_in_hint(message)))
        return results
    method = "a key pair" if key_path is not None else "a password or token"
    results.append(
        CheckResult("sign_in", ok=True, detail=f"signed in to {snowflake.account} with {method}")
    )

    with closing(conn), closing(conn.cursor(DictCursor)) as cursor:
        results.append(_database(cursor, table))
        results.append(_source_table(cursor, table))
        if seed.strategy == "sample_share":
            results.append(_sample_share(cursor, seed.sample_schema))
    return results


def _database(cursor: DictCursor, table: SourceTable) -> CheckResult:
    cursor.execute("SHOW DATABASES LIKE %(name)s", {"name": table.database})
    if any(str(row["name"]).upper() == table.database.upper() for row in cursor.fetchall()):
        return CheckResult("database", ok=True, detail=f"{table.database} is visible to the role")
    return CheckResult(
        "database",
        ok=False,
        detail=f"{table.database} does not exist or the role cannot see it",
        hint="run snowflake/setup.sql (creates the database and grants it to the role)",
    )


def _source_table(cursor: DictCursor, table: SourceTable) -> CheckResult:
    key, version, updated_at = table.contract_columns
    try:
        # Selecting the contract columns also proves they exist and a warehouse runs.
        cursor.execute(
            f"SELECT COUNT(*) AS N, MAX({updated_at}) AS LATEST, MAX({version}) AS VERSION, "  # noqa: S608 - validated identifiers
            f"MIN({key}) AS FIRST_KEY FROM {table.qualified_name}"
        )
        row = cursor.fetchone()
    except SnowflakeError as exc:
        return CheckResult(
            "source_table",
            ok=False,
            detail=_first_line(exc),
            hint="run `make seed`, which creates the table; otherwise check the role's grants",
        )
    rows = row["N"] if row else 0
    latest = row["LATEST"] if row else None
    return CheckResult(
        "source_table",
        ok=True,
        detail=f"{table.qualified_name}: {rows} rows, newest {updated_at} {latest}",
    )


def _sample_share(cursor: DictCursor, sample_schema: str) -> CheckResult:
    try:
        cursor.execute(f"SELECT COUNT(*) AS N FROM {sample_schema}.ORDERS")  # noqa: S608 - validated
        row = cursor.fetchone()
    except SnowflakeError as exc:
        return CheckResult(
            "sample_share",
            ok=False,
            detail=_first_line(exc),
            hint="GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA to the role "
            "(snowflake/setup.sql), or set SEED_STRATEGY=synthetic",
        )
    return CheckResult(
        "sample_share", ok=True, detail=f"{sample_schema}.ORDERS: {row['N'] if row else 0} rows"
    )


def sign_in_hint(message: str) -> str:
    lowered = message.lower()
    for needle, hint in _CONNECT_HINTS:
        if needle in lowered:
            return hint
    return _ACCOUNT_HINT


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip() or type(exc).__name__
    return text.splitlines()[0][:300]
