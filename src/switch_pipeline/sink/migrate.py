"""Forward-only SQL migrations, applied in file-name order under an advisory lock.

Each file runs in its own transaction and is recorded with a checksum; editing
an already-applied migration is detected and refused.
"""

import hashlib
from importlib import resources
from typing import Any

import psycopg

from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.observability import get_logger
from switch_pipeline.settings import PostgresSettings
from switch_pipeline.sink.connection import connect, unavailable

log = get_logger(__name__)

_LOCK_NAME = "switch-pipeline:migrations"


def apply_migrations(postgres: PostgresSettings, *, application_name: str) -> list[str]:
    """Apply pending migrations; return the versions applied by this call."""
    # The advisory lock belongs to this session: closing the connection releases
    # it, including when a migration fails halfway.
    with connect(postgres, application_name=application_name) as conn:
        try:
            return _apply_pending(conn)
        except psycopg.OperationalError as exc:
            raise unavailable(exc) from exc


def _apply_pending(conn: psycopg.Connection[Any]) -> list[str]:
    conn.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (_LOCK_NAME,))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    TEXT PRIMARY KEY,
            checksum   TEXT        NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    rows = conn.execute("SELECT version, checksum FROM schema_migrations").fetchall()
    recorded: dict[str, str] = {str(version): str(checksum) for version, checksum in rows}
    applied_now: list[str] = []
    for version, script in _migration_scripts():
        checksum = hashlib.sha256(script.encode("utf-8")).hexdigest()
        if version in recorded:
            if recorded[version] != checksum:
                raise FatalPipelineError(
                    f"migration {version} changed after it was applied; add a new one"
                )
            continue
        with conn.transaction():
            conn.execute(script)
            conn.execute(
                "INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s)",
                (version, checksum),
            )
        applied_now.append(version)
        log.info("migration_applied", version=version)
    return applied_now


def _migration_scripts() -> list[tuple[str, str]]:
    folder = resources.files("switch_pipeline.sink").joinpath("migrations")
    scripts = [
        (entry.name.removesuffix(".sql"), entry.read_text(encoding="utf-8"))
        for entry in folder.iterdir()
        if entry.name.endswith(".sql")
    ]
    return sorted(scripts)
