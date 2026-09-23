"""Forward-only SQL migrations, applied in file-name order under an advisory lock.

Each file runs in its own transaction and is recorded with a checksum; editing
an already-applied migration is detected and refused.
"""

import hashlib
from importlib import resources

import psycopg

from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.observability import get_logger

log = get_logger(__name__)

_LOCK_NAME = "switch-pipeline:migrations"


def apply_migrations(conninfo: str) -> list[str]:
    """Apply pending migrations; return the versions applied by this call."""
    applied_now: list[str] = []
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (_LOCK_NAME,))
        try:
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
        finally:
            conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (_LOCK_NAME,))
    return applied_now


def _migration_scripts() -> list[tuple[str, str]]:
    folder = resources.files("switch_pipeline.db").joinpath("migrations")
    scripts = [
        (entry.name.removesuffix(".sql"), entry.read_text(encoding="utf-8"))
        for entry in folder.iterdir()
        if entry.name.endswith(".sql")
    ]
    return sorted(scripts)
