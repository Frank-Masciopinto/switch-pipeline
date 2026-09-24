"""The real CLI entry points, wired from environment variables the way the
containers run them: proves the composition roots, not just the components."""

import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest

from switch_pipeline import cli
from switch_pipeline.settings import KafkaSettings, PostgresSettings
from tests.helpers import exception_types, logged, read_env_example
from tests.integration.conftest import RULES_PATH

SEEDED_ROWS = 40


@pytest.fixture
def pipeline_env(
    entrypoint_state: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    db: str,
    postgres_settings: PostgresSettings,
    kafka_settings: KafkaSettings,
    snowflake_server: dict[str, Any],
) -> None:
    """.env.example, pointed at the test containers and the emulator."""
    overrides = {
        "SNOWFLAKE_ACCOUNT": snowflake_server["account"],
        "SNOWFLAKE_USER": snowflake_server["user"],
        "SNOWFLAKE_PASSWORD": snowflake_server["password"],
        "SNOWFLAKE_PRIVATE_KEY_PATH": "",
        "SNOWFLAKE_HOST": snowflake_server["host"],
        "SNOWFLAKE_PORT": str(snowflake_server["port"]),
        "SNOWFLAKE_PROTOCOL": "http",
        "SNOWFLAKE_DATABASE": f"CLI_{uuid.uuid4().hex[:8].upper()}",
        "SEED_STRATEGY": "synthetic",
        "SEED_ROW_COUNT": str(SEEDED_ROWS),
        "ADAPTER_SETTLE_SECONDS": "0",
        "ADAPTER_HEARTBEAT_PATH": str(tmp_path / "adapter.heartbeat"),
        "CONSUMER_HEARTBEAT_PATH": str(tmp_path / "consumer.heartbeat"),
        "QUALITY_RULES_PATH": str(RULES_PATH),
        "KAFKA_BOOTSTRAP_SERVERS": kafka_settings.bootstrap_servers,
        "KAFKA_TOPIC": kafka_settings.topic,
        "KAFKA_CONSUMER_GROUP": kafka_settings.consumer_group,
        "KAFKA_TOPIC_PARTITIONS": "3",
        "POSTGRES_HOST": postgres_settings.host,
        "POSTGRES_PORT": str(postgres_settings.port),
        "POSTGRES_DB": postgres_settings.db,
        "POSTGRES_USER": postgres_settings.user,
        "POSTGRES_PASSWORD": postgres_settings.password.get_secret_value(),
    }
    for name, value in {**read_env_example(), **overrides}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.chdir(tmp_path)  # no .env file: the environment is the only source


def count(db: str, table: str) -> int:
    with psycopg.connect(db) as conn:
        row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
    return int(row[0]) if row else 0


def test_the_cli_runs_the_pipeline_from_environment_variables(pipeline_env: None, db: str) -> None:
    assert cli.main(["init"]) == 0
    assert cli.main(["check-config"]) == 0
    assert cli.main(["seed"]) == 0
    assert cli.main(["check-snowflake"]) == 0
    assert cli.main(["adapter", "--once"]) == 0
    assert cli.main(["simulate", "--inserts", "2", "--updates", "3", "--invalid", "1"]) == 0
    assert cli.main(["adapter", "--once"]) == 0
    assert cli.main(["adapter", "--once"]) == 0, "a rerun with nothing new must succeed"

    # Consumes everything as the group, then rebuilds from offset 0 and compares.
    assert cli.main(["replay", "--rebuild"]) == 0
    assert count(db, "entity_current_state") == SEEDED_ROWS + 2
    assert count(db, "event_log") == SEEDED_ROWS + 2 + 3
    assert count(db, "quarantine") == 1
    assert count(db, "sync_batch") == 2


def test_a_consumer_started_before_init_stops_with_a_fatal_error(
    pipeline_env: None, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["consumer"]) == 1, "the topic was never created"
    record = logged(capsys.readouterr().out, "fatal_error")
    assert record["level"] == "error"
    assert "FatalPipelineError" in exception_types(record)
