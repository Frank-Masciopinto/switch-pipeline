"""Real PostgreSQL and Redpanda (testcontainers, same images as docker compose,
read from .env.example) plus the fakesnow Snowflake emulator over HTTP, so the
real snowflake-connector code path is exercised."""

import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import fakesnow
import psycopg
import pytest
from testcontainers.community.kafka import RedpandaContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.config import testcontainers_config

from switch_pipeline.settings import (
    ConsumerSettings,
    KafkaSettings,
    PostgresSettings,
    SeedSettings,
    SnowflakeSettings,
    SourceSettings,
)
from switch_pipeline.sink.migrate import apply_migrations
from switch_pipeline.sink.store import PostgresSink
from tests.helpers import REPO_ROOT, read_env_example

ENV = read_env_example()
RULES_PATH = REPO_ROOT / "config" / "quality_rules.yaml"
SOURCE_SETTINGS = SourceSettings(
    table="CUSTOMER_ORDERS",
    entity_type="order",
    key_column="O_ORDERKEY",
    version_column="ROW_VERSION",
    updated_at_column="UPDATED_AT",
)


def pytest_configure(config: pytest.Config) -> None:
    # testcontainers' reaper bind-mounts the Docker socket; inside Docker Desktop's
    # VM (and on Linux) it lives at /var/run/docker.sock, not at the macOS host path.
    if not os.environ.get("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"):
        testcontainers_config.ryuk_docker_socket = "/var/run/docker.sock"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/integration" in str(item.path):
            item.add_marker(pytest.mark.integration)


def synthetic_seed(rows: int) -> SeedSettings:
    return SeedSettings(strategy="synthetic", sample_schema="UNUSED.UNUSED", row_count=rows)


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    with PostgresContainer(
        ENV["POSTGRES_IMAGE"], username="switch", password="switch", dbname="switch"
    ) as container:
        yield container


@pytest.fixture(scope="session")
def postgres_settings(postgres_container: PostgresContainer) -> PostgresSettings:
    return PostgresSettings(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        db="switch",
        user="switch",
        password="switch",  # type: ignore[arg-type]
        connect_timeout_seconds=5,
        pool_min_size=1,
        pool_max_size=4,
    )


@pytest.fixture(scope="session")
def migrated(postgres_settings: PostgresSettings) -> str:
    conninfo = postgres_settings.conninfo(application_name="tests")
    apply_migrations(conninfo)
    return conninfo


@pytest.fixture
def db(migrated: str) -> str:
    """Connection string to an empty sink and sync state."""
    with psycopg.connect(migrated, autocommit=True) as conn:
        conn.execute(
            "TRUNCATE entity_current_state, event_log, quarantine, consumer_counter, "
            "sync_state, sync_batch RESTART IDENTITY"
        )
    return migrated


@pytest.fixture
def sink(db: str, postgres_settings: PostgresSettings) -> Iterator[PostgresSink]:
    store = PostgresSink.open(postgres_settings, application_name="tests")
    yield store
    store.close()


@pytest.fixture(scope="session")
def redpanda() -> Iterator[RedpandaContainer]:
    container = RedpandaContainer(ENV["REDPANDA_IMAGE"])
    container.start(timeout=120)
    yield container
    container.stop()


@pytest.fixture
def kafka_settings(redpanda: RedpandaContainer) -> KafkaSettings:
    topic = f"test.{uuid.uuid4().hex[:12]}"
    return KafkaSettings(
        bootstrap_servers=redpanda.get_bootstrap_server(),
        topic=topic,
        topic_partitions=3,
        topic_replication_factor=1,
        topic_retention_ms=-1,
        consumer_group=f"group-{topic}",
        delivery_timeout_ms=10_000,
        publish_max_attempts=5,
        publish_backoff_initial_seconds=0.5,
        publish_backoff_max_seconds=2.0,
        admin_timeout_seconds=10.0,
    )


@pytest.fixture
def consumer_settings(tmp_path: Path) -> ConsumerSettings:
    return ConsumerSettings(
        batch_size=200,
        poll_timeout_seconds=0.5,
        db_max_attempts=3,
        db_backoff_initial_seconds=0.1,
        db_backoff_max_seconds=0.5,
        heartbeat_path=tmp_path / "heartbeat",
        quality_rules_path=RULES_PATH,
    )


@pytest.fixture(scope="session")
def snowflake_server() -> Iterator[dict[str, Any]]:
    with fakesnow.server() as connection_kwargs:
        yield connection_kwargs


@pytest.fixture
def snowflake_settings(snowflake_server: dict[str, Any]) -> SnowflakeSettings:
    """A fresh database per test on the shared emulator."""
    return SnowflakeSettings(
        account=snowflake_server["account"],
        user=snowflake_server["user"],
        password=snowflake_server["password"],
        role="SYSADMIN",
        warehouse="TEST_WH",
        database=f"TEST_{uuid.uuid4().hex[:10].upper()}",
        schema_name="RAW",
        login_timeout_seconds=10,
        network_timeout_seconds=30,
        host=snowflake_server["host"],
        port=snowflake_server["port"],
        protocol="http",
    )
