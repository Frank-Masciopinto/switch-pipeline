"""Configuration lives in exactly one place: these tests keep .env.example, the
settings classes and docker-compose.yml consistent with each other."""

import re
from pathlib import Path

import pytest
from pydantic_settings import BaseSettings

from switch_pipeline.settings import (
    AdapterSettings,
    ApiSettings,
    ConfigurationError,
    ConsumerSettings,
    KafkaSettings,
    LogSettings,
    PostgresSettings,
    SeedSettings,
    SimulateSettings,
    SnowflakeSettings,
    SourceSettings,
    load_settings,
)
from tests.helpers import REPO_ROOT, read_env_example

GROUPS: tuple[type[BaseSettings], ...] = (
    SnowflakeSettings,
    SourceSettings,
    AdapterSettings,
    KafkaSettings,
    ConsumerSettings,
    PostgresSettings,
    ApiSettings,
    LogSettings,
    SeedSettings,
    SimulateSettings,
)


def settings_variables() -> set[str]:
    names: set[str] = set()
    for group in GROUPS:
        prefix = str(group.model_config.get("env_prefix", ""))
        for field_name, field in group.model_fields.items():
            alias = field.validation_alias
            names.add(alias if isinstance(alias, str) else f"{prefix}{field_name}".upper())
    return names


@pytest.fixture
def isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """No inherited variables and no .env file in the working directory."""
    for name in settings_variables():
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_env_example_configures_every_settings_group(
    isolated_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (isolated_env / ".env").write_text((REPO_ROOT / ".env.example").read_text())
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myorg-myaccount")  # the one value users must fill in
    for group in GROUPS:
        load_settings(group)


def test_every_settings_field_has_an_entry_in_env_example() -> None:
    missing = settings_variables() - read_env_example().keys()
    assert not missing, f"add to .env.example: {sorted(missing)}"


def test_every_env_example_entry_is_used() -> None:
    compose = (REPO_ROOT / "docker-compose.yml").read_text()
    compose_variables = set(re.findall(r"\$\{([A-Z0-9_]+)", compose))
    compose_variables |= {"COMPOSE_PROJECT_NAME", "COMPOSE_PROFILES"}  # read by compose itself
    unused = read_env_example().keys() - settings_variables() - compose_variables
    assert not unused, f"stale variables in .env.example: {sorted(unused)}"


def test_missing_variables_are_reported_by_their_env_name(isolated_env: Path) -> None:
    with pytest.raises(ConfigurationError) as caught:
        load_settings(KafkaSettings)
    assert "KAFKA_TOPIC: Field required" in str(caught.value)
    assert "KAFKA_BOOTSTRAP_SERVERS: Field required" in str(caught.value)


def test_snowflake_needs_exactly_one_authentication_method(
    isolated_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = {
        name: value
        for name, value in read_env_example().items()
        if name.startswith("SNOWFLAKE_") and value
    }
    for name, value in base.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myorg-myaccount")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "also-a-password")
    with pytest.raises(ConfigurationError, match="exactly one"):
        load_settings(SnowflakeSettings)
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    assert load_settings(SnowflakeSettings).password is not None


def test_api_connections_are_read_only(isolated_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in read_env_example().items():
        if name.startswith("POSTGRES_"):
            monkeypatch.setenv(name, value)
    postgres = load_settings(PostgresSettings)
    assert "default_transaction_read_only=on" in postgres.conninfo(
        application_name="api", read_only=True
    )
    assert "read_only" not in postgres.conninfo(application_name="consumer")
