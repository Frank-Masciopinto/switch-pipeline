"""Typed configuration: the only module that reads the environment.

Values live exclusively in ``.env`` (template: ``.env.example``). No tunable
has a default here, so a missing variable fails fast at startup instead of
silently falling back to a value hidden in code. Each service loads only the
groups it needs, so e.g. the API never requires Snowflake credentials.
"""

from pathlib import Path
from typing import Annotated, Literal, TypeVar

from psycopg.conninfo import make_conninfo
from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SNOWFLAKE_IDENTIFIER = r"^[A-Za-z_][A-Za-z0-9_$]*$"

Identifier = Annotated[str, Field(pattern=SNOWFLAKE_IDENTIFIER, max_length=255)]
PositiveInt = Annotated[int, Field(gt=0)]
PositiveFloat = Annotated[float, Field(gt=0)]
Port = Annotated[int, Field(ge=1, le=65535)]


class ConfigurationError(Exception):
    """Required settings are missing or invalid."""


class _EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
        frozen=True,
        populate_by_name=True,
    )


class SnowflakeSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_")

    account: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password: SecretStr | None = None
    private_key_path: Path | None = None
    private_key_passphrase: SecretStr | None = None
    role: Identifier
    warehouse: Identifier
    database: Identifier
    schema_name: Identifier = Field(validation_alias="SNOWFLAKE_SCHEMA")
    login_timeout_seconds: PositiveInt
    network_timeout_seconds: PositiveInt
    # Only set when targeting the local emulator instead of Snowflake itself.
    host: str | None = None
    port: Port | None = None
    protocol: Literal["https", "http"] | None = None

    @model_validator(mode="after")
    def _exactly_one_auth_method(self) -> "SnowflakeSettings":
        if (self.password is None) == (self.private_key_path is None):
            raise ValueError("set exactly one of SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
        return self


class SourceSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="SOURCE_")

    table: Identifier
    entity_type: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    key_column: Identifier
    version_column: Identifier
    updated_at_column: Identifier


class AdapterSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="ADAPTER_")

    batch_size: Annotated[int, Field(gt=0, le=100_000)]
    poll_interval_seconds: PositiveFloat
    settle_seconds: Annotated[int, Field(ge=0)]
    retry_backoff_initial_seconds: PositiveFloat
    retry_backoff_max_seconds: PositiveFloat
    heartbeat_path: Path


class KafkaSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = Field(min_length=1)
    topic: str = Field(pattern=r"^[A-Za-z0-9._-]{1,249}$")
    topic_partitions: PositiveInt
    topic_replication_factor: PositiveInt
    topic_retention_ms: Annotated[int, Field(ge=-1)]
    consumer_group: str = Field(min_length=1)
    delivery_timeout_ms: Annotated[int, Field(ge=1_000)]
    publish_max_attempts: PositiveInt
    publish_backoff_initial_seconds: PositiveFloat
    publish_backoff_max_seconds: PositiveFloat
    admin_timeout_seconds: PositiveFloat


class ConsumerSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="CONSUMER_")

    batch_size: Annotated[int, Field(gt=0, le=10_000)]
    poll_timeout_seconds: PositiveFloat
    db_max_attempts: PositiveInt
    db_backoff_initial_seconds: PositiveFloat
    db_backoff_max_seconds: PositiveFloat
    heartbeat_path: Path
    quality_rules_path: Path = Field(validation_alias="QUALITY_RULES_PATH")


class PostgresSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="POSTGRES_")

    host: str = Field(min_length=1)
    port: Port
    db: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password: SecretStr
    connect_timeout_seconds: PositiveInt
    pool_min_size: PositiveInt
    pool_max_size: PositiveInt

    def conninfo(self, *, application_name: str, read_only: bool = False) -> str:
        options = {"options": "-c default_transaction_read_only=on"} if read_only else {}
        return make_conninfo(
            host=self.host,
            port=self.port,
            dbname=self.db,
            user=self.user,
            password=self.password.get_secret_value(),
            connect_timeout=self.connect_timeout_seconds,
            application_name=application_name,
            **options,
        )


class ApiSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="API_")

    host: str = Field(min_length=1)
    port: Port
    page_size_default: PositiveInt
    page_size_max: PositiveInt
    entity_history_limit: PositiveInt
    lag_sample_size: PositiveInt
    # Optional: when set, every data endpoint requires "Authorization: Bearer <token>".
    auth_token: SecretStr | None = Field(default=None, min_length=16)

    @model_validator(mode="after")
    def _default_page_fits(self) -> "ApiSettings":
        if self.page_size_default > self.page_size_max:
            raise ValueError("API_PAGE_SIZE_DEFAULT must not exceed API_PAGE_SIZE_MAX")
        return self


class LogSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="LOG_")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]
    format: Literal["json", "console"]

    @field_validator("level", mode="before")
    @classmethod
    def _upper(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value


class SeedSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="SEED_")

    strategy: Literal["sample_share", "synthetic"]
    sample_schema: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*$")
    row_count: Annotated[int, Field(gt=0, le=1_000_000)]


class SimulateSettings(_EnvSettings):
    model_config = SettingsConfigDict(env_prefix="SIMULATE_")

    inserts: Annotated[int, Field(ge=0, le=100_000)]
    updates: Annotated[int, Field(ge=0, le=100_000)]
    invalid_rows: Annotated[int, Field(ge=0, le=1_000)]


SettingsT = TypeVar("SettingsT", bound=BaseSettings)


def load_settings(settings_class: type[SettingsT]) -> SettingsT:
    """Instantiate a settings group from the environment, or explain what is wrong."""
    try:
        return settings_class()
    except ValidationError as exc:
        raise ConfigurationError(_describe(settings_class, exc)) from None


def _describe(settings_class: type[BaseSettings], exc: ValidationError) -> str:
    prefix = str(settings_class.model_config.get("env_prefix", ""))
    problems = []
    for error in exc.errors(include_url=False, include_input=False):
        location = error["loc"]
        if not location:
            variable = settings_class.__name__
        else:
            name = str(location[0])
            field = settings_class.model_fields.get(name)
            alias = field.validation_alias if field is not None else None
            if isinstance(alias, str):
                variable = alias
            elif name.isupper():
                variable = name
            else:
                variable = f"{prefix}{name}".upper()
        problems.append(f"  - {variable}: {error['msg']}")
    return "invalid configuration (edit .env; see .env.example):\n" + "\n".join(problems)
