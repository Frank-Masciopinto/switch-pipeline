"""Test data builders shared by unit and integration tests."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from pydantic import JsonValue
from pydantic_settings import BaseSettings

from switch_pipeline.domain.envelope import ChangeEvent, EventType, SourceRef
from switch_pipeline.settings import (
    AdapterSettings,
    ApiSettings,
    ConsumerSettings,
    KafkaSettings,
    LogSettings,
    PostgresSettings,
    SeedSettings,
    SimulateSettings,
    SnowflakeSettings,
    SourceSettings,
)
from switch_pipeline.transport.codec import InboundMessage

REPO_ROOT = Path(__file__).resolve().parents[1]
SETTINGS_GROUPS: tuple[type[BaseSettings], ...] = (
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
T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
SOURCE = SourceRef(system="snowflake", object="SWITCH_DEMO.RAW.CUSTOMER_ORDERS")


def order_payload(key: int, **overrides: JsonValue) -> dict[str, JsonValue]:
    payload: dict[str, JsonValue] = {
        "o_orderkey": key,
        "o_custkey": 7,
        "o_orderstatus": "O",
        "o_totalprice": "1234.50",
        "o_orderdate": "1996-03-01",
        "o_comment": "a comment",
    }
    payload.update(overrides)
    return payload


def make_event(
    *,
    key: int = 1,
    version: int = 1,
    entity_type: str = "order",
    payload: dict[str, JsonValue] | None = None,
    occurred_at: datetime | None = None,
    captured_at: datetime | None = None,
    batch_id: UUID | None = None,
) -> ChangeEvent:
    occurred = occurred_at or T0 + timedelta(seconds=version)
    return ChangeEvent.capture(
        event_type=EventType.INSERT if version == 1 else EventType.UPDATE,
        source=SOURCE,
        entity_type=entity_type,
        entity_key=str(key),
        entity_version=version,
        occurred_at=occurred,
        captured_at=captured_at or occurred + timedelta(seconds=5),
        batch_id=batch_id or uuid4(),
        payload=payload if payload is not None else order_payload(key, o_comment=f"v{version}"),
    )


def make_message(
    value: bytes | None,
    *,
    offset: int = 0,
    partition: int = 0,
    key: bytes | None = b"1",
    headers: dict[str, str] | None = None,
    topic: str = "test-topic",
) -> InboundMessage:
    return InboundMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        value=value,
        headers=headers or {},
    )


def message_for(event: ChangeEvent, *, offset: int = 0, partition: int = 0) -> InboundMessage:
    return make_message(
        event.to_json_bytes(),
        offset=offset,
        partition=partition,
        key=event.entity_key.encode(),
        headers={"event_id": str(event.event_id), "batch_id": str(event.batch_id)},
    )


def settings_variables() -> set[str]:
    """Every environment variable a settings group reads."""
    names: set[str] = set()
    for group in SETTINGS_GROUPS:
        prefix = str(group.model_config.get("env_prefix", ""))
        for field_name, field in group.model_fields.items():
            alias = field.validation_alias
            names.add(alias if isinstance(alias, str) else f"{prefix}{field_name}".upper())
    return names


def logged(output: str, event: str) -> dict[str, Any]:
    """The last JSON log line for ``event`` in captured output."""
    records: list[dict[str, Any]] = [
        json.loads(line) for line in output.splitlines() if line.startswith("{")
    ]
    matching = [record for record in records if record.get("event") == event]
    assert matching, f"no {event!r} line in the output"
    return matching[-1]


def exception_types(record: dict[str, Any]) -> set[str]:
    """Every exception type in a logged traceback, causes included."""
    return {stack["exc_type"] for stack in record["exception"]}


def read_env_example() -> dict[str, str]:
    """Active (uncommented) assignments of .env.example."""
    values: dict[str, str] = {}
    for line in (REPO_ROOT / ".env.example").read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            name, _, value = stripped.partition("=")
            values[name] = value
    return values


def rule(check: str, *, field: str = "f", severity: str = "reject", **extra: Any) -> dict[str, Any]:
    return {
        "name": f"rule_{check}",
        "description": f"{check} check",
        "field": field,
        "severity": severity,
        "check": check,
        **extra,
    }
