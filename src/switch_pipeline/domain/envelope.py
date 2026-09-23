"""The change-event envelope: the contract between the adapter and every consumer.

The envelope is strict (unknown fields are rejected) and explicitly versioned:
any change to it bumps ``schema_version``, and consumers quarantine versions
they do not understand instead of guessing. Row-level schema drift does not
touch the envelope because ``payload`` carries the source row as-is.
"""

import hashlib
import json
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

SCHEMA_VERSION: Final = 1
ENTITY_TYPE_PATTERN: Final = r"^[a-z][a-z0-9_]*$"

# Changing this namespace would change every event id and break deduplication.
EVENT_ID_NAMESPACE: Final = uuid.UUID("5b0f2b8e-4f7c-4c1e-9a53-2f0d8f3c6a17")

# Content that defines "the same change": captured_at and batch_id differ when a
# change is re-emitted after a crash, so they are excluded from the fingerprint.
_FINGERPRINT_FIELDS: Final = frozenset(
    {
        "event_type",
        "source",
        "entity_type",
        "entity_key",
        "entity_version",
        "occurred_at",
        "payload",
    }
)


class EventType(StrEnum):
    INSERT = "insert"
    UPDATE = "update"


class SourceRef(BaseModel):
    """Where the change was captured."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    system: str = Field(min_length=1, description="Source system kind, e.g. 'snowflake'.")
    object: str = Field(
        min_length=1, description="Fully qualified source object, e.g. 'DB.SCHEMA.TABLE'."
    )


class ChangeEvent(BaseModel):
    """One captured row change (envelope schema version 1)."""

    model_config = ConfigDict(frozen=True, extra="forbid", title="ChangeEvent")

    schema_version: Literal[1] = Field(description="Envelope version; bumped on any change.")
    event_id: uuid.UUID = Field(
        description=(
            "UUIDv5 of (source, entity_type, entity_key, entity_version). Deterministic, so a "
            "change re-emitted after a crash keeps its id and is deduplicated downstream."
        )
    )
    event_type: EventType
    source: SourceRef
    entity_type: str = Field(
        pattern=ENTITY_TYPE_PATTERN, description="Logical entity, e.g. 'order'."
    )
    entity_key: str = Field(min_length=1, max_length=512, description="Business key; Kafka key.")
    entity_version: int = Field(
        ge=1, le=2**63 - 1, description="Per-entity version; increases on every source write."
    )
    occurred_at: AwareDatetime = Field(description="When the change happened at the source (UTC).")
    captured_at: AwareDatetime = Field(description="When the adapter read the change (UTC).")
    batch_id: uuid.UUID = Field(description="Adapter batch that captured the change (correlation).")
    payload: dict[str, JsonValue] = Field(description="The source row, JSON-typed.")

    @field_validator("occurred_at", "captured_at")
    @classmethod
    def _normalize_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _event_id_is_deterministic(self) -> Self:
        expected = derive_event_id(
            self.source, self.entity_type, self.entity_key, self.entity_version
        )
        if self.event_id != expected:
            raise ValueError(
                "event_id must be derived from (source, entity_type, entity_key, entity_version)"
            )
        return self

    @classmethod
    def capture(
        cls,
        *,
        event_type: EventType,
        source: SourceRef,
        entity_type: str,
        entity_key: str,
        entity_version: int,
        occurred_at: datetime,
        captured_at: datetime,
        batch_id: uuid.UUID,
        payload: dict[str, JsonValue],
    ) -> Self:
        return cls(
            schema_version=SCHEMA_VERSION,
            event_id=derive_event_id(source, entity_type, entity_key, entity_version),
            event_type=event_type,
            source=source,
            entity_type=entity_type,
            entity_key=entity_key,
            entity_version=entity_version,
            occurred_at=occurred_at,
            captured_at=captured_at,
            batch_id=batch_id,
            payload=payload,
        )

    def fingerprint(self) -> str:
        """SHA-256 of the canonical JSON of the fields that define the change."""
        content = self.model_dump(mode="json", include=set(_FINGERPRINT_FIELDS))
        canonical = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


def derive_event_id(
    source: SourceRef, entity_type: str, entity_key: str, version: int
) -> uuid.UUID:
    name = "|".join((source.system, source.object, entity_type, entity_key, str(version)))
    return uuid.uuid5(EVENT_ID_NAMESPACE, name)


def envelope_json_schema() -> dict[str, JsonValue]:
    schema: dict[str, JsonValue] = ChangeEvent.model_json_schema(mode="validation")
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = f"https://switch.example/schemas/change-event.v{SCHEMA_VERSION}.json"
    return schema
