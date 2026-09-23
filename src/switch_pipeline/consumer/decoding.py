"""Raw Kafka records -> validated envelopes, or schema violations to quarantine."""

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

from confluent_kafka import Message
from pydantic import JsonValue, ValidationError

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.kafka import decode_headers


@dataclass(frozen=True, slots=True)
class InboundMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: Mapping[str, str]

    @classmethod
    def from_kafka(cls, record: Message) -> "InboundMessage":
        topic, partition, offset = record.topic(), record.partition(), record.offset()
        if topic is None or partition is None or offset is None:
            raise ValueError("consumed record has no topic/partition/offset")
        return cls(
            topic=topic,
            partition=partition,
            offset=offset,
            key=record.key(),
            value=record.value(),
            headers=decode_headers(record.headers()),
        )


def storable_text(raw: bytes | None) -> str | None:
    """Bytes as text PostgreSQL accepts: TEXT rejects NUL and invalid UTF-8."""
    if raw is None:
        return None
    return raw.decode("utf-8", "replace").replace("\x00", "\ufffd")


@dataclass(frozen=True, slots=True)
class SchemaViolation:
    errors: tuple[dict[str, JsonValue], ...]
    # Best-effort correlation recovered from headers/key, since the body is unusable.
    event_id: UUID | None
    batch_id: UUID | None
    entity_key: str | None


def decode(message: InboundMessage) -> ChangeEvent | SchemaViolation:
    if message.value is None:
        return _violation(
            message, [{"type": "missing_value", "loc": "", "msg": "record has no value"}]
        )
    try:
        return ChangeEvent.model_validate_json(message.value)
    except ValidationError as exc:
        errors: list[dict[str, JsonValue]] = [
            {
                "type": error["type"],
                "loc": ".".join(str(part) for part in error["loc"]),
                "msg": error["msg"],
            }
            for error in exc.errors(include_url=False, include_input=False, include_context=False)
        ]
        return _violation(message, errors)


def _violation(message: InboundMessage, errors: list[dict[str, JsonValue]]) -> SchemaViolation:
    return SchemaViolation(
        errors=tuple(errors),
        event_id=_uuid_or_none(message.headers.get("event_id")),
        batch_id=_uuid_or_none(message.headers.get("batch_id")),
        entity_key=storable_text(message.key),
    )


def _uuid_or_none(value: str | None) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(value)
    except ValueError:
        return None
