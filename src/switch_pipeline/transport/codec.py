"""The wire format of change events.

The envelope's JSON is the record value, the entity key is the record key, and
correlation ids travel as headers. Encoding cannot fail; decoding never raises:
anything unreadable becomes a SchemaViolation for the consumer to quarantine.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from uuid import UUID

from pydantic import JsonValue, ValidationError

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.domain.log import LogPosition

HeaderValue = str | bytes | None
RawHeaders = Mapping[str, HeaderValue] | Sequence[tuple[str, HeaderValue]]


@dataclass(frozen=True, slots=True)
class OutboundMessage:
    key: bytes
    value: bytes
    headers: list[tuple[str, HeaderValue]]


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """A consumed record, independent of the client library that read it."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: Mapping[str, str]

    @property
    def position(self) -> LogPosition:
        return LogPosition(topic=self.topic, partition=self.partition, offset=self.offset)


@dataclass(frozen=True, slots=True)
class SchemaViolation:
    errors: tuple[dict[str, JsonValue], ...]
    # Best-effort correlation recovered from headers/key, since the body is unusable.
    event_id: UUID | None
    batch_id: UUID | None
    entity_key: str | None


def encode(event: ChangeEvent) -> OutboundMessage:
    return OutboundMessage(
        key=event.entity_key.encode("utf-8"),
        value=event.to_json_bytes(),
        headers=event_headers(event),
    )


def event_headers(event: ChangeEvent) -> list[tuple[str, HeaderValue]]:
    """Correlation ids travel as headers so they survive even an unparseable body."""
    return [
        ("event_id", str(event.event_id)),
        ("batch_id", str(event.batch_id)),
        ("event_type", event.event_type.value),
        ("entity_type", event.entity_type),
        ("schema_version", str(event.schema_version)),
        ("content_type", "application/json"),
    ]


def decode_headers(raw: RawHeaders | None) -> dict[str, str]:
    items = raw.items() if isinstance(raw, Mapping) else raw or ()
    decoded: dict[str, str] = {}
    for name, value in items:
        if isinstance(value, bytes):
            decoded[name] = value.decode("utf-8", "replace")
        elif value is not None:
            decoded[name] = value
    return decoded


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
        entity_key=None if message.key is None else message.key.decode("utf-8", "replace"),
    )


def _uuid_or_none(value: str | None) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(value)
    except ValueError:
        return None
