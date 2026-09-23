"""Turns a captured source row into a change-event envelope."""

import base64
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import JsonValue

from switch_pipeline.adapter.source import SourceRow
from switch_pipeline.domain.envelope import ChangeEvent, EventType, SourceRef


class EventMapper:
    def __init__(self, *, source: SourceRef, entity_type: str) -> None:
        self._source = source
        self._entity_type = entity_type

    def to_event(self, row: SourceRow, *, batch_id: UUID, captured_at: datetime) -> ChangeEvent:
        # The source contract starts versions at 1 and increments on every write,
        # so version 1 is the insert. A row updated before its first capture is
        # emitted as an update; consumers upsert either way.
        event_type = EventType.INSERT if row.version == 1 else EventType.UPDATE
        return ChangeEvent.capture(
            event_type=event_type,
            source=self._source,
            entity_type=self._entity_type,
            entity_key=str(row.key),
            entity_version=row.version,
            occurred_at=row.updated_at,
            captured_at=captured_at,
            batch_id=batch_id,
            payload=to_payload(row.values),
        )


def to_payload(values: Mapping[str, Any]) -> dict[str, JsonValue]:
    return {name.lower(): to_json_value(value) for name, value in values.items()}


def to_json_value(value: object) -> JsonValue:
    """Map Snowflake connector values to JSON without losing precision."""
    match value:
        case None | bool() | int() | str():
            return value
        case float():
            return value if math.isfinite(value) else str(value)
        case Decimal():
            # Money must not go through binary floating point; fixed-point text
            # keeps e.g. NUMBER(12,2) exact and avoids exponent notation.
            return format(value, "f")
        case datetime():
            # TIMESTAMP_NTZ columns hold UTC wall time by contract.
            aware = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            return aware.isoformat()
        case date() | time():
            return value.isoformat()
        case bytes() | bytearray():
            return base64.b64encode(value).decode("ascii")
        case _:
            raise TypeError(f"unsupported source value type: {type(value).__name__}")
