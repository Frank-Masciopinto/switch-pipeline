import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from switch_pipeline.domain.envelope import ChangeEvent, EventType, envelope_json_schema
from tests.helpers import REPO_ROOT, T0, make_event, order_payload

Document = dict[str, Any]


def test_event_id_is_derived_from_identity_so_re_emission_keeps_it() -> None:
    first = make_event(key=7, version=3)
    re_emitted = make_event(key=7, version=3, batch_id=uuid4(), captured_at=T0 + timedelta(hours=1))
    assert re_emitted.event_id == first.event_id
    assert make_event(key=7, version=4).event_id != first.event_id
    assert make_event(key=8, version=3).event_id != first.event_id


def test_round_trips_through_json() -> None:
    event = make_event()
    assert ChangeEvent.model_validate_json(event.to_json_bytes()) == event


def test_fingerprint_ignores_capture_metadata() -> None:
    event = make_event()
    later = make_event(batch_id=uuid4(), captured_at=event.captured_at + timedelta(minutes=5))
    assert later.fingerprint() == event.fingerprint()


def test_fingerprint_changes_when_the_change_itself_differs() -> None:
    event = make_event()
    assert (
        make_event(payload=order_payload(1, o_comment="edited")).fingerprint()
        != event.fingerprint()
    )
    assert make_event(occurred_at=T0 + timedelta(days=1)).fingerprint() != event.fingerprint()


def test_timestamps_are_normalized_to_utc() -> None:
    rome = timezone(timedelta(hours=2))
    event = make_event(occurred_at=datetime(2026, 1, 1, 14, tzinfo=rome))
    assert event.occurred_at == datetime(2026, 1, 1, 12, tzinfo=UTC)
    assert event.model_dump(mode="json")["occurred_at"] == "2026-01-01T12:00:00Z"


def test_event_type_follows_the_version() -> None:
    assert make_event(version=1).event_type is EventType.INSERT
    assert make_event(version=2).event_type is EventType.UPDATE


def _without(name: str) -> Callable[[Document], None]:
    return lambda document: document.pop(name)


def _set(name: str, value: object) -> Callable[[Document], None]:
    return lambda document: document.__setitem__(name, value)


@pytest.mark.parametrize(
    ("mutate", "location"),
    [
        (_without("event_type"), "event_type"),
        (_without("payload"), "payload"),
        (_set("event_type", "delete"), "event_type"),
        (_set("schema_version", 2), "schema_version"),
        (_set("occurred_at", "2026-01-01T12:00:00"), "occurred_at"),
        (_set("entity_key", ""), "entity_key"),
        (_set("entity_version", 0), "entity_version"),
        (_set("entity_type", "Order!"), "entity_type"),
        (_set("payload", [1, 2]), "payload"),
        (_set("unexpected", "field"), "unexpected"),
        (_set("event_id", str(uuid4())), ""),
    ],
    ids=[
        "missing-event-type",
        "missing-payload",
        "unknown-event-type",
        "unsupported-schema-version",
        "naive-timestamp",
        "empty-entity-key",
        "zero-version",
        "bad-entity-type",
        "payload-not-object",
        "unknown-field",
        "non-deterministic-event-id",
    ],
)
def test_invalid_envelopes_are_rejected(mutate: Callable[[Document], None], location: str) -> None:
    document = make_event().model_dump(mode="json")
    mutate(document)
    with pytest.raises(ValidationError) as caught:
        ChangeEvent.model_validate_json(json.dumps(document))
    locations = {".".join(str(part) for part in error["loc"]) for error in caught.value.errors()}
    assert any(found == location or found.startswith(f"{location}.") for found in locations)


def test_committed_json_schema_matches_the_model() -> None:
    committed = json.loads((REPO_ROOT / "schemas" / "change_event.v1.schema.json").read_text())
    assert committed == envelope_json_schema(), "run `make schema` to regenerate"
