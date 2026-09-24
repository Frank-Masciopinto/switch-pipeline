import json
from uuid import uuid4

from switch_pipeline.consumer.repository import storable_text
from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.transport.codec import SchemaViolation, decode
from tests.helpers import make_event, make_message, message_for


def test_a_valid_record_decodes_to_the_envelope() -> None:
    event = make_event()
    assert decode(message_for(event)) == event


def test_a_record_without_value_is_a_violation() -> None:
    result = decode(make_message(None))
    assert isinstance(result, SchemaViolation)
    assert result.errors[0]["type"] == "missing_value"


def test_malformed_json_is_a_violation() -> None:
    result = decode(make_message(b'{"event_id": "not closed'))
    assert isinstance(result, SchemaViolation)
    assert result.errors[0]["type"] == "json_invalid"


def test_invalid_utf8_is_a_violation() -> None:
    assert isinstance(decode(make_message(b"\xff\xfe{}")), SchemaViolation)


def test_violations_name_the_fields_but_never_echo_values() -> None:
    document = make_event().model_dump(mode="json")
    document["entity_version"] = "secret-looking-value"
    del document["payload"]
    result = decode(make_message(json.dumps(document).encode()))
    assert isinstance(result, SchemaViolation)
    assert {error["loc"] for error in result.errors} == {"entity_version", "payload"}
    assert "secret-looking-value" not in json.dumps(result.errors)


def test_correlation_ids_are_recovered_from_headers_and_key() -> None:
    event_id, batch_id = uuid4(), uuid4()
    result = decode(
        make_message(
            b"garbage",
            key=b"order-9",
            headers={"event_id": str(event_id), "batch_id": str(batch_id)},
        )
    )
    assert isinstance(result, SchemaViolation)
    assert (result.event_id, result.batch_id, result.entity_key) == (event_id, batch_id, "order-9")


def test_unparseable_correlation_headers_are_ignored() -> None:
    result = decode(make_message(b"garbage", headers={"event_id": "nope"}))
    assert isinstance(result, SchemaViolation)
    assert result.event_id is None


def test_storable_text_replaces_bytes_postgres_cannot_hold() -> None:
    assert storable_text(b"a\x00b\xffc") == "a\ufffdb\ufffdc"
    assert storable_text(None) is None


def test_decoded_events_keep_their_type() -> None:
    assert isinstance(decode(message_for(make_event(version=2))), ChangeEvent)
