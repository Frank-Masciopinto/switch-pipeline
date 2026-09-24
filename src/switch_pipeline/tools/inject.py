"""Publishes deliberately bad records to the topic to exercise the consumer's checks.

The adapter cannot produce these (it builds envelopes through the validated
model), but a consumer must never trust its input: other producers, bugs and
replays of old data all happen in practice.
"""

import json
import uuid
from contextlib import closing
from typing import Any

from switch_pipeline.observability import get_logger
from switch_pipeline.settings import KafkaSettings, PostgresSettings
from switch_pipeline.sink.store import PostgresSink
from switch_pipeline.transport.codec import OutboundMessage, event_headers
from switch_pipeline.transport.producer import publish_raw

log = get_logger(__name__)


def inject_bad_events(kafka: KafkaSettings, postgres: PostgresSettings) -> list[dict[str, Any]]:
    with closing(PostgresSink.open(postgres, application_name="switch-inject")) as sink:
        event = sink.latest_event()
    cases: list[tuple[str, OutboundMessage]] = [
        ("malformed_json", OutboundMessage(b"garbage", b'{"event_id": "not closed', [])),
        (
            "missing_required_fields",
            OutboundMessage(
                b"partial", json.dumps({"schema_version": 1, "event_type": "insert"}).encode(), []
            ),
        ),
    ]
    if event is None:
        log.warning("no_logged_event_to_copy", note="run a sync first for the duplicate cases")
    else:
        key, headers = event.entity_key.encode(), event_headers(event)
        document = event.model_dump(mode="json")
        tampered = event.model_copy(
            update={"payload": {**event.payload, "o_comment": "tampered after publication"}}
        )
        cases += [
            ("exact_duplicate", OutboundMessage(key, event.to_json_bytes(), headers)),
            ("event_id_conflict", OutboundMessage(key, tampered.to_json_bytes(), headers)),
            (
                "unsupported_schema_version",
                OutboundMessage(
                    key, json.dumps({**document, "schema_version": 2}).encode(), headers
                ),
            ),
            (
                "non_deterministic_event_id",
                OutboundMessage(
                    key, json.dumps({**document, "event_id": str(uuid.uuid4())}).encode(), headers
                ),
            ),
        ]

    publish_raw(kafka, [record for _, record in cases], client_id="switch-inject")
    injected = [{"case": case, "key": record.key.decode()} for case, record in cases]
    log.info("bad_events_injected", cases=[item["case"] for item in injected])
    return injected
