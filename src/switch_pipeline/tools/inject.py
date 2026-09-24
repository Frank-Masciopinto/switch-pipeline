"""Publishes deliberately bad records to the topic to exercise the consumer's checks.

The adapter cannot produce these (it builds envelopes through the validated
model), but a consumer must never trust its input: other producers, bugs and
replays of old data all happen in practice.
"""

import json
import uuid
from contextlib import closing
from typing import Any

from confluent_kafka import Producer

from switch_pipeline.observability import get_logger
from switch_pipeline.settings import KafkaSettings, PostgresSettings
from switch_pipeline.sink.store import PostgresSink
from switch_pipeline.transport.codec import HeaderValue, event_headers
from switch_pipeline.transport.config import producer_config

log = get_logger(__name__)


def inject_bad_events(kafka: KafkaSettings, postgres: PostgresSettings) -> list[dict[str, Any]]:
    with closing(PostgresSink.open(postgres, application_name="switch-inject")) as sink:
        event = sink.latest_event()
    records: list[tuple[str, str, bytes, list[tuple[str, HeaderValue]]]] = [
        ("malformed_json", "garbage", b'{"event_id": "not closed', []),
        (
            "missing_required_fields",
            "partial",
            json.dumps({"schema_version": 1, "event_type": "insert"}).encode(),
            [],
        ),
    ]
    if event is None:
        log.warning("no_logged_event_to_copy", note="run a sync first for the duplicate cases")
    else:
        headers = event_headers(event)
        document = event.model_dump(mode="json")
        tampered = event.model_copy(
            update={"payload": {**event.payload, "o_comment": "tampered after publication"}}
        )
        records += [
            ("exact_duplicate", event.entity_key, event.to_json_bytes(), headers),
            ("event_id_conflict", event.entity_key, tampered.to_json_bytes(), headers),
            (
                "unsupported_schema_version",
                event.entity_key,
                json.dumps({**document, "schema_version": 2}).encode(),
                headers,
            ),
            (
                "non_deterministic_event_id",
                event.entity_key,
                json.dumps({**document, "event_id": str(uuid.uuid4())}).encode(),
                headers,
            ),
        ]

    producer = Producer(producer_config(kafka, client_id="switch-inject"))
    for _, key, value, record_headers in records:
        producer.produce(kafka.topic, value=value, key=key.encode(), headers=record_headers)
    undelivered = producer.flush(kafka.delivery_timeout_ms / 1000)
    if undelivered:
        raise RuntimeError(f"{undelivered} injected records were not delivered")
    injected = [{"case": case, "key": key} for case, key, _, _ in records]
    log.info("bad_events_injected", cases=[item["case"] for item in injected])
    return injected
