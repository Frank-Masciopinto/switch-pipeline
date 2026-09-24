"""Publishes deliberately bad records to the topic to exercise the consumer's checks.

The adapter cannot produce these (it builds envelopes through the validated
model), but a consumer must never trust its input: other producers, bugs and
replays of old data all happen in practice.
"""

import json
import uuid
from typing import Any

import psycopg
from confluent_kafka import Producer
from psycopg.rows import dict_row

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.observability import get_logger
from switch_pipeline.settings import KafkaSettings, PostgresSettings
from switch_pipeline.transport.codec import HeaderValue, event_headers
from switch_pipeline.transport.config import producer_config

log = get_logger(__name__)

_LATEST_EVENT = """
SELECT event_id, event_type, schema_version, source, entity_type, entity_key, entity_version,
       payload, occurred_at, captured_at, batch_id
FROM event_log ORDER BY log_seq DESC LIMIT 1
"""


def inject_bad_events(kafka: KafkaSettings, postgres: PostgresSettings) -> list[dict[str, Any]]:
    with psycopg.connect(postgres.conninfo(application_name="switch-inject")) as conn:
        row = conn.cursor(row_factory=dict_row).execute(_LATEST_EVENT).fetchone()
    records: list[tuple[str, str, bytes, list[tuple[str, HeaderValue]]]] = [
        ("malformed_json", "garbage", b'{"event_id": "not closed', []),
        (
            "missing_required_fields",
            "partial",
            json.dumps({"schema_version": 1, "event_type": "insert"}).encode(),
            [],
        ),
    ]
    if row is None:
        log.warning("no_logged_event_to_copy", note="run a sync first for the duplicate cases")
    else:
        event = ChangeEvent.model_validate(row)
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
