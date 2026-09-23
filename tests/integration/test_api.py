from collections.abc import Iterator
from datetime import timedelta
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from switch_pipeline.adapter.cursor import SyncCursor
from switch_pipeline.adapter.state import PostgresSyncStateStore, SyncMode
from switch_pipeline.api.app import create_app
from switch_pipeline.consumer.decoding import InboundMessage
from switch_pipeline.consumer.processor import EventProcessor
from switch_pipeline.kafka import TopicAdmin
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import ApiSettings, KafkaSettings, PostgresSettings
from tests.helpers import T0, make_event, make_message, message_for, order_payload
from tests.integration.conftest import RULES_PATH

API = ApiSettings(
    host="127.0.0.1",
    port=8000,
    page_size_default=50,
    page_size_max=100,
    entity_history_limit=10,
    lag_sample_size=100,
)


def process(db: str, messages: list[InboundMessage]) -> None:
    with psycopg.connect(db) as conn, conn.transaction():
        EventProcessor(load_rules(RULES_PATH)).process_batch(conn, messages)


@pytest.fixture
def client(
    db: str, postgres_settings: PostgresSettings, kafka_settings: KafkaSettings
) -> Iterator[TestClient]:
    TopicAdmin(kafka_settings, client_id="tests").ensure_topic()
    app = create_app(postgres=postgres_settings, api=API, kafka=kafka_settings)
    with TestClient(app) as test_client:
        yield test_client


def test_events_are_filterable_and_paginated_newest_first(client: TestClient, db: str) -> None:
    batch_id = uuid4()
    events = [
        make_event(key=key, version=version, batch_id=batch_id if key == 2 else None)
        for key in (1, 2)
        for version in (1, 2, 3)
    ]
    process(db, [message_for(event, offset=i) for i, event in enumerate(events)])

    first = client.get("/events", params={"limit": 4}).json()
    second = client.get("/events", params={"limit": 4, "cursor": first["next_cursor"]}).json()
    assert second["next_cursor"] is None
    listed = [item["event_id"] for item in first["items"] + second["items"]]
    assert listed == [str(event.event_id) for event in reversed(events)]

    by_key = client.get("/events", params={"entity_key": "1"}).json()["items"]
    assert {item["entity_key"] for item in by_key} == {"1"}
    inserts = client.get("/events", params={"event_type": "insert"}).json()["items"]
    assert [item["entity_version"] for item in inserts] == [1, 1]
    in_batch = client.get("/events", params={"batch_id": str(batch_id)}).json()["items"]
    assert {item["entity_key"] for item in in_batch} == {"2"}
    window = client.get(
        "/events",
        params={
            "occurred_after": (T0 + timedelta(seconds=2)).isoformat(),
            "occurred_before": (T0 + timedelta(seconds=3)).isoformat(),
        },
    ).json()["items"]
    assert {item["entity_version"] for item in window} == {2}
    assert first["items"][0]["kafka"]["topic"] == "test-topic"


def test_entity_view_combines_current_state_history_and_rejections(
    client: TestClient, db: str
) -> None:
    good = [make_event(key=9, version=1), make_event(key=9, version=2)]
    bad = make_event(key=9, version=3, payload=order_payload(9, o_orderstatus="X"))
    process(db, [message_for(event, offset=i) for i, event in enumerate([*good, bad])])

    view = client.get("/entities/9").json()
    assert view["current"]["entity_version"] == 2
    assert [item["entity_version"] for item in view["history"]] == [1, 2]
    assert [item["reason"] for item in view["quarantined"]] == ["quality_rule_failed"]
    assert client.get("/entities/404404").status_code == 404
    assert client.get(f"/events/{good[0].event_id}").json()["entity_version"] == 1
    assert client.get(f"/events/{uuid4()}").status_code == 404


def test_stats_report_counts_lag_watermark_and_checksums(client: TestClient, db: str) -> None:
    first = make_event(key=1, version=1)
    process(
        db,
        [
            message_for(first),
            message_for(make_event(key=1, version=2), offset=1),
            message_for(first, offset=2),  # redelivery
            make_message(b"garbage", offset=3),
        ],
    )
    store = PostgresSyncStateStore(db)
    store.open(timeout=10)
    batch_id = uuid4()
    store.begin_batch(
        batch_id=batch_id,
        source_id="snowflake:X",
        mode=SyncMode.FULL,
        cursor_start=None,
        upper_bound=T0,
    )
    store.commit_batch(
        batch_id=batch_id, source_id="snowflake:X", cursor_end=SyncCursor(T0, 42), row_count=2
    )
    store.close()

    assert client.get("/stats").json()["checksums"] is None
    stats = client.get("/stats", params={"checksums": "true"}).json()
    assert stats["events"] == {
        "total": 2,
        "by_type": {"insert": 1, "update": 1},
        "with_quality_warnings": 0,
    }
    assert stats["quarantine"] == {"total": 1, "by_reason": {"schema_violation": 1}}
    assert stats["duplicates_skipped"] == 1
    assert stats["lag_seconds"]["sample_size"] == 2
    assert stats["lag_seconds"]["occurred_to_processed"]["max"] > 0
    assert stats["watermarks"][0]["cursor_key"] == 42
    assert stats["recent_batches"][0]["status"] == "committed"
    assert stats["consumer_lag"]["error"] is None
    assert stats["checksums"]["entities"] == 1


@pytest.mark.parametrize(
    ("path", "status"),
    [
        ("/events?cursor=not-a-cursor", 400),
        ("/events?occurred_after=2026-01-01T00:00:00", 422),
        (
            "/events?occurred_after=2026-01-02T00:00:00Z&occurred_before=2026-01-01T00:00:00Z",
            422,
        ),
        ("/events?limit=101", 422),
        ("/events?batch_id=nope", 422),
        ("/events/not-a-uuid", 422),
        ("/quarantine?reason=unknown", 422),
    ],
)
def test_invalid_requests_are_rejected(client: TestClient, path: str, status: int) -> None:
    assert client.get(path).status_code == status


def test_data_endpoints_require_the_bearer_token_when_one_is_configured(
    db: str, postgres_settings: PostgresSettings, kafka_settings: KafkaSettings
) -> None:
    token = "a-long-enough-test-token"
    secured = API.model_copy(update={"auth_token": SecretStr(token)})
    app = create_app(postgres=postgres_settings, api=secured, kafka=kafka_settings)
    with TestClient(app) as client:
        denied = client.get("/events")
        assert (denied.status_code, denied.headers["www-authenticate"]) == (401, "Bearer")
        wrong = {"Authorization": "Bearer not-the-right-token"}
        assert client.get("/stats", headers=wrong).status_code == 401
        right = {"Authorization": f"Bearer {token}"}
        assert client.get("/events", headers=right).status_code == 200
        assert client.get("/quarantine", headers=right).status_code == 200
        assert client.get("/healthz").status_code == 200, "probes stay open"
        assert client.get("/readyz").status_code == 200


def test_health_endpoints_and_request_ids(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}
    ready = client.get("/readyz", headers={"x-request-id": "trace-me"})
    assert ready.json() == {"status": "ready"}
    assert ready.headers["x-request-id"] == "trace-me"
    assert client.get("/healthz").headers["x-request-id"]
