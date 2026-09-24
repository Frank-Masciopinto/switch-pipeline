"""With its database down the API keeps serving and says so, instead of hanging or crashing."""

import socket
import time

from fastapi.testclient import TestClient

from switch_pipeline.api.app import create_app
from switch_pipeline.settings import ApiSettings, KafkaSettings, PostgresSettings


def unused_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_an_unreachable_database_makes_the_api_unready_and_data_requests_fail_fast() -> None:
    postgres = PostgresSettings(
        host="127.0.0.1",
        port=unused_port(),
        db="switch",
        user="switch",
        password="unused",  # type: ignore[arg-type]
        connect_timeout_seconds=1,
        pool_min_size=1,
        pool_max_size=2,
    )
    kafka = KafkaSettings(
        bootstrap_servers="127.0.0.1:1",
        topic="unused",
        topic_partitions=1,
        topic_replication_factor=1,
        topic_retention_ms=-1,
        consumer_group="unused",
        delivery_timeout_ms=1_000,
        publish_max_attempts=1,
        publish_backoff_initial_seconds=0.1,
        publish_backoff_max_seconds=0.1,
        admin_timeout_seconds=1.0,
    )
    api = ApiSettings(
        host="127.0.0.1",
        port=8000,
        page_size_default=10,
        page_size_max=10,
        entity_history_limit=10,
        lag_sample_size=10,
    )
    with TestClient(create_app(postgres=postgres, api=api, kafka=kafka)) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        ready = client.get("/readyz")
        assert (ready.status_code, ready.json()) == (503, {"status": "unavailable"})
        started = time.monotonic()
        events = client.get("/events")
        assert (events.status_code, events.json()) == (503, {"detail": "database unavailable"})
        assert time.monotonic() - started < 5, "bounded by POSTGRES_CONNECT_TIMEOUT_SECONDS"
