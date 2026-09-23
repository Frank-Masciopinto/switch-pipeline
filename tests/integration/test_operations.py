"""Long-running and operator paths: the live consumer loop, replay, bad-event
injection and topic provisioning."""

import threading
import time
from collections import Counter
from collections.abc import Callable

import psycopg
import pytest
from confluent_kafka import Producer
from psycopg_pool import ConnectionPool

from switch_pipeline.adapter.publisher import KafkaEventPublisher
from switch_pipeline.api.kafka_lag import ConsumerLagInspector
from switch_pipeline.consumer.processor import EventProcessor, Outcome
from switch_pipeline.consumer.runner import ConsumerRunner
from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.kafka import TopicAdmin, producer_config
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import ConsumerSettings, KafkaSettings, PostgresSettings
from switch_pipeline.tools.inject import inject_bad_events
from switch_pipeline.tools.replay import replay_topic
from tests.helpers import make_event
from tests.integration.conftest import RULES_PATH


def publish(kafka: KafkaSettings, events: list[ChangeEvent]) -> None:
    publisher = KafkaEventPublisher(kafka, client_id="tests", sleep=Shutdown().sleep)
    try:
        publisher.publish(events)
    finally:
        publisher.close()


def runner(
    kafka: KafkaSettings,
    consumer: ConsumerSettings,
    pool: ConnectionPool,
    shutdown: Shutdown | None = None,
) -> ConsumerRunner:
    return ConsumerRunner(
        kafka=kafka,
        settings=consumer,
        pool=pool,
        processor=EventProcessor(load_rules(RULES_PATH)),
        shutdown=shutdown or Shutdown(),
        heartbeat=Heartbeat(consumer.heartbeat_path),
        client_id="tests",
    )


def count(db: str, table: str) -> int:
    with psycopg.connect(db) as conn:
        row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
    return int(row[0]) if row else 0


def wait_until(condition: Callable[[], bool], *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.5)


@pytest.fixture
def topic(kafka_settings: KafkaSettings) -> KafkaSettings:
    TopicAdmin(kafka_settings, client_id="tests").ensure_topic()
    return kafka_settings


def test_topic_provisioning_is_idempotent_and_required(kafka_settings: KafkaSettings) -> None:
    admin = TopicAdmin(kafka_settings, client_id="tests")
    with pytest.raises(FatalPipelineError, match="does not exist"):
        admin.require_topic()
    admin.ensure_topic()
    admin.ensure_topic()
    assert admin.require_topic() == kafka_settings.topic_partitions


def test_the_live_consumer_materializes_events_and_commits_offsets(
    topic: KafkaSettings, consumer_settings: ConsumerSettings, sink_pool: ConnectionPool, db: str
) -> None:
    publish(topic, [make_event(key=key) for key in range(1, 11)])
    shutdown = Shutdown()
    worker = threading.Thread(target=runner(topic, consumer_settings, sink_pool, shutdown).run)
    worker.start()
    try:
        wait_until(lambda: count(db, "event_log") == 10, timeout=60)
    finally:
        shutdown.request()
        worker.join(timeout=30)
    assert not worker.is_alive(), "the consumer must stop promptly on shutdown"
    assert consumer_settings.heartbeat_path.exists()

    inspector = ConsumerLagInspector(topic)
    try:
        lag = inspector.snapshot()
    finally:
        inspector.close()
    assert (lag.error, lag.total) == (None, 0), "offsets are committed after the database"


def test_the_replay_tool_rebuilds_the_sink_and_verifies_convergence(
    topic: KafkaSettings,
    consumer_settings: ConsumerSettings,
    postgres_settings: PostgresSettings,
    sink_pool: ConnectionPool,
    db: str,
) -> None:
    publish(topic, [make_event(key=key, version=v) for key in range(1, 11) for v in (1, 2)])
    garbage = Producer(producer_config(topic, client_id="tests"))
    garbage.produce(topic.topic, value=b"not an envelope", key=b"x")
    garbage.flush(10)
    runner(topic, consumer_settings, sink_pool).catch_up(from_beginning=False)

    report = replay_topic(
        topic, consumer_settings, postgres_settings, rebuild=True, force=False, shutdown=Shutdown()
    )
    assert report.converged
    assert report.outcomes == {"applied": 20, "quarantined": 1}
    assert report.before["state_checksum"] == report.after["state_checksum"]
    assert report.after["entities"] == 10


def test_injected_bad_records_are_each_quarantined_or_skipped(
    topic: KafkaSettings,
    consumer_settings: ConsumerSettings,
    postgres_settings: PostgresSettings,
    sink_pool: ConnectionPool,
    db: str,
) -> None:
    publish(topic, [make_event(key=1)])
    consumer = runner(topic, consumer_settings, sink_pool)
    assert consumer.catch_up(from_beginning=False).outcomes == {Outcome.APPLIED: 1}

    injected = inject_bad_events(topic, postgres_settings)
    assert [item["case"] for item in injected] == [
        "malformed_json",
        "missing_required_fields",
        "exact_duplicate",
        "event_id_conflict",
        "unsupported_schema_version",
        "non_deterministic_event_id",
    ]
    report = consumer.catch_up(from_beginning=False)
    assert report.outcomes == {Outcome.QUARANTINED: 5, Outcome.DUPLICATE: 1}
    with psycopg.connect(db) as conn:
        reasons = Counter(row[0] for row in conn.execute("SELECT reason FROM quarantine"))
    assert reasons == {"schema_violation": 4, "event_id_conflict": 1}
