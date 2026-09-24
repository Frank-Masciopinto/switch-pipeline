"""Long-running and operator paths: the live consumer loop, database outages,
replay, bad-event injection and topic provisioning."""

import threading
import time
from collections import Counter
from collections.abc import Callable
from contextlib import AbstractContextManager

import psycopg
import pytest
from confluent_kafka import Producer
from structlog.testing import capture_logs

from switch_pipeline.consumer.ports import Sink, SinkWriter
from switch_pipeline.consumer.processor import EventProcessor, Outcome
from switch_pipeline.consumer.runner import CatchUpReport, ConsumerRunner
from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.errors import DatabaseUnavailableError, FatalPipelineError
from switch_pipeline.lifecycle import Heartbeat, Shutdown
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import ConsumerSettings, KafkaSettings, PostgresSettings
from switch_pipeline.sink.store import PostgresSink
from switch_pipeline.tools.inject import inject_bad_events
from switch_pipeline.tools.replay import replay_topic
from switch_pipeline.transport.admin import TopicAdmin
from switch_pipeline.transport.config import producer_config
from switch_pipeline.transport.consumer import KafkaStreams
from switch_pipeline.transport.lag import ConsumerLagInspector
from switch_pipeline.transport.producer import KafkaEventPublisher
from tests.helpers import make_event
from tests.integration.conftest import RULES_PATH


def publish(kafka: KafkaSettings, events: list[ChangeEvent]) -> None:
    publisher = KafkaEventPublisher(kafka, client_id="tests", sleep=Shutdown().sleep)
    try:
        publisher.publish(events)
    finally:
        publisher.close()


def runner(
    consumer: ConsumerSettings, sink: Sink, shutdown: Shutdown | None = None
) -> ConsumerRunner:
    return ConsumerRunner(
        sink=sink,
        processor=EventProcessor(load_rules(RULES_PATH)),
        settings=consumer,
        shutdown=shutdown or Shutdown(),
        heartbeat=Heartbeat(consumer.heartbeat_path),
    )


def catch_up(kafka: KafkaSettings, consumer: ConsumerRunner) -> CatchUpReport:
    with KafkaStreams(kafka, client_id="tests").bounded(from_beginning=False) as stream:
        return consumer.catch_up(stream)


def group_lag(kafka: KafkaSettings) -> int | None:
    inspector = ConsumerLagInspector(kafka)
    try:
        lag = inspector.snapshot()
    finally:
        inspector.close()
    assert lag.error is None
    return lag.total


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


class OutageSink:
    """The real sink behind a database outage lasting ``failed_transactions`` attempts."""

    def __init__(self, sink: PostgresSink, *, failed_transactions: int) -> None:
        self._sink = sink
        self._failures_left = failed_transactions

    def transaction(self) -> AbstractContextManager[SinkWriter]:
        if self._failures_left:
            self._failures_left -= 1
            raise DatabaseUnavailableError("connection refused")
        return self._sink.transaction()


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
    topic: KafkaSettings, consumer_settings: ConsumerSettings, sink: PostgresSink, db: str
) -> None:
    publish(topic, [make_event(key=key) for key in range(1, 11)])
    shutdown = Shutdown()

    def consume() -> None:
        with KafkaStreams(topic, client_id="tests").live() as stream:
            runner(consumer_settings, sink, shutdown).run(stream)

    worker = threading.Thread(target=consume)
    worker.start()
    try:
        wait_until(lambda: count(db, "event_log") == 10, timeout=60)
    finally:
        shutdown.request()
        worker.join(timeout=30)
    assert not worker.is_alive(), "the consumer must stop promptly on shutdown"
    assert consumer_settings.heartbeat_path.exists()
    assert group_lag(topic) == 0, "offsets are committed after the database"


def test_a_database_outage_within_the_retry_budget_loses_and_duplicates_nothing(
    topic: KafkaSettings, consumer_settings: ConsumerSettings, sink: PostgresSink, db: str
) -> None:
    publish(topic, [make_event(key=key) for key in range(1, 6)])
    retries = consumer_settings.db_max_attempts - 1
    outage = OutageSink(sink, failed_transactions=retries)
    with capture_logs() as logs:
        report = catch_up(topic, runner(consumer_settings, outage))
    assert report.outcomes == {Outcome.APPLIED: 5}
    assert [entry["event"] for entry in logs].count("sink_write_retry_scheduled") == retries
    assert count(db, "event_log") == 5
    assert group_lag(topic) == 0


def test_a_database_outage_beyond_the_retry_budget_stops_without_committing_offsets(
    topic: KafkaSettings, consumer_settings: ConsumerSettings, sink: PostgresSink, db: str
) -> None:
    publish(topic, [make_event(key=key) for key in range(1, 6)])
    outage = OutageSink(sink, failed_transactions=consumer_settings.db_max_attempts)
    with pytest.raises(DatabaseUnavailableError):
        catch_up(topic, runner(consumer_settings, outage))
    assert count(db, "event_log") == 0
    assert group_lag(topic) == 5, "a restarted consumer reads every record again"


def test_the_replay_tool_rebuilds_the_sink_and_verifies_convergence(
    topic: KafkaSettings,
    consumer_settings: ConsumerSettings,
    postgres_settings: PostgresSettings,
    sink: PostgresSink,
) -> None:
    publish(topic, [make_event(key=key, version=v) for key in range(1, 11) for v in (1, 2)])
    garbage = Producer(producer_config(topic, client_id="tests"))
    garbage.produce(topic.topic, value=b"not an envelope", key=b"x")
    garbage.flush(10)
    catch_up(topic, runner(consumer_settings, sink))

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
    sink: PostgresSink,
    db: str,
) -> None:
    publish(topic, [make_event(key=1)])
    consumer = runner(consumer_settings, sink)
    assert catch_up(topic, consumer).outcomes == {Outcome.APPLIED: 1}

    injected = inject_bad_events(topic, postgres_settings)
    assert [item["case"] for item in injected] == [
        "malformed_json",
        "missing_required_fields",
        "exact_duplicate",
        "event_id_conflict",
        "unsupported_schema_version",
        "non_deterministic_event_id",
    ]
    report = catch_up(topic, consumer)
    assert report.outcomes == {Outcome.QUARANTINED: 5, Outcome.DUPLICATE: 1}
    with psycopg.connect(db) as conn:
        reasons = Counter(row[0] for row in conn.execute("SELECT reason FROM quarantine"))
    assert reasons == {"schema_violation": 4, "event_id_conflict": 1}
