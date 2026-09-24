"""The adapter tolerates a briefly unavailable broker without losing events."""

import threading
import time
import uuid

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from testcontainers.community.kafka import RedpandaContainer

from switch_pipeline.lifecycle import Shutdown
from switch_pipeline.settings import KafkaSettings
from switch_pipeline.transport.admin import TopicAdmin
from switch_pipeline.transport.producer import KafkaEventPublisher
from tests.helpers import make_event


def read_topic(settings: KafkaSettings) -> list[str]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"reader-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(settings.topic, timeout=10)
        partitions = sorted(metadata.topics[settings.topic].partitions)
        ends = {
            p: consumer.get_watermark_offsets(TopicPartition(settings.topic, p), timeout=10)[1]
            for p in partitions
        }
        consumer.assign([TopicPartition(settings.topic, p, OFFSET_BEGINNING) for p in partitions])
        keys: list[str] = []
        deadline = time.monotonic() + 30
        while sum(ends.values()) > len(keys) and time.monotonic() < deadline:
            records = consumer.consume(num_messages=500, timeout=1)
            keys.extend((r.key() or b"").decode() for r in records if r.error() is None)
        return keys
    finally:
        consumer.close()


def test_events_published_during_a_broker_outage_are_delivered_once_it_returns(
    redpanda: RedpandaContainer, kafka_settings: KafkaSettings
) -> None:
    # A delivery timeout shorter than the outage forces application-level re-sends.
    settings = kafka_settings.model_copy(
        update={"delivery_timeout_ms": 2_000, "publish_max_attempts": 10}
    )
    TopicAdmin(settings, client_id="tests").ensure_topic()
    publisher = KafkaEventPublisher(settings, client_id="tests", sleep=Shutdown().sleep)
    events = [make_event(key=key) for key in range(1, 41)]

    broker = redpanda.get_wrapped_container()
    broker.pause()
    resume = threading.Timer(6.0, broker.unpause)
    resume.start()
    started = time.monotonic()
    try:
        publisher.publish(events)
    finally:
        resume.join()
        publisher.close()

    assert time.monotonic() - started >= 5.0, "publish returned before the broker came back"
    delivered = read_topic(settings)
    assert set(delivered) == {event.entity_key for event in events}, "no event may be lost"
