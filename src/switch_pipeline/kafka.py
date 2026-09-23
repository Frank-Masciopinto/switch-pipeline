"""Kafka client configuration and topic administration shared by every service."""

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.observability import get_logger
from switch_pipeline.settings import KafkaSettings

log = get_logger(__name__)

# librdkafka's own logs go through stdlib logging, hence end up as JSON too.
_CLIENT_LOGGER = logging.getLogger("switch_pipeline.librdkafka")


HeaderValue = str | bytes | None
RawHeaders = Mapping[str, HeaderValue] | Sequence[tuple[str, HeaderValue]]


def event_headers(event: ChangeEvent) -> list[tuple[str, HeaderValue]]:
    """Correlation ids travel as headers so they survive even an unparseable body."""
    return [
        ("event_id", str(event.event_id)),
        ("batch_id", str(event.batch_id)),
        ("event_type", event.event_type.value),
        ("entity_type", event.entity_type),
        ("schema_version", str(event.schema_version)),
        ("content_type", "application/json"),
    ]


def decode_headers(raw: RawHeaders | None) -> dict[str, str]:
    items = raw.items() if isinstance(raw, Mapping) else raw or ()
    decoded: dict[str, str] = {}
    for name, value in items:
        if isinstance(value, bytes):
            decoded[name] = value.decode("utf-8", "replace")
        elif value is not None:
            decoded[name] = value
    return decoded


def producer_config(settings: KafkaSettings, *, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        # The delivery guarantee depends on these, so they are not configurable:
        # every in-sync replica must persist a record before it counts as sent, and
        # the idempotent producer lets the broker drop duplicates caused by internal
        # retries while keeping per-partition order.
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "delivery.timeout.ms": settings.delivery_timeout_ms,
        "compression.type": "zstd",
        "logger": _CLIENT_LOGGER,
    }


def consumer_config(settings: KafkaSettings, *, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.consumer_group,
        "client.id": client_id,
        # Offsets are committed explicitly, and only after the database transaction
        # that materialized the records has committed (at-least-once processing).
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "logger": _CLIENT_LOGGER,
    }


class TopicAdmin:
    """Explicit topic provisioning: auto-creation is disabled on the broker, so a
    misspelt topic name fails loudly instead of creating a 1-partition topic."""

    def __init__(self, settings: KafkaSettings, *, client_id: str) -> None:
        self._settings = settings
        self._admin = AdminClient(
            {"bootstrap.servers": settings.bootstrap_servers, "client.id": client_id},
            logger=_CLIENT_LOGGER,
        )

    def ensure_topic(self) -> None:
        settings = self._settings
        existing = self._partition_count()
        if existing is not None:
            if existing != settings.topic_partitions:
                # Changing the partition count would remap keys to partitions and
                # break per-entity ordering for in-flight data; leave it to an operator.
                log.warning(
                    "topic_partition_count_differs",
                    topic=settings.topic,
                    existing=existing,
                    configured=settings.topic_partitions,
                )
            log.info("topic_exists", topic=settings.topic, partitions=existing)
            return
        topic = NewTopic(
            settings.topic,
            num_partitions=settings.topic_partitions,
            replication_factor=settings.topic_replication_factor,
            config={"retention.ms": str(settings.topic_retention_ms), "cleanup.policy": "delete"},
        )
        future = self._admin.create_topics(
            [topic], operation_timeout=settings.admin_timeout_seconds
        )[settings.topic]
        try:
            future.result(timeout=settings.admin_timeout_seconds)
        except KafkaException as exc:
            error: KafkaError = exc.args[0]
            if error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        log.info(
            "topic_created",
            topic=settings.topic,
            partitions=settings.topic_partitions,
            retention_ms=settings.topic_retention_ms,
        )

    def require_topic(self) -> int:
        partitions = self._partition_count()
        if partitions is None:
            raise FatalPipelineError(
                f"topic {self._settings.topic!r} does not exist; run `switch-pipeline init`"
            )
        return partitions

    def active_members(self, group_id: str) -> int:
        future = self._admin.describe_consumer_groups(
            [group_id], request_timeout=self._settings.admin_timeout_seconds
        )[group_id]
        description = future.result(timeout=self._settings.admin_timeout_seconds)
        return len(description.members)

    def _partition_count(self) -> int | None:
        metadata = self._admin.list_topics(timeout=self._settings.admin_timeout_seconds)
        topic = metadata.topics.get(self._settings.topic)
        if topic is None or topic.error is not None:
            return None
        return len(topic.partitions)
