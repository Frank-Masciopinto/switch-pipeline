"""Explicit topic provisioning and consumer-group inspection."""

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from switch_pipeline.errors import FatalPipelineError
from switch_pipeline.observability import get_logger
from switch_pipeline.settings import KafkaSettings
from switch_pipeline.transport.config import CLIENT_LOGGER

log = get_logger(__name__)


class TopicAdmin:
    """Auto-creation is disabled on the broker, so a misspelt topic name fails
    loudly instead of silently creating a 1-partition topic."""

    def __init__(self, settings: KafkaSettings, *, client_id: str) -> None:
        self._settings = settings
        self._admin = AdminClient(
            {"bootstrap.servers": settings.bootstrap_servers, "client.id": client_id},
            logger=CLIENT_LOGGER,
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
