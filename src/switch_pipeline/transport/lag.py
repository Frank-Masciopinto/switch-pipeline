"""Consumer-group lag in messages: end offsets minus the group's committed offsets."""

import threading
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException, TopicPartition

from switch_pipeline.settings import KafkaSettings
from switch_pipeline.transport.config import consumer_config


@dataclass(frozen=True, slots=True)
class PartitionLag:
    partition: int
    committed_offset: int | None
    end_offset: int
    lag: int


@dataclass(frozen=True, slots=True)
class GroupLag:
    group: str
    topic: str
    total: int | None
    partitions: tuple[PartitionLag, ...]
    error: str | None


class ConsumerLagInspector:
    """Reads the materializer group's committed offsets without ever joining the
    group (it never subscribes), so it cannot trigger a rebalance."""

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._consumer: Consumer | None = None

    def snapshot(self) -> GroupLag:
        settings = self._settings
        with self._lock:
            try:
                consumer = self._client()
                metadata = consumer.list_topics(
                    settings.topic, timeout=settings.admin_timeout_seconds
                )
                topic = metadata.topics.get(settings.topic)
                if topic is None or topic.error is not None:
                    return self._unavailable(f"topic {settings.topic!r} not found")
                partitions = [TopicPartition(settings.topic, p) for p in sorted(topic.partitions)]
                committed = consumer.committed(partitions, timeout=settings.admin_timeout_seconds)
                lags = []
                for position in committed:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(settings.topic, position.partition),
                        timeout=settings.admin_timeout_seconds,
                    )
                    offset = position.offset if position.offset >= 0 else None
                    lags.append(
                        PartitionLag(
                            partition=position.partition,
                            committed_offset=offset,
                            end_offset=high,
                            lag=high - (offset if offset is not None else low),
                        )
                    )
            except KafkaException as exc:
                return self._unavailable(str(exc))
        return GroupLag(
            group=settings.consumer_group,
            topic=settings.topic,
            total=sum(lag.lag for lag in lags),
            partitions=tuple(lags),
            error=None,
        )

    def close(self) -> None:
        with self._lock:
            if self._consumer is not None:
                self._consumer.close()
                self._consumer = None

    def _client(self) -> Consumer:
        if self._consumer is None:
            self._consumer = Consumer(consumer_config(self._settings, client_id="switch-api-lag"))
        return self._consumer

    def _unavailable(self, error: str) -> GroupLag:
        return GroupLag(
            group=self._settings.consumer_group,
            topic=self._settings.topic,
            total=None,
            partitions=(),
            error=error,
        )
