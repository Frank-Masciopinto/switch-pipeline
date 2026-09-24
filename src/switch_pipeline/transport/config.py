"""Kafka client configuration; the delivery guarantees live here, not in .env."""

import logging
from typing import Any

from switch_pipeline.settings import KafkaSettings

# librdkafka's own logs go through stdlib logging, hence end up as JSON too.
CLIENT_LOGGER = logging.getLogger("switch_pipeline.librdkafka")


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
        "logger": CLIENT_LOGGER,
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
        "logger": CLIENT_LOGGER,
    }
