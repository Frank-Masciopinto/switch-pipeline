from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LogPosition:
    """Where a record sits in the event log (for Kafka: topic, partition, offset)."""

    topic: str
    partition: int
    offset: int
