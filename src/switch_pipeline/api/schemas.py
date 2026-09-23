"""Response models of the event-inspection API."""

from datetime import datetime
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, Field, JsonValue

from switch_pipeline.domain.envelope import EventType


class KafkaCoordinates(BaseModel):
    topic: str
    partition: int
    offset: int


class EventRecord(BaseModel):
    event_id: UUID
    event_type: EventType
    schema_version: int
    source: dict[str, JsonValue]
    entity_type: str
    entity_key: str
    entity_version: int
    occurred_at: datetime
    captured_at: datetime
    processed_at: datetime
    lag_seconds: float = Field(description="processed_at - occurred_at")
    batch_id: UUID
    payload: dict[str, JsonValue]
    quality_warnings: list[dict[str, str]]
    kafka: KafkaCoordinates

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        return cls(
            event_id=row["event_id"],
            event_type=row["event_type"],
            schema_version=row["schema_version"],
            source=row["source"],
            entity_type=row["entity_type"],
            entity_key=row["entity_key"],
            entity_version=row["entity_version"],
            occurred_at=row["occurred_at"],
            captured_at=row["captured_at"],
            processed_at=row["processed_at"],
            lag_seconds=(row["processed_at"] - row["occurred_at"]).total_seconds(),
            batch_id=row["batch_id"],
            payload=row["payload"],
            quality_warnings=row["quality_warnings"],
            kafka=KafkaCoordinates(
                topic=row["kafka_topic"],
                partition=row["kafka_partition"],
                offset=row["kafka_offset"],
            ),
        )


class EventPage(BaseModel):
    items: list[EventRecord]
    next_cursor: str | None = Field(description="Pass as `cursor` to fetch the next (older) page.")


class CurrentState(BaseModel):
    entity_type: str
    entity_key: str
    entity_version: int
    payload: dict[str, JsonValue]
    source: dict[str, JsonValue]
    last_event_id: UUID
    last_event_type: EventType
    occurred_at: datetime
    updated_at: datetime


class QuarantineRecord(BaseModel):
    quarantine_id: UUID
    reason: str
    details: dict[str, JsonValue]
    event_id: UUID | None
    entity_type: str | None
    entity_key: str | None
    batch_id: UUID | None
    ruleset_fingerprint: str | None
    raw_value: str | None
    kafka: KafkaCoordinates
    quarantined_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        return cls(
            quarantine_id=row["quarantine_id"],
            reason=row["reason"],
            details=row["details"],
            event_id=row["event_id"],
            entity_type=row["entity_type"],
            entity_key=row["entity_key"],
            batch_id=row["batch_id"],
            ruleset_fingerprint=row["ruleset_fingerprint"],
            raw_value=row["raw_value"],
            kafka=KafkaCoordinates(
                topic=row["kafka_topic"],
                partition=row["kafka_partition"],
                offset=row["kafka_offset"],
            ),
            quarantined_at=row["quarantined_at"],
        )


class QuarantinePage(BaseModel):
    items: list[QuarantineRecord]
    next_cursor: str | None


class EntityView(BaseModel):
    entity_type: str | None
    entity_key: str
    current: CurrentState | None = Field(description="Latest accepted version, if any.")
    history: list[EventRecord] = Field(description="Accepted events, oldest first.")
    history_truncated: bool
    quarantined: list[QuarantineRecord] = Field(description="Rejected records for this key.")


class EventCounts(BaseModel):
    total: int
    by_type: dict[str, int]
    with_quality_warnings: int


class QuarantineCounts(BaseModel):
    total: int
    by_reason: dict[str, int]


class LagStats(BaseModel):
    avg: float | None
    p50: float | None
    p95: float | None
    max: float | None


class LagSummary(BaseModel):
    sample_size: int = Field(description="Most recent events the figures are computed over.")
    occurred_to_processed: LagStats = Field(description="End to end: source write -> sink.")
    occurred_to_captured: LagStats = Field(description="Source write -> adapter read.")
    captured_to_processed: LagStats = Field(description="Adapter read -> sink (broker + consumer).")


class LatestTimestamps(BaseModel):
    occurred_at: datetime | None
    processed_at: datetime | None


class Watermark(BaseModel):
    source_id: str
    cursor_updated_at: datetime | None
    cursor_key: JsonValue
    initial_sync_completed_at: datetime | None
    last_batch_id: UUID | None
    updated_at: datetime


class BatchSummary(BaseModel):
    batch_id: UUID
    source_id: str
    sync_mode: str
    status: str
    row_count: int
    started_at: datetime
    finished_at: datetime | None
    error: str | None


class PartitionLag(BaseModel):
    partition: int
    committed_offset: int | None
    end_offset: int
    lag: int


class ConsumerLag(BaseModel):
    group: str
    topic: str
    total: int | None
    partitions: list[PartitionLag]
    error: str | None


class SinkChecksums(BaseModel):
    entities: int
    state_checksum: str
    events: int
    event_log_checksum: str
    quarantined: int
    quarantine_checksum: str


class Stats(BaseModel):
    generated_at: datetime
    events: EventCounts
    entities: int
    duplicates_skipped: int
    quarantine: QuarantineCounts
    lag_seconds: LagSummary
    latest: LatestTimestamps
    watermarks: list[Watermark]
    recent_batches: list[BatchSummary]
    consumer_lag: ConsumerLag
    checksums: SinkChecksums | None = Field(description="Only with ?checksums=true.")
