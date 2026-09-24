from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from switch_pipeline.domain.log import LogPosition


class QuarantineReason(StrEnum):
    """Why a record was rejected (mirrors the CHECK constraint on quarantine.reason)."""

    SCHEMA_VIOLATION = "schema_violation"
    QUALITY_RULE_FAILED = "quality_rule_failed"
    EVENT_ID_CONFLICT = "event_id_conflict"
    SINK_REJECTED = "sink_rejected"


@dataclass(frozen=True, slots=True, kw_only=True)
class QuarantineEntry:
    """A rejected record, kept with its raw bytes so it can be inspected and replayed."""

    quarantine_id: UUID  # deterministic, so reprocessing never duplicates an entry
    reason: QuarantineReason
    details: Mapping[str, object]
    event_id: UUID | None
    entity_type: str | None
    entity_key: str | None
    batch_id: UUID | None
    ruleset_fingerprint: str | None
    raw_value: bytes | None
    position: LogPosition
