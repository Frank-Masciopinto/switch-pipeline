from enum import StrEnum


class QuarantineReason(StrEnum):
    """Why a record was rejected (mirrors the CHECK constraint on quarantine.reason)."""

    SCHEMA_VIOLATION = "schema_violation"
    QUALITY_RULE_FAILED = "quality_rule_failed"
    EVENT_ID_CONFLICT = "event_id_conflict"
    SINK_REJECTED = "sink_rejected"
