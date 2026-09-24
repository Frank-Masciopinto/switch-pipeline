"""Sink writes. Every statement is idempotent, so reprocessing a record (a
redelivery, a replay from offset 0, a rebuild) converges to the same state."""

from collections.abc import Collection, Mapping
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from switch_pipeline.domain.envelope import ChangeEvent
from switch_pipeline.transport.codec import InboundMessage


def storable_text(raw: bytes | str | None) -> str | None:
    """Text PostgreSQL accepts: TEXT rejects NUL and invalid UTF-8."""
    if raw is None:
        return None
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    return text.replace("\x00", "\ufffd")


class SinkRepository:
    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self._conn = conn

    def known_fingerprints(self, event_ids: Collection[UUID]) -> dict[UUID, str]:
        if not event_ids:
            return {}
        rows = self._conn.execute(
            "SELECT event_id, fingerprint FROM event_log WHERE event_id = ANY(%s)",
            (list(event_ids),),
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def fingerprint_of(self, event_id: UUID) -> str | None:
        row = self._conn.execute(
            "SELECT fingerprint FROM event_log WHERE event_id = %s", (event_id,)
        ).fetchone()
        return None if row is None else str(row[0])

    def insert_event(
        self,
        event: ChangeEvent,
        *,
        fingerprint: str,
        warnings: list[dict[str, str]],
        message: InboundMessage,
    ) -> bool:
        """Append to the event log; False if the event id is already logged."""
        row = self._conn.execute(
            """
            INSERT INTO event_log (
                event_id, event_type, schema_version, source, entity_type, entity_key,
                entity_version, payload, occurred_at, captured_at, batch_id, fingerprint,
                quality_warnings, kafka_topic, kafka_partition, kafka_offset
            ) VALUES (
                %(event_id)s, %(event_type)s, %(schema_version)s, %(source)s, %(entity_type)s,
                %(entity_key)s, %(entity_version)s, %(payload)s, %(occurred_at)s, %(captured_at)s,
                %(batch_id)s, %(fingerprint)s, %(warnings)s, %(topic)s, %(partition)s, %(offset)s
            )
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "schema_version": event.schema_version,
                "source": Jsonb(event.source.model_dump(mode="json")),
                "entity_type": event.entity_type,
                "entity_key": event.entity_key,
                "entity_version": event.entity_version,
                "payload": Jsonb(event.payload),
                "occurred_at": event.occurred_at,
                "captured_at": event.captured_at,
                "batch_id": event.batch_id,
                "fingerprint": fingerprint,
                "warnings": Jsonb(warnings),
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
            },
        ).fetchone()
        return row is not None

    def upsert_current_state(self, event: ChangeEvent) -> bool:
        """Move the entity to this version unless an equal or newer one is current.

        The version guard makes the result independent of arrival order, which
        is what lets replays and out-of-order redeliveries converge.
        """
        row = self._conn.execute(
            """
            INSERT INTO entity_current_state AS current (
                entity_type, entity_key, entity_version, payload, source,
                last_event_id, last_event_type, occurred_at
            ) VALUES (
                %(entity_type)s, %(entity_key)s, %(entity_version)s, %(payload)s, %(source)s,
                %(event_id)s, %(event_type)s, %(occurred_at)s
            )
            ON CONFLICT (entity_type, entity_key) DO UPDATE SET
                entity_version = EXCLUDED.entity_version,
                payload = EXCLUDED.payload,
                source = EXCLUDED.source,
                last_event_id = EXCLUDED.last_event_id,
                last_event_type = EXCLUDED.last_event_type,
                occurred_at = EXCLUDED.occurred_at,
                updated_at = clock_timestamp()
            WHERE current.entity_version < EXCLUDED.entity_version
            RETURNING entity_version
            """,
            {
                "entity_type": event.entity_type,
                "entity_key": event.entity_key,
                "entity_version": event.entity_version,
                "payload": Jsonb(event.payload),
                "source": Jsonb(event.source.model_dump(mode="json")),
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "occurred_at": event.occurred_at,
            },
        ).fetchone()
        return row is not None

    def quarantine(
        self,
        *,
        quarantine_id: UUID,
        reason: str,
        details: Mapping[str, object],
        event_id: UUID | None,
        entity_type: str | None,
        entity_key: str | None,
        batch_id: UUID | None,
        ruleset_fingerprint: str | None,
        message: InboundMessage,
    ) -> bool:
        """Record a rejected record; False if it was already quarantined."""
        row = self._conn.execute(
            """
            INSERT INTO quarantine (
                quarantine_id, reason, details, event_id, entity_type, entity_key, batch_id,
                ruleset_fingerprint, raw_value, kafka_topic, kafka_partition, kafka_offset
            ) VALUES (
                %(quarantine_id)s, %(reason)s, %(details)s, %(event_id)s, %(entity_type)s,
                %(entity_key)s, %(batch_id)s, %(ruleset)s, %(raw_value)s, %(topic)s,
                %(partition)s, %(offset)s
            )
            ON CONFLICT (quarantine_id) DO NOTHING
            RETURNING quarantine_id
            """,
            {
                "quarantine_id": quarantine_id,
                "reason": reason,
                "details": Jsonb(dict(details)),
                "event_id": event_id,
                "entity_type": entity_type,
                "entity_key": storable_text(entity_key),
                "batch_id": batch_id,
                "ruleset": ruleset_fingerprint,
                "raw_value": storable_text(message.value),
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
            },
        ).fetchone()
        return row is not None

    def increment_counter(self, name: str, amount: int) -> None:
        self._conn.execute(
            """
            INSERT INTO consumer_counter (name, value) VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE
            SET value = consumer_counter.value + EXCLUDED.value, updated_at = clock_timestamp()
            """,
            (name, amount),
        )
