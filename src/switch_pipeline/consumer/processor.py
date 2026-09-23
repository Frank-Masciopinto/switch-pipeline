"""Decides the fate of each record: apply, skip as a duplicate, or quarantine.

Order of checks, per record:
1. envelope schema        -> quarantine (schema_violation)
2. event id already logged -> identical content: skip (duplicate, expected under
                              at-least-once); different content: quarantine
                              (event_id_conflict)
3. quality rules          -> any 'reject' violation: quarantine (quality_rule_failed)
4. append to event log + version-guarded upsert of current state; a value the
   database cannot store -> quarantine (sink_rejected)
Nothing is dropped silently: every record ends up logged, skipped-and-counted,
or quarantined with a reason.
"""

from collections import Counter
from collections.abc import Mapping, MutableMapping, Sequence
from enum import StrEnum
from typing import Any, assert_never
from uuid import UUID, uuid5

import psycopg

from switch_pipeline.consumer.decoding import InboundMessage, SchemaViolation, decode
from switch_pipeline.consumer.repository import SinkRepository
from switch_pipeline.domain.envelope import ChangeEvent, EventType
from switch_pipeline.domain.quarantine import QuarantineReason
from switch_pipeline.observability import bound_contextvars, get_logger
from switch_pipeline.quality.rules import LoadedRuleSet, QualityReport

log = get_logger(__name__)

# Changing this namespace would re-key existing quarantine rows.
_QUARANTINE_NAMESPACE = UUID("0c6f0e5a-7d2b-4bb8-8a4e-3f1d7c9b2e61")


class Outcome(StrEnum):
    APPLIED = "applied"  # new event; the entity's current state moved to it
    STALE = "stale"  # new event, logged; an equal or newer version is already current
    DUPLICATE = "duplicate"  # redelivery of an already logged event; skipped
    QUARANTINED = "quarantined"


class EventProcessor:
    def __init__(self, rules: LoadedRuleSet) -> None:
        self._rules = rules

    @property
    def rules(self) -> LoadedRuleSet:
        return self._rules

    def process_batch(
        self, conn: psycopg.Connection[Any], messages: Sequence[InboundMessage]
    ) -> Counter[Outcome]:
        """Process records inside the caller's transaction (one per batch)."""
        repo = SinkRepository(conn)
        decoded = [(message, decode(message)) for message in messages]
        known = repo.known_fingerprints(
            {item.event_id for _, item in decoded if isinstance(item, ChangeEvent)}
        )
        outcomes: Counter[Outcome] = Counter()
        for message, item in decoded:
            with bound_contextvars(**_correlation(message, item)):
                outcomes[self._process(conn, repo, message, item, known)] += 1
        if outcomes[Outcome.DUPLICATE]:
            repo.increment_counter("duplicates_skipped", outcomes[Outcome.DUPLICATE])
        return outcomes

    def _process(
        self,
        conn: psycopg.Connection[Any],
        repo: SinkRepository,
        message: InboundMessage,
        item: ChangeEvent | SchemaViolation,
        known: MutableMapping[UUID, str],
    ) -> Outcome:
        if isinstance(item, SchemaViolation):
            return self._quarantine_violation(repo, message, item)
        event = item
        fingerprint = event.fingerprint()
        logged = known.get(event.event_id)
        if logged is not None:
            return self._on_logged(repo, message, event, fingerprint, logged)
        report = self._rules.evaluate(event)
        if report.rejections:
            return self._quarantine_event(
                repo,
                message,
                event,
                fingerprint,
                QuarantineReason.QUALITY_RULE_FAILED,
                {"violations": [violation.as_dict() for violation in report.violations]},
                summary=[violation.rule for violation in report.rejections],
            )
        try:
            with conn.transaction():  # savepoint: one unstorable record must not fail the batch
                inserted = repo.insert_event(
                    event,
                    fingerprint=fingerprint,
                    warnings=[violation.as_dict() for violation in report.warnings],
                    message=message,
                )
                applied = inserted and self._apply(repo, event)
        except psycopg.DataError as exc:
            return self._quarantine_event(
                repo,
                message,
                event,
                fingerprint,
                QuarantineReason.SINK_REJECTED,
                {"error": str(exc).strip()},
                summary=[type(exc).__name__],
            )
        if not inserted:
            # Another consumer instance logged this event after our lookup.
            logged = repo.fingerprint_of(event.event_id) or ""
            return self._on_logged(repo, message, event, fingerprint, logged)
        known[event.event_id] = fingerprint
        self._log_accepted(event, report, applied=applied)
        return Outcome.APPLIED if applied else Outcome.STALE

    def _apply(self, repo: SinkRepository, event: ChangeEvent) -> bool:
        match event.event_type:
            case EventType.INSERT | EventType.UPDATE:
                return repo.upsert_current_state(event)
            case _:
                assert_never(event.event_type)

    def _on_logged(
        self,
        repo: SinkRepository,
        message: InboundMessage,
        event: ChangeEvent,
        fingerprint: str,
        logged_fingerprint: str,
    ) -> Outcome:
        if logged_fingerprint == fingerprint:
            log.debug("duplicate_event_skipped")
            return Outcome.DUPLICATE
        return self._quarantine_event(
            repo,
            message,
            event,
            fingerprint,
            QuarantineReason.EVENT_ID_CONFLICT,
            {"logged_fingerprint": logged_fingerprint, "incoming_fingerprint": fingerprint},
            summary=["same event_id, different content"],
        )

    def _quarantine_event(
        self,
        repo: SinkRepository,
        message: InboundMessage,
        event: ChangeEvent,
        fingerprint: str,
        reason: QuarantineReason,
        details: Mapping[str, object],
        *,
        summary: list[str],
    ) -> Outcome:
        stored = repo.quarantine(
            quarantine_id=uuid5(
                _QUARANTINE_NAMESPACE, f"event:{event.event_id}:{fingerprint}:{reason.value}"
            ),
            reason=reason.value,
            details=details,
            event_id=event.event_id,
            entity_type=event.entity_type,
            entity_key=event.entity_key,
            batch_id=event.batch_id,
            ruleset_fingerprint=self._rules.fingerprint,
            message=message,
        )
        log.warning("event_quarantined", reason=reason.value, summary=summary, new=stored)
        return Outcome.QUARANTINED

    def _quarantine_violation(
        self, repo: SinkRepository, message: InboundMessage, violation: SchemaViolation
    ) -> Outcome:
        record = f"record:{message.topic}:{message.partition}:{message.offset}"
        stored = repo.quarantine(
            quarantine_id=uuid5(_QUARANTINE_NAMESPACE, record),
            reason=QuarantineReason.SCHEMA_VIOLATION.value,
            details={"errors": list(violation.errors)},
            event_id=violation.event_id,
            entity_type=None,
            entity_key=violation.entity_key,
            batch_id=violation.batch_id,
            ruleset_fingerprint=None,
            message=message,
        )
        log.warning(
            "event_quarantined",
            reason=QuarantineReason.SCHEMA_VIOLATION.value,
            summary=[f"{error['loc']}: {error['type']}" for error in violation.errors],
            new=stored,
        )
        return Outcome.QUARANTINED

    def _log_accepted(self, event: ChangeEvent, report: QualityReport, *, applied: bool) -> None:
        if report.warnings:
            log.warning("quality_warnings", rules=[violation.rule for violation in report.warnings])
        log.debug("event_applied" if applied else "event_stale", version=event.entity_version)


def _correlation(message: InboundMessage, item: ChangeEvent | SchemaViolation) -> dict[str, object]:
    context: dict[str, object] = {
        "kafka_partition": message.partition,
        "kafka_offset": message.offset,
    }
    if isinstance(item, ChangeEvent):
        context.update(
            event_id=str(item.event_id), batch_id=str(item.batch_id), entity_key=item.entity_key
        )
    else:
        context.update(
            {
                name: message.headers[name]
                for name in ("event_id", "batch_id")
                if name in message.headers
            }
        )
    return context
