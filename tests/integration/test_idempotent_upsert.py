"""Idempotent materialization against a real PostgreSQL.

The guarantees live in SQL (ON CONFLICT + the version guard), so they are
tested against the database rather than a mock of it.
"""

import random
import threading
from collections import Counter
from typing import Any

import psycopg
from psycopg.rows import dict_row

from switch_pipeline.consumer.processor import EventProcessor, Outcome
from switch_pipeline.quality.rules import load_rules
from switch_pipeline.settings import PostgresSettings
from switch_pipeline.sink.store import PostgresSink
from switch_pipeline.transport.codec import InboundMessage
from tests.helpers import make_event, make_message, message_for, order_payload
from tests.integration.conftest import RULES_PATH

RULES = load_rules(RULES_PATH)


def process(sink: PostgresSink, messages: list[InboundMessage]) -> Counter[Outcome]:
    with sink.transaction() as writer:
        return EventProcessor(RULES).process_batch(writer, messages)


def query(conninfo: str, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with psycopg.connect(conninfo) as conn:
        return conn.cursor(row_factory=dict_row).execute(sql, params).fetchall()


def current(conninfo: str, key: str) -> dict[str, Any]:
    rows = query(conninfo, "SELECT * FROM entity_current_state WHERE entity_key = %s", (key,))
    assert len(rows) == 1
    return rows[0]


def test_newer_versions_replace_older_ones(sink: PostgresSink, db: str) -> None:
    assert process(sink, [message_for(make_event(key=1, version=1))]) == {Outcome.APPLIED: 1}
    assert process(sink, [message_for(make_event(key=1, version=2), offset=1)]) == {
        Outcome.APPLIED: 1
    }
    state = current(db, "1")
    assert (state["entity_version"], state["last_event_type"]) == (2, "update")


def test_a_redelivered_event_is_skipped_and_counted(sink: PostgresSink, db: str) -> None:
    event = make_event()
    process(sink, [message_for(event, offset=0)])
    assert process(sink, [message_for(event, offset=9)]) == {Outcome.DUPLICATE: 1}
    assert query(db, "SELECT count(*) AS n FROM event_log")[0]["n"] == 1
    assert query(db, "SELECT value FROM consumer_counter")[0]["value"] == 1


def test_duplicates_within_one_batch_are_skipped(sink: PostgresSink) -> None:
    event = make_event()
    outcomes = process(sink, [message_for(event, offset=0), message_for(event, offset=1)])
    assert outcomes == {Outcome.APPLIED: 1, Outcome.DUPLICATE: 1}


def test_an_event_another_instance_logs_concurrently_counts_as_a_duplicate(
    sink: PostgresSink, postgres_settings: PostgresSettings, db: str
) -> None:
    # Two consumer instances can briefly hold the same record, e.g. during a rebalance.
    event = make_event(key=11)
    other = PostgresSink.open(postgres_settings, application_name="tests-other-instance")
    outcomes: list[Counter[Outcome]] = []

    def second_instance() -> None:
        with other.transaction() as writer:
            outcomes.append(EventProcessor(RULES).process_batch(writer, [message_for(event)]))

    try:
        with sink.transaction() as first:
            assert EventProcessor(RULES).process_batch(first, [message_for(event)]) == {
                Outcome.APPLIED: 1
            }
            worker = threading.Thread(target=second_instance)
            worker.start()
            worker.join(timeout=1)
            assert worker.is_alive(), "waits on the first instance's uncommitted insert"
        worker.join(timeout=10)
    finally:
        other.close()
    assert outcomes == [{Outcome.DUPLICATE: 1}]
    assert query(db, "SELECT count(*) AS n FROM event_log")[0]["n"] == 1


def test_a_re_emitted_change_with_new_capture_metadata_is_a_duplicate(sink: PostgresSink) -> None:
    first = make_event(key=3, version=2)
    re_emitted = make_event(key=3, version=2, captured_at=first.captured_at.replace(year=2027))
    process(sink, [message_for(first)])
    assert process(sink, [message_for(re_emitted, offset=1)]) == {Outcome.DUPLICATE: 1}


def test_out_of_order_delivery_never_moves_state_backwards(sink: PostgresSink, db: str) -> None:
    v1, v2, v3 = (make_event(key=5, version=version) for version in (1, 2, 3))
    outcomes = process(
        sink, [message_for(v3), message_for(v1, offset=1), message_for(v2, offset=2)]
    )
    assert outcomes == {Outcome.APPLIED: 1, Outcome.STALE: 2}
    assert current(db, "5")["entity_version"] == 3
    assert query(db, "SELECT count(*) AS n FROM event_log")[0]["n"] == 3


def test_replaying_in_any_order_converges_to_the_same_state(sink: PostgresSink) -> None:
    events = [make_event(key=key, version=version) for key in range(1, 6) for version in (1, 2, 3)]
    messages = [message_for(event, offset=i) for i, event in enumerate(events)]
    process(sink, messages)
    expected = sink.checksums()

    process(sink, messages)  # replay on top of the existing state
    assert sink.checksums() == expected

    for seed in range(3):  # rebuild from scratch, in a different order each time
        sink.truncate()
        process(sink, random.Random(seed).sample(messages, len(messages)))
        assert sink.checksums() == expected


def test_same_event_id_with_different_content_is_quarantined(sink: PostgresSink, db: str) -> None:
    original = make_event(key=7)
    tampered = original.model_copy(update={"payload": order_payload(7, o_comment="tampered")})
    process(sink, [message_for(original)])
    assert process(sink, [message_for(tampered, offset=1)]) == {Outcome.QUARANTINED: 1}
    assert current(db, "7")["payload"]["o_comment"] == original.payload["o_comment"]
    [row] = query(db, "SELECT reason, event_id FROM quarantine")
    assert (row["reason"], row["event_id"]) == ("event_id_conflict", original.event_id)


def test_rule_failures_are_quarantined_and_state_keeps_the_last_good_version(
    sink: PostgresSink, db: str
) -> None:
    good = make_event(key=9, version=1)
    bad = make_event(key=9, version=2, payload=order_payload(9, o_totalprice="-1.00"))
    outcomes = process(sink, [message_for(good), message_for(bad, offset=1)])
    assert outcomes == {Outcome.APPLIED: 1, Outcome.QUARANTINED: 1}
    assert current(db, "9")["entity_version"] == 1
    [row] = query(db, "SELECT reason, details, ruleset_fingerprint FROM quarantine")
    assert row["reason"] == "quality_rule_failed"
    assert [v["rule"] for v in row["details"]["violations"]] == ["total_price_non_negative"]
    assert row["ruleset_fingerprint"] == RULES.fingerprint


def test_warnings_are_accepted_and_recorded_on_the_event(sink: PostgresSink, db: str) -> None:
    event = make_event(key=4, payload=order_payload(4, o_comment="x" * 100))
    assert process(sink, [message_for(event)]) == {Outcome.APPLIED: 1}
    [row] = query(db, "SELECT quality_warnings FROM event_log")
    assert [w["rule"] for w in row["quality_warnings"]] == ["comment_within_source_limit"]


def test_schema_violations_are_quarantined_with_the_exact_record(
    sink: PostgresSink, db: str
) -> None:
    unreadable = make_message(b"not json\x00\xff", key=b"k\x00\xfe", offset=7)
    assert process(sink, [unreadable]) == {Outcome.QUARANTINED: 1}
    [row] = query(
        db, "SELECT reason, raw_bytes, raw_value, entity_key, kafka_offset FROM quarantine"
    )
    assert (row["reason"], row["raw_bytes"], row["kafka_offset"]) == (
        "schema_violation",
        b"not json\x00\xff",
        7,
    )
    # TEXT columns cannot hold NUL or invalid UTF-8: the readable copies show U+FFFD.
    assert (row["raw_value"], row["entity_key"]) == ("not json\ufffd\ufffd", "k\ufffd\ufffd")


def test_quarantine_is_idempotent_under_replay(sink: PostgresSink, db: str) -> None:
    bad = make_event(key=2, payload=order_payload(2, o_orderstatus="X"))
    messages = [make_message(b"garbage", offset=3), message_for(bad, offset=4)]
    process(sink, messages)
    process(sink, messages)
    assert query(db, "SELECT count(*) AS n FROM quarantine")[0]["n"] == 2


def test_a_value_postgres_cannot_store_is_quarantined_without_failing_the_batch(
    sink: PostgresSink, db: str
) -> None:
    unstorable = make_event(key=1, payload=order_payload(1, o_comment="nul\u0000byte"))
    outcomes = process(sink, [message_for(unstorable), message_for(make_event(key=2), offset=1)])
    assert outcomes == {Outcome.QUARANTINED: 1, Outcome.APPLIED: 1}
    [row] = query(db, "SELECT reason, details FROM quarantine")
    assert row["reason"] == "sink_rejected"
    assert "unsupported Unicode escape sequence" in row["details"]["error"]
