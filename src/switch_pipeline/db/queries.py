"""SQL shared by the replay tool (sync psycopg) and the API (async psycopg)."""

from typing import LiteralString

# Fingerprints of the materialized sink. Processing timestamps and Kafka
# coordinates are excluded: a rebuild legitimately changes those, while the
# content of the event log, the current state and the quarantine must not.
SINK_CHECKSUMS: LiteralString = """
SELECT
    (SELECT count(*) FROM entity_current_state) AS entities,
    (SELECT md5(coalesce(string_agg(
            concat_ws('|', entity_type, entity_key, entity_version, payload::text, last_event_id),
            E'\\n' ORDER BY entity_type, entity_key), ''))
       FROM entity_current_state) AS state_checksum,
    (SELECT count(*) FROM event_log) AS events,
    (SELECT md5(coalesce(string_agg(
            concat_ws('|', event_id, fingerprint), E'\\n' ORDER BY event_id), ''))
       FROM event_log) AS event_log_checksum,
    (SELECT count(*) FROM quarantine) AS quarantined,
    (SELECT md5(coalesce(string_agg(quarantine_id::text, E'\\n' ORDER BY quarantine_id), ''))
       FROM quarantine) AS quarantine_checksum
"""

# Rebuild the sink from the topic: TRUNCATE (not DELETE) because the event log
# and quarantine are append-only at the row level.
TRUNCATE_SINK: LiteralString = """
TRUNCATE entity_current_state, event_log, quarantine, consumer_counter RESTART IDENTITY
"""
