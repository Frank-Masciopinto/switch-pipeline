-- ---------------------------------------------------------------------------
-- Adapter state (control plane): watermark per source and a record per batch.
-- ---------------------------------------------------------------------------
CREATE TABLE sync_state (
    source_id                 TEXT PRIMARY KEY,
    -- Composite watermark: (updated_at, key) of the last row published and acknowledged.
    cursor_updated_at         TIMESTAMPTZ,
    cursor_key                JSONB,
    initial_sync_completed_at TIMESTAMPTZ,
    last_batch_id             UUID,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sync_state_cursor_complete CHECK ((cursor_updated_at IS NULL) = (cursor_key IS NULL))
);

CREATE TABLE sync_batch (
    batch_id     UUID PRIMARY KEY,
    source_id    TEXT        NOT NULL,
    sync_mode    TEXT        NOT NULL CHECK (sync_mode IN ('full', 'incremental')),
    status       TEXT        NOT NULL CHECK (status IN ('running', 'committed', 'failed')),
    cursor_start JSONB,
    cursor_end   JSONB,
    upper_bound  TIMESTAMPTZ NOT NULL,
    row_count    INTEGER     NOT NULL DEFAULT 0,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at  TIMESTAMPTZ,
    error        TEXT
);
CREATE INDEX sync_batch_source_started_idx ON sync_batch (source_id, started_at DESC);

-- ---------------------------------------------------------------------------
-- Sink: every accepted event exactly once, the latest version per entity, and
-- every rejected record with the reason it was rejected.
-- ---------------------------------------------------------------------------
CREATE TABLE event_log (
    log_seq          BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_id         UUID PRIMARY KEY,
    event_type       TEXT        NOT NULL,
    schema_version   INTEGER     NOT NULL,
    source           JSONB       NOT NULL,
    entity_type      TEXT        NOT NULL,
    entity_key       TEXT        NOT NULL,
    entity_version   BIGINT      NOT NULL,
    payload          JSONB       NOT NULL,
    occurred_at      TIMESTAMPTZ NOT NULL,
    captured_at      TIMESTAMPTZ NOT NULL,
    processed_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    batch_id         UUID        NOT NULL,
    fingerprint      TEXT        NOT NULL,
    quality_warnings JSONB       NOT NULL DEFAULT '[]'::jsonb,
    kafka_topic      TEXT        NOT NULL,
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL
);
CREATE INDEX event_log_entity_idx ON event_log (entity_type, entity_key, log_seq);
CREATE INDEX event_log_key_idx ON event_log (entity_key, log_seq);
CREATE INDEX event_log_type_idx ON event_log (event_type, log_seq);
CREATE INDEX event_log_batch_idx ON event_log (batch_id);
CREATE INDEX event_log_occurred_idx ON event_log (occurred_at);
CREATE INDEX event_log_processed_idx ON event_log (processed_at);

CREATE TABLE entity_current_state (
    entity_type     TEXT        NOT NULL,
    entity_key      TEXT        NOT NULL,
    entity_version  BIGINT      NOT NULL,
    payload         JSONB       NOT NULL,
    source          JSONB       NOT NULL,
    last_event_id   UUID        NOT NULL REFERENCES event_log (event_id),
    last_event_type TEXT        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (entity_type, entity_key)
);

CREATE TABLE quarantine (
    quarantine_seq      BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    quarantine_id       UUID PRIMARY KEY,
    reason              TEXT        NOT NULL CHECK (
        reason IN ('schema_violation', 'quality_rule_failed', 'event_id_conflict', 'sink_rejected')
    ),
    details             JSONB       NOT NULL,
    event_id            UUID,
    entity_type         TEXT,
    entity_key          TEXT,
    batch_id            UUID,
    ruleset_fingerprint TEXT,
    raw_value           TEXT,
    kafka_topic         TEXT        NOT NULL,
    kafka_partition     INTEGER     NOT NULL,
    kafka_offset        BIGINT      NOT NULL,
    quarantined_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX quarantine_reason_idx ON quarantine (reason, quarantine_seq);
CREATE INDEX quarantine_key_idx ON quarantine (entity_key, quarantine_seq);

-- Operational counters (e.g. duplicate deliveries skipped); not part of the
-- materialized state, so they are excluded from convergence checksums.
CREATE TABLE consumer_counter (
    name       TEXT PRIMARY KEY,
    value      BIGINT      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Append-only tables reject UPDATE and DELETE. A rebuild uses TRUNCATE, which
-- is a deliberate, privileged operation rather than a row-level mutation.
CREATE FUNCTION reject_mutation() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    RAISE EXCEPTION '% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$;

CREATE TRIGGER event_log_append_only
    BEFORE UPDATE OR DELETE ON event_log
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER quarantine_append_only
    BEFORE UPDATE OR DELETE ON quarantine
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();
