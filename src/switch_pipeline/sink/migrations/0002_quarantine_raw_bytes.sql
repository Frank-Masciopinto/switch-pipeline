-- The exact bytes of each quarantined record. raw_value stays as the readable
-- copy, in which what TEXT cannot hold (NUL, invalid UTF-8) shows as U+FFFD.
ALTER TABLE quarantine ADD COLUMN raw_bytes BYTEA;

COMMENT ON COLUMN quarantine.raw_bytes IS 'record value exactly as consumed';
COMMENT ON COLUMN quarantine.raw_value IS 'readable copy of raw_bytes; unstorable bytes become U+FFFD';
