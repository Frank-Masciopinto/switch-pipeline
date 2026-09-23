# Switch pipeline: Snowflake → Kafka → PostgreSQL

A miniature of Switch's core pattern. An adapter captures changes from a
Snowflake table incrementally, publishes them as versioned change events to
Redpanda (Kafka API), and a consumer materializes them into PostgreSQL with
idempotent, replayable writes. A FastAPI service exposes the event log, the
current state per entity, the rejected records and pipeline statistics.

- **Incremental capture that survives restarts.** A composite `(UPDATED_AT, key)`
  watermark, a settle window for in-flight source transactions, and a batch
  ledger in PostgreSQL. A rerun emits nothing; a restart resumes after the last
  batch the broker confirmed.
- **At-least-once delivery, exactly-once effect.** Idempotent producer with
  `acks=all` and retries with backoff; the watermark only moves after every
  event of a batch is acknowledged. Deterministic event ids let the consumer
  absorb every duplicate, and replaying the topic from offset 0 converges to
  byte-identical state (`make rebuild` proves it with checksums).
- **No silent drops.** Every record is applied, skipped as a counted duplicate,
  or quarantined with a reason that the API shows.
- **One place for configuration, one place for rules.** Every setting lives in
  `.env` (template: [`.env.example`](.env.example)); every data-quality rule
  lives in [`config/quality_rules.yaml`](config/quality_rules.yaml).

## Contents

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [How it works](#how-it-works)
- [Configuration](#configuration)
- [Operations](#operations)
- [Troubleshooting](#troubleshooting)
- [Testing](#testing)
- [Design decisions and trade-offs](#design-decisions-and-trade-offs)
- [Exactly-once effect: the failure windows that remain](#exactly-once-effect-the-failure-windows-that-remain)
- [Scope cuts](#scope-cuts)
- [What I would do with more time](#what-i-would-do-with-more-time)
- [Multi-tenant operation](#multi-tenant-operation)
- [CDC with Snowflake Streams or Debezium](#cdc-with-snowflake-streams-or-debezium)
- [Repository layout](#repository-layout)

## Quick start

You need Docker (Compose v2.20+) and `make`. [uv](https://docs.astral.sh/uv/)
is only needed to run the tests locally.

**With Snowflake** (a free trial account is enough):

```bash
make env                 # creates .env from .env.example: set SNOWFLAKE_ACCOUNT
make snowflake-keypair   # then run snowflake/setup.sql in Snowsight, pasting the printed key
make up                  # Redpanda, PostgreSQL, adapter, consumer, API on http://localhost:8000/docs
make seed                # 20,000 TPC-H orders joined with customers -> SWITCH_DEMO.RAW.CUSTOMER_ORDERS
make simulate            # inserts, updates and a few invalid rows in Snowflake
```

Then look around with `make stats`, `make events` and `make quarantine`, and
prove idempotency with `make replay` and `make rebuild`. `make demo` runs the
whole walkthrough used for the screen recording (start from `make reset`).
If anything on the Snowflake side fails, `make check-snowflake` says which part
and how to fix it (see [Troubleshooting](#troubleshooting)).

**Without a Snowflake account:** uncomment the emulator block at the end of
`.env` and run the same commands. The stack then starts
[fakesnow](https://github.com/tekumara/fakesnow), an open-source Snowflake
emulator backed by DuckDB, and seeds synthetic TPC-H-shaped data. The adapter
still talks to it through the real `snowflake-connector-python`.

## Architecture

```text
  Snowflake                                    docker compose
  ─────────                                    ─────────────────────────────────────────────────────

  SWITCH_DEMO.RAW.CUSTOMER_ORDERS              ┌──────────┐   change events   ┌───────────────────┐
  TPC-H orders ⋈ customers                     │ adapter  │ ────────────────► │ Redpanda          │
  + ROW_VERSION, UPDATED_AT (UTC)  ◄───────────│          │  acks=all,        │ topic, 6 parts    │
        ▲                          keyset read │ map to   │  idempotent       │ key = entity_key  │
        │                          by (UPDATED │ envelope │  producer         │ retention: ∞      │
  make seed / make simulate        _AT, key)   └────┬─────┘                   └─────────┬─────────┘
                                                    │ watermark + batch ledger          │
                                                    ▼ (after every event is acked)      ▼
                                   ┌─────────────────────── PostgreSQL ────────┐   ┌──────────────┐
                                   │ control   sync_state, sync_batch          │   │ consumer     │
                                   │ sink      event_log (append-only)         │◄──│ schema check │
                                   │           entity_current_state            │   │ dedup by id  │
                                   │           quarantine, consumer_counter    │   │ quality rules│
                                   └──────────────────────▲────────────────────┘   │ upsert, then │
                                                          │ read-only sessions     │ commit offset│
                                                   ┌──────┴──────┐                 └──────────────┘
                                                   │ FastAPI     │ /events  /events/{id}  /entities/{key}
                                                   └─────────────┘ /quarantine  /stats  /healthz  /readyz
```

One image serves every role (`switch-pipeline adapter|consumer|api|...`). A
one-shot `init` service applies the database migrations and creates the topic
before the workers start.

## How it works

### Source and incremental capture

**Data.** `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS` joined with `CUSTOMER`, the
first 20,000 orders by key, copied into our own table because the share is
read-only and the change simulation must insert and update rows. Writers
maintain two contract columns: `ROW_VERSION` (1 on insert, +1 on every write)
and `UPDATED_AT` (`SYSDATE()`, UTC). `SEED_STRATEGY=synthetic` generates the
same shape in the warehouse with `GENERATOR`, for the emulator or accounts
without the share.

**Watermark.** The cursor is the pair `(UPDATED_AT, O_ORDERKEY)` and each batch
is a keyset query:

```sql
SELECT * FROM SWITCH_DEMO.RAW.CUSTOMER_ORDERS
WHERE UPDATED_AT <= :upper_bound
  AND UPDATED_AT >= :cursor_ts AND (UPDATED_AT > :cursor_ts OR O_ORDERKEY > :cursor_key)
ORDER BY UPDATED_AT, O_ORDERKEY
LIMIT :batch_size
```

- `O_ORDERDATE` would be the wrong cursor: it is a business date that updates
  do not change, so updates would never be captured.
- A timestamp alone is not enough either: a bulk load gives thousands of rows
  the same `UPDATED_AT` (the seed does exactly that), and `UPDATED_AT > last`
  would skip the rest of a tie after a batch boundary. The initial sync of the
  seed is therefore 10 batches that all share one timestamp.
- The second predicate is the tuple comparison `(ts, key) > (cursor_ts,
  cursor_key)`, written so that Snowflake can prune micro-partitions on the
  timestamp.
- `UPDATED_AT` is `TIMESTAMP_NTZ(6)`: microseconds, like a Python `datetime`.
  With Snowflake's default nanoseconds the stored watermark would be truncated
  and the last row of every batch would be read, and emitted, again.
- Before publishing, the adapter checks that the batch continues strictly after
  the watermark in ascending order. A broken assumption (collation, a writer
  rewinding timestamps) stops the adapter instead of losing or re-emitting rows.

**Settle window.** Each cycle fixes `upper_bound = SYSDATE() - ADAPTER_SETTLE_SECONDS`
on Snowflake's clock. A writer transaction stamps `UPDATED_AT` when a statement
runs but its rows only become visible at commit; without the window the
watermark could move past rows that commit later with an older timestamp. The
window must exceed the longest source write transaction (default 10 s).

**Insert or update** comes from the row itself: version 1 is an insert. That
keeps the event type deterministic, which matters for replays.

**Sync state lives in PostgreSQL.** `sync_state` holds the watermark per source
and `sync_batch` is a ledger (mode, cursor range, upper bound, row count,
status, error). A batch is committed, in one transaction together with the new
watermark, only after the broker acknowledged every event in it. A crash at any
point resumes from the last committed batch; batches a crash left `running` are
closed out at startup. A session-level advisory lock ensures a single adapter
per source. Why PostgreSQL rather than the alternatives:

| Option | Why not (here) |
| --- | --- |
| Local file / volume | Not durable across hosts, no atomic "ledger + watermark", no single-writer lock, invisible to `/stats`. |
| Snowflake table | The adapter would need write privileges on the source, and every state write would run a warehouse (credits). |
| Compacted Kafka topic | Keeps state next to the data and enables Kafka transactions (see *more time*), but adds bootstrap and recovery complexity. |
| **PostgreSQL** | Transactional, durable, queryable by the API, supports advisory locks, already in the stack. In production it would be a separate control-plane database. |

### Change events and the topic

The envelope is a Pydantic model ([`envelope.py`](src/switch_pipeline/domain/envelope.py));
its JSON Schema is generated into
[`schemas/change_event.v1.schema.json`](schemas/change_event.v1.schema.json) and a
test fails if the two drift apart.

| Field | Meaning |
| --- | --- |
| `schema_version` | Envelope version (currently `1`); bumped on any envelope change. |
| `event_id` | UUIDv5 of `(source, entity_type, entity_key, entity_version)`. |
| `event_type` | `insert` or `update`. |
| `source` | `{system: "snowflake", object: "SWITCH_DEMO.RAW.CUSTOMER_ORDERS"}`. |
| `entity_type`, `entity_key`, `entity_version` | Which entity, and which version of it. |
| `occurred_at` | `UPDATED_AT` of the row: when the change happened (UTC). |
| `captured_at` | When the adapter read it (UTC). |
| `batch_id` | The adapter batch, for correlation. |
| `payload` | The source row, JSON-typed: decimals as exact strings, dates as ISO-8601. |

- **Deterministic ids** are what makes deduplication work across crashes: a
  change re-emitted after a restart keeps its id. The consumer rejects an
  envelope whose id is not derived this way.
- **Fingerprint.** The consumer hashes the fields that define a change, not
  `captured_at` or `batch_id`, so a re-emitted change is recognized as a
  duplicate while the same id with different content is a conflict.
- **Strict and versioned.** Unknown fields are rejected and unknown versions are
  quarantined rather than guessed at. After a consumer upgrade they can be
  replayed. Source-level schema drift, such as a new column, does not touch the
  envelope: it arrives in `payload` and is stored in JSONB.
- **Headers** repeat `event_id`, `batch_id`, `event_type` and `schema_version`,
  so correlation survives even when a body cannot be parsed.

**Partitioning key: `entity_key` (the order key).** Every version of an order
lands in the same partition and is consumed in order. The key has high
cardinality, so load spreads evenly over the 6 partitions (the consumer
parallelism ceiling). The customer key was the alternative: it would co-locate a
customer's orders, but nothing downstream needs per-customer ordering, and busy
customers would create hot partitions. The consumer's version guard means even
a future repartitioning (which remaps keys) cannot move state backwards.

The topic is created explicitly by `init`, with auto-creation disabled on the
broker so a misspelt name fails loudly, and with infinite retention: the topic
is the log the sink is rebuilt from.

**Delivery guarantee: at-least-once.** The producer runs with `acks=all` and
`enable.idempotence=true` (the broker drops duplicates caused by the producer's
own retries). `publish` blocks until every event has a delivery report.
Unconfirmed events are re-sent with exponential backoff and jitter. If some are
still unconfirmed after `KAFKA_PUBLISH_MAX_ATTEMPTS`, the batch is marked failed,
the watermark stays put, and the cycle is retried with capped backoff while the
adapter keeps running. Non-retriable errors (oversized record, authorization)
stop the adapter instead of skipping a record. The consequence, duplicates, is
handled downstream. Verified two ways: `test_publisher_resilience.py` pauses the
broker mid-publish and checks that nothing is lost, and stopping Redpanda for 45
seconds in the compose stack shows the adapter retrying and then committing all
3,500 pending events within a second of the broker's return.

### Consumer and event-inspection API

Each consumed batch is processed in one PostgreSQL transaction, with a savepoint
per record, and the Kafka offsets are committed only after that transaction
commits. Per record:

1. **Envelope schema.** Invalid JSON, missing or ill-typed fields, an unknown
   version or a non-derived id: quarantined as `schema_violation`, with the raw
   record.
2. **Duplicate check** on `event_id`. Identical content is a redelivery, which
   is expected under at-least-once: skipped and counted in
   `duplicates_skipped`. Different content under a known id is quarantined as
   `event_id_conflict`.
3. **Quality rules.** A `reject` violation quarantines the event as
   `quality_rule_failed` and the entity keeps its last good version. `warn`
   violations are stored on the event-log row.
4. **Append** to `event_log` (unique `event_id`, append-only: UPDATE and DELETE
   are rejected by a trigger), then **upsert** `entity_current_state` guarded by
   `WHERE current.entity_version < new.entity_version`. Arrival order therefore
   does not matter. A value PostgreSQL cannot store (for example a NUL
   character) rolls back to the savepoint and is quarantined as `sink_rejected`,
   without failing the batch.

Transient database errors retry the whole batch with backoff; nothing is
committed to Kafka until the database has it.

**Replay.** `make replay` stops the consumer, snapshots the end offsets, lets
the group catch up to them, takes checksums of the current state, event log
and quarantine, replays offsets 0 to the snapshot through the same code, and
compares. `make rebuild` truncates the sink in between, so the state is rebuilt
from scratch. On the compose stack, after an initial sync, one simulation and
the bad-event injection:

```text
make replay    mode reprocess  replayed 20059  {duplicate: 20051, quarantined: 8}             converged: true
make rebuild   mode rebuild    replayed 20059  {applied: 20050, duplicate: 1, quarantined: 8} converged: true
               state_checksum  ea13e1d3…  ->  ea13e1d3…   (event log and quarantine checksums identical too)
```

**API** (OpenAPI docs at `/docs`):

| Endpoint | What it returns |
| --- | --- |
| `GET /events` | Accepted events, newest first. Filters: `entity_key`, `entity_type`, `event_type`, `batch_id`, `occurred_after`/`occurred_before` (ISO-8601 with offset). Keyset pagination via `limit` and the returned `next_cursor`, which stays stable while new events arrive. |
| `GET /events/{event_id}` | One event, with its Kafka coordinates and lag. |
| `GET /entities/{key}` | Current state, accepted history (oldest first) and quarantined records for the key. `409` if the key exists for several entity types (then pass `entity_type`). |
| `GET /quarantine` | Rejected records with reason, details and raw value. Filters: `reason`, `entity_key`. |
| `GET /stats` | Counts by event type, quarantine counts by reason, duplicates skipped, lag percentiles (source write to sink, split into capture and delivery legs), latest timestamps, the adapter's watermark, recent batches, consumer-group lag per partition, and with `?checksums=true` the convergence checksums. |
| `GET /healthz`, `GET /readyz` | Liveness; readiness (database reachable). |

The API's database sessions run with `default_transaction_read_only=on`, so it
cannot write to the sink even by mistake.

### Data quality and failure routing

| Check | Where | What happens to a failing record |
| --- | --- | --- |
| Envelope schema | consumer | quarantined `schema_violation`, raw bytes kept |
| Duplicate event id, same content | consumer | skipped, counted in `duplicates_skipped` |
| Duplicate event id, different content | consumer | quarantined `event_id_conflict` |
| Rules with `severity: reject` (not null, non-negative total, known status, key matches envelope, ...) | consumer | quarantined `quality_rule_failed`; state keeps the last good version |
| Rules with `severity: warn` (order date after capture, comment length) | consumer | accepted, violation stored in `event_log.quality_warnings` |
| Value the database cannot store | consumer | quarantined `sink_rejected`; the rest of the batch continues |
| Row violating the source contract (NULL key/version/timestamp) | adapter | adapter stops (fail loudly); nothing is skipped |

The checks run in the consumer because that is the trust boundary of the sink:
it cannot trust its input, whatever the producer. The topic stays a faithful
record of what the source said. After fixing a rule, a replay lets previously
quarantined events flow in. Rules are pure functions of the event (there is
deliberately no "not in the future of the wall clock" check), so a replay
always reaches the same verdicts. Each quarantine row records the fingerprint
of the ruleset that rejected it. `make simulate` produces rule failures through
the real pipeline; `make inject-bad-events` publishes malformed JSON, missing
fields, an unsupported version, a non-derived id, an exact duplicate and a
conflicting duplicate straight to the topic.

### Observability

- **Structured JSON logs** on stdout (librdkafka, uvicorn and Snowflake logs
  included), with the correlation ids bound as context:
  - `batch_id`: logged by the adapter (`batch_committed`), carried in the
    record and its headers, logged by the consumer (`batch_processed` lists the
    adapter batches), stored in `event_log.batch_id`, and filterable in
    `GET /events?batch_id=`.
  - `event_id`, `kafka_partition`, `kafka_offset`: bound for each record in the
    consumer (quarantines and warnings log at WARNING, per-event logs at DEBUG).
  - `request_id`: every API request, echoed in `x-request-id`.

  `make trace ID=<batch_id|event_id>` greps one id across the adapter and
  consumer logs. Tracebacks are structured but omit frame locals, which could
  otherwise copy payloads into the logs.
- **`/stats`**: see above.
- **Health**: the workers touch a heartbeat file in every loop iteration
  (including backoff waits) and compose healthchecks flag a stale one; the API
  is checked through `/readyz`.

## Configuration

**Every setting lives in `.env`**, and only there. [`settings.py`](src/switch_pipeline/settings.py)
is the only module that reads the environment. It declares and validates each
variable without a default, so a missing value fails at startup with the
variable's name instead of silently using something hidden in code.
`docker-compose.yml` interpolates ports, image versions and credentials from the
same file. Tests enforce that `.env.example`, the settings classes and the
compose file agree: no variable is missing and none is stale. Each service
loads only the groups it needs, so the API never requires Snowflake
credentials.

**Every data-quality rule lives in [`config/quality_rules.yaml`](config/quality_rules.yaml).**
Adding a rule for an existing check (`not_null`, `min`, `max`,
`allowed_values`, `pattern`, `max_length`, `matches_entity_key`,
`not_after_captured_at`) is a YAML edit followed by `make restart-consumer`. The
file is mounted, so no rebuild is needed. A new kind of check is one small model
class in [`rules.py`](src/switch_pipeline/quality/rules.py). The file is
validated at startup: an unknown check, a typo in an option or a duplicate rule
name stops the consumer.

**What is deliberately not configurable:** the delivery guarantee depends on
`acks=all` and producer idempotence, and correctness depends on committing
offsets after the database; those are fixed in code with the reason next to
them.

**Credentials** are never in code or git: `.env` and `secrets/` are
git-ignored. Snowflake no longer allows password sign-in for service users, so
the pipeline uses key-pair (JWT) authentication as a `TYPE = SERVICE` user,
created by [`snowflake/setup.sql`](snowflake/setup.sql). The private key is
mounted read-only at `/run/secrets` (on Linux hosts, `chown 10001` the key file
for the container user). `SNOWFLAKE_PASSWORD` accepts a password or a
programmatic access token instead. Containers run as a non-root user.

## Operations

| Command | Purpose |
| --- | --- |
| `make up` / `down` / `reset` | Start (build) / stop / stop and delete all data |
| `make ps`, `make logs` | Status and health; follow the pipeline logs |
| `make seed` | Create and fill the source table (skipped if it already has rows) |
| `make simulate` | Inserts, updates and invalid rows (counts from `SIMULATE_*`; override with `docker compose run --rm tools simulate --inserts N`) |
| `make inject-bad-events` | Malformed, duplicate and conflicting records straight to the topic |
| `make replay` / `make rebuild` | Replay from offset 0 over the sink / into an empty sink, and verify convergence |
| `make stats`, `make events`, `make quarantine` | Query the API |
| `make trace ID=...` | Follow a batch or event id through the logs |
| `make restart-consumer` | Apply edited quality rules |
| `make check-config` | Validate `.env` and the rules file |
| `make check-snowflake` | Check the key, sign-in, grants, source table and sample share |
| `make check` | Lint, type-check and all tests (what CI runs) |
| `COMPOSE_PROFILES=ui` in `.env` | Adds Redpanda Console on `REDPANDA_CONSOLE_HOST_PORT` |

## Troubleshooting

`make check-snowflake` walks through what the first sync needs and prints the
likely fix for the first thing that fails:

```text
ok    private_key   /run/secrets/snowflake_rsa_key.p8 is readable
ok    sign_in       signed in to myorg-myaccount with a key pair
ok    database      SWITCH_DEMO is visible to the role
FAIL  source_table  SQL compilation error: Object 'SWITCH_DEMO.RAW.CUSTOMER_ORDERS' does not exist or not authorized.
                    hint: run `make seed`, which creates the table; otherwise check the role's grants
```

| Symptom | Fix |
| --- | --- |
| `JWT token is invalid` | The public key on the user does not match the private key: rerun the `CREATE USER ... RSA_PUBLIC_KEY` step of `snowflake/setup.sql` with the key printed by `make snowflake-keypair` (or `ALTER USER SWITCH_PIPELINE SET RSA_PUBLIC_KEY = '...'`). |
| `Failed to connect` / `404` at sign-in | `SNOWFLAKE_ACCOUNT` must be `<orgname>-<account_name>`; the last query of `setup.sql` prints it. |
| `Incorrect username or password` / MFA required | Service users cannot use passwords; use the key pair, or put a programmatic access token in `SNOWFLAKE_PASSWORD`. |
| Key file unreadable on a Linux host | The container runs as uid 10001: `sudo chown 10001 secrets/snowflake_rsa_key.p8`. |
| Adapter logs `source_unavailable` | Expected until `make seed` has created the table; the adapter retries with backoff and starts syncing on its own. |
| `make replay` waits on `waiting_for_idle_consumer_group` | A consumer that was stopped mid-join stays listed until the broker evicts it (up to 45 s); the tool waits for it. |
| Ports already in use | Change `API_HOST_PORT`, `POSTGRES_HOST_PORT` or `KAFKA_HOST_PORT` in `.env`. |

## Testing

`make check` runs ruff, `mypy --strict` and pytest: 121 unit tests and 39
integration tests.

- **Unit** (no Docker): watermark ordering and keyset query construction,
  envelope validation and deterministic ids, every quality check, row-to-event
  mapping, configuration consistency, and the sync loop against in-memory
  implementations of its ports. The sync-loop tests cover restarts, reruns
  emitting nothing, ties at batch boundaries, the settle window, a failed
  publish leaving the watermark in place (and the retry re-emitting identical
  ids), and shutdown between batches.
- **Integration** (Docker, via testcontainers, with the same images as the
  compose stack): idempotent upserts on a real PostgreSQL (redeliveries,
  out-of-order delivery, shuffled replays converging to identical checksums,
  conflicts, quarantine idempotency, unstorable values), the state store
  (restart, failed and interrupted batches, the single-writer lock, append-only
  trigger, migration checksums), the adapter's Snowflake SQL through the real
  connector against fakesnow, the full pipeline end to end including a rebuild
  from offset 0, a broker outage, and the API.

The idempotency guarantees live in SQL (`ON CONFLICT` and the version guard),
so those tests run against PostgreSQL rather than a mock of it. fakesnow is a
test double: it validates the adapter's SQL and the connector path, not full
Snowflake semantics. Everything that differs from real Snowflake (the account,
the sample share, key-pair auth) is plain configuration.

## Design decisions and trade-offs

- **Query-based capture with a watermark** rather than Snowflake Streams. It
  follows the brief, is transparent to reason about and needs only `SELECT` on
  the source. It costs deletes and intermediate versions (see scope cuts), and
  it depends on writers honoring the contract.
- **At-least-once plus an idempotent sink** rather than Kafka transactions. It
  is simpler, works with any consumer, and the dedup table (the event log) is
  needed for replays anyway.
- **Deterministic event ids and fingerprints.** They separate "the same change
  again" (skip) from "a different change claiming the same identity"
  (quarantine), which a random UUID cannot do.
- **The version guard on current state** makes materialization independent of
  arrival order, which is what replays and producer retries need.
- **Quality enforced at the sink, not at the source.** The stream is a faithful
  log and one enforcement point covers every producer. The cost is that bad
  records travel through Kafka.
- **A strict, versioned envelope** rather than a tolerant reader. Mistakes
  surface immediately, and evolution happens through `schema_version` with a
  consumer-first rollout; payload-level drift needs no envelope change.
- **One topic per source entity with infinite retention.** Replay and rebuild
  are always possible. The price is unbounded growth, which production would
  handle with tiered storage or snapshots.
- **One image, many commands, and crash-only workers.** Every error either
  retries with backoff or exits for the orchestrator to restart; because state
  only moves after a durable write, a crash is always safe.

## Exactly-once effect: the failure windows that remain

This is the stretch goal I attempted. The event log keyed by the deterministic
`event_id` is a consumer-side dedup table, and it commits in the same
transaction as the state it protects. Offsets are committed afterwards.

| Failure | Result |
| --- | --- |
| Producer-internal retries | Deduplicated by the broker (idempotent producer). |
| Adapter crashes after the broker acknowledged a batch but before the watermark commit | The batch is re-emitted with identical ids, and the consumer counts duplicates. |
| A delivery report times out although the broker stored the record | The record is re-sent; the consumer skips the duplicate. |
| Consumer crashes after the database commit but before the offset commit | The batch is redelivered; every write is a no-op. |
| Consumer crashes before the database commit | The transaction rolls back and the batch is redelivered. |

What still breaks the guarantee, or only detects the problem:

1. **A writer changes a row without bumping its version.** The consumer sees the
   same id with different content and quarantines it (`event_id_conflict`).
   That is detection, not prevention.
2. **A source transaction longer than the settle window** can commit rows below
   a watermark that already moved past them. Those rows would be missed. This
   is inherent to timestamp-based capture; Streams remove it.
3. **Several writes between two polls** collapse into the latest version. The
   state converges, but the event log lacks the intermediate versions.
4. **Hard deletes are invisible.**
5. **Effects outside the sink transaction** (calling an external API, say) would
   not be covered; the dedup only protects writes in the same database
   transaction.
6. **A rule change followed by a replay without rebuild** re-evaluates only
   records not yet logged. That is deliberate: fixing a rule lets quarantined
   events flow in, while accepted events stay accepted. A rebuild re-evaluates
   everything.

## Scope cuts

Cut features, not correctness:

- **No deletes.** Hard deletes are invisible to query-based capture, and a
  soft-delete event type is not implemented. `EventType` is an exhaustive
  `match` in the consumer, so adding `delete` fails the type check until it is
  handled end to end.
- **No schema registry.** JSON plus a generated JSON Schema, versioned in git,
  with a drift test.
- **A single-node broker** (replication factor 1) and a single consumer instance.
  Consumers scale out to the partition count without code changes.
- **No metrics endpoint or tracing.** `/stats` and structured logs cover the
  brief.
- **One source table per adapter process.** The adapter is generic over the
  table contract (`SOURCE_*` settings), but there is no scheduler for many
  sources.
- **Snowflake verified through an emulator in tests.** The code path is the
  real connector; the first run against a real account happens with your
  credentials.

## What I would do with more time

- **Commit-time capture with Snowflake Streams:** `INSERT INTO staging SELECT ... FROM stream`
  advances the stream offset transactionally, which captures deletes and removes
  the settle-window trade-off.
- **Atomic publish and watermark.** Store the watermark in a compacted topic and
  write events and watermark in one Kafka transaction (the KIP-618 pattern),
  closing the re-emission window at the source.
- **Soft deletes end to end** (a `delete` event type that tombstones current
  state).
- **Prometheus metrics** (lag, throughput, quarantine rate, backoff state) and
  **OpenTelemetry tracing**, with the trace context in Kafka headers.
- A **schema registry** with compatibility checks in CI.
- **Separate database roles** for adapter, consumer and API, and a
  control-plane database separate from the sink.
- **Time-partitioned `event_log`** with archival, plus a "replay quarantined
  records" tool for after a rule fix.
- **Load tests**, several consumer instances, and Helm charts with secrets from
  a vault.

## Multi-tenant operation

*Design only; not implemented.*

**Identity everywhere.** Add `tenant_id` and `connection_id` to the envelope, to
the event-id derivation (a per-tenant UUID namespace, so ids can never collide
across tenants), to every state key, and to log context and metrics labels.

**State isolation.**

- The control plane (connections, watermarks, batch ledgers, locks) lives in its
  own database, keyed by `(tenant_id, source_id)`, with row-level security. The
  advisory lock becomes a lease with a fencing token held by the worker that
  owns the task.
- Sink data is isolated per tenant: a schema per tenant (or a database per
  tenant for large and regulated ones), or shared tables partitioned by
  `tenant_id` with row-level security for many small tenants.
- Deleting a tenant must be a cheap, provable operation: drop a schema, delete
  its topics, delete its secrets.

**Topics.** Namespaced topics, `tenant.<id>.<source>.<entity>.v1`, give
per-tenant retention, quotas, prefix ACLs and trivial deletion. The cost is
partition count: 100 tenants × 10 sources × 6 partitions is 6,000 partitions,
fine for Kafka or Redpanda, but the reason to size partitions per source by
throughput (1 to 3 for most) instead of a flat 6. For a long tail of tiny
tenants, pooled topics with `tenant_id` in key and headers, and a router at the
sink, keep the partition count bounded. The choice can be made per tenant tier.

**Credentials.** No shared `.env`. Each connection references a secret in a
vault (Vault, AWS Secrets Manager), fetched at task start and refreshed:
key-pair or OAuth / workload identity federation per tenant, a separate Snowflake
service user per tenant connection, and rotation without a redeploy. Kafka
principals are per tenant with prefixed ACLs; database access goes through
per-tenant roles.

**Compute and backpressure.** Replace one process per source with a pool of
workers and a scheduler that assigns `(tenant, source)` tasks, like Kafka
Connect's distributed mode: per-tenant concurrency limits, fair scheduling, and
bounded in-flight batches per task. When the sink falls behind, consumer lag
per tenant drives both alerts and throttling of that tenant's adapters. Polling
frequency adapts to change rate, since every poll costs the tenant warehouse
credits.

**What breaks first at 100 tenants × 10 sources** with this design as it
stands: 1,000 adapter processes and Snowflake sessions (fixed by the scheduler);
warehouse cost of polling every 10 seconds (adaptive polling, or Streams with
change notifications); PostgreSQL connections and write contention from many
consumers (PgBouncer, batching, partitioned or per-tenant sinks); and
operational load from topics and ACLs (automated provisioning from the control
plane).

## CDC with Snowflake Streams or Debezium

- **Snowflake Streams** track changes on a table from an offset and return net
  changes with `METADATA$ACTION`, `METADATA$ISUPDATE` and `METADATA$ROW_ID`. The
  offset only advances when the stream is read inside a DML transaction, so a
  correct design reads it into a staging table (the transaction boundary) and
  publishes from there with its own watermark. That gives commit-time
  consistency, captures deletes and needs no writer contract. It still yields
  net changes only (no intermediate versions), must be consumed within the data
  retention period or it goes stale, and requires change tracking on the table.
- **Debezium** does log-based CDC for OLTP databases (PostgreSQL WAL, MySQL
  binlog, and others): every change including deletes and intermediate
  versions, in commit order, with before and after images. Snowflake exposes no
  such log, so Debezium applies to the operational sources behind a warehouse,
  not to Snowflake itself. Its event model (`op` c/u/d/r, `before`/`after`,
  `source`, `ts_ms`) maps directly onto this envelope; a snapshot read would be
  an additional `event_type`.

## Repository layout

```text
.env.example               every setting (copy to .env)
config/quality_rules.yaml  every data-quality rule
docker-compose.yml, Dockerfile, Makefile
snowflake/setup.sql        one-time Snowflake role, warehouse, user (key pair)
schemas/                   generated JSON Schema of the envelope
scripts/demo.sh            recording walkthrough
src/switch_pipeline/
  settings.py              the only module that reads the environment
  domain/                  envelope, quarantine reasons
  adapter/                 cursor, Snowflake source, mapper, state store + lock, publisher, sync loop
  consumer/                decoding, processor, repository, consume loop
  quality/                 rule engine
  api/                     FastAPI app, queries, consumer-lag inspector
  db/                      migrations and shared SQL
  tools/                   seed, simulate, inject-bad-events, replay
  cli.py                   `switch-pipeline <command>` for every service and tool
tests/unit, tests/integration
```
