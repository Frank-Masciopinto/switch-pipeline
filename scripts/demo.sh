#!/usr/bin/env bash
# Walkthrough for the 3-5 minute screen recording:
# stack up -> initial sync -> change simulation -> /events and /stats ->
# quarantine -> replay and rebuild with verified convergence.
# Interactive when run from a terminal (press enter between steps).
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo "Missing .env: run 'make env' and configure Snowflake first."; exit 1; }
api_port=$(grep -E '^API_HOST_PORT=' .env | tail -n 1 | cut -d= -f2)
API="http://localhost:${api_port}"

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
pause() { if [[ -t 0 ]]; then read -rp "    press enter to continue "; fi; }
json() { python3 -c "import json, sys; data = json.load(sys.stdin); $1"; }

stats_line() {
  curl -fsS "$API/stats" | json '
e = data["events"]; w = (data["watermarks"] or [{}])[0]
print("    events=%s by_type=%s entities=%s quarantined=%s duplicates_skipped=%s consumer_lag=%s"
      % (e["total"], e["by_type"], data["entities"], data["quarantine"]["total"],
         data["duplicates_skipped"], data["consumer_lag"]["total"]))
print("    watermark=(%s, %s)" % (w.get("cursor_updated_at"), w.get("cursor_key")))'
}

entities() {
  curl -fsS "$API/stats" 2>/dev/null | json 'print(data["entities"])' 2>/dev/null || echo 0
}

wait_for_entities() {
  local expected=$1 current=0
  for _ in $(seq 1 90); do
    current=$(entities)
    ((current >= expected)) && { stats_line; return 0; }
    sleep 2
  done
  echo "    timed out waiting for $expected entities (have: $current)"
  return 1
}

wait_for_api() {
  for _ in $(seq 1 60); do
    curl -fsS "$API/readyz" >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "    API did not become ready"
  return 1
}

step "1. Start the stack: Redpanda, PostgreSQL, adapter, consumer, API"
make up
wait_for_api
docker compose ps
pause

step "2. Seed the Snowflake source table; the adapter runs the initial full sync"
seeded=$(docker compose run --rm -T tools seed 2>/dev/null)
echo "$seeded"
rows=$(echo "$seeded" | json 'print(data["rows"])')
wait_for_entities "$rows"
pause

step "3. Simulate inserts, updates and invalid rows in Snowflake"
baseline=$(entities)
changes=$(docker compose run --rm -T tools simulate 2>/dev/null)
echo "$changes" | json '
print("    inserted:", data["inserted"][:5], "... updated:", data["updated"][:5], "...")
print("    invalid: ", data["invalid"])'
accepted=$(echo "$changes" | json 'print(len(data["inserted"]))')
echo "    waiting for the adapter (settle window + poll interval) and the consumer..."
wait_for_entities "$((baseline + accepted))"
pause

step "4. GET /events: the newest change events, with Kafka coordinates and lag"
curl -fsS "$API/events?limit=3" | json '
for e in data["items"]:
    print("    %-6s key=%6s v%s batch=%s partition=%s offset=%s lag=%.1fs"
          % (e["event_type"], e["entity_key"], e["entity_version"], e["batch_id"],
             e["kafka"]["partition"], e["kafka"]["offset"], e["lag_seconds"]))'
pause

step "5. GET /entities/{key} for the rejected update: state keeps the last good version"
key=$(echo "$changes" | json 'print(next(i["key"] for i in data["invalid"] if i["kind"] == "unknown_order_status"))')
curl -fsS "$API/entities/$key" | json '
print("    current version:", data["current"]["entity_version"], "status:", data["current"]["payload"]["o_orderstatus"])
print("    quarantined:", [(q["reason"], [v["rule"] for v in q["details"]["violations"]]) for q in data["quarantined"]])'
pause

step "6. GET /stats: counts by type, lag, watermark, consumer lag, checksums"
curl -fsS "$API/stats?checksums=true" | python3 -m json.tool | head -n 60
pause

step "7. Replay the topic from offset 0 over the live sink: nothing changes"
make replay 2>/dev/null | tail -n 25
pause

step "8. Truncate the sink and rebuild it from the topic: identical checksums"
make rebuild 2>/dev/null | tail -n 25
