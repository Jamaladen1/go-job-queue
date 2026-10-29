#!/usr/bin/env bash
# Crash/recovery demo: submit long jobs, SIGKILL worker-1 mid-job, watch its
# leases expire and worker-2 reclaim them. Requires curl + jq and the compose
# stack running (docker compose up -d --build).
set -euo pipefail
cd "$(dirname "$0")/.."
API="${API:-http://localhost:8080}"
KEY_HDR=(); [[ -n "${API_KEY:-}" ]] && KEY_HDR=(-H "X-API-Key: $API_KEY")
LEASE=$(curl -fsS "$API/api/config" | jq -r .lease_seconds)

say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }

say "Submitting 6 sleep jobs of 6s each (lease=${LEASE}s, 2 workers × 2 slots)"
IDS=()
for i in $(seq 1 6); do
  IDS+=("$(curl -fsS "${KEY_HDR[@]+"${KEY_HDR[@]}"}" -X POST "$API/api/jobs" \
    -d '{"type":"sleep","payload":{"duration_ms":6000},"max_attempts":3}' | jq -r .id)")
done
printf '  %s\n' "${IDS[@]}"

say "Waiting for worker-1 to hold leases in both slots…"
for _ in $(seq 1 30); do
  VICTIMS=$(curl -fsS "$API/api/jobs?status=running&limit=50" | jq -r '[.jobs[] | select(.lease_owner=="worker-1") | .id] | join(" ")')
  [[ $(wc -w <<<"$VICTIMS") -ge 2 ]] && break
  sleep 0.5
done
[[ -z "$VICTIMS" ]] && { echo "worker-1 never claimed a job"; exit 1; }
curl -fsS "$API/api/jobs?status=running&limit=50" | jq -r '.jobs[] | "  \(.id[0:8])  running on \(.lease_owner)  attempt \(.attempts)  lease_until \(.lease_until)"'

say "💥 docker compose kill worker-1   (SIGKILL: no cleanup, no release, leases left dangling)"
docker compose kill worker-1 >/dev/null 2>&1
sleep 1
curl -fsS "$API/api/workers" | jq -r '.workers[] | "  \(.worker_id): \(.state)  heartbeat \(.heartbeat_age_ms)ms ago"'

say "Waiting for the leases to expire and worker-2's reaper to reclaim them (≈${LEASE}s)…"
START=$(date +%s)
for _ in $(seq 1 90); do
  DONE=0
  for id in $VICTIMS; do
    S=$(curl -fsS "$API/api/jobs/$id" | jq -r .job.status)
    [[ "$S" == "succeeded" ]] && DONE=$((DONE+1))
  done
  printf '\r  %ds elapsed — %d/%d orphaned jobs recovered' $(( $(date +%s) - START )) "$DONE" "$(wc -w <<<"$VICTIMS")"
  [[ "$DONE" -eq "$(wc -w <<<"$VICTIMS")" ]] && break
  sleep 1
done
echo

say "Timeline of each orphaned job"
for id in $VICTIMS; do
  echo "  job ${id:0:8}:"
  curl -fsS "$API/api/jobs/$id" | jq -r '.events[] | "    \(.created_at[11:19])  \(.event_type | .[0:18])  \(.detail | del(.lease_token, .lease_until) | tostring)"'
done

say "Acknowledgement check: each job has exactly one 'succeeded' event"
for id in "${IDS[@]}"; do
  N=$(curl -fsS "$API/api/jobs/$id" | jq '[.events[] | select(.event_type=="succeeded")] | length')
  echo "  ${id:0:8}: $N ack(s)"
done

say "Restarting worker-1"
docker compose up -d worker-1 >/dev/null 2>&1
sleep 3
curl -fsS "$API/api/workers" | jq -r '.workers[] | "  \(.worker_id): \(.state)"'
curl -fsS "$API/api/metrics" | jq -c '{counts, leases_expired, stale_acks_rejected, retries_scheduled}'
