#!/usr/bin/env bash
# Fencing demo: freeze worker-1 (docker pause = SIGSTOP-like freeze) while it
# holds leases, let the leases expire and worker-2 reclaim the jobs, then
# unpause. worker-1 "wakes up", finishes its handlers and tries to ack with its
# old lease tokens — the database refuses (stale_ack_rejected).
set -euo pipefail
cd "$(dirname "$0")/.."
API="${API:-http://localhost:8080}"
KEY_HDR=(); [[ -n "${API_KEY:-}" ]] && KEY_HDR=(-H "X-API-Key: $API_KEY")
LEASE=$(curl -fsS "$API/api/config" | jq -r .lease_seconds)
say() { printf '\n\033[1;35m▶ %s\033[0m\n' "$*"; }

say "Submitting 4 sleep jobs of 5s"
for i in 1 2 3 4; do
  curl -fsS "${KEY_HDR[@]+"${KEY_HDR[@]}"}" -X POST "$API/api/jobs" -d '{"type":"sleep","payload":{"duration_ms":5000}}' >/dev/null
done
for _ in $(seq 1 30); do
  VICTIMS=$(curl -fsS "$API/api/jobs?status=running&limit=50" | jq -r '[.jobs[] | select(.lease_owner=="worker-1") | .id] | join(" ")')
  [[ -n "$VICTIMS" ]] && break; sleep 0.3
done
echo "  worker-1 holds: $VICTIMS"

say "⏸  docker compose pause worker-1 for $((LEASE + 6))s (lease ${LEASE}s + reaper)"
docker compose pause worker-1 >/dev/null 2>&1
sleep $((LEASE + 6))
for id in $VICTIMS; do
  curl -fsS "$API/api/jobs/$id" | jq -r '"  \(.job.id[0:8]) now \(.job.status) on \(.job.lease_owner // "-") (attempt \(.job.attempts))"'
done

say "▶  docker compose unpause worker-1 — its late acks should be fenced"
docker compose unpause worker-1 >/dev/null 2>&1
sleep 3
for id in $VICTIMS; do
  curl -fsS "$API/api/jobs/$id" | jq -r '.events[] | select(.event_type=="stale_ack_rejected" or .event_type=="succeeded" or .event_type=="lease_expired") | "  \(.job_id[0:8])  \(.event_type)  \(.detail | del(.presented_token) | tostring)"'
done
curl -fsS "$API/api/metrics" | jq -c '{stale_acks_rejected, leases_expired}'
