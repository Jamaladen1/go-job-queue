#!/usr/bin/env bash
# Seeds a realistic mix of demo jobs: successes, retries that recover,
# dead letters, a permanent failure, and CPU-bound compute jobs.
set -euo pipefail
API="${API:-http://localhost:8080}"
KEY_HDR=(); [[ -n "${API_KEY:-}" ]] && KEY_HDR=(-H "X-API-Key: $API_KEY")
post() { curl -fsS "${KEY_HDR[@]+"${KEY_HDR[@]}"}" -X POST "$API/api/jobs" -d "$1" >/dev/null; }

for i in $(seq 1 6); do post '{"type":"compute","payload":{"input":"seed-'"$i"'","iterations":300000}}'; done
for d in 400 800 1200 1600 2000 2400 900 1500; do post '{"type":"sleep","payload":{"duration_ms":'"$d"'}}'; done
for i in 1 2 3; do post '{"type":"sleep","payload":{"duration_ms":500,"fail_until_attempt":3},"max_attempts":3}'; done
for i in 1 2; do post '{"type":"sleep","payload":{"duration_ms":300,"fail_until_attempt":9},"max_attempts":3}'; done
post '{"type":"sleep","payload":{"duration_ms":200,"permanent_fail":true}}'
post '{"type":"compute","payload":{"input":"urgent","iterations":200000},"priority":50}'
echo "Seeded 21 jobs. Retries finish after ~6s of backoff; open $API"
