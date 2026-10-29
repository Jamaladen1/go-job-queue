# Implementation Status — MVP v1.0 (29 Oct 2026)

## M1 — scaffold, config, migrations, compose, health ✅
- [x] Go module, `internal/config` (env-only, validates timeout < lease)
- [x] Embedded SQL migrations + advisory-locked runner, applied on API and worker startup
- [x] `compose.yaml`: postgres + api + worker-1 + worker-2, healthchecks, `depends_on: service_healthy`
- [x] `/health/live`, `/health/ready` (503 when the DB is unreachable)
- Verified: `docker compose up -d --build` → all four services up, api healthy; `curl /health/ready` → `{"database":"ok","status":"ready"}`

## M2 — schema + repository with atomic claim and fencing ✅
- [x] `jobs`, `job_events`, `workers` tables; status CHECK; lease/status consistency CHECK
- [x] Claim with `FOR UPDATE SKIP LOCKED`, priority DESC / created_at ASC, attempts++ on claim, per-attempt `lease_token`
- [x] Fenced `Complete` / `Fail` / `Release` (`status='running' AND lease_token=$t AND lease_until > now()`)
- [x] `stale_ack_rejected` event recorded when a fenced ack is refused
- [x] Integration tests: concurrent claims, SKIP LOCKED, ordering, fencing, attempts cap

## M3 — worker loop, handlers, retry/backoff, recovery, shutdown ✅
- [x] Handler registry: `sleep`, `compute` (deterministic SHA-256), deterministic failure simulation
- [x] Backoff `min(2^(n-1)·2s, 60s)` ±10% jitter, injectable for tests
- [x] Reaper on every worker (`REAP_INTERVAL_SECONDS`), atomic and safe to run concurrently
- [x] Heartbeats, graceful shutdown (stop claiming → drain within grace → cancel + release)
- [x] Tests: two worker loops sharing work, crash → reclaim, graceful-shutdown release

## M4 — API validation, pagination, metrics, events ✅
- [x] Strict JSON validation (unknown fields rejected), 64 KB body cap, 16 KB payload cap
- [x] Keyset pagination with opaque cursor
- [x] `/api/metrics`, `/api/workers`, `/api/events`, `/api/config`
- [x] `POST /api/jobs/{id}/retry` (dead/failed only, 409 otherwise, attempts reset, logged)
- [x] Optional API key on writes (401)
- [x] API unit tests with a fake store + curl smoke test against the running stack

## M5 — dashboard, demo, docs ✅ (screenshots taken by the parent session)
- [x] Embedded dashboard: status cards, workers + heartbeat, jobs table with filters, job detail + timeline, event stream, batch form + presets, at-least-once label
- [x] `scripts/demo.sh` crash/reclaim demo (verified: worker-1 SIGKILLed, job reclaimed by worker-2 as attempt 2, exactly one ack per job)
- [x] `scripts/fencing-demo.sh` live fencing demo (pause/unpause past lease → stale ack rejected)
- [x] `scripts/seed.sh` demo data
- [x] README (architecture, API, trade-offs, limitations, hosted deployment), `.env.example`, CI workflow
- [ ] Live hosted URL (README has instructions; not deployed)

## Commands run and results
| Command | Result |
|---|---|
| `docker compose up -d --build` | 4 services up; api healthy |
| `./scripts/test.sh -v` | all packages `ok`: 17 unit + 13 integration tests PASS |
| `./scripts/test.sh -race` | all packages `ok`, no races |
| `./scripts/fencing-demo.sh` | worker-1 paused past lease → jobs reclaimed by worker-2 → on unpause, 2 × `stale_ack_rejected`, worker-2's results kept |
| `./scripts/demo.sh` | orphaned job: `claimed(worker-1) → lease_expired → claimed(worker-2, attempt 2) → succeeded`; 1 ack per job |

## Deviations from spec (intentional)
- No webhook handler (SSRF risk on a public demo). `compute` covers the "deterministic compute" option.
- `failed` status = non-retriable handler error (the spec listed it but defined no transition into it).
- Demo compose uses lease 15s / timeout 8s / max sleep 7s so recovery is visible quickly. Code defaults are the spec's 60s / 20s / 10s.
- Claim, ack, and recovery are each **one** SQL statement (data-modifying CTEs) rather than multi-statement transactions. They are equally atomic, with one round trip.
- The worker table has extra columns (`inflight`, `jobs_succeeded`, `jobs_failed`) for the dashboard.
- There's an extra endpoint, `GET /api/events` (global event stream), and `GET /api/config` for the UI.
