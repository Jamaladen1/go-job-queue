# Test Report — go-job-queue

Date: 29 Oct 2026 · Environment: macOS (darwin 25.5), Docker via Colima, `golang:1.23` test container,
`postgres:16-alpine`. No local Go toolchain; every build and test ran in Docker as the README documents.

The design reference is `README.md` (architecture, invariants, API contract, status codes) and
`IMPLEMENTATION_STATUS.md` (milestones and intentional deviations). There is no separate spec document.

## Summary

| | |
|---|---|
| Automated tests | **42 pass**, 0 fail, under both `go test` and `go test -race`. 30 existed before; 12 are new. The integration package was rerun 5× (`-count=5`) with no flakes |
| Live HTTP / Docker checks | Every endpoint, both demo scripts, seed script, DB outage, SIGTERM drain, grace-expiry release, embedded-worker mode, API-key mode, and concurrent migrations were exercised against the real compose stack |
| Bugs found | **2 functional bugs** (below), both fixed, covered by tests, and re-verified on rebuilt containers |
| Verdict | Ready for the documented demo / portfolio deployment. See "Remaining issues" before production use |

## Bugs discovered and fixed

### 1. Graceful-shutdown release let a job exceed `max_attempts` (invariant violation)
- **Spec:** "`max_attempts=3` ⇒ at most 3 claims"; `attempts` increments on claim.
- **Observed:** `Store.Release` requeued the job without touching `attempts`. A job released on its final
  attempt was `queued` with `attempts == max_attempts`. The next claim pushed it to `max_attempts + 1`.
  Reproduced by `TestReleaseDoesNotExceedMaxAttempts`, which failed with `attempts=1, want 0` for `max_attempts=1`.
- **Fix (`internal/queue/store.go`):** Release now refunds the interrupted attempt (`attempts = attempts - 1`),
  because a release is a voluntary hand-back, not a job failure. The `released` event still records the attempt
  number that was interrupted. The README trade-offs section documents this.
- **Re-verified live:** a `max_attempts=1` job of 7s, with the worker stopped using a 2s grace, went to
  `queued, attempts=0` with `released {"attempt":1}`. It was then reclaimed and `succeeded` as attempt 1/1.

### 2. Invalid configuration crashed workers or was silently accepted
- **Observed on the real binary:**
  - `HEARTBEAT_SECONDS=0` and `REAP_INTERVAL_SECONDS=0` → `panic: non-positive interval for NewTicker` (exit 2).
  - `POLL_INTERVAL_MS=0` → accepted. An idle worker then busy-polls the database.
  - `LEASE_SECONDS=0 HANDLER_TIMEOUT_SECONDS=-1` → accepted, even though every ack would be fenced.
  - `LEASE_SECONDS=abc` (or `15s`) → silently replaced with the 60s default.
- **Fix (`internal/config/config.go`):** `Validate` now requires lease, handler timeout, poll, heartbeat, reap and
  worker-stale intervals to be positive, and shutdown grace to be non-negative. `Load` rejects non-integer values.
- **Re-verified live:** each case above now logs a clear `invalid config` error and exits with code 1.
  Unit tests: `TestLoadRejectsNonPositiveDurations`, `TestLoadRejectsNonIntegerValues`.

## Requirements verified

| Requirement (README) | How verified | Result |
|---|---|---|
| Two workers never hold the same job | `TestConcurrentWorkersNeverClaimSameJob` (200 jobs × 12 claimers); live: 300 jobs POSTed with 40-way parallelism → SQL check "jobs with >1 claim not explained by retry/expiry/release" = 0 | PASS |
| SKIP LOCKED doesn't block | `TestSkipLockedPassesOverLockedRow` | PASS |
| Priority DESC, then age | `TestClaimOrderPriorityThenAge` | PASS |
| Only current lease token can ack (complete + fail) | `TestFencingRejectsStaleToken`, `TestCrashedWorkerJobIsReclaimed`; live `fencing-demo.sh` → 2× `stale_ack_rejected`, worker-2's result kept | PASS |
| Expired lease → requeued, or dead at max attempts | `TestExpiredLeaseRecovery`, `TestExpiredLeaseAtMaxAttemptsGoesDead`; live `demo.sh` (SIGKILL) | PASS |
| Reaper safe to run concurrently | **new** `TestConcurrentReapersRecoverEachLeaseOnce` (8 reapers, 50 expired leases → exactly 50 recoveries/events) | PASS |
| Ack racing reaper at lease boundary stays consistent | **new** `TestAckRacingReaperAtLeaseBoundary` (150 jobs, 30ms leases, 8 workers, 3 reapers in a loop): every accepted ack = the job's only `succeeded` event, no job left non-terminal, no `attempts > max_attempts` | PASS |
| Retries with backoff `min(2^(n-1)·2s, 60s)` ±10% | unit `TestBackoff*`; live timeline: `backoff_ms` 2006 then 4001, reclaims ~2s/~4s later | PASS |
| Retriable failure at max → dead; permanent → failed | `TestAttemptsCapLeadsToDead`, **new** `TestWorkerPermanentFailureNotRetried`; live jobs ended `dead 2/2` and `failed 1/3` | PASS |
| Handler timeout < lease enforced and handled | config test; **new** `TestHandlerTimeoutRetriesThenDead` (timeout → `retry_scheduled` → `dead`, `last_error` = "handler timeout…") | PASS |
| `attempts` never exceeds `max_attempts` | **new** `TestReleaseDoesNotExceedMaxAttempts` | **FAILED before fix**, PASS after |
| Manual retry: dead/failed only, else 409, attempts reset, logged | `TestPermanentFailureAndManualRetry`, **new** `TestConcurrentManualRetryRequeuesOnce` (20 concurrent → 1 OK / 19 conflict / 1 event); live: 30 parallel HTTP retries → 1×200, 29×409 | PASS |
| `running` row always carries a full lease (DB CHECK) | **new** `TestLeaseStatusConstraintEnforced` | PASS |
| Graceful shutdown: drain, then cancel + release | `TestGracefulShutdownReleasesInflightJob`; live `docker compose stop worker-1`: both 6s jobs finished and acked, worker `stopped`; live 2s-grace container: job `released` | PASS |
| Crash recovery (kill -9) | live `demo.sh`: worker-1 SIGKILLed, leases reaped by worker-2 after ~17s, attempt 2 succeeded, 1 ack per job | PASS |
| DB outage handling | live: Postgres stopped 8s mid-processing → `/health/ready` 503, `/health/live` 200, POST/GET 503 `database unavailable`; workers logged errors and kept running; after restart all 4 in-flight jobs were recovered by lease expiry and succeeded (attempt 2) | PASS |
| Migrations safe under concurrent startup | **new** `TestConcurrentMigrateIsSafe`; live: 5 worker containers started at once on an empty DB → all running, 1 `schema_migrations` row | PASS |
| Keyset pagination stable | `TestListPaginationIsStable`; live `status=succeeded&limit=7` → 56 pages, 392 IDs, 392 unique = count in metrics | PASS |
| Deterministic `compute` | unit test; live digest for `hello`/100000 matched an independent Python SHA-256 loop | PASS |
| API ↔ real store ↔ worker end-to-end | **new** `TestHTTPEndToEnd` (httptest server on the real Postgres store with API key; submit → worker → GET result, retry 409/200, timeline, metrics, workers) | PASS |
| Embedded workers (Render mode) | live: API container with `EMBEDDED_WORKERS=2`, `API_KEY` on a separate DB → 21 jobs split 12/9 across `render-embedded-1/2`; SIGTERM → both workers and API stopped cleanly, rows marked `stopped` | PASS |
| Docker build / compose run | `docker compose build --no-cache` OK (38 MB image); `docker compose up -d` → 4 services, API healthy | PASS |
| Dashboard | browser: page loads, cards/workers/jobs/event stream populated from live API, preset button submitted 10 jobs that completed | PASS (smoke only) |

### Live HTTP contract checks (real `curl` against `localhost:8080`)

| Request | Expected | Actual |
|---|---|---|
| `GET /health/live`, `/health/ready` | 200 | 200, 200 |
| `GET /api/config`, `GET /` | 200 | 200, 200 (HTML) |
| POST sleep / compute / fail_until_attempt / permanent_fail | 201 `{"id","status":"queued"}` | 201 each; final states `succeeded`, `succeeded`, `succeeded` (att 3/3), `failed` |
| POST unknown type, malformed JSON, empty body, unknown top-level field, unknown payload field, trailing data | 400 | 400 each |
| `duration_ms` 0 / 7001 (compose MAX_SLEEP_MS=7000) / 7000 | 400 / 400 / 201 | as expected |
| `max_attempts` 0, `"3"`; `priority` 101, 1.5; payload `[1]`, `"x"` | 400 | 400 each |
| body 70 KB | 413 | 413 |
| `GET /api/jobs?status=bogus`, `limit=201`, `limit=abc`, `cursor=zzz` | 400 | 400 each |
| `GET /api/jobs/<missing>` / `<non-uuid>` | 404 / 400 | 404 / 400 |
| `POST /retry` missing / non-uuid / succeeded job / dead job / failed job | 404 / 400 / 409 / 200 / 200 | as expected |
| `DELETE /api/jobs/<id>` | 405 | 405 |
| `API_KEY` set: POST without key, wrong key, `Authorization: Basic`, retry without key | 401 | 401 each |
| `API_KEY` set: `X-API-Key` and `Authorization: Bearer` with the right key; GET without key | 201 / 201 / 200 | as expected |
| `X-Request-ID: test-123` | echoed | echoed |

### Data consistency (SQL directly on Postgres after the live load test)
Results: 0 succeeded jobs with ≠1 `succeeded` event, 0 jobs missing an `enqueued` event, 0 jobs with `attempts > max_attempts`,
0 succeeded jobs missing `result`/`finished_at`, and 0 re-claims not explained by a retry, expiry, release, or manual retry.
The work split was worker-1=150 and worker-2=171.

## Commands executed

```
./scripts/test.sh -v                 # before changes: 30/30 PASS
./scripts/test.sh -race              # before changes: all ok
go vet ./...                         # clean (before and after)
./scripts/test.sh -v -run ...        # new tests before fix: TestReleaseDoesNotExceedMaxAttempts FAIL, rest PASS
./scripts/test.sh -race -v           # after fixes: 42/42 PASS
go test -count=5 ./tests/...         # after fixes: ok (26s)
docker compose build --no-cache && docker compose up -d
./scripts/demo.sh ; ./scripts/fencing-demo.sh ; ./scripts/seed.sh
```

## Remaining issues and untested areas (not fixed)

- **Docker stop timeout equals default grace.** Docker's default stop timeout is 10s, the same as
  `SHUTDOWN_GRACE_SECONDS` (default 10s). With the spec defaults (20s handler timeout), a job longer than 10s gets SIGKILLed
  about when the release would run, so it is recovered only after the 60s lease expires. The compose demo is not
  affected because its jobs are capped at 8s. Recommendation: set the orchestrator's stop timeout above grace plus about 5s.
- **Payload over 16 KB returns 400, not 413.** Only a body over 64 KB returns 413. The README's status table
  is ambiguous on this, so the behavior was left as is.
- **`GET /api/events?limit=` is lenient.** An invalid value silently falls back to 30, while `/api/jobs` returns 400.
- **Manual retry keeps the previous `last_error`** on the requeued job until the next outcome overwrites it.
- **Theoretical orphaned claim on shutdown.** If the run context is cancelled while a `Claim` statement is in flight and
  it commits server-side, the row stays `running` until its lease expires and the reaper recovers it. This was not reproduced, and recovery covers it.
- **Fixed pool size (`MaxConns=10`).** It was not load-tested at the config extremes (`EMBEDDED_WORKERS=8` × `WORKER_CONCURRENCY=32`),
  where pool starvation could make acks hit their 5s timeout and fall back to lease expiry.
- **HTTP server timeouts.** The server sets only `ReadHeaderTimeout`; there is no `ReadTimeout` or `WriteTimeout`.
- **`gofmt` drift.** `gofmt -l` flags `internal/handlers/handlers.go` and `internal/queue/store.go` (comment alignment only).
  CI does not run gofmt.
- **Not executed:** the real Render + Neon hosted deploy (no account or credentials; approximated by running the same image with
  `EMBEDDED_WORKERS=2` locally), the GitHub Actions workflow itself (its steps were run locally instead), throughput
  benchmarking (only observed: 300 × 20ms jobs drained in about 3s with 4 slots), and dashboard behavior beyond a smoke test.

## Deployment readiness

All documented guarantees held under real processes, real Postgres, SIGKILL, SIGSTOP, DB outage and concurrent load:
no duplicate claims, fenced stale acks, lease recovery, bounded attempts, and backoff. That includes the one invariant
violation found (bug 1), which is now fixed. The image builds cleanly and the compose stack runs as documented.
The system is **ready for its stated scope** (demo/portfolio scale, at-least-once delivery, single shared API key).
Before production use, address the stop-timeout setting and HTTP timeouts above, set `API_KEY`, and plan for
`job_events` retention, as the README already notes.
