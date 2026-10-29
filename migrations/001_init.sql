-- Jobs, their event history, and worker heartbeats.
CREATE TABLE IF NOT EXISTS jobs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type          VARCHAR(64) NOT NULL,
    payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT NOT NULL DEFAULT 'queued'
                  CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'dead')),
    priority      INTEGER NOT NULL DEFAULT 0,
    attempts      INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts  INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
    run_after     TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner   TEXT,
    lease_token   UUID,
    lease_until   TIMESTAMPTZ,
    result        JSONB,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    -- A running job always carries a full lease; other states never do.
    CONSTRAINT lease_matches_status CHECK (
        (status = 'running' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (status <> 'running' AND lease_owner IS NULL AND lease_token IS NULL AND lease_until IS NULL)
    )
);

-- Claim path: only queued rows, in claim order.
CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs (priority DESC, created_at ASC) WHERE status = 'queued';
-- Reaper path: only running rows, by lease expiry.
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs (lease_until) WHERE status = 'running';
-- Keyset pagination.
CREATE INDEX IF NOT EXISTS jobs_list_idx ON jobs (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS jobs_status_list_idx ON jobs (status, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS job_events (
    id          BIGSERIAL PRIMARY KEY,
    job_id      UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL,
    detail      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id, id);

CREATE TABLE IF NOT EXISTS workers (
    worker_id       TEXT PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'running',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    inflight        INTEGER NOT NULL DEFAULT 0,
    jobs_succeeded  BIGINT NOT NULL DEFAULT 0,
    jobs_failed     BIGINT NOT NULL DEFAULT 0
);
