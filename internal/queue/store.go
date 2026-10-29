// Package queue is the PostgreSQL repository: enqueue, atomic claim with
// SKIP LOCKED, fenced acknowledgement, stale-lease recovery, and read models.
// All SQL lives here; state decisions live in transitions.go.
package queue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound     = errors.New("job not found")
	ErrLeaseLost    = errors.New("lease lost: job is no longer owned by this attempt")
	ErrNotRetryable = errors.New("only dead or failed jobs can be retried")
)

type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Priority    int             `json:"priority"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAfter    time.Time       `json:"run_after"`
	LeaseOwner  *string         `json:"lease_owner"`
	LeaseToken  *string         `json:"lease_token"`
	LeaseUntil  *time.Time      `json:"lease_until"`
	Result      json.RawMessage `json:"result"`
	LastError   *string         `json:"last_error"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	StartedAt   *time.Time      `json:"started_at"`
	FinishedAt  *time.Time      `json:"finished_at"`
}

type NewJob struct {
	Type        string
	Payload     json.RawMessage
	Priority    int
	MaxAttempts int
}

// Claimed is what a worker holds while executing one attempt.
type Claimed struct {
	ID          string
	Type        string
	Payload     json.RawMessage
	Attempts    int
	MaxAttempts int
	Token       string
	LeaseUntil  time.Time
}

type Event struct {
	ID        int64           `json:"id"`
	JobID     string          `json:"job_id"`
	JobType   string          `json:"job_type,omitempty"`
	Type      string          `json:"event_type"`
	Detail    json.RawMessage `json:"detail"`
	CreatedAt time.Time       `json:"created_at"`
}

type Worker struct {
	ID             string    `json:"worker_id"`
	Status         string    `json:"status"`
	State          string    `json:"state"` // active | stale | stopped (derived)
	StartedAt      time.Time `json:"started_at"`
	LastHeartbeat  time.Time `json:"last_heartbeat"`
	HeartbeatAgeMS int64     `json:"heartbeat_age_ms"`
	Inflight       int       `json:"inflight"`
	JobsSucceeded  int64     `json:"jobs_succeeded"`
	JobsFailed     int64     `json:"jobs_failed"`
}

type Metrics struct {
	Counts            map[string]int64 `json:"counts"`
	Total             int64            `json:"total"`
	QueueDepth        int64            `json:"queue_depth"`       // queued and runnable now
	Delayed           int64            `json:"delayed"`           // queued, waiting for backoff
	ThroughputPerMin  int64            `json:"throughput_per_min"` // succeeded in the last 60s
	AvgDurationMS     float64          `json:"avg_duration_ms"`
	LeasesExpired     int64            `json:"leases_expired"`
	StaleAcksRejected int64            `json:"stale_acks_rejected"`
	RetriesScheduled  int64            `json:"retries_scheduled"`
}

type Cursor struct {
	CreatedAt time.Time
	ID        string
}

func (c Cursor) Encode() string {
	return base64.RawURLEncoding.EncodeToString([]byte(c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID))
}

func DecodeCursor(s string) (*Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	ts, id, ok := strings.Cut(string(b), "|")
	if !ok {
		return nil, fmt.Errorf("invalid cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil || !IsUUID(id) {
		return nil, fmt.Errorf("invalid cursor")
	}
	return &Cursor{CreatedAt: t, ID: id}, nil
}

// IsUUID checks the canonical 8-4-4-4-12 hex form.
func IsUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const jobCols = `id::text, type, payload, status, priority, attempts, max_attempts, run_after,
	lease_owner, lease_token::text, lease_until, result, last_error, created_at, updated_at, started_at, finished_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var payload, result []byte
	err := row.Scan(&j.ID, &j.Type, &payload, &j.Status, &j.Priority, &j.Attempts, &j.MaxAttempts, &j.RunAfter,
		&j.LeaseOwner, &j.LeaseToken, &j.LeaseUntil, &result, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt)
	j.Payload = payload
	if result != nil {
		j.Result = result
	}
	return j, err
}

// Enqueue inserts a job and its "enqueued" event in one statement.
func (s *Store) Enqueue(ctx context.Context, n NewJob) (Job, error) {
	row := s.pool.QueryRow(ctx, `
		WITH ins AS (
			INSERT INTO jobs (type, payload, priority, max_attempts)
			VALUES ($1, $2, $3, $4)
			RETURNING *
		), ev AS (
			INSERT INTO job_events (job_id, event_type, detail)
			SELECT id, 'enqueued', jsonb_build_object('priority', priority, 'max_attempts', max_attempts) FROM ins
		)
		SELECT `+jobCols+` FROM ins`, n.Type, []byte(n.Payload), n.Priority, n.MaxAttempts)
	return scanJob(row)
}

// Claim atomically takes the highest-priority, oldest runnable job. The row
// lock (FOR UPDATE SKIP LOCKED) lets concurrent workers pass over each other's
// candidates instead of blocking. attempts increments here and a fresh
// lease_token fences this attempt. Returns (nil, nil) when nothing is runnable.
// The whole claim is one statement, i.e. one implicit transaction; no
// transaction is held while the handler runs.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (*Claimed, error) {
	row := s.pool.QueryRow(ctx, `
		WITH next AS (
			SELECT id FROM jobs
			WHERE status = 'queued' AND run_after <= now()
			ORDER BY priority DESC, created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE jobs j SET
				status = 'running',
				attempts = j.attempts + 1,
				lease_owner = $1,
				lease_token = gen_random_uuid(),
				lease_until = now() + ($2::bigint * interval '1 millisecond'),
				started_at = now(),
				finished_at = NULL,
				updated_at = now()
			FROM next WHERE j.id = next.id
			RETURNING j.id, j.type, j.payload, j.attempts, j.max_attempts, j.lease_token, j.lease_until
		), ev AS (
			INSERT INTO job_events (job_id, event_type, detail)
			SELECT id, 'claimed', jsonb_build_object('worker', $1::text, 'attempt', attempts,
				'lease_token', lease_token, 'lease_until', lease_until) FROM claimed
		)
		SELECT id::text, type, payload, attempts, max_attempts, lease_token::text, lease_until FROM claimed`,
		workerID, lease.Milliseconds())
	var c Claimed
	var payload []byte
	err := row.Scan(&c.ID, &c.Type, &payload, &c.Attempts, &c.MaxAttempts, &c.Token, &c.LeaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.Payload = payload
	return &c, nil
}

// fence is the acknowledgement guard shared by Complete/Fail/Release.
const fence = `id = $1 AND status = 'running' AND lease_token = $2::uuid AND lease_until > now()`

// Complete marks a job succeeded only if this attempt still holds an unexpired lease.
func (s *Store) Complete(ctx context.Context, id, token, workerID string, result json.RawMessage) error {
	if result == nil {
		result = json.RawMessage(`null`)
	}
	tag, err := s.pool.Exec(ctx, `
		WITH upd AS (
			UPDATE jobs SET status = 'succeeded', result = $4, last_error = NULL,
				finished_at = now(), updated_at = now(),
				lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE `+fence+`
			RETURNING id, attempts, (extract(epoch FROM finished_at - started_at) * 1000)::bigint AS dur
		)
		INSERT INTO job_events (job_id, event_type, detail)
		SELECT id, 'succeeded', jsonb_build_object('worker', $3::text, 'attempt', attempts, 'duration_ms', dur) FROM upd`,
		id, token, workerID, []byte(result))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.rejectStaleAck(ctx, id, token, workerID, "complete")
		return ErrLeaseLost
	}
	return nil
}

// Fail records a handler failure. nextStatus comes from NextStatusOnFailure;
// delay applies only when the job is requeued.
func (s *Store) Fail(ctx context.Context, id, token, workerID, nextStatus string, delay time.Duration, errMsg string) error {
	if len(errMsg) > 2000 {
		errMsg = errMsg[:2000]
	}
	tag, err := s.pool.Exec(ctx, `
		WITH upd AS (
			UPDATE jobs SET status = $4::text,
				run_after = CASE WHEN $4::text = 'queued' THEN now() + ($5::bigint * interval '1 millisecond') ELSE run_after END,
				finished_at = CASE WHEN $4::text = 'queued' THEN NULL ELSE now() END,
				last_error = $6::text, updated_at = now(),
				lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE `+fence+`
			RETURNING id, attempts
		)
		INSERT INTO job_events (job_id, event_type, detail)
		SELECT id, CASE WHEN $4::text = 'queued' THEN 'retry_scheduled' ELSE $4::text END,
			jsonb_build_object('worker', $3::text, 'attempt', attempts, 'error', $6::text,
				'backoff_ms', CASE WHEN $4::text = 'queued' THEN $5::bigint END)
		FROM upd`,
		id, token, workerID, nextStatus, delay.Milliseconds(), errMsg)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.rejectStaleAck(ctx, id, token, workerID, "fail")
		return ErrLeaseLost
	}
	return nil
}

// Release hands a job back immediately (graceful shutdown that ran out of grace time).
// The interrupted attempt is not counted: otherwise a job released on its last
// attempt would be requeued with attempts = max_attempts and claimed once more.
func (s *Store) Release(ctx context.Context, id, token, workerID string) error {
	tag, err := s.pool.Exec(ctx, `
		WITH upd AS (
			UPDATE jobs SET status = 'queued', run_after = now(), updated_at = now(),
				attempts = attempts - 1,
				lease_owner = NULL, lease_token = NULL, lease_until = NULL
			WHERE `+fence+`
			RETURNING id, attempts + 1 AS attempt
		)
		INSERT INTO job_events (job_id, event_type, detail)
		SELECT id, 'released', jsonb_build_object('worker', $3::text, 'attempt', attempt) FROM upd`,
		id, token, workerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) rejectStaleAck(ctx context.Context, id, token, workerID, op string) {
	_, _ = s.pool.Exec(ctx, `
		INSERT INTO job_events (job_id, event_type, detail)
		SELECT id, 'stale_ack_rejected', jsonb_build_object('worker', $3::text, 'op', $4::text,
			'presented_token', $2::text, 'current_status', status)
		FROM jobs WHERE id = $1`, id, token, workerID, op)
}

// RecoverExpired requeues (or kills, when attempts are exhausted) every running
// job whose lease has expired by database time. Safe to run from many workers.
func (s *Store) RecoverExpired(ctx context.Context, recoveredBy string) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH expired AS (
			SELECT id, lease_owner AS old_owner FROM jobs
			WHERE status = 'running' AND lease_until < now()
			FOR UPDATE SKIP LOCKED
		), upd AS (
			UPDATE jobs j SET
				status = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'queued' END,
				run_after = now(),
				finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
				last_error = 'lease expired (owner ' || coalesce(expired.old_owner, '?') || ' stopped responding)',
				lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = now()
			FROM expired WHERE j.id = expired.id
			RETURNING j.id, j.status, j.attempts, expired.old_owner
		)
		INSERT INTO job_events (job_id, event_type, detail)
		SELECT id, 'lease_expired', jsonb_build_object('previous_owner', old_owner, 'attempt', attempts,
			'new_status', status, 'recovered_by', $1::text) FROM upd`, recoveredBy)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

// List returns jobs newest first with keyset pagination on (created_at, id).
func (s *Store) List(ctx context.Context, status string, limit int, cur *Cursor) ([]Job, *Cursor, error) {
	q := `SELECT ` + jobCols + ` FROM jobs WHERE true`
	args := []any{}
	if status != "" {
		args = append(args, status)
		q += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if cur != nil {
		args = append(args, cur.CreatedAt, cur.ID)
		q += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d::uuid)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	q += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, nil, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	var next *Cursor
	if len(jobs) > limit {
		jobs = jobs[:limit]
		last := jobs[len(jobs)-1]
		next = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return jobs, next, nil
}

func (s *Store) Events(ctx context.Context, jobID string, limit int) ([]Event, error) {
	return s.queryEvents(ctx, `
		SELECT * FROM (
			SELECT e.id, e.job_id::text, '' AS job_type, e.event_type, e.detail, e.created_at
			FROM job_events e WHERE e.job_id = $1 ORDER BY e.id DESC LIMIT $2
		) t ORDER BY id ASC`, jobID, limit)
}

func (s *Store) RecentEvents(ctx context.Context, limit int) ([]Event, error) {
	return s.queryEvents(ctx, `
		SELECT e.id, e.job_id::text, j.type, e.event_type, e.detail, e.created_at
		FROM job_events e JOIN jobs j ON j.id = e.job_id
		ORDER BY e.id DESC LIMIT $1`, limit)
}

func (s *Store) queryEvents(ctx context.Context, q string, args ...any) ([]Event, error) {
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		var detail []byte
		if err := rows.Scan(&e.ID, &e.JobID, &e.JobType, &e.Type, &detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Detail = detail
		out = append(out, e)
	}
	return out, rows.Err()
}

// Retry requeues a dead/failed job. Semantics (documented): attempts reset to 0
// so the job gets a fresh max_attempts budget; a manual_retry event records the
// previous status and attempt count.
func (s *Store) Retry(ctx context.Context, id string) (Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `
		WITH old AS (
			SELECT id, status, attempts FROM jobs
			WHERE id = $1 AND status IN ('dead', 'failed') FOR UPDATE
		), upd AS (
			UPDATE jobs j SET status = 'queued', attempts = 0, run_after = now(), result = NULL,
				started_at = NULL, finished_at = NULL, updated_at = now()
			FROM old WHERE j.id = old.id
			RETURNING j.*
		), ev AS (
			INSERT INTO job_events (job_id, event_type, detail)
			SELECT id, 'manual_retry', jsonb_build_object('previous_status', status, 'previous_attempts', attempts) FROM old
		)
		SELECT `+jobCols+` FROM upd`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		if _, gerr := s.Get(ctx, id); gerr != nil {
			return j, gerr
		}
		return j, ErrNotRetryable
	}
	return j, err
}

func (s *Store) RegisterWorker(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (worker_id, status, started_at, last_heartbeat, inflight)
		VALUES ($1, 'running', now(), now(), 0)
		ON CONFLICT (worker_id) DO UPDATE SET status = 'running', started_at = now(), last_heartbeat = now(), inflight = 0`, id)
	return err
}

func (s *Store) Heartbeat(ctx context.Context, id, status string, inflight int) error {
	_, err := s.pool.Exec(ctx, `UPDATE workers SET last_heartbeat = now(), status = $2, inflight = $3 WHERE worker_id = $1`,
		id, status, inflight)
	return err
}

func (s *Store) RecordOutcome(ctx context.Context, id string, succeeded bool) error {
	col := "jobs_failed"
	if succeeded {
		col = "jobs_succeeded"
	}
	_, err := s.pool.Exec(ctx, `UPDATE workers SET `+col+` = `+col+` + 1 WHERE worker_id = $1`, id)
	return err
}

func (s *Store) Workers(ctx context.Context, staleAfter time.Duration) ([]Worker, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT worker_id, status, started_at, last_heartbeat,
			(extract(epoch FROM now() - last_heartbeat) * 1000)::bigint, inflight, jobs_succeeded, jobs_failed
		FROM workers ORDER BY worker_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Worker{}
	for rows.Next() {
		var w Worker
		if err := rows.Scan(&w.ID, &w.Status, &w.StartedAt, &w.LastHeartbeat, &w.HeartbeatAgeMS, &w.Inflight, &w.JobsSucceeded, &w.JobsFailed); err != nil {
			return nil, err
		}
		switch {
		case w.Status == "stopped":
			w.State = "stopped"
		case time.Duration(w.HeartbeatAgeMS)*time.Millisecond > staleAfter:
			w.State = "stale"
		default:
			w.State = "active"
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *Store) Metrics(ctx context.Context) (Metrics, error) {
	m := Metrics{Counts: map[string]int64{}}
	var q, r, su, f, d int64
	err := s.pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status = 'queued'),
			count(*) FILTER (WHERE status = 'running'),
			count(*) FILTER (WHERE status = 'succeeded'),
			count(*) FILTER (WHERE status = 'failed'),
			count(*) FILTER (WHERE status = 'dead'),
			count(*),
			count(*) FILTER (WHERE status = 'queued' AND run_after <= now()),
			count(*) FILTER (WHERE status = 'queued' AND run_after > now()),
			count(*) FILTER (WHERE status = 'succeeded' AND finished_at > now() - interval '60 seconds'),
			coalesce(avg(extract(epoch FROM finished_at - started_at) * 1000)
				FILTER (WHERE status = 'succeeded' AND finished_at > now() - interval '5 minutes'), 0)::float8
		FROM jobs`).Scan(&q, &r, &su, &f, &d, &m.Total, &m.QueueDepth, &m.Delayed, &m.ThroughputPerMin, &m.AvgDurationMS)
	if err != nil {
		return m, err
	}
	m.Counts = map[string]int64{"queued": q, "running": r, "succeeded": su, "failed": f, "dead": d}
	err = s.pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE event_type = 'lease_expired'),
			count(*) FILTER (WHERE event_type = 'stale_ack_rejected'),
			count(*) FILTER (WHERE event_type = 'retry_scheduled')
		FROM job_events`).Scan(&m.LeasesExpired, &m.StaleAcksRejected, &m.RetriesScheduled)
	return m, err
}
