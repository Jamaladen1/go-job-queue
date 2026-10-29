// Integration tests against a real PostgreSQL. They run only when
// TEST_DATABASE_URL is set; the database is created if missing and its
// tables are truncated before every test. All DB tests live in this one
// package so `go test ./...` never runs two of them against the DB at once.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jamaladen/go-job-queue/internal/handlers"
	"github.com/jamaladen/go-job-queue/internal/queue"
	"github.com/jamaladen/go-job-queue/internal/worker"
	"github.com/jamaladen/go-job-queue/migrations"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func setup(t *testing.T) (*pgxpool.Pool, *queue.Store) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	ensureDatabase(t, dsn)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := queue.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_events, workers RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	return pool, queue.NewStore(pool)
}

func ensureDatabase(t *testing.T, dsn string) {
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := u.Path[1:]
	u.Path = "/postgres"
	conn, err := pgx.Connect(context.Background(), u.String())
	if err != nil {
		t.Fatalf("connect maintenance db: %v", err)
	}
	defer conn.Close(context.Background())
	var exists bool
	_ = conn.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, name).Scan(&exists)
	if !exists {
		if _, err := conn.Exec(context.Background(), `CREATE DATABASE "`+name+`"`); err != nil {
			t.Logf("create database: %v (may already exist)", err)
		}
	}
}

func enqueue(t *testing.T, s *queue.Store, typ, payload string, maxAttempts, priority int) queue.Job {
	t.Helper()
	j, err := s.Enqueue(context.Background(), queue.NewJob{Type: typ, Payload: json.RawMessage(payload), MaxAttempts: maxAttempts, Priority: priority})
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func eventTypes(t *testing.T, s *queue.Store, id string) []string {
	ev, err := s.Events(context.Background(), id, 100)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(ev))
	for i, e := range ev {
		out[i] = e.Type
	}
	return out
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// Many goroutines with independent connections race to claim; every job must
// be claimed exactly once.
func TestConcurrentWorkersNeverClaimSameJob(t *testing.T) {
	_, s := setup(t)
	const jobs, claimers = 200, 12
	for i := 0; i < jobs; i++ {
		enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	}
	var mu sync.Mutex
	seen := map[string]string{}
	dups := 0
	var wg sync.WaitGroup
	for w := 0; w < claimers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := fmt.Sprintf("claimer-%d", w)
			for {
				c, err := s.Claim(context.Background(), id, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if c == nil {
					return
				}
				mu.Lock()
				if _, dup := seen[c.ID]; dup {
					dups++
				}
				seen[c.ID] = id
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if dups != 0 || len(seen) != jobs {
		t.Fatalf("claimed %d distinct of %d jobs with %d duplicates", len(seen), jobs, dups)
	}
	owners := map[string]int{}
	for _, o := range seen {
		owners[o]++
	}
	t.Logf("claims spread across %d claimers: %v", len(owners), owners)
}

// A row locked by another transaction is skipped, not waited on.
func TestSkipLockedPassesOverLockedRow(t *testing.T) {
	pool, s := setup(t)
	high := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 10)
	low := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT 1 FROM jobs WHERE id=$1 FOR UPDATE`, high.ID); err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	c, err := s.Claim(cctx, "w", time.Minute)
	if err != nil {
		t.Fatalf("claim blocked or failed: %v", err)
	}
	if c == nil || c.ID != low.ID {
		t.Fatalf("expected to skip locked high-priority row and claim %s, got %+v", low.ID, c)
	}
	_ = tx.Rollback(ctx)
	c2, _ := s.Claim(ctx, "w", time.Minute)
	if c2 == nil || c2.ID != high.ID {
		t.Fatalf("after unlock expected %s, got %+v", high.ID, c2)
	}
}

func TestClaimOrderPriorityThenAge(t *testing.T) {
	_, s := setup(t)
	a := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	b := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 5)
	c := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	want := []string{b.ID, a.ID, c.ID}
	for i, w := range want {
		got, _ := s.Claim(context.Background(), "w", time.Minute)
		if got == nil || got.ID != w {
			t.Fatalf("claim %d: want %s got %+v", i, w, got)
		}
	}
}

func TestExpiredLeaseRecovery(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	c, _ := s.Claim(ctx, "crashed-worker", 300*time.Millisecond)
	if c == nil || c.Attempts != 1 {
		t.Fatalf("bad claim %+v", c)
	}
	if n, _ := s.RecoverExpired(ctx, "reaper"); n != 0 {
		t.Fatalf("recovered a live lease (%d)", n)
	}
	time.Sleep(500 * time.Millisecond)
	n, err := s.RecoverExpired(ctx, "reaper")
	if err != nil || n != 1 {
		t.Fatalf("recover: n=%d err=%v", n, err)
	}
	got, _ := s.Get(ctx, j.ID)
	if got.Status != queue.StatusQueued || got.LeaseOwner != nil || got.LeaseToken != nil {
		t.Fatalf("expected queued without lease, got %+v", got)
	}
	if !contains(eventTypes(t, s, j.ID), "lease_expired") {
		t.Fatal("missing lease_expired event")
	}
	c2, _ := s.Claim(ctx, "healthy-worker", time.Minute)
	if c2 == nil || c2.ID != j.ID || c2.Attempts != 2 {
		t.Fatalf("expected reclaim as attempt 2, got %+v", c2)
	}
}

// The core fencing guarantee: a worker whose lease expired cannot overwrite
// the outcome of the newer attempt.
func TestFencingRejectsStaleToken(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)

	old, _ := s.Claim(ctx, "slow-worker", 200*time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	// Expired-but-not-yet-reaped lease is already rejected.
	if err := s.Complete(ctx, j.ID, old.Token, "slow-worker", json.RawMessage(`{"by":"slow"}`)); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("expired lease acked: %v", err)
	}
	if _, err := s.RecoverExpired(ctx, "reaper"); err != nil {
		t.Fatal(err)
	}
	cur, _ := s.Claim(ctx, "fast-worker", time.Minute)
	if cur == nil || cur.Token == old.Token {
		t.Fatalf("expected new attempt with new token, got %+v", cur)
	}
	// The old worker wakes up and tries both kinds of acknowledgement.
	if err := s.Complete(ctx, j.ID, old.Token, "slow-worker", json.RawMessage(`{"by":"slow"}`)); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale complete accepted: %v", err)
	}
	if err := s.Fail(ctx, j.ID, old.Token, "slow-worker", queue.StatusDead, 0, "late"); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale fail accepted: %v", err)
	}
	if err := s.Complete(ctx, j.ID, cur.Token, "fast-worker", json.RawMessage(`{"by":"fast"}`)); err != nil {
		t.Fatalf("current owner rejected: %v", err)
	}
	got, _ := s.Get(ctx, j.ID)
	if got.Status != queue.StatusSucceeded || string(got.Result) != `{"by": "fast"}` {
		t.Fatalf("wrong final state: %s %s", got.Status, got.Result)
	}
	// A duplicate acknowledgement after success is also rejected.
	if err := s.Complete(ctx, j.ID, cur.Token, "fast-worker", nil); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("double ack accepted: %v", err)
	}
	evs := eventTypes(t, s, j.ID)
	succeeded := 0
	for _, e := range evs {
		if e == "succeeded" {
			succeeded++
		}
	}
	if succeeded != 1 || !contains(evs, "stale_ack_rejected") {
		t.Fatalf("events: %v", evs)
	}
}

func TestAttemptsCapLeadsToDead(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	for attempt := 1; attempt <= 3; attempt++ {
		c, _ := s.Claim(ctx, "w", time.Minute)
		if c == nil || c.Attempts != attempt {
			t.Fatalf("attempt %d: %+v", attempt, c)
		}
		next := queue.NextStatusOnFailure(c.Attempts, c.MaxAttempts, false)
		if err := s.Fail(ctx, c.ID, c.Token, "w", next, 0, "boom"); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.Get(ctx, j.ID)
	if got.Status != queue.StatusDead || got.Attempts != 3 || got.FinishedAt == nil {
		t.Fatalf("expected dead after 3 attempts: %+v", got)
	}
	if c, _ := s.Claim(ctx, "w", time.Minute); c != nil {
		t.Fatalf("dead job claimed again: %+v", c)
	}
}

func TestExpiredLeaseAtMaxAttemptsGoesDead(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":1}`, 1, 0)
	_, _ = s.Claim(ctx, "w", 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	if n, _ := s.RecoverExpired(ctx, "reaper"); n != 1 {
		t.Fatal("not recovered")
	}
	if got, _ := s.Get(ctx, j.ID); got.Status != queue.StatusDead {
		t.Fatalf("want dead, got %s", got.Status)
	}
}

func TestBackoffDelaysReclaim(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	c, _ := s.Claim(ctx, "w", time.Minute)
	if err := s.Fail(ctx, c.ID, c.Token, "w", queue.StatusQueued, 400*time.Millisecond, "transient"); err != nil {
		t.Fatal(err)
	}
	if c2, _ := s.Claim(ctx, "w", time.Minute); c2 != nil {
		t.Fatal("claimed before run_after")
	}
	time.Sleep(500 * time.Millisecond)
	if c3, _ := s.Claim(ctx, "w", time.Minute); c3 == nil || c3.Attempts != 2 {
		t.Fatalf("not claimable after backoff: %+v", c3)
	}
}

func TestPermanentFailureAndManualRetry(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	if _, err := s.Retry(ctx, j.ID); !errors.Is(err, queue.ErrNotRetryable) {
		t.Fatalf("queued job retried: %v", err)
	}
	c, _ := s.Claim(ctx, "w", time.Minute)
	_ = s.Fail(ctx, c.ID, c.Token, "w", queue.StatusFailed, 0, "bad input")
	r, err := s.Retry(ctx, j.ID)
	if err != nil || r.Status != queue.StatusQueued || r.Attempts != 0 {
		t.Fatalf("retry: %+v %v", r, err)
	}
	if !contains(eventTypes(t, s, j.ID), "manual_retry") {
		t.Fatal("missing manual_retry event")
	}
	if _, err := s.Retry(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
}

func TestListPaginationIsStable(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	}
	seen := map[string]bool{}
	var cur *queue.Cursor
	pages := 0
	for {
		jobs, next, err := s.List(ctx, "", 10, cur)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, j := range jobs {
			if seen[j.ID] {
				t.Fatalf("duplicate across pages: %s", j.ID)
			}
			seen[j.ID] = true
		}
		if next == nil {
			break
		}
		cur, _ = queue.DecodeCursor(next.Encode())
	}
	if len(seen) != 25 || pages != 3 {
		t.Fatalf("saw %d jobs in %d pages", len(seen), pages)
	}
}

func workerOpts(id string) worker.Options {
	return worker.Options{
		ID: id, Concurrency: 2, PollInterval: 20 * time.Millisecond, Lease: 2 * time.Second,
		HandlerTimeout: time.Second, HeartbeatInterval: 100 * time.Millisecond, ReapInterval: 100 * time.Millisecond,
		ShutdownGrace: 2 * time.Second, Jitter: func(time.Duration) time.Duration { return 50 * time.Millisecond },
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// Two real worker loops share the queue; jobs are spread across both and each
// job is acknowledged exactly once. Retries with deterministic jitter succeed.
func TestTwoWorkersProcessConcurrently(t *testing.T) {
	pool, s := setup(t)
	reg := handlers.NewRegistry(10000)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, id := range []string{"worker-a", "worker-b"} {
		w := worker.New(s, reg, workerOpts(id), quiet)
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Run(ctx) }()
	}
	for i := 0; i < 20; i++ {
		enqueue(t, s, "sleep", `{"duration_ms":80}`, 3, 0)
	}
	retry := enqueue(t, s, "sleep", `{"duration_ms":10,"fail_until_attempt":2}`, 3, 0)

	waitFor(t, 15*time.Second, func() bool {
		m, _ := s.Metrics(context.Background())
		return m.Counts["succeeded"] == 21
	})
	cancel()
	wg.Wait()

	var owners int
	_ = pool.QueryRow(context.Background(),
		`SELECT count(DISTINCT detail->>'worker') FROM job_events WHERE event_type='succeeded'`).Scan(&owners)
	if owners != 2 {
		t.Fatalf("expected both workers to complete jobs, got %d", owners)
	}
	var dupAcks int
	_ = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM (SELECT job_id FROM job_events WHERE event_type='succeeded' GROUP BY job_id HAVING count(*) > 1) d`).Scan(&dupAcks)
	if dupAcks != 0 {
		t.Fatalf("%d jobs acknowledged more than once", dupAcks)
	}
	got, _ := s.Get(context.Background(), retry.ID)
	if got.Attempts != 2 || !contains(eventTypes(t, s, retry.ID), "retry_scheduled") {
		t.Fatalf("retry job: attempts=%d events=%v", got.Attempts, eventTypes(t, s, retry.ID))
	}
	ws, _ := s.Workers(context.Background(), time.Second)
	for _, w := range ws {
		if w.State != "stopped" {
			t.Errorf("worker %s not marked stopped after shutdown: %s", w.ID, w.State)
		}
	}
}

// Failure/restart: a worker claims a job then "crashes" (never acks). A healthy
// worker's reaper recovers the expired lease and completes the job as attempt 2.
func TestCrashedWorkerJobIsReclaimed(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":10}`, 3, 0)
	crashed, _ := s.Claim(ctx, "crashed-worker", 500*time.Millisecond)
	if crashed == nil {
		t.Fatal("no claim")
	}

	wctx, cancel := context.WithCancel(ctx)
	w := worker.New(s, handlers.NewRegistry(10000), workerOpts("survivor"), quiet)
	done := make(chan struct{})
	go func() { _ = w.Run(wctx); close(done) }()
	waitFor(t, 10*time.Second, func() bool {
		got, _ := s.Get(ctx, j.ID)
		return got.Status == queue.StatusSucceeded
	})
	cancel()
	<-done

	got, _ := s.Get(ctx, j.ID)
	if got.Attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", got.Attempts)
	}
	evs := eventTypes(t, s, j.ID)
	if !contains(evs, "lease_expired") {
		t.Fatalf("events: %v", evs)
	}
	// The crashed worker comes back and tries to ack: fenced.
	if err := s.Complete(ctx, j.ID, crashed.Token, "crashed-worker", nil); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("zombie ack accepted: %v", err)
	}
}

// Graceful shutdown: a job longer than the grace period is cancelled and released.
func TestGracefulShutdownReleasesInflightJob(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":5000}`, 3, 0)
	opts := workerOpts("draining")
	opts.HandlerTimeout = 8 * time.Second
	opts.Lease = 10 * time.Second
	opts.ShutdownGrace = 200 * time.Millisecond
	wctx, cancel := context.WithCancel(ctx)
	w := worker.New(s, handlers.NewRegistry(10000), opts, quiet)
	done := make(chan struct{})
	go func() { _ = w.Run(wctx); close(done) }()
	waitFor(t, 5*time.Second, func() bool {
		got, _ := s.Get(ctx, j.ID)
		return got.Status == queue.StatusRunning
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop within grace + margin")
	}
	got, _ := s.Get(ctx, j.ID)
	if got.Status != queue.StatusQueued || !contains(eventTypes(t, s, j.ID), "released") {
		t.Fatalf("expected released+queued, got %s %v", got.Status, eventTypes(t, s, j.ID))
	}
}
