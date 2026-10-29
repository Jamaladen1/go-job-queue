package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jamaladen/go-job-queue/internal/handlers"
	"github.com/jamaladen/go-job-queue/internal/httpapi"
	"github.com/jamaladen/go-job-queue/internal/queue"
	"github.com/jamaladen/go-job-queue/internal/worker"
	"github.com/jamaladen/go-job-queue/migrations"
)

// A job released during graceful shutdown must not be claimed more than
// max_attempts times: the release gives back the attempt it was charged.
func TestReleaseDoesNotExceedMaxAttempts(t *testing.T) {
	_, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":1}`, 1, 0)
	c, _ := s.Claim(ctx, "w", time.Minute)
	if err := s.Release(ctx, c.ID, c.Token, "w"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, j.ID)
	if got.Status != queue.StatusQueued || got.Attempts != 0 {
		t.Fatalf("after release: status=%s attempts=%d, want queued/0", got.Status, got.Attempts)
	}
	c2, _ := s.Claim(ctx, "w", time.Minute)
	if c2 == nil || c2.Attempts > c2.MaxAttempts {
		t.Fatalf("reclaim after release: %+v", c2)
	}
	ev, _ := s.Events(ctx, j.ID, 100)
	for _, e := range ev {
		if e.Type == "released" && !strings.Contains(string(e.Detail), `"attempt": 1`) {
			t.Fatalf("released event should record the released attempt: %s", e.Detail)
		}
	}
	// A stale token cannot release.
	if err := s.Release(ctx, c.ID, c.Token, "w"); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale release accepted: %v", err)
	}
}

// A handler that overruns HANDLER_TIMEOUT is cancelled and treated as a
// retriable failure until attempts are exhausted.
func TestHandlerTimeoutRetriesThenDead(t *testing.T) {
	_, s := setup(t)
	j := enqueue(t, s, "sleep", `{"duration_ms":2000}`, 2, 0)
	opts := workerOpts("timeout-worker")
	opts.HandlerTimeout = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	w := worker.New(s, handlers.NewRegistry(10000), opts, quiet)
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	waitFor(t, 10*time.Second, func() bool {
		got, _ := s.Get(context.Background(), j.ID)
		return got.Status == queue.StatusDead
	})
	cancel()
	<-done
	got, _ := s.Get(context.Background(), j.ID)
	if got.Attempts != 2 || got.LastError == nil || !strings.Contains(*got.LastError, "handler timeout") {
		t.Fatalf("attempts=%d last_error=%v", got.Attempts, got.LastError)
	}
	if !contains(eventTypes(t, s, j.ID), "retry_scheduled") {
		t.Fatalf("events: %v", eventTypes(t, s, j.ID))
	}
}

// permanent_fail goes straight to "failed" after one attempt, no retries.
func TestWorkerPermanentFailureNotRetried(t *testing.T) {
	_, s := setup(t)
	j := enqueue(t, s, "sleep", `{"duration_ms":1,"permanent_fail":true}`, 3, 0)
	ctx, cancel := context.WithCancel(context.Background())
	w := worker.New(s, handlers.NewRegistry(10000), workerOpts("perm"), quiet)
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	waitFor(t, 5*time.Second, func() bool {
		got, _ := s.Get(context.Background(), j.ID)
		return got.Status == queue.StatusFailed
	})
	cancel()
	<-done
	got, _ := s.Get(context.Background(), j.ID)
	if got.Attempts != 1 || got.FinishedAt == nil || got.LeaseToken != nil {
		t.Fatalf("unexpected failed job: %+v", got)
	}
	if contains(eventTypes(t, s, j.ID), "retry_scheduled") {
		t.Fatal("permanent failure was retried")
	}
}

// Many concurrent manual retries of one dead job: exactly one wins.
func TestConcurrentManualRetryRequeuesOnce(t *testing.T) {
	pool, s := setup(t)
	ctx := context.Background()
	j := enqueue(t, s, "sleep", `{"duration_ms":1}`, 1, 0)
	c, _ := s.Claim(ctx, "w", time.Minute)
	if err := s.Fail(ctx, c.ID, c.Token, "w", queue.StatusDead, 0, "boom"); err != nil {
		t.Fatal(err)
	}
	var ok, conflict atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Retry(ctx, j.ID)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, queue.ErrNotRetryable):
				conflict.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var events int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM job_events WHERE job_id=$1 AND event_type='manual_retry'`, j.ID).Scan(&events)
	if ok.Load() != 1 || conflict.Load() != 19 || events != 1 {
		t.Fatalf("ok=%d conflict=%d manual_retry events=%d", ok.Load(), conflict.Load(), events)
	}
}

// Many reapers racing over the same expired leases recover each job once.
func TestConcurrentReapersRecoverEachLeaseOnce(t *testing.T) {
	pool, s := setup(t)
	ctx := context.Background()
	const n = 50
	for i := 0; i < n; i++ {
		enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	}
	for i := 0; i < n; i++ {
		if c, _ := s.Claim(ctx, "crashed", 50*time.Millisecond); c == nil {
			t.Fatal("claim failed")
		}
	}
	time.Sleep(100 * time.Millisecond)
	var total atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := s.RecoverExpired(ctx, fmt.Sprintf("reaper-%d", i))
			if err != nil {
				t.Error(err)
			}
			total.Add(int32(got))
		}(i)
	}
	wg.Wait()
	var events, queued int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM job_events WHERE event_type='lease_expired'`).Scan(&events)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status='queued' AND attempts=1`).Scan(&queued)
	if total.Load() != n || events != n || queued != n {
		t.Fatalf("recovered=%d events=%d queued=%d, want %d each", total.Load(), events, queued, n)
	}
}

// Acks racing the reaper right at the lease boundary: every job ends in
// exactly one consistent state, and every ack the store accepted is the
// job's only success.
func TestAckRacingReaperAtLeaseBoundary(t *testing.T) {
	pool, s := setup(t)
	ctx := context.Background()
	const n = 150
	for i := 0; i < n; i++ {
		enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	}
	stop := make(chan struct{})
	var reapers sync.WaitGroup
	for i := 0; i < 3; i++ {
		reapers.Add(1)
		go func() {
			defer reapers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = s.RecoverExpired(ctx, "reaper")
				}
			}
		}()
	}
	var accepted sync.Map
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := fmt.Sprintf("w%d", w)
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				c, err := s.Claim(ctx, id, 30*time.Millisecond)
				if err != nil {
					t.Error(err)
					return
				}
				if c == nil {
					var left int
					_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status IN ('queued','running')`).Scan(&left)
					if left == 0 {
						return
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}
				time.Sleep(time.Duration(rand.IntN(50)) * time.Millisecond)
				err = s.Complete(ctx, c.ID, c.Token, id, json.RawMessage(`{}`))
				if err == nil {
					if _, dup := accepted.LoadOrStore(c.ID, c.Token); dup {
						t.Errorf("job %s acked twice", c.ID)
					}
				} else if !errors.Is(err, queue.ErrLeaseLost) {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	reapers.Wait()

	var badSucceeded, nonTerminal, overAttempts, acked int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT j.id FROM jobs j LEFT JOIN job_events e
		ON e.job_id=j.id AND e.event_type='succeeded' GROUP BY j.id, j.status
		HAVING count(e.id) <> CASE WHEN j.status='succeeded' THEN 1 ELSE 0 END) x`).Scan(&badSucceeded)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status NOT IN ('succeeded','dead')`).Scan(&nonTerminal)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE attempts > max_attempts`).Scan(&overAttempts)
	accepted.Range(func(_, _ any) bool { acked++; return true })
	var succeeded int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status='succeeded'`).Scan(&succeeded)
	if badSucceeded != 0 || nonTerminal != 0 || overAttempts != 0 || acked != succeeded {
		t.Fatalf("badSucceeded=%d nonTerminal=%d overAttempts=%d acked=%d succeeded=%d",
			badSucceeded, nonTerminal, overAttempts, acked, succeeded)
	}
	t.Logf("%d succeeded, %d dead after racing the reaper", succeeded, n-succeeded)
}

// Concurrent migrations (API + workers starting together) apply once.
func TestConcurrentMigrateIsSafe(t *testing.T) {
	pool, _ := setup(t)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := queue.Migrate(context.Background(), pool, migrations.FS); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var n int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&n)
	if n != 1 {
		t.Fatalf("schema_migrations rows = %d", n)
	}
}

// The database rejects a running row without a full lease.
func TestLeaseStatusConstraintEnforced(t *testing.T) {
	pool, s := setup(t)
	j := enqueue(t, s, "sleep", `{"duration_ms":1}`, 3, 0)
	if _, err := pool.Exec(context.Background(), `UPDATE jobs SET status='running' WHERE id=$1`, j.ID); err == nil {
		t.Fatal("running row without lease accepted")
	}
}

// Real HTTP API + real store + real worker: submit, observe, retry.
func TestHTTPEndToEnd(t *testing.T) {
	_, s := setup(t)
	reg := handlers.NewRegistry(10000)
	srv := httptest.NewServer(httpapi.New(httpapi.Options{Store: s, Registry: reg, APIKey: "k",
		StaleAfter: time.Second, MaxSleepMS: 10000, LeaseDur: 2 * time.Second, HandlerTime: time.Second, Log: quiet}))
	defer srv.Close()

	post := func(path, body string) (int, map[string]any) {
		req, _ := http.NewRequest("POST", srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-API-Key", "k")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}

	code, ok := post("/api/jobs", `{"type":"compute","payload":{"input":"e2e","iterations":10}}`)
	if code != 201 {
		t.Fatalf("create: %d %v", code, ok)
	}
	okID := ok["id"].(string)
	code, bad := post("/api/jobs", `{"type":"sleep","payload":{"duration_ms":1,"permanent_fail":true}}`)
	if code != 201 {
		t.Fatalf("create: %d", code)
	}
	badID := bad["id"].(string)
	if code, _ := post("/api/jobs/"+okID+"/retry", ``); code != 409 {
		t.Fatalf("retry of queued job: %d", code)
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := worker.New(s, reg, workerOpts("e2e"), quiet)
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	status := func(id string) string {
		_, m := get("/api/jobs/" + id)
		return m["job"].(map[string]any)["status"].(string)
	}
	waitFor(t, 5*time.Second, func() bool { return status(okID) == "succeeded" && status(badID) == "failed" })

	_, detail := get("/api/jobs/" + okID)
	result := detail["job"].(map[string]any)["result"].(map[string]any)
	if result["digest"] == "" || result["iterations"].(float64) != 10 {
		t.Fatalf("result: %v", result)
	}
	if code, _ := post("/api/jobs/"+badID+"/retry", ``); code != 200 {
		t.Fatalf("retry failed job: %d", code)
	}
	waitFor(t, 5*time.Second, func() bool { return status(badID) == "failed" })
	_, d := get("/api/jobs/" + badID)
	var types []string
	for _, e := range d["events"].([]any) {
		types = append(types, e.(map[string]any)["event_type"].(string))
	}
	if strings.Join(types, ",") != "enqueued,claimed,failed,manual_retry,claimed,failed" {
		t.Fatalf("timeline: %v", types)
	}
	_, m := get("/api/metrics")
	counts := m["counts"].(map[string]any)
	if counts["succeeded"].(float64) != 1 || counts["failed"].(float64) != 1 {
		t.Fatalf("metrics: %v", m)
	}
	_, ws := get("/api/workers")
	if ws["active"].(float64) != 1 {
		t.Fatalf("workers: %v", ws)
	}
}
