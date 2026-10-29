package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jamaladen/go-job-queue/internal/handlers"
	"github.com/jamaladen/go-job-queue/internal/queue"
)

type fakeStore struct {
	jobs     map[string]queue.Job
	enqueued []queue.NewJob
}

const knownID = "2e72cdcf-e478-4448-a61a-67189be0f72b"
const deadID = "11111111-1111-1111-1111-111111111111"

func newFake() *fakeStore {
	return &fakeStore{jobs: map[string]queue.Job{
		knownID: {ID: knownID, Type: "sleep", Status: "running"},
		deadID:  {ID: deadID, Type: "sleep", Status: "dead"},
	}}
}

func (f *fakeStore) Ping(context.Context) error { return nil }
func (f *fakeStore) Enqueue(_ context.Context, n queue.NewJob) (queue.Job, error) {
	f.enqueued = append(f.enqueued, n)
	return queue.Job{ID: knownID, Status: "queued"}, nil
}
func (f *fakeStore) Get(_ context.Context, id string) (queue.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return j, queue.ErrNotFound
	}
	return j, nil
}
func (f *fakeStore) List(context.Context, string, int, *queue.Cursor) ([]queue.Job, *queue.Cursor, error) {
	return []queue.Job{}, nil, nil
}
func (f *fakeStore) Events(context.Context, string, int) ([]queue.Event, error) { return nil, nil }
func (f *fakeStore) RecentEvents(context.Context, int) ([]queue.Event, error)   { return nil, nil }
func (f *fakeStore) Retry(_ context.Context, id string) (queue.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return j, queue.ErrNotFound
	}
	if !queue.CanRetryManually(j.Status) {
		return j, queue.ErrNotRetryable
	}
	j.Status = "queued"
	return j, nil
}
func (f *fakeStore) Workers(context.Context, time.Duration) ([]queue.Worker, error) { return nil, nil }
func (f *fakeStore) Metrics(context.Context) (queue.Metrics, error)                 { return queue.Metrics{}, nil }

func newServer(key string) (http.Handler, *fakeStore) {
	f := newFake()
	return New(Options{Store: f, Registry: handlers.NewRegistry(10000), APIKey: key, StaleAfter: 10 * time.Second,
		MaxSleepMS: 10000, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}), f
}

func do(h http.Handler, method, path, body string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateJobValidation(t *testing.T) {
	h, f := newServer("")
	cases := []struct {
		body string
		want int
	}{
		{`{"type":"sleep","payload":{"duration_ms":500},"max_attempts":3,"priority":0}`, 201},
		{`{"type":"compute","payload":{"iterations":10}}`, 201},
		{`{"type":"sleep","payload":{"duration_ms":10001}}`, 400},
		{`{"type":"webhook","payload":{}}`, 400},
		{`{"type":"sleep","payload":[1,2]}`, 400},
		{`{"type":"sleep","payload":{"duration_ms":5},"max_attempts":0}`, 400},
		{`{"type":"sleep","payload":{"duration_ms":5},"max_attempts":11}`, 400},
		{`{"type":"sleep","payload":{"duration_ms":5},"priority":1000}`, 400},
		{`{"type":"sleep","payload":{"duration_ms":5},"extra":true}`, 400},
		{`{"payload":{"duration_ms":5}}`, 400},
		{`not json`, 400},
		{`{"type":"compute","payload":{"input":"` + strings.Repeat("a", 70*1024) + `"}}`, 413},
	}
	for _, c := range cases {
		rec := do(h, "POST", "/api/jobs", c.body)
		if rec.Code != c.want {
			t.Errorf("POST %.60s -> %d (%s), want %d", c.body, rec.Code, rec.Body.String(), c.want)
		}
	}
	if len(f.enqueued) != 2 || f.enqueued[0].MaxAttempts != 3 || f.enqueued[1].MaxAttempts != 3 {
		t.Errorf("unexpected enqueued jobs: %+v", f.enqueued)
	}
	rec := do(h, "POST", "/api/jobs", `{"type":"sleep","payload":{"duration_ms":5}}`)
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["id"] == "" || resp["status"] != "queued" {
		t.Errorf("bad 201 body: %s", rec.Body.String())
	}
}

func TestAPIKeyProtectsWrites(t *testing.T) {
	h, _ := newServer("s3cret")
	body := `{"type":"sleep","payload":{"duration_ms":5}}`
	if rec := do(h, "POST", "/api/jobs", body); rec.Code != 401 {
		t.Errorf("no key: %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/jobs", body, "X-API-Key", "wrong"); rec.Code != 401 {
		t.Errorf("wrong key: %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/jobs", body, "X-API-Key", "s3cret"); rec.Code != 201 {
		t.Errorf("right key: %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/jobs", body, "Authorization", "Bearer s3cret"); rec.Code != 201 {
		t.Errorf("bearer: %d", rec.Code)
	}
	if rec := do(h, "GET", "/api/jobs", ""); rec.Code != 200 {
		t.Errorf("reads stay public: %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/jobs/"+deadID+"/retry", ""); rec.Code != 401 {
		t.Errorf("retry without key: %d", rec.Code)
	}
}

func TestStatusCodes(t *testing.T) {
	h, _ := newServer("")
	cases := []struct {
		method, path string
		want         int
	}{
		{"GET", "/health/live", 200},
		{"GET", "/health/ready", 200},
		{"GET", "/api/jobs/" + knownID, 200},
		{"GET", "/api/jobs/00000000-0000-0000-0000-000000000000", 404},
		{"GET", "/api/jobs/not-a-uuid", 400},
		{"GET", "/api/jobs?status=bogus", 400},
		{"GET", "/api/jobs?limit=0", 400},
		{"GET", "/api/jobs?cursor=garbage", 400},
		{"POST", "/api/jobs/" + knownID + "/retry", 409}, // running jobs can't be retried
		{"POST", "/api/jobs/" + deadID + "/retry", 200},
		{"POST", "/api/jobs/00000000-0000-0000-0000-000000000000/retry", 404},
		{"DELETE", "/api/jobs/" + knownID, 405},
		{"GET", "/", 200},
		{"GET", "/nope", 404},
	}
	for _, c := range cases {
		rec := do(h, c.method, c.path, "")
		if rec.Code != c.want {
			t.Errorf("%s %s -> %d, want %d", c.method, c.path, rec.Code, c.want)
		}
	}
	rec := do(h, "GET", "/health/live", "", "X-Request-ID", "abc123")
	if rec.Header().Get("X-Request-ID") != "abc123" {
		t.Error("request id not propagated")
	}
}
