// Package httpapi exposes the REST API and serves the embedded dashboard.
package httpapi

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jamaladen/go-job-queue/internal/handlers"
	"github.com/jamaladen/go-job-queue/internal/observability"
	"github.com/jamaladen/go-job-queue/internal/queue"
	"github.com/jamaladen/go-job-queue/web"
)

const maxBodyBytes = 64 * 1024

// Store is the subset of queue.Store the API needs (faked in unit tests).
type Store interface {
	Ping(ctx context.Context) error
	Enqueue(ctx context.Context, n queue.NewJob) (queue.Job, error)
	Get(ctx context.Context, id string) (queue.Job, error)
	List(ctx context.Context, status string, limit int, cur *queue.Cursor) ([]queue.Job, *queue.Cursor, error)
	Events(ctx context.Context, jobID string, limit int) ([]queue.Event, error)
	RecentEvents(ctx context.Context, limit int) ([]queue.Event, error)
	Retry(ctx context.Context, id string) (queue.Job, error)
	Workers(ctx context.Context, staleAfter time.Duration) ([]queue.Worker, error)
	Metrics(ctx context.Context) (queue.Metrics, error)
}

type Options struct {
	Store       Store
	Registry    *handlers.Registry
	APIKey      string
	StaleAfter  time.Duration
	MaxSleepMS  int
	LeaseDur    time.Duration
	HandlerTime time.Duration
	Log         *slog.Logger
}

type server struct{ Options }

func New(o Options) http.Handler {
	s := &server{o}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.HandleFunc("GET /api/config", s.config)
	mux.HandleFunc("POST /api/jobs", s.auth(s.createJob))
	mux.HandleFunc("GET /api/jobs", s.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /api/jobs/{id}/retry", s.auth(s.retryJob))
	mux.HandleFunc("GET /api/workers", s.workers)
	mux.HandleFunc("GET /api/metrics", s.metrics)
	mux.HandleFunc("GET /api/events", s.events)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(web.Index)
	})
	return s.middleware(mux)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }

func (s *server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 64 {
			id = observability.NewRequestID()
		}
		w.Header().Set("X-Request-ID", id)
		r = r.WithContext(observability.WithRequestID(r.Context(), id))
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		start := time.Now()
		defer func() {
			if p := recover(); p != nil {
				s.Log.Error("panic", "request_id", id, "panic", fmt.Sprint(p))
				writeErr(rec, r, 500, "internal error")
			}
			// Skip noisy polling reads at info level; log writes and errors.
			if r.Method != http.MethodGet || rec.status >= 400 {
				s.Log.Info("http request", "request_id", id, "method", r.Method, "path", r.URL.Path,
					"status", rec.status, "duration_ms", time.Since(start).Milliseconds())
			}
		}()
		next.ServeHTTP(rec, r)
	})
}

func (s *server) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.APIKey != "" {
			got := r.Header.Get("X-API-Key")
			if got == "" {
				got = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			}
			if subtle.ConstantTimeCompare([]byte(got), []byte(s.APIKey)) != 1 {
				writeErr(w, r, http.StatusUnauthorized, "missing or invalid API key")
				return
			}
		}
		h(w, r)
	}
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		writeJSON(w, 503, map[string]string{"status": "unavailable", "database": "unreachable"})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready", "database": "ok"})
}

func (s *server) config(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"auth_required":           s.APIKey != "",
		"job_types":               s.Registry.Types(),
		"max_sleep_ms":            s.MaxSleepMS,
		"lease_seconds":           s.LeaseDur.Seconds(),
		"handler_timeout_seconds": s.HandlerTime.Seconds(),
		"delivery":                "at-least-once",
	})
}

type createRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts"`
	Priority    *int            `json:"priority"`
}

// validateCreate turns a raw request body into a NewJob or a 400 message.
func validateCreate(body []byte, reg *handlers.Registry) (queue.NewJob, error) {
	var req createRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return queue.NewJob{}, fmt.Errorf("malformed JSON body: %v", err)
	}
	if dec.More() {
		return queue.NewJob{}, errors.New("malformed JSON body: trailing data")
	}
	if req.Type == "" {
		return queue.NewJob{}, errors.New("type is required")
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		req.Payload = json.RawMessage(`{}`)
	}
	if req.Payload[0] != '{' {
		return queue.NewJob{}, errors.New("payload must be a JSON object")
	}
	if err := reg.Validate(req.Type, req.Payload); err != nil {
		return queue.NewJob{}, err
	}
	n := queue.NewJob{Type: req.Type, Payload: req.Payload, MaxAttempts: 3}
	if req.MaxAttempts != nil {
		if *req.MaxAttempts < 1 || *req.MaxAttempts > 10 {
			return n, errors.New("max_attempts must be between 1 and 10")
		}
		n.MaxAttempts = *req.MaxAttempts
	}
	if req.Priority != nil {
		if *req.Priority < -100 || *req.Priority > 100 {
			return n, errors.New("priority must be between -100 and 100")
		}
		n.Priority = *req.Priority
	}
	return n, nil
}

func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r.Body); err != nil {
		writeErr(w, r, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	n, err := validateCreate(buf.Bytes(), s.Registry)
	if err != nil {
		writeErr(w, r, 400, err.Error())
		return
	}
	job, err := s.Store.Enqueue(r.Context(), n)
	if err != nil {
		s.dbErr(w, r, err)
		return
	}
	writeJSON(w, 201, map[string]string{"id": job.ID, "status": job.Status})
}

var validStatus = map[string]bool{"queued": true, "running": true, "succeeded": true, "failed": true, "dead": true}

func (s *server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := q.Get("status")
	if status != "" && !validStatus[status] {
		writeErr(w, r, 400, "status must be one of queued, running, succeeded, failed, dead")
		return
	}
	limit := 50
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeErr(w, r, 400, "limit must be between 1 and 200")
			return
		}
		limit = n
	}
	var cur *queue.Cursor
	if v := q.Get("cursor"); v != "" {
		c, err := queue.DecodeCursor(v)
		if err != nil {
			writeErr(w, r, 400, err.Error())
			return
		}
		cur = c
	}
	jobs, next, err := s.Store.List(r.Context(), status, limit, cur)
	if err != nil {
		s.dbErr(w, r, err)
		return
	}
	resp := map[string]any{"jobs": jobs, "next_cursor": nil}
	if next != nil {
		resp["next_cursor"] = next.Encode()
	}
	writeJSON(w, 200, resp)
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !queue.IsUUID(id) {
		writeErr(w, r, 400, "id must be a UUID")
		return
	}
	job, err := s.Store.Get(r.Context(), id)
	if errors.Is(err, queue.ErrNotFound) {
		writeErr(w, r, 404, "job not found")
		return
	}
	if err != nil {
		s.dbErr(w, r, err)
		return
	}
	events, err := s.Store.Events(r.Context(), id, 100)
	if err != nil {
		s.dbErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"job": job, "events": events})
}

func (s *server) retryJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !queue.IsUUID(id) {
		writeErr(w, r, 400, "id must be a UUID")
		return
	}
	job, err := s.Store.Retry(r.Context(), id)
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeErr(w, r, 404, "job not found")
	case errors.Is(err, queue.ErrNotRetryable):
		writeErr(w, r, 409, err.Error())
	case err != nil:
		s.dbErr(w, r, err)
	default:
		writeJSON(w, 200, map[string]string{"id": job.ID, "status": job.Status})
	}
}

func (s *server) workers(w http.ResponseWriter, r *http.Request) {
	ws, err := s.Store.Workers(r.Context(), s.StaleAfter)
	if err != nil {
		s.dbErr(w, r, err)
		return
	}
	active := 0
	for _, x := range ws {
		if x.State == "active" {
			active++
		}
	}
	writeJSON(w, 200, map[string]any{"workers": ws, "active": active})
}

func (s *server) metrics(w http.ResponseWriter, r *http.Request) {
	m, err := s.Store.Metrics(r.Context())
	if err != nil {
		s.dbErr(w, r, err)
		return
	}
	writeJSON(w, 200, m)
}

func (s *server) events(w http.ResponseWriter, r *http.Request) {
	limit := 30
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	ev, err := s.Store.RecentEvents(r.Context(), limit)
	if err != nil {
		s.dbErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"events": ev})
}

func (s *server) dbErr(w http.ResponseWriter, r *http.Request, err error) {
	s.Log.Error("database error", "request_id", observability.RequestID(r.Context()), "error", err.Error())
	writeErr(w, r, 503, "database unavailable")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, r *http.Request, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "request_id": observability.RequestID(r.Context())})
}
