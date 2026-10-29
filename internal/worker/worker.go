// Package worker runs the claim -> execute -> fenced-ack loop.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamaladen/go-job-queue/internal/handlers"
	"github.com/jamaladen/go-job-queue/internal/queue"
)

// Store is the subset of queue.Store the worker needs.
type Store interface {
	Claim(ctx context.Context, workerID string, lease time.Duration) (*queue.Claimed, error)
	Complete(ctx context.Context, id, token, workerID string, result json.RawMessage) error
	Fail(ctx context.Context, id, token, workerID, nextStatus string, delay time.Duration, errMsg string) error
	Release(ctx context.Context, id, token, workerID string) error
	RecoverExpired(ctx context.Context, recoveredBy string) (int, error)
	RegisterWorker(ctx context.Context, id string) error
	Heartbeat(ctx context.Context, id, status string, inflight int) error
	RecordOutcome(ctx context.Context, id string, succeeded bool) error
}

type Options struct {
	ID                string
	Concurrency       int
	PollInterval      time.Duration
	Lease             time.Duration
	HandlerTimeout    time.Duration
	HeartbeatInterval time.Duration
	ReapInterval      time.Duration
	ShutdownGrace     time.Duration
	Jitter            func(time.Duration) time.Duration // nil = queue.DefaultJitter
}

type Worker struct {
	store    Store
	reg      *handlers.Registry
	opt      Options
	log      *slog.Logger
	inflight atomic.Int32
}

func New(store Store, reg *handlers.Registry, opt Options, log *slog.Logger) *Worker {
	if opt.Jitter == nil {
		opt.Jitter = queue.DefaultJitter
	}
	return &Worker{store: store, reg: reg, opt: opt, log: log.With("worker_id", opt.ID)}
}

// Run blocks until ctx is cancelled, then stops claiming, waits up to
// ShutdownGrace for in-flight jobs, and cancels (and releases) any stragglers.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.store.RegisterWorker(ctx, w.opt.ID); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	w.log.Info("worker started", "concurrency", w.opt.Concurrency, "lease", w.opt.Lease.String(), "handler_timeout", w.opt.HandlerTimeout.String())

	execCtx, cancelExec := context.WithCancel(context.Background())
	defer cancelExec()

	var slots sync.WaitGroup
	for i := 0; i < w.opt.Concurrency; i++ {
		slots.Add(1)
		go func(slot int) {
			defer slots.Done()
			w.slot(ctx, execCtx, slot)
		}(i)
	}

	bgCtx, stopBg := context.WithCancel(context.Background())
	var bg sync.WaitGroup
	bg.Add(2)
	go func() { defer bg.Done(); w.heartbeatLoop(bgCtx, ctx) }()
	go func() { defer bg.Done(); w.reapLoop(ctx) }()

	<-ctx.Done()
	w.log.Info("shutdown: stopped claiming, draining in-flight jobs", "inflight", w.inflight.Load(), "grace", w.opt.ShutdownGrace.String())
	done := make(chan struct{})
	go func() { slots.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.opt.ShutdownGrace):
		w.log.Warn("shutdown: grace period over, cancelling in-flight jobs")
		cancelExec()
		<-done
	}
	stopBg()
	bg.Wait()

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = w.store.Heartbeat(sctx, w.opt.ID, "stopped", 0)
	w.log.Info("worker stopped")
	return nil
}

func (w *Worker) slot(ctx, execCtx context.Context, slot int) {
	for ctx.Err() == nil {
		job, err := w.store.Claim(ctx, w.opt.ID, w.opt.Lease)
		if err != nil && ctx.Err() == nil {
			w.log.Error("claim failed", "slot", slot, "error", err.Error())
		}
		if job == nil {
			select {
			case <-ctx.Done():
			case <-time.After(w.opt.PollInterval):
			}
			continue
		}
		w.execute(execCtx, job)
	}
}

func (w *Worker) execute(execCtx context.Context, job *queue.Claimed) {
	w.inflight.Add(1)
	defer w.inflight.Add(-1)
	log := w.log.With("job_id", job.ID, "job_type", job.Type, "attempt", job.Attempts)
	log.Info("job claimed", "lease_until", job.LeaseUntil)

	hctx, cancel := context.WithTimeout(execCtx, w.opt.HandlerTimeout)
	start := time.Now()
	result, runErr := w.reg.Run(hctx, job.Type, handlers.Attempt{Number: job.Attempts, Max: job.MaxAttempts}, job.Payload)
	timedOut := errors.Is(hctx.Err(), context.DeadlineExceeded)
	cancel()

	// Persisting the outcome must survive shutdown cancellation.
	pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pcancel()

	if runErr == nil {
		err := w.store.Complete(pctx, job.ID, job.Token, w.opt.ID, result)
		switch {
		case errors.Is(err, queue.ErrLeaseLost):
			log.Warn("ack rejected: lease expired or reassigned (fencing)")
		case err != nil:
			log.Error("ack failed; lease will expire and the job will be retried", "error", err.Error())
		default:
			_ = w.store.RecordOutcome(pctx, w.opt.ID, true)
			log.Info("job succeeded", "duration_ms", time.Since(start).Milliseconds())
		}
		return
	}

	if execCtx.Err() != nil && !timedOut {
		if err := w.store.Release(pctx, job.ID, job.Token, w.opt.ID); err == nil {
			log.Warn("job released during shutdown")
		}
		return
	}

	msg := runErr.Error()
	if timedOut {
		msg = fmt.Sprintf("handler timeout after %s", w.opt.HandlerTimeout)
	}
	var perm *handlers.PermanentError
	next := queue.NextStatusOnFailure(job.Attempts, job.MaxAttempts, errors.As(runErr, &perm))
	var delay time.Duration
	if next == queue.StatusQueued {
		delay = queue.Backoff(job.Attempts, w.opt.Jitter)
	}
	err := w.store.Fail(pctx, job.ID, job.Token, w.opt.ID, next, delay, msg)
	switch {
	case errors.Is(err, queue.ErrLeaseLost):
		log.Warn("failure report rejected: lease expired or reassigned (fencing)")
	case err != nil:
		log.Error("failure report failed", "error", err.Error())
	default:
		_ = w.store.RecordOutcome(pctx, w.opt.ID, false)
		log.Warn("job failed", "error", msg, "next_status", next, "backoff_ms", delay.Milliseconds())
	}
}

func (w *Worker) heartbeatLoop(bgCtx, runCtx context.Context) {
	t := time.NewTicker(w.opt.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-bgCtx.Done():
			return
		case <-t.C:
			status := "running"
			if runCtx.Err() != nil {
				status = "draining"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := w.store.Heartbeat(ctx, w.opt.ID, status, int(w.inflight.Load())); err != nil {
				w.log.Error("heartbeat failed", "error", err.Error())
			}
			cancel()
		}
	}
}

func (w *Worker) reapLoop(ctx context.Context) {
	t := time.NewTicker(w.opt.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := w.store.RecoverExpired(ctx, w.opt.ID)
			if err != nil && ctx.Err() == nil {
				w.log.Error("lease recovery failed", "error", err.Error())
			} else if n > 0 {
				w.log.Warn("recovered expired leases", "count", n)
			}
		}
	}
}
