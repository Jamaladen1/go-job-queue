// Command worker claims and executes jobs. Run as many as you like; they
// coordinate only through PostgreSQL.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/jamaladen/go-job-queue/internal/config"
	"github.com/jamaladen/go-job-queue/internal/handlers"
	"github.com/jamaladen/go-job-queue/internal/observability"
	"github.com/jamaladen/go-job-queue/internal/queue"
	"github.com/jamaladen/go-job-queue/internal/worker"
	"github.com/jamaladen/go-job-queue/migrations"
)

func main() {
	log := observability.NewLogger("worker")
	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid config", "error", err.Error())
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := queue.Connect(ctx, cfg.DatabaseURL, log)
	if err != nil {
		log.Error("database connect failed", "error", err.Error())
		os.Exit(1)
	}
	defer pool.Close()
	if err := queue.Migrate(ctx, pool, migrations.FS); err != nil {
		log.Error("migration failed", "error", err.Error())
		os.Exit(1)
	}

	w := worker.New(queue.NewStore(pool), handlers.NewRegistry(cfg.MaxSleepMS), worker.Options{
		ID:                cfg.WorkerID,
		Concurrency:       cfg.Concurrency,
		PollInterval:      cfg.PollInterval,
		Lease:             cfg.LeaseDuration,
		HandlerTimeout:    cfg.HandlerTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
		ReapInterval:      cfg.ReapInterval,
		ShutdownGrace:     cfg.ShutdownGrace,
	}, log)
	if err := w.Run(ctx); err != nil {
		log.Error("worker exited", "error", err.Error())
		os.Exit(1)
	}
}
