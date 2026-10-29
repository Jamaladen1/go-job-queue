// Command api serves the HTTP API and dashboard.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jamaladen/go-job-queue/internal/config"
	"github.com/jamaladen/go-job-queue/internal/handlers"
	"github.com/jamaladen/go-job-queue/internal/httpapi"
	"github.com/jamaladen/go-job-queue/internal/observability"
	"github.com/jamaladen/go-job-queue/internal/queue"
	"github.com/jamaladen/go-job-queue/internal/worker"
	"github.com/jamaladen/go-job-queue/migrations"
)

func main() {
	log := observability.NewLogger("api")
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
	log.Info("migrations applied")

	handler := httpapi.New(httpapi.Options{
		Store:       queue.NewStore(pool),
		Registry:    handlers.NewRegistry(cfg.MaxSleepMS),
		APIKey:      cfg.APIKey,
		StaleAfter:  cfg.WorkerStaleAfter,
		MaxSleepMS:  cfg.MaxSleepMS,
		LeaseDur:    cfg.LeaseDuration,
		HandlerTime: cfg.HandlerTimeout,
		Log:         log,
	})
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("api listening", "port", cfg.Port, "auth_required", cfg.APIKey != "")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err.Error())
			stop()
		}
	}()
	// Optional: run worker loops inside this process for single-container hosts
	// (e.g. free tiers with no background workers). Each loop has its own worker
	// ID and still coordinates only through PostgreSQL leases.
	var wg sync.WaitGroup
	for i := 1; i <= cfg.EmbeddedWorkers; i++ {
		id := fmt.Sprintf("%s-embedded-%d", cfg.WorkerID, i)
		w := worker.New(queue.NewStore(pool), handlers.NewRegistry(cfg.MaxSleepMS), worker.Options{
			ID:                id,
			Concurrency:       cfg.Concurrency,
			PollInterval:      cfg.PollInterval,
			Lease:             cfg.LeaseDuration,
			HandlerTimeout:    cfg.HandlerTimeout,
			HeartbeatInterval: cfg.HeartbeatInterval,
			ReapInterval:      cfg.ReapInterval,
			ShutdownGrace:     cfg.ShutdownGrace,
		}, observability.NewLogger("worker"))
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				log.Error("embedded worker exited", "worker_id", id, "error", err.Error())
			}
		}()
		log.Info("embedded worker started", "worker_id", id)
	}

	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	wg.Wait()
	log.Info("api stopped")
}
