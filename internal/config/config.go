// Package config loads process settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL       string
	APIKey            string
	Port              string
	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HandlerTimeout    time.Duration
	HeartbeatInterval time.Duration
	ReapInterval      time.Duration
	ShutdownGrace     time.Duration
	WorkerStaleAfter  time.Duration
	MaxSleepMS        int
	EmbeddedWorkers   int
}

func Load() (Config, error) {
	host, _ := os.Hostname()
	var bad []string
	getint := func(k string, def int) int {
		v := os.Getenv(k)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			bad = append(bad, k)
			return def
		}
		return n
	}
	c := Config{
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		APIKey:            os.Getenv("API_KEY"),
		Port:              getenv("PORT", "8080"),
		WorkerID:          getenv("WORKER_ID", host),
		Concurrency:       getint("WORKER_CONCURRENCY", 2),
		PollInterval:      time.Duration(getint("POLL_INTERVAL_MS", 500)) * time.Millisecond,
		LeaseDuration:     time.Duration(getint("LEASE_SECONDS", 60)) * time.Second,
		HandlerTimeout:    time.Duration(getint("HANDLER_TIMEOUT_SECONDS", 20)) * time.Second,
		HeartbeatInterval: time.Duration(getint("HEARTBEAT_SECONDS", 2)) * time.Second,
		ReapInterval:      time.Duration(getint("REAP_INTERVAL_SECONDS", 5)) * time.Second,
		ShutdownGrace:     time.Duration(getint("SHUTDOWN_GRACE_SECONDS", 10)) * time.Second,
		WorkerStaleAfter:  time.Duration(getint("WORKER_STALE_SECONDS", 10)) * time.Second,
		MaxSleepMS:        getint("MAX_SLEEP_MS", 10000),
		EmbeddedWorkers:   getint("EMBEDDED_WORKERS", 0),
	}
	if len(bad) > 0 {
		return c, fmt.Errorf("not an integer: %s", strings.Join(bad, ", "))
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	for name, d := range map[string]time.Duration{
		"LEASE_SECONDS": c.LeaseDuration, "HANDLER_TIMEOUT_SECONDS": c.HandlerTimeout,
		"POLL_INTERVAL_MS": c.PollInterval, "HEARTBEAT_SECONDS": c.HeartbeatInterval,
		"REAP_INTERVAL_SECONDS": c.ReapInterval, "WORKER_STALE_SECONDS": c.WorkerStaleAfter,
	} {
		if d <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.ShutdownGrace < 0 {
		return fmt.Errorf("SHUTDOWN_GRACE_SECONDS must not be negative")
	}
	if c.HandlerTimeout >= c.LeaseDuration {
		return fmt.Errorf("HANDLER_TIMEOUT_SECONDS (%s) must be shorter than LEASE_SECONDS (%s)", c.HandlerTimeout, c.LeaseDuration)
	}
	if c.MaxSleepMS < 1 || c.MaxSleepMS > 10000 {
		return fmt.Errorf("MAX_SLEEP_MS must be between 1 and 10000")
	}
	if c.Concurrency < 1 || c.Concurrency > 32 {
		return fmt.Errorf("WORKER_CONCURRENCY must be between 1 and 32")
	}
	if c.EmbeddedWorkers < 0 || c.EmbeddedWorkers > 8 {
		return fmt.Errorf("EMBEDDED_WORKERS must be between 0 and 8")
	}
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
