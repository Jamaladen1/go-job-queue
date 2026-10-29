// Package config loads process settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
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
}

func Load() (Config, error) {
	host, _ := os.Hostname()
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
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
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
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getint(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
