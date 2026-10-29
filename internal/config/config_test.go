package config

import (
	"testing"
	"time"
)

func TestValidateRequiresTimeoutShorterThanLease(t *testing.T) {
	base := Config{DatabaseURL: "postgres://x", Concurrency: 2, MaxSleepMS: 10000,
		LeaseDuration: 60 * time.Second, HandlerTimeout: 20 * time.Second, PollInterval: 500 * time.Millisecond,
		HeartbeatInterval: 2 * time.Second, ReapInterval: 5 * time.Second, WorkerStaleAfter: 10 * time.Second}
	if err := base.Validate(); err != nil {
		t.Fatalf("spec defaults rejected: %v", err)
	}
	bad := base
	bad.HandlerTimeout = 60 * time.Second
	if bad.Validate() == nil {
		t.Fatal("handler timeout == lease must be rejected")
	}
	bad = base
	bad.DatabaseURL = ""
	if bad.Validate() == nil {
		t.Fatal("missing DATABASE_URL must be rejected")
	}
	bad = base
	bad.MaxSleepMS = 20000
	if bad.Validate() == nil {
		t.Fatal("MAX_SLEEP_MS above spec cap must be rejected")
	}
}

// Zero intervals would panic in time.NewTicker (heartbeat/reaper) or busy-poll
// the database; a zero lease would fence every acknowledgement.
func TestLoadRejectsNonPositiveDurations(t *testing.T) {
	for _, k := range []string{"LEASE_SECONDS", "HANDLER_TIMEOUT_SECONDS", "POLL_INTERVAL_MS",
		"HEARTBEAT_SECONDS", "REAP_INTERVAL_SECONDS", "WORKER_STALE_SECONDS"} {
		for _, v := range []string{"0", "-1"} {
			t.Setenv("DATABASE_URL", "postgres://x")
			t.Setenv(k, v)
			if _, err := Load(); err == nil {
				t.Errorf("%s=%s accepted", k, v)
			}
			t.Setenv(k, "")
		}
	}
	t.Setenv("SHUTDOWN_GRACE_SECONDS", "0")
	if _, err := Load(); err != nil {
		t.Errorf("SHUTDOWN_GRACE_SECONDS=0 should be allowed: %v", err)
	}
}

func TestLoadRejectsNonIntegerValues(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("LEASE_SECONDS", "15s")
	if _, err := Load(); err == nil {
		t.Fatal("LEASE_SECONDS=15s silently fell back to the default")
	}
}

func TestLoadDefaultsMatchSpec(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.LeaseDuration != 60*time.Second || c.HandlerTimeout != 20*time.Second {
		t.Fatalf("defaults: lease=%s timeout=%s", c.LeaseDuration, c.HandlerTimeout)
	}
}
