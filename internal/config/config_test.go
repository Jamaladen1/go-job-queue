package config

import (
	"testing"
	"time"
)

func TestValidateRequiresTimeoutShorterThanLease(t *testing.T) {
	base := Config{DatabaseURL: "postgres://x", Concurrency: 2, MaxSleepMS: 10000,
		LeaseDuration: 60 * time.Second, HandlerTimeout: 20 * time.Second}
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
