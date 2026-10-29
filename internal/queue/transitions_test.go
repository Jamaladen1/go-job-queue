package queue

import (
	"testing"
	"time"
)

func TestBackoffSchedule(t *testing.T) {
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 2 * time.Second}, {1, 2 * time.Second}, {2, 4 * time.Second}, {3, 8 * time.Second},
		{4, 16 * time.Second}, {5, 32 * time.Second}, {6, 60 * time.Second}, {7, 60 * time.Second}, {100, 60 * time.Second},
	}
	for _, c := range cases {
		if got := Backoff(c.attempts, nil); got != c.want {
			t.Errorf("Backoff(%d) = %s, want %s", c.attempts, got, c.want)
		}
	}
}

func TestBackoffJitterIsInjectableAndClamped(t *testing.T) {
	plus := func(d time.Duration) time.Duration { return d + 500*time.Millisecond }
	if got := Backoff(2, plus); got != 4500*time.Millisecond {
		t.Fatalf("got %s", got)
	}
	if got := Backoff(9, plus); got != 60*time.Second {
		t.Fatalf("jitter must not exceed cap, got %s", got)
	}
	neg := func(d time.Duration) time.Duration { return -d }
	if got := Backoff(1, neg); got != 0 {
		t.Fatalf("jitter must not go negative, got %s", got)
	}
	for i := 0; i < 1000; i++ {
		d := DefaultJitter(10 * time.Second)
		if d < 9*time.Second || d > 11*time.Second {
			t.Fatalf("default jitter out of ±10%%: %s", d)
		}
	}
}

func TestNextStatusOnFailure(t *testing.T) {
	cases := []struct {
		attempts, max int
		permanent     bool
		want          string
	}{
		{1, 3, false, StatusQueued},
		{2, 3, false, StatusQueued},
		{3, 3, false, StatusDead}, // max_attempts=3 means at most three claims
		{4, 3, false, StatusDead},
		{1, 3, true, StatusFailed},
		{1, 1, false, StatusDead},
	}
	for _, c := range cases {
		if got := NextStatusOnFailure(c.attempts, c.max, c.permanent); got != c.want {
			t.Errorf("NextStatusOnFailure(%d,%d,%v) = %s, want %s", c.attempts, c.max, c.permanent, got, c.want)
		}
	}
}

func TestCanRetryManually(t *testing.T) {
	for s, want := range map[string]bool{StatusDead: true, StatusFailed: true, StatusQueued: false, StatusRunning: false, StatusSucceeded: false} {
		if CanRetryManually(s) != want {
			t.Errorf("CanRetryManually(%s) != %v", s, want)
		}
	}
}

func TestLeaseTokenFencing(t *testing.T) {
	now := time.Now()
	future, past := now.Add(time.Second), now.Add(-time.Second)
	cases := []struct {
		name                   string
		status, cur, presented string
		until                  time.Time
		want                   bool
	}{
		{"owner with live lease", StatusRunning, "tok-2", "tok-2", future, true},
		{"stale token from previous attempt", StatusRunning, "tok-2", "tok-1", future, false},
		{"lease expired", StatusRunning, "tok-1", "tok-1", past, false},
		{"job already requeued", StatusQueued, "", "tok-1", future, false},
		{"job already succeeded", StatusSucceeded, "", "tok-1", future, false},
		{"empty token never matches", StatusRunning, "", "", future, false},
	}
	for _, c := range cases {
		if got := CanAcknowledge(c.status, c.cur, c.presented, c.until, now); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := Cursor{CreatedAt: time.Date(2026, 10, 29, 12, 0, 0, 123456000, time.UTC), ID: "2e72cdcf-e478-4448-a61a-67189be0f72b"}
	got, err := DecodeCursor(c.Encode())
	if err != nil || !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
		t.Fatalf("round trip failed: %+v %v", got, err)
	}
	for _, bad := range []string{"", "!!!", "bm9waXBl", c.Encode() + "x"} {
		if _, err := DecodeCursor(bad); err == nil {
			t.Errorf("DecodeCursor(%q) should fail", bad)
		}
	}
}

func TestIsUUID(t *testing.T) {
	if !IsUUID("2e72cdcf-e478-4448-a61a-67189be0f72b") {
		t.Fatal("valid uuid rejected")
	}
	for _, s := range []string{"", "nope", "2e72cdcf-e478-4448-a61a-67189be0f72", "2e72cdcfxe478-4448-a61a-67189be0f72b", "2e72cdcf-e478-4448-a61a-67189be0f72z"} {
		if IsUUID(s) {
			t.Errorf("IsUUID(%q) should be false", s)
		}
	}
}
