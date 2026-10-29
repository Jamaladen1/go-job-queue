package queue

import (
	"math/rand/v2"
	"time"
)

// Job statuses.
const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed" // non-retriable handler error
	StatusDead      = "dead"   // retries exhausted
)

const (
	backoffBase = 2 * time.Second
	backoffCap  = 60 * time.Second
)

// Backoff returns the delay before the next attempt after `attempts` claims:
// min(2^(attempts-1) * 2s, 60s). jitter may adjust it (tests inject a fixed one);
// the result is always clamped to [0, 60s].
func Backoff(attempts int, jitter func(time.Duration) time.Duration) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := backoffCap
	if attempts <= 6 {
		d = min(backoffBase<<(attempts-1), backoffCap)
	}
	if jitter != nil {
		d = jitter(d)
	}
	return max(0, min(d, backoffCap))
}

// DefaultJitter spreads retries by up to ±10%.
func DefaultJitter(d time.Duration) time.Duration {
	spread := int64(d) / 10
	if spread <= 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(2*spread+1)-spread)
}

// NextStatusOnFailure decides where a running job goes after a handler error.
// attempts already includes the current attempt (it increments on claim).
func NextStatusOnFailure(attempts, maxAttempts int, permanent bool) string {
	switch {
	case permanent:
		return StatusFailed
	case attempts >= maxAttempts:
		return StatusDead
	default:
		return StatusQueued
	}
}

// CanRetryManually reports whether POST /retry may requeue a job.
func CanRetryManually(status string) bool {
	return status == StatusDead || status == StatusFailed
}

// CanAcknowledge is the fencing rule, mirrored by the SQL WHERE clause on
// Complete/Fail: the job must still be running under the same lease token,
// and the lease must not have expired (by database time).
func CanAcknowledge(status, currentToken, presentedToken string, leaseUntil, now time.Time) bool {
	return status == StatusRunning &&
		currentToken != "" &&
		currentToken == presentedToken &&
		now.Before(leaseUntil)
}
