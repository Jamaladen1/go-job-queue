// Package handlers holds the safe, built-in job types. No user code is ever
// executed: every handler is compiled in and validates its payload strictly.
package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const MaxPayloadBytes = 16 * 1024

var ErrUnknownType = errors.New("unsupported job type")

// Attempt tells a handler which claim it is running under (1-based).
type Attempt struct {
	Number int
	Max    int
}

type Handler interface {
	Validate(payload json.RawMessage) error
	Run(ctx context.Context, a Attempt, payload json.RawMessage) (any, error)
}

// PermanentError marks a failure that retrying cannot fix -> status "failed".
type PermanentError struct{ Msg string }

func (e *PermanentError) Error() string { return e.Msg }

type Registry struct{ m map[string]Handler }

// NewRegistry registers the built-in handlers. maxSleepMS caps sleep jobs so
// they fit inside the handler timeout.
func NewRegistry(maxSleepMS int) *Registry {
	r := &Registry{m: map[string]Handler{}}
	r.Register("sleep", sleepHandler{maxMS: maxSleepMS})
	r.Register("compute", computeHandler{})
	return r
}

func (r *Registry) Register(name string, h Handler) { r.m[name] = h }

func (r *Registry) Types() []string {
	out := make([]string, 0, len(r.m))
	for k := range r.m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (r *Registry) Validate(typ string, payload json.RawMessage) error {
	h, ok := r.m[typ]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownType, typ)
	}
	if len(payload) > MaxPayloadBytes {
		return fmt.Errorf("payload exceeds %d bytes", MaxPayloadBytes)
	}
	return h.Validate(payload)
}

// Run executes a handler, turning panics into permanent errors.
func (r *Registry) Run(ctx context.Context, typ string, a Attempt, payload json.RawMessage) (result json.RawMessage, err error) {
	h, ok := r.m[typ]
	if !ok {
		return nil, &PermanentError{Msg: fmt.Sprintf("unsupported job type %q", typ)}
	}
	defer func() {
		if p := recover(); p != nil {
			err = &PermanentError{Msg: fmt.Sprintf("handler panic: %v", p)}
		}
	}()
	out, err := h.Run(ctx, a, payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

// decodeStrict rejects unknown fields and trailing data.
func decodeStrict(payload json.RawMessage, v any) error {
	if len(bytes.TrimSpace(payload)) == 0 {
		payload = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("malformed payload: %v", err)
	}
	if dec.More() {
		return fmt.Errorf("malformed payload: trailing data")
	}
	return nil
}

// Simulation lets demos fail deterministically.
//   fail_until_attempt: N -> attempts 1..N-1 fail with a retriable error.
//   permanent_fail: true  -> fail with a non-retriable error.
type Simulation struct {
	FailUntilAttempt int  `json:"fail_until_attempt,omitempty"`
	PermanentFail    bool `json:"permanent_fail,omitempty"`
}

func (s Simulation) validate() error {
	if s.FailUntilAttempt < 0 || s.FailUntilAttempt > 20 {
		return fmt.Errorf("fail_until_attempt must be between 0 and 20")
	}
	return nil
}

func (s Simulation) check(a Attempt) error {
	if s.PermanentFail {
		return &PermanentError{Msg: "simulated permanent failure (permanent_fail=true)"}
	}
	if a.Number < s.FailUntilAttempt {
		return fmt.Errorf("simulated failure on attempt %d (succeeds from attempt %d)", a.Number, s.FailUntilAttempt)
	}
	return nil
}

// --- sleep ---

type sleepPayload struct {
	DurationMS int `json:"duration_ms"`
	Simulation
}

type sleepHandler struct{ maxMS int }

func (h sleepHandler) parse(payload json.RawMessage) (sleepPayload, error) {
	var p sleepPayload
	if err := decodeStrict(payload, &p); err != nil {
		return p, err
	}
	if p.DurationMS < 1 || p.DurationMS > h.maxMS {
		return p, fmt.Errorf("duration_ms must be between 1 and %d", h.maxMS)
	}
	return p, p.Simulation.validate()
}

func (h sleepHandler) Validate(payload json.RawMessage) error {
	_, err := h.parse(payload)
	return err
}

func (h sleepHandler) Run(ctx context.Context, a Attempt, payload json.RawMessage) (any, error) {
	p, err := h.parse(payload)
	if err != nil {
		return nil, &PermanentError{Msg: err.Error()}
	}
	t := time.NewTimer(time.Duration(p.DurationMS) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("sleep interrupted: %w", ctx.Err())
	case <-t.C:
	}
	if err := p.check(a); err != nil {
		return nil, err
	}
	return map[string]any{"slept_ms": p.DurationMS, "attempt": a.Number}, nil
}

// --- compute: iterated SHA-256, deterministic and therefore idempotent ---

type computePayload struct {
	Input      string `json:"input"`
	Iterations int    `json:"iterations"`
	Simulation
}

type computeHandler struct{}

const maxIterations = 2_000_000

func (computeHandler) parse(payload json.RawMessage) (computePayload, error) {
	var p computePayload
	if err := decodeStrict(payload, &p); err != nil {
		return p, err
	}
	if p.Iterations == 0 {
		p.Iterations = 100_000
	}
	if p.Iterations < 1 || p.Iterations > maxIterations {
		return p, fmt.Errorf("iterations must be between 1 and %d", maxIterations)
	}
	if len(p.Input) > 256 {
		return p, fmt.Errorf("input must be at most 256 characters")
	}
	return p, p.Simulation.validate()
}

func (h computeHandler) Validate(payload json.RawMessage) error {
	_, err := h.parse(payload)
	return err
}

func (h computeHandler) Run(ctx context.Context, a Attempt, payload json.RawMessage) (any, error) {
	p, err := h.parse(payload)
	if err != nil {
		return nil, &PermanentError{Msg: err.Error()}
	}
	sum := sha256.Sum256([]byte(p.Input))
	for i := 1; i < p.Iterations; i++ {
		if i%10_000 == 0 && ctx.Err() != nil {
			return nil, fmt.Errorf("compute interrupted: %w", ctx.Err())
		}
		sum = sha256.Sum256(sum[:])
	}
	if err := p.check(a); err != nil {
		return nil, err
	}
	return map[string]any{"digest": hex.EncodeToString(sum[:]), "iterations": p.Iterations}, nil
}
