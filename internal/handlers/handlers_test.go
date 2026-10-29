package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPayloadValidation(t *testing.T) {
	r := NewRegistry(10000)
	cases := []struct {
		typ, payload string
		ok           bool
	}{
		{"sleep", `{"duration_ms":500}`, true},
		{"sleep", `{"duration_ms":10000}`, true},
		{"sleep", `{"duration_ms":10001}`, false}, // spec: reject > 10000
		{"sleep", `{"duration_ms":0}`, false},
		{"sleep", `{}`, false},
		{"sleep", `{"duration_ms":"500"}`, false},
		{"sleep", `{"duration_ms":5,"unknown":1}`, false},
		{"sleep", `{"duration_ms":5} {}`, false},
		{"sleep", `{"duration_ms":5,"fail_until_attempt":3}`, true},
		{"sleep", `{"duration_ms":5,"fail_until_attempt":-1}`, false},
		{"compute", `{"input":"x","iterations":1000}`, true},
		{"compute", `{}`, true},
		{"compute", `{"iterations":3000000}`, false},
		{"compute", `{"input":"` + strings.Repeat("a", 300) + `"}`, false},
		{"shell", `{}`, false},
		{"", `{}`, false},
	}
	for _, c := range cases {
		err := r.Validate(c.typ, json.RawMessage(c.payload))
		if (err == nil) != c.ok {
			t.Errorf("Validate(%s, %s) err=%v, want ok=%v", c.typ, c.payload, err, c.ok)
		}
	}
	if err := r.Validate("shell", nil); !errors.Is(err, ErrUnknownType) {
		t.Errorf("expected ErrUnknownType, got %v", err)
	}
	big := `{"input":"` + strings.Repeat("a", MaxPayloadBytes) + `"}`
	if err := r.Validate("compute", json.RawMessage(big)); err == nil {
		t.Error("oversized payload accepted")
	}
}

func TestSleepHandlerHonoursCancellation(t *testing.T) {
	r := NewRegistry(10000)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := r.Run(ctx, "sleep", Attempt{1, 3}, json.RawMessage(`{"duration_ms":5000}`))
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("handler ignored cancellation (%s)", time.Since(start))
	}
}

func TestComputeHandlerCancellationAndDeterminism(t *testing.T) {
	r := NewRegistry(10000)
	p := json.RawMessage(`{"input":"abc","iterations":20000}`)
	a, err := r.Run(context.Background(), "compute", Attempt{1, 3}, p)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := r.Run(context.Background(), "compute", Attempt{2, 3}, p)
	if string(a) != string(b) {
		t.Fatalf("compute is not deterministic: %s vs %s", a, b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Run(ctx, "compute", Attempt{1, 3}, json.RawMessage(`{"iterations":2000000}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestSimulatedFailures(t *testing.T) {
	r := NewRegistry(10000)
	p := json.RawMessage(`{"duration_ms":1,"fail_until_attempt":3}`)
	for attempt, wantErr := range map[int]bool{1: true, 2: true, 3: false} {
		_, err := r.Run(context.Background(), "sleep", Attempt{attempt, 3}, p)
		if (err != nil) != wantErr {
			t.Errorf("attempt %d: err=%v wantErr=%v", attempt, err, wantErr)
		}
		var perm *PermanentError
		if errors.As(err, &perm) {
			t.Errorf("fail_until_attempt must be retriable")
		}
	}
	_, err := r.Run(context.Background(), "sleep", Attempt{1, 3}, json.RawMessage(`{"duration_ms":1,"permanent_fail":true}`))
	var perm *PermanentError
	if !errors.As(err, &perm) {
		t.Fatalf("permanent_fail must return PermanentError, got %v", err)
	}
}

type panicky struct{}

func (panicky) Validate(json.RawMessage) error { return nil }
func (panicky) Run(context.Context, Attempt, json.RawMessage) (any, error) {
	panic("boom")
}

func TestPanicBecomesPermanentError(t *testing.T) {
	r := NewRegistry(10000)
	r.Register("panicky", panicky{})
	_, err := r.Run(context.Background(), "panicky", Attempt{1, 1}, nil)
	var perm *PermanentError
	if !errors.As(err, &perm) {
		t.Fatalf("got %v", err)
	}
}
