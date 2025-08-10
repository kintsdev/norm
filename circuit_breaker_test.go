package norm

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_Transitions(t *testing.T) {
	var states []string
	cb := newCircuitBreaker(circuitBreakerConfig{failureThreshold: 2, openTimeout: 50 * time.Millisecond, halfOpenMaxInFlight: 1, onStateChange: func(s string) { states = append(states, s) }})

	// two failures -> open
	if err := cb.before(); err != nil {
		t.Fatalf("before: %v", err)
	}
	cb.after(errors.New("x"))
	if err := cb.before(); err != nil {
		t.Fatalf("before2: %v", err)
	}
	cb.after(errors.New("x"))
	if err := cb.before(); err == nil {
		t.Fatalf("expected open error")
	}

	// wait and move to half-open
	time.Sleep(60 * time.Millisecond)
	if err := cb.before(); err != nil {
		t.Fatalf("half-open before: %v", err)
	}
	// successful trial -> closed
	cb.after(nil)
	if err := cb.before(); err != nil {
		t.Fatalf("closed again: %v", err)
	}
}
