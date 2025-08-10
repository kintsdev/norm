package norm

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Test withRetry attempts logic without requiring DB
func TestWithRetry_SucceedsOnLaterAttempt(t *testing.T) {
	kn := &KintsNorm{config: &Config{RetryAttempts: 3, RetryBackoff: 10 * time.Millisecond}}
	attempts := 0
	err := kn.withRetry(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestWithRetry_NoRetryWhenAttemptsZero(t *testing.T) {
	kn := &KintsNorm{config: &Config{RetryAttempts: 0}}
	attempts := 0
	err := kn.withRetry(context.Background(), func() error {
		attempts++
		return errors.New("fail")
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if attempts != 1 {
		t.Fatalf("expected single call when no retry, got %d", attempts)
	}
}
