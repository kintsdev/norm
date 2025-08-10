package norm

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithRetry_RespectsBackoffAndAttempts(t *testing.T) {
	kn := &KintsNorm{config: &Config{RetryAttempts: 2, RetryBackoff: 1 * time.Millisecond}}
	calls := 0
	err := kn.withRetry(context.Background(), func() error { calls++; return errors.New("x") })
	if err == nil || calls != 2 {
		t.Fatalf("expected 2 attempts, got %d err=%v", calls, err)
	}
}
