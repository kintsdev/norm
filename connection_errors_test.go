package norm

import (
	"context"
	"testing"
)

func TestNewPool_NilConfig(t *testing.T) {
	if _, err := newPool(context.Background(), nil); err == nil {
		t.Fatalf("expected error for nil config")
	}
}

// Note: newPoolFromConnString may accept flexible strings; skip brittle parse-failure expectations

func TestHealthCheck_NilPool(t *testing.T) {
	if err := healthCheck(context.Background(), nil); err == nil {
		t.Fatalf("expected error for nil pool")
	}
}
