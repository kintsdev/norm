package norm

import (
    "context"
    "testing"
)

func TestNewPool_NilConfig(t *testing.T) {
    if _, err := newPool(context.Background(), nil); err == nil { t.Fatalf("expected error for nil config") }
}

func TestNewPoolFromConnString_Bad(t *testing.T) {
    if _, err := newPoolFromConnString(context.Background(), "bad=xxx"); err == nil { t.Fatalf("expected parse error") }
}

func TestHealthCheck_NilPool(t *testing.T) {
    if err := healthCheck(context.Background(), nil); err == nil { t.Fatalf("expected error for nil pool") }
}


