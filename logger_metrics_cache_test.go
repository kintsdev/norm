package norm

import (
	"context"
	"testing"
	"time"
)

func TestNoopLoggerAndMetricsAndCache(t *testing.T) {
	l := NoopLogger{}
	l.Debug("x")
	l.Info("x")
	l.Warn("x")
	l.Error("x")

	m := NoopMetrics{}
	m.QueryDuration(10*time.Millisecond, "select 1")
	m.ConnectionCount(1, 1)
	m.ErrorCount("x")
	m.CircuitStateChanged("open")

	c := NoopCache{}
	if _, ok, err := c.Get(context.Background(), "k"); ok || err != nil {
		t.Fatalf("noop get")
	}
	if err := c.Set(context.Background(), "k", []byte("v"), time.Second); err != nil {
		t.Fatalf("noop set")
	}
	if err := c.Invalidate(context.Background(), "k"); err != nil {
		t.Fatalf("noop invalidate")
	}
}
