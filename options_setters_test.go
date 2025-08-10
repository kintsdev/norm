package norm

import (
	"context"
	"testing"
	"time"
)

type testLogger struct{}

func (testLogger) Debug(string, ...Field) {}
func (testLogger) Info(string, ...Field)  {}
func (testLogger) Warn(string, ...Field)  {}
func (testLogger) Error(string, ...Field) {}

type testMetrics struct{}

func (testMetrics) QueryDuration(_ time.Duration, _ string) {}
func (testMetrics) ConnectionCount(_ int32, _ int32)        {}
func (testMetrics) ErrorCount(_ string)                     {}
func (testMetrics) CircuitStateChanged(_ string)            {}

type testCache struct{}

func (testCache) Get(_ context.Context, _ string) ([]byte, bool, error)            { return nil, false, nil }
func (testCache) Set(_ context.Context, _ string, _ []byte, _ time.Duration) error { return nil }
func (testCache) Invalidate(_ context.Context, _ ...string) error                  { return nil }

func TestOptionSetters(t *testing.T) {
	o := defaultOptions()
	WithLogger(testLogger{})(&o)
	WithMetrics(testMetrics{})(&o)
	WithCache(testCache{})(&o)
	if _, ok := o.logger.(testLogger); !ok {
		t.Fatalf("logger not set")
	}
	if _, ok := o.metrics.(testMetrics); !ok {
		t.Fatalf("metrics not set")
	}
	if _, ok := o.cache.(testCache); !ok {
		t.Fatalf("cache not set")
	}
}
