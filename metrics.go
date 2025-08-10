package norm

import "time"

type Metrics interface {
	QueryDuration(duration time.Duration, query string)
	ConnectionCount(active, idle int32)
	ErrorCount(errorType string)
	// Circuit breaker metrics
	CircuitStateChanged(state string)
}

// NoopMetrics is a default no-op metrics collector
type NoopMetrics struct{}

func (NoopMetrics) QueryDuration(duration time.Duration, query string) {}
func (NoopMetrics) ConnectionCount(active, idle int32)                 {}
func (NoopMetrics) ErrorCount(errorType string)                        {}
func (NoopMetrics) CircuitStateChanged(state string)                   {}
