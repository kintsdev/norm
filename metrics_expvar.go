package norm

import (
	"expvar"
	"time"
)

// ExpvarMetrics is a simple stdlib adapter using expvar to expose counters/gauges
// It is intended as a minimal example adapter; for production use prefer Prometheus/OpenTelemetry.
// Avoids extra dependencies.
type ExpvarMetrics struct{}

// Exported variables under /debug/vars if the expvar handler is mounted by the app.
var (
	expvarQueryCount        = expvar.NewInt("norm_query_count")
	expvarLastQueryMs       = expvar.NewInt("norm_last_query_ms")
	expvarErrorCount        = expvar.NewMap("norm_error_count")
	expvarCircuitState      = expvar.NewString("norm_circuit_state")
	expvarConnectionsActive = expvar.NewInt("norm_connections_active")
	expvarConnectionsIdle   = expvar.NewInt("norm_connections_idle")
)

func (ExpvarMetrics) QueryDuration(duration time.Duration, _ string) {
	expvarQueryCount.Add(1)
	expvarLastQueryMs.Set(duration.Milliseconds())
}
func (ExpvarMetrics) ConnectionCount(active, idle int32) {
	expvarConnectionsActive.Set(int64(active))
	expvarConnectionsIdle.Set(int64(idle))
}
func (ExpvarMetrics) ErrorCount(errorType string) {
	expvarErrorCount.Add(errorType, 1)
}
func (ExpvarMetrics) CircuitStateChanged(state string) {
	expvarCircuitState.Set(state)
}
