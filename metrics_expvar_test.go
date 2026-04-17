package norm

import (
	"expvar"
	"strings"
	"testing"
	"time"
)

func TestExpvarMetricsUpdatesVars(t *testing.T) {
	m := ExpvarMetrics{}
	m.QueryDuration(12*time.Millisecond, "select 1")
	m.ConnectionCount(3, 4)
	m.ErrorCount("timeout")
	m.CircuitStateChanged("open")

	if got := expvar.Get("norm_query_count").String(); got == "0" {
		t.Fatalf("query count not updated")
	}
	if got := expvar.Get("norm_last_query_ms").String(); got != "12" {
		t.Fatalf("last query ms mismatch: %s", got)
	}
	if got := expvar.Get("norm_connections_active").String(); got != "3" {
		t.Fatalf("active connections mismatch: %s", got)
	}
	if got := expvar.Get("norm_connections_idle").String(); got != "4" {
		t.Fatalf("idle connections mismatch: %s", got)
	}
	if got := expvar.Get("norm_circuit_state").String(); got != "\"open\"" {
		t.Fatalf("circuit state mismatch: %s", got)
	}
	if got := expvar.Get("norm_error_count").String(); !strings.Contains(got, "timeout") {
		t.Fatalf("error count mismatch: %s", got)
	}
}
