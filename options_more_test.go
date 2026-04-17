package norm

import (
	"context"
	"testing"
	"time"
)

func TestOptionSetters_More(t *testing.T) {
	o := defaultOptions()
	ctxFn := func(context.Context) []Field { return []Field{{Key: "req_id", Value: "abc"}} }
	audit := AuditHookFunc(func(context.Context, AuditEntry) {})

	WithLogMode(LogDebug)(&o)
	WithLogContextFields(ctxFn)(&o)
	WithSlowQueryThreshold(2 * time.Second)(&o)
	WithLogParameterMasking(true)(&o)
	WithAuditHook(audit)(&o)

	if o.logMode != LogDebug {
		t.Fatalf("log mode not set")
	}
	if o.logContextFields == nil || len(o.logContextFields(context.Background())) != 1 {
		t.Fatalf("context field extractor not set")
	}
	if o.slowQueryThreshold != 2*time.Second {
		t.Fatalf("slow threshold not set")
	}
	if !o.maskParams {
		t.Fatalf("mask flag not set")
	}
	if o.auditHook == nil {
		t.Fatalf("audit hook not set")
	}
}
