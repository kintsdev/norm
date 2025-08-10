package norm

import (
	"context"
	"testing"
)

func TestNorm_Close_Health_Query_QueryRead_ReadPool(t *testing.T) {
	kn := &KintsNorm{}
	if err := kn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := kn.Health(context.Background()); err == nil {
		t.Fatalf("expected health error with nil pool")
	}
	qb := kn.Query()
	if qb == nil || qb.kn != kn {
		t.Fatalf("query builder nil")
	}
	qb2 := kn.QueryRead()
	if qb2 == nil || qb2.kn != kn {
		t.Fatalf("query read nil")
	}
	if kn.ReadPool() != nil {
		t.Fatalf("read pool fallback should be nil when pool nil")
	}
}
