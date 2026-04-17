package norm

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMakeLogFieldsAndPoolHelpers(t *testing.T) {
	pool := &pgxpool.Pool{}
	readPool := &pgxpool.Pool{}
	kn := &KintsNorm{
		pool:     pool,
		readPool: readPool,
		logContextFields: func(context.Context) []Field {
			return []Field{{Key: "req_id", Value: "r1"}}
		},
	}

	fields := kn.makeLogFields(context.Background(), "SELECT $1", []any{"x"})
	if len(fields) != 4 {
		t.Fatalf("unexpected field count: %d", len(fields))
	}
	if fields[0].Key != "req_id" || fields[1].Key != "sql" || fields[2].Key != "args" || fields[3].Key != "stmt" {
		t.Fatalf("unexpected fields: %#v", fields)
	}

	kn.maskParams = true
	fields = kn.makeLogFields(context.Background(), "SELECT $1", []any{"x"})
	if len(fields) != 3 || fields[2].Value != "[masked]" {
		t.Fatalf("masked fields mismatch: %#v", fields)
	}

	if kn.Pool() != pool {
		t.Fatalf("pool accessor mismatch")
	}
	if kn.ReadPool() != readPool {
		t.Fatalf("read pool accessor mismatch")
	}
	if qb := kn.QueryRead(); qb == nil || qb.exec == nil {
		t.Fatalf("query read builder not initialized")
	}

	kn2 := &KintsNorm{}
	if err := kn2.Close(); err != nil {
		t.Fatalf("close nil pools: %v", err)
	}
	if defaultIfZeroDuration(0, time.Second) != time.Second {
		t.Fatalf("helper regression")
	}
}
