package norm

import (
	"context"
	"testing"
)

func TestLast_SuccessFlipsOrder(t *testing.T) {
	kn := &KintsNorm{}
	f := &fakeExecRU{rows: [][]any{{int64(42)}}, fields: []string{"id"}}
	qb := (&QueryBuilder{kn: kn, exec: f}).Table("t").OrderBy("id ASC").Returning("id")
	var out []map[string]any
	if err := qb.Last(context.Background(), &out); err != nil {
		t.Fatalf("last: %v", err)
	}
	if len(out) != 1 || out[0]["id"].(int64) != 42 {
		t.Fatalf("out=%v", out)
	}
}

func TestWhereNamed_ArgsOrder(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn}).Table("t").WhereNamed("a = :a AND b = :b", map[string]any{"b": 2, "a": 1})
	sql, args := qb.buildSelect()
	if sql == "" || len(args) != 2 || args[0] != 1 || args[1] != 2 {
		t.Fatalf("args=%v sql=%s", args, sql)
	}
}
