package norm

import "testing"

func TestValuesRows_BuildInsertMultiple(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn}).Table("t").Insert("a", "b").ValuesRows([][]any{{1, 2}, {3, 4}})
	sql, args := qb.buildInsert()
	want := "INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)"
	if sql != want {
		t.Fatalf("sql=%s", sql)
	}
	if len(args) != 4 || args[0] != 1 || args[3] != 4 {
		t.Fatalf("args=%v", args)
	}
}
