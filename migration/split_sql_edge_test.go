package migration

import "testing"

func TestSplitSQLStatements_Edges(t *testing.T) {
	in := " ;  CREATE TABLE x(a int); ;  CREATE INDEX i ON x(a);  ;"
	out := splitSQLStatements(in)
	if len(out) != 2 || out[0] != "CREATE TABLE x(a int)" || out[1] != "CREATE INDEX i ON x(a)" {
		t.Fatalf("split: %v", out)
	}
}
