package migration

import "testing"

func TestCanonicalPgType_Extra(t *testing.T) {
	if canonicalPgType("character varying", -1) != "varchar" {
		t.Fatalf("varchar no len")
	}
	if canonicalPgType("INTEGER", 0) != "INTEGER" {
		t.Fatalf("case preserve")
	}
}
