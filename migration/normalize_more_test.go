package migration

import "testing"

func TestNormalizeType_Cases(t *testing.T) {
	cases := map[fieldTag]string{
		{DBType: "varchar(50)"}:      "varchar(50)",
		{DBType: "text"}:             "TEXT",
		{DBType: "timestamptz"}:      "TIMESTAMPTZ",
		{DBType: "bigint"}:           "BIGINT",
		{DBType: "integer"}:          "INTEGER",
		{DBType: "boolean"}:          "BOOLEAN",
		{DBType: "double precision"}: "DOUBLE PRECISION",
		{DBType: "real"}:             "REAL",
		{DBType: "unknown_custom"}:   "unknown_custom",
	}
	for in, want := range cases {
		if got := normalizeType(in); got != want {
			t.Fatalf("%v -> %s (got %s)", in.DBType, want, got)
		}
	}
}
