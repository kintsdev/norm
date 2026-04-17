package norm

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

func TestStdLoggerAndFormatHelpers(t *testing.T) {
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	defer log.SetOutput(oldWriter)
	defer log.SetFlags(oldFlags)

	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)

	StdLogger{}.Debug("query", Field{Key: "id", Value: 7}, Field{Key: "name", Value: "alice"})
	out := strings.TrimSpace(buf.String())
	if !strings.Contains(out, "[DEBUG] query id=7 name=alice") {
		t.Fatalf("unexpected log output: %q", out)
	}

	buf.Reset()
	StdLogger{}.Info("ignored", Field{Key: "stmt", Value: "SELECT 1;"})
	if got := strings.TrimSpace(buf.String()); got != "SELECT 1;" {
		t.Fatalf("stmt shortcut mismatch: %q", got)
	}

	if got := formatFields([]Field{{Key: "n", Value: 3}, {Key: "s", Value: "x"}}); got != "n=3 s=x" {
		t.Fatalf("formatFields=%q", got)
	}
	if got := formatFields(nil); got != "" {
		t.Fatalf("expected empty fields, got %q", got)
	}
}

func TestInlineSQLAndSQLLiteral(t *testing.T) {
	ts := time.Unix(1700000000, 0).UTC()
	got := inlineSQL("INSERT INTO t VALUES ($1, $2, $3, $4, $5, $6)", []any{"O'Reilly", []byte{0xAB, 0xCD}, true, nil, ts, 7})
	checks := []string{
		"'O''Reilly'",
		"decode('ABCD','hex')",
		"TRUE",
		"NULL",
		ts.Format(time.RFC3339Nano),
		"7",
	}
	for _, check := range checks {
		if !strings.Contains(got, check) {
			t.Fatalf("inlineSQL missing %q in %q", check, got)
		}
	}
	if !strings.HasSuffix(got, ";") {
		t.Fatalf("inlineSQL should end with semicolon: %q", got)
	}

	if got := inlineSQL("SELECT 1", nil); got != "SELECT 1;" {
		t.Fatalf("inlineSQL no-args mismatch: %q", got)
	}
	if got := sqlLiteral(false); got != "FALSE" {
		t.Fatalf("sqlLiteral false mismatch: %q", got)
	}
	if got := sqlLiteral(struct{ Name string }{Name: "bob"}); !strings.Contains(got, "bob") {
		t.Fatalf("sqlLiteral struct mismatch: %q", got)
	}
	if got := escapeSQLString("a'b"); got != "a''b" {
		t.Fatalf("escapeSQLString mismatch: %q", got)
	}
}
