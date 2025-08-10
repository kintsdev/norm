package norm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fake executer to avoid DB
type fakeExec struct {
	lastSQL  string
	lastArgs []any
	rows     [][]any
	fields   []string
	err      error
}

func (f *fakeExec) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.lastSQL, f.lastArgs = sql, args
	return pgconn.CommandTag{}, f.err
}
func (f *fakeExec) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.lastSQL, f.lastArgs = sql, args
	if f.err != nil {
		return nil, f.err
	}
	return &fakeRows{rows: f.rows, fields: f.fields}, nil
}
func (f *fakeExec) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	f.lastSQL, f.lastArgs = sql, args
	if f.err != nil {
		return qbRowErr{err: f.err}
	}
	r, _ := f.Query(ctx, sql, args...)
	return r.(*fakeRows)
}

// fakeRows implements pgx.Rows
type fakeRows struct {
	rows   [][]any
	fields []string
	i      int
	err    error
}

func (r *fakeRows) Values() ([]any, error) {
	if r.i >= len(r.rows) {
		return nil, errors.New("eof")
	}
	v := r.rows[r.i]
	r.i++
	return v, nil
}
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	out := make([]pgconn.FieldDescription, len(r.fields))
	for i, n := range r.fields {
		out[i] = pgconn.FieldDescription{Name: n}
	}
	return out
}
func (r *fakeRows) Next() bool                    { return r.i < len(r.rows) }
func (r *fakeRows) Err() error                    { return r.err }
func (r *fakeRows) Close()                        {}
func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *fakeRows) RawValues() [][]byte           { return nil }
func (r *fakeRows) Conn() *pgx.Conn               { return nil }

// implement Row Scan to satisfy pgx.Rows as pgx.Row for QueryRow path
func (r *fakeRows) Scan(dest ...any) error { return nil }

type qbRowErr struct{ err error }

func (r qbRowErr) Scan(dest ...any) error { return r.err }

func TestQuoteIdentifierAndQualified(t *testing.T) {
	if QuoteIdentifier(`a"b`) != `"a""b"` {
		t.Fatalf("quote")
	}
	if quoteQualified("public.users") != `"public"."users"` {
		t.Fatalf("qualified")
	}
}

func TestBuildSelectAndFind_MapScan(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn}).Table("users").Select("id").Where("id = ?", 1).OrderBy("id ASC").Limit(1)
	sql, args := qb.buildSelect()
	if sql != "SELECT id FROM users WHERE id = $1 ORDER BY id ASC LIMIT 1" {
		t.Fatalf("sql=%s", sql)
	}
	if len(args) != 1 || args[0] != 1 {
		t.Fatalf("args")
	}

	f := &fakeExec{rows: [][]any{{int64(1)}}, fields: []string{"id"}}
	qb.exec = f
	var out []map[string]any
	if err := qb.Find(context.Background(), &out); err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(out) != 1 || out[0]["id"].(int64) != 1 {
		t.Fatalf("scan map")
	}
}

func TestRawAndDelete(t *testing.T) {
	kn := &KintsNorm{}
	f := &fakeExec{}
	qb := (&QueryBuilder{kn: kn, exec: f}).Raw("DELETE FROM users WHERE id = ?", 5)
	if err := qb.Exec(context.Background()); err != nil {
		t.Fatalf("exec: %v", err)
	}
	if f.lastSQL != "DELETE FROM users WHERE id = $1" || f.lastArgs[0] != 5 {
		t.Fatalf("raw convert")
	}

	qb2 := (&QueryBuilder{kn: kn, exec: f}).Table("users").Where("id = ?", 5)
	_, _ = qb2.Delete(context.Background())
	if f.lastSQL != "DELETE FROM users WHERE id = $1" {
		t.Fatalf("delete sql")
	}
}

func TestKeysetPredicate(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn}).Table("t").OrderBy("id ASC").After("id", 10).Before("id", 20)
	sql, args := qb.buildSelect()
	if sql != "SELECT * FROM t WHERE id > $1 AND id < $2 ORDER BY id ASC" {
		t.Fatalf("keyset: %s", sql)
	}
	if len(args) != 2 || args[0] != 10 || args[1] != 20 {
		t.Fatalf("args")
	}
}

func TestFirstNotFoundAndLastRequiresOrder(t *testing.T) {
	kn := &KintsNorm{}
	f := &fakeExec{rows: [][]any{}, fields: []string{"id"}}
	qb := (&QueryBuilder{kn: kn, exec: f}).Table("users").Select("id").Where("id = ?", 1)
	var row map[string]any
	if err := qb.First(context.Background(), &row); err == nil {
		t.Fatalf("expected not found")
	}
	qb2 := (&QueryBuilder{kn: kn}).Table("t")
	var dest []map[string]any
	if err := qb2.Last(context.Background(), &dest); err == nil {
		t.Fatalf("expected validation error for Last without OrderBy")
	}
}

func TestRawNamed_FallbackOnError(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn}).RawNamed("select * from t where id = :id and x = :missing", map[string]any{"id": 1})
	if qb.raw != "select * from t where id = :id and x = :missing" || !qb.isRaw {
		t.Fatalf("raw named fallback not set")
	}
}
