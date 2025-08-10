package norm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeExecRU struct {
	rows     [][]any
	fields   []string
	lastSQL  string
	lastArgs []any
}

func (f *fakeExecRU) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.lastSQL, f.lastArgs = sql, args
	return pgconn.CommandTag{}, nil
}
func (f *fakeExecRU) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.lastSQL, f.lastArgs = sql, args
	return &fakeRowsRU{rows: f.rows, fields: f.fields}, nil
}
func (f *fakeExecRU) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	f.lastSQL, f.lastArgs = sql, args
	return &fakeRowsRU{rows: f.rows, fields: f.fields}
}

type fakeRowsRU struct {
	rows   [][]any
	fields []string
	i      int
}

func (r *fakeRowsRU) Values() ([]any, error) {
	if r.i >= len(r.rows) {
		return nil, errors.New("eof")
	}
	v := r.rows[r.i]
	r.i++
	return v, nil
}
func (r *fakeRowsRU) FieldDescriptions() []pgconn.FieldDescription {
	out := make([]pgconn.FieldDescription, len(r.fields))
	for i, n := range r.fields {
		out[i] = pgconn.FieldDescription{Name: n}
	}
	return out
}
func (r *fakeRowsRU) Next() bool                    { return r.i < len(r.rows) }
func (r *fakeRowsRU) Err() error                    { return nil }
func (r *fakeRowsRU) Close()                        {}
func (r *fakeRowsRU) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *fakeRowsRU) RawValues() [][]byte           { return nil }
func (r *fakeRowsRU) Conn() *pgx.Conn               { return nil }
func (r *fakeRowsRU) Scan(dest ...any) error        { return nil }

func TestExecInsertReturningIntoMapSlice(t *testing.T) {
	kn := &KintsNorm{}
	f := &fakeExecRU{rows: [][]any{{int64(1)}}, fields: []string{"id"}}
	qb := (&QueryBuilder{kn: kn, exec: f}).Table("users").Insert("name").Values("a").Returning("id")
	var out []map[string]any
	n, err := qb.ExecInsert(context.Background(), &out)
	if err != nil || n != 1 {
		t.Fatalf("err=%v n=%d", err, n)
	}
	if len(out) != 1 || out[0]["id"].(int64) != 1 {
		t.Fatalf("out=%v", out)
	}
}

func TestExecUpdateReturningIntoMapSlice(t *testing.T) {
	kn := &KintsNorm{}
	f := &fakeExecRU{rows: [][]any{{int64(1)}}, fields: []string{"id"}}
	qb := (&QueryBuilder{kn: kn, exec: f}).Table("users").Set("name = ?", "b").Where("id = ?", 1).Returning("id")
	var out []map[string]any
	n, err := qb.ExecUpdate(context.Background(), &out)
	if err != nil || n != 1 {
		t.Fatalf("err=%v n=%d", err, n)
	}
	if len(out) != 1 || out[0]["id"].(int64) != 1 {
		t.Fatalf("out=%v", out)
	}
}
