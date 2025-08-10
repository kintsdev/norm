package norm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type rowsStruct struct {
	rows   [][]any
	fields []string
	i      int
}

func (r *rowsStruct) Values() ([]any, error) {
	if r.i >= len(r.rows) {
		return nil, errors.New("eof")
	}
	v := r.rows[r.i]
	r.i++
	return v, nil
}
func (r *rowsStruct) FieldDescriptions() []pgconn.FieldDescription {
	out := make([]pgconn.FieldDescription, len(r.fields))
	for i, n := range r.fields {
		out[i] = pgconn.FieldDescription{Name: n}
	}
	return out
}
func (r *rowsStruct) Next() bool                    { return r.i < len(r.rows) }
func (r *rowsStruct) Err() error                    { return nil }
func (r *rowsStruct) Close()                        {}
func (r *rowsStruct) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *rowsStruct) RawValues() [][]byte           { return nil }
func (r *rowsStruct) Conn() *pgx.Conn               { return nil }
func (r *rowsStruct) Scan(dest ...any) error        { return nil }

type execStruct struct {
	rows   [][]any
	fields []string
}

func (e *execStruct) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (e *execStruct) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return &rowsStruct{rows: e.rows, fields: e.fields}, nil
}
func (e *execStruct) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return &rowsStruct{rows: e.rows, fields: e.fields}
}

type sUser struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

func TestFind_StructScan(t *testing.T) {
	kn := &KintsNorm{}
	ex := &execStruct{rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}, fields: []string{"id", "name"}}
	qb := (&QueryBuilder{kn: kn, exec: ex}).Table("users").Select("id", "name")
	var out []sUser
	if err := qb.Find(context.Background(), &out); err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(out) != 2 || out[0].Name != "a" || out[1].ID != 2 {
		t.Fatalf("out=%v", out)
	}
}
