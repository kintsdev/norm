package norm

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type oneRow struct {
	vals   [][]any
	fields []string
	i      int
}

func (r *oneRow) Values() ([]any, error) { v := r.vals[r.i]; r.i++; return v, nil }
func (r *oneRow) FieldDescriptions() []pgconn.FieldDescription {
	out := make([]pgconn.FieldDescription, len(r.fields))
	for i, n := range r.fields {
		out[i] = pgconn.FieldDescription{Name: n}
	}
	return out
}
func (r *oneRow) Next() bool                    { return r.i < len(r.vals) }
func (r *oneRow) Err() error                    { return nil }
func (r *oneRow) Close()                        {}
func (r *oneRow) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *oneRow) RawValues() [][]byte           { return nil }
func (r *oneRow) Conn() *pgx.Conn               { return nil }
func (r *oneRow) Scan(_ ...any) error           { return nil }

type execOne struct{ rows pgx.Rows }

func (e execOne) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (e execOne) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return e.rows, nil }
func (e execOne) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row        { return e.rows }

type fsUser struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

func TestFirst_StructPtrDest(t *testing.T) {
	kn := &KintsNorm{}
	rows := &oneRow{vals: [][]any{{int64(7), "x"}}, fields: []string{"id", "name"}}
	qb := (&QueryBuilder{kn: kn, exec: execOne{rows: rows}}).Table("users").Select("id", "name").OrderBy("id ASC")
	var u fsUser
	if err := qb.First(context.Background(), &u); err != nil {
		t.Fatalf("first: %v", err)
	}
	if u.ID != 7 || u.Name != "x" {
		t.Fatalf("got %+v", u)
	}
}
