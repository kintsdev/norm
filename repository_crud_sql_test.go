package norm

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recExec2 struct {
	lastSQL  string
	lastArgs []any
}

func (r *recExec2) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.lastSQL, r.lastArgs = sql, args
	return pgconn.CommandTag{}, nil
}
func (r *recExec2) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.lastSQL, r.lastArgs = sql, args
	return nil, nil
}
func (r *recExec2) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	r.lastSQL, r.lastArgs = sql, args
	return errorRow{err: nil}
}

type rUser struct {
	ID      int64  `db:"id" norm:"primary_key,auto_increment"`
	Name    string `db:"name"`
	Version int64  `db:"version" norm:"version"`
}

func TestRepo_Create_SQL(t *testing.T) {
	kn := &KintsNorm{}
	rex := &recExec2{}
	r := &repo[rUser]{kn: kn, exec: rex}
	_ = r.Create(context.Background(), &rUser{ID: 10, Name: "a"})
	if rex.lastSQL == "" {
		t.Fatalf("no sql")
	}
}

func TestRepo_UpdatePartial_SQL(t *testing.T) {
	kn := &KintsNorm{}
	rex := &recExec2{}
	r := &repo[rUser]{kn: kn, exec: rex}
	_ = r.UpdatePartial(context.Background(), int64(1), map[string]any{"name": "b"})
	if rex.lastSQL == "" {
		t.Fatalf("no sql")
	}
}
