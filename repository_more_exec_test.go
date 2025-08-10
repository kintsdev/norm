package norm

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recExecRepo struct {
	lastSQL  string
	lastArgs []any
}

func (r *recExecRepo) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.lastSQL, r.lastArgs = sql, args
	return pgconn.CommandTag{}, nil
}

// Satisfy dbExecuter for repository paths that do not call Query
func (r *recExecRepo) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return nil, nil }
func (r *recExecRepo) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return errorRow{err: nil}
}

type repUser struct {
	ID      int64  `db:"id" orm:"primary_key,auto_increment"`
	Name    string `db:"name"`
	Version int64  `db:"version" orm:"version"`
}

func TestRepository_Update_SQL(t *testing.T) {
	kn := &KintsNorm{}
	ex := &recExecRepo{}
	r := &repo[repUser]{kn: kn, exec: ex}
	_ = r.Update(context.Background(), &repUser{ID: 1, Name: "a", Version: 3})
	if ex.lastSQL == "" {
		t.Fatalf("no sql")
	}
}

func TestRepository_Upsert_SQL(t *testing.T) {
	kn := &KintsNorm{}
	ex := &recExecRepo{}
	r := &repo[repUser]{kn: kn, exec: ex}
	_ = r.Upsert(context.Background(), &repUser{ID: 1, Name: "a"}, []string{"id"}, []string{"name"})
	if ex.lastSQL == "" {
		t.Fatalf("no sql")
	}
}

func TestRepository_Delete_CreateBatch(t *testing.T) {
	kn := &KintsNorm{}
	ex := &recExecRepo{}
	r := &repo[repUser]{kn: kn, exec: ex}
	_ = r.Delete(context.Background(), 1)
	if ex.lastSQL == "" {
		t.Fatalf("delete no sql")
	}
	_ = r.CreateBatch(context.Background(), []*repUser{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}})
}
