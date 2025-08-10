package norm

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recExecQB struct {
	lastSQL  string
	lastArgs []any
}

func (r *recExecQB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.lastSQL, r.lastArgs = sql, args
	return pgconn.CommandTag{}, nil
}
func (r *recExecQB) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.lastSQL, r.lastArgs = sql, args
	return nil, nil
}
func (r *recExecQB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	r.lastSQL, r.lastArgs = sql, args
	return errorRow{err: nil}
}

type qUser struct {
	ID    int64  `db:"id" norm:"primary_key,auto_increment"`
	Name  string `db:"name"`
	Email string `db:"email" norm:"default:now()"`
}

func TestInsertStructBuildsSQL(t *testing.T) {
	kn := &KintsNorm{}
	ex := &recExecQB{}
	qb := (&QueryBuilder{kn: kn, exec: ex}).Table("users")
	_, _ = qb.InsertStruct(context.Background(), &qUser{ID: 10, Name: "a"})
	if ex.lastSQL == "" || len(ex.lastArgs) == 0 {
		t.Fatalf("no exec recorded")
	}
}

func TestUpdateStructByPKBuildsSQL(t *testing.T) {
	kn := &KintsNorm{}
	ex := &recExecQB{}
	qb := (&QueryBuilder{kn: kn, exec: ex}).Table("users")
	_, _ = qb.UpdateStructByPK(context.Background(), &qUser{ID: 10, Name: "b"}, "id")
	if ex.lastSQL == "" || len(ex.lastArgs) == 0 {
		t.Fatalf("no exec recorded")
	}
}
