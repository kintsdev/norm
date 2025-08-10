package norm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type execErr struct{ err error }

func (e execErr) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, e.err
}
func (e execErr) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return nil, e.err }
func (e execErr) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row        { return errorRow{err: e.err} }

func TestQueryBuilder_Exec_ErrorWrapped(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn, exec: execErr{err: errors.New("context canceled")}}).Raw("DELETE FROM t WHERE id = ?", 1)
	if err := qb.Exec(context.Background()); err == nil {
		t.Fatalf("expected error")
	}
}

type execDel struct {
	lastSQL  string
	lastArgs []any
}

func (e *execDel) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	e.lastSQL, e.lastArgs = sql, args
	return pgconn.CommandTag{}, nil
}
func (e *execDel) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return nil, nil }
func (e *execDel) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row        { return errorRow{} }

func TestQueryBuilder_Delete_SQL(t *testing.T) {
	kn := &KintsNorm{}
	ex := &execDel{}
	qb := (&QueryBuilder{kn: kn, exec: ex}).Table("t").Where("id = ?", 1).WithInvalidateKeys("k1", "k2")
	_, _ = qb.Delete(context.Background())
	if ex.lastSQL == "" || len(ex.lastArgs) != 1 {
		t.Fatalf("no delete exec")
	}
	if ex.lastSQL != "UPDATE t SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL" {
		t.Fatalf("soft delete sql: %s", ex.lastSQL)
	}

	// Hard delete
	qb2 := (&QueryBuilder{kn: kn, exec: ex}).Table("t").Where("id = ?", 2).HardDelete()
	_, _ = qb2.Delete(context.Background())
	if ex.lastSQL != "DELETE FROM t WHERE id = $1" {
		t.Fatalf("hard delete sql: %s", ex.lastSQL)
	}
}
