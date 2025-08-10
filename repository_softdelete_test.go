package norm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fake exec that records last SQL
type recExec struct{ lastSQL string }

func (r *recExec) Exec(ctx context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	r.lastSQL = sql
	return pgconn.CommandTag{}, nil
}
func (r *recExec) Query(ctx context.Context, sql string, _ ...any) (pgx.Rows, error) {
	r.lastSQL = sql
	return nil, errors.New("no")
}
func (r *recExec) QueryRow(ctx context.Context, sql string, _ ...any) pgx.Row {
	r.lastSQL = sql
	return errorRow{err: errors.New("no")}
}

type softUser struct {
	ID        int64  `db:"id" norm:"primary_key"`
	DeletedAt *int64 `db:"deleted_at"`
}

func TestSoftDeleteGuards(t *testing.T) {
	kn := &KintsNorm{}
	r := &repo[softUser]{kn: kn, exec: &recExec{}}
	if err := r.SoftDelete(context.Background(), 1); err != nil {
		t.Fatalf("soft delete with field should pass: %v", err)
	}
	if _, err := r.SoftDeleteAll(context.Background()); err != nil {
		t.Fatalf("soft delete all: %v", err)
	}
	if err := r.Restore(context.Background(), 1); err != nil {
		t.Fatalf("restore: %v", err)
	}
}
