package norm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type execErrQB struct{ err error }

func (e execErrQB) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, e.err
}
func (e execErrQB) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return nil, e.err }
func (e execErrQB) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return errorRow{err: e.err}
}

func TestQueryBuilder_JoinAndExecError(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn, exec: execErrQB{err: errors.New("context canceled")}}).Table("a").Join("b", "a.id=b.aid").Raw("update a set x = ?", 1)
	if err := qb.Exec(context.Background()); err == nil {
		t.Fatalf("expected error")
	}
}
