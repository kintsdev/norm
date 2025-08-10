package norm

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type execTag struct{ n int64 }

func (e execTag) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (e execTag) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return nil, nil }
func (e execTag) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row        { return errorRow{} }

type trashy struct {
	ID        int64  `db:"id" orm:"primary_key"`
	DeletedAt *int64 `db:"deleted_at"`
}

func TestRepository_PurgeTrashed_SQL(t *testing.T) {
	kn := &KintsNorm{}
	r := &repo[trashy]{kn: kn, exec: execTag{}}
	_, _ = r.PurgeTrashed(context.Background())
}
