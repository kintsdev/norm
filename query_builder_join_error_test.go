package norm

import (
	"context"
	"errors"
	"strings"
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

func TestQueryBuilder_JoinVariants_Build(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn, exec: execErrQB{err: errors.New("boom")}}).
		Table("a").
		InnerJoin("b", "a.id=b.aid").
		LeftJoin("c", "a.id=c.aid").
		RightJoin("d", "a.id=d.aid").
		FullJoin("e", "a.id=e.aid").
		CrossJoin("f")
	// ensure SQL contains all join forms in order
	sql, _ := qb.buildSelect()
	wantParts := []string{
		" FROM a ",
		"JOIN b ON a.id=b.aid",
		"LEFT JOIN c ON a.id=c.aid",
		"RIGHT JOIN d ON a.id=d.aid",
		"FULL JOIN e ON a.id=e.aid",
		"CROSS JOIN f",
	}
	for _, p := range wantParts {
		if !strings.Contains(sql, p) {
			t.Fatalf("expected sql to contain %q, got %s", p, sql)
		}
	}
}
