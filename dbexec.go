package norm

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// dbExecuter abstracts pgxpool.Pool and pgx.Tx
type dbExecuter interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// breakerExecuter wraps a dbExecuter with circuit breaker checks
type breakerExecuter struct {
	kn   *KintsNorm
	exec dbExecuter
}

func (b breakerExecuter) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if br := b.kn.breaker; br != nil {
		if err := br.before(); err != nil {
			return pgconn.CommandTag{}, err
		}
		tag, err := b.exec.Exec(ctx, sql, arguments...)
		br.after(err)
		return tag, err
	}
	return b.exec.Exec(ctx, sql, arguments...)
}

func (b breakerExecuter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if br := b.kn.breaker; br != nil {
		if err := br.before(); err != nil {
			return nil, err
		}
		rows, err := b.exec.Query(ctx, sql, args...)
		br.after(err)
		return rows, err
	}
	return b.exec.Query(ctx, sql, args...)
}

func (b breakerExecuter) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if br := b.kn.breaker; br != nil {
		if err := br.before(); err != nil {
			// emulate a Row with immediate error; pgx.Row is interface with Scan method
			return errorRow{err: err}
		}
		row := b.exec.QueryRow(ctx, sql, args...)
		return rowWithAfter{Row: row, after: func(err error) { br.after(err) }}
	}
	return b.exec.QueryRow(ctx, sql, args...)
}

// errorRow implements pgx.Row that always returns error on Scan
type errorRow struct{ err error }

func (e errorRow) Scan(dest ...any) error { return e.err }

// rowWithAfter wraps a Row to call a callback after Scan
type rowWithAfter struct {
	pgx.Row
	after func(error)
}

func (r rowWithAfter) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	if r.after != nil {
		r.after(err)
	}
	return err
}
