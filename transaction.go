package norm

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type TxOptions struct{}

type TxManager interface {
	WithTransaction(ctx context.Context, fn func(tx Transaction) error) error
	BeginTx(ctx context.Context, opts *TxOptions) (Transaction, error)
}

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Repository() Repository[map[string]any]
	Exec() dbExecuter
	Query() *QueryBuilder
}

type txManager struct{ kn *KintsNorm }

func (kn *KintsNorm) Tx() TxManager { return &txManager{kn: kn} }

type txImpl struct {
	kn *KintsNorm
	tx pgx.Tx
}

func (m *txManager) WithTransaction(ctx context.Context, fn func(tx Transaction) error) error {
	txx, err := m.BeginTx(ctx, &TxOptions{})
	if err != nil {
		return err
	}
	if err := fn(txx); err != nil {
		_ = txx.Rollback(ctx)
		return err
	}
	return txx.Commit(ctx)
}

func (m *txManager) BeginTx(ctx context.Context, opts *TxOptions) (Transaction, error) {
	tx, err := m.kn.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &txImpl{kn: m.kn, tx: tx}, nil
}

func (t *txImpl) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *txImpl) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

func (t *txImpl) Repository() Repository[map[string]any] {
	return NewRepositoryWithExecutor[map[string]any](t.kn, t.tx)
}

func (t *txImpl) Exec() dbExecuter {
	if t.kn.breaker != nil {
		return breakerExecuter{kn: t.kn, exec: t.tx}
	}
	return t.tx
}
func (t *txImpl) Query() *QueryBuilder {
	qb := t.kn.Query()
	if t.kn.breaker != nil {
		qb.exec = breakerExecuter{kn: t.kn, exec: t.tx}
	} else {
		qb.exec = t.tx
	}
	return qb
}
