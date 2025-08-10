package kintsnorm

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newPool(ctx context.Context, cfg *Config) (*pgxpool.Pool, error) {
	if cfg == nil {
		return nil, errors.New("nil config")
	}
	conf, err := pgxpool.ParseConfig(cfg.ConnString())
	if err != nil {
		return nil, err
	}
	if cfg.MaxConnections > 0 {
		conf.MaxConns = cfg.MaxConnections
	}
	if cfg.MinConnections > 0 {
		conf.MinConns = cfg.MinConnections
	}
	if cfg.MaxConnLifetime > 0 {
		conf.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		conf.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		conf.HealthCheckPeriod = cfg.HealthCheckPeriod
	}
    if cfg.StatementCacheCapacity > 0 {
        conf.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
        conf.ConnConfig.StatementCacheCapacity = cfg.StatementCacheCapacity
    }

	pool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func newPoolFromConnString(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	conf, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func healthCheck(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("nil pool")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var one int
	if err := pool.QueryRow(ctx, "select 1").Scan(&one); err != nil {
		return err
	}
	if one != 1 {
		return errors.New("health check failed")
	}
	return nil
}

// acquireConn provides a connection for lower-level operations
func (kn *KintsNorm) acquireConn(ctx context.Context) (pgx.Tx, func(context.Context) error, error) {
	// For future: support read/write splitting by context hints
	tx, err := kn.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nil, err
	}
	cleanup := func(c context.Context) error { return tx.Rollback(c) }
	return tx, cleanup, nil
}

// withRetry executes fn with basic retry on transient errors
func (kn *KintsNorm) withRetry(ctx context.Context, fn func() error) error {
	attempts := 0
	backoff := 0 * time.Millisecond
	if kn.config != nil {
		attempts = kn.config.RetryAttempts
		backoff = kn.config.RetryBackoff
	}
	if attempts <= 0 {
		return fn()
	}
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		// naive backoff
		if backoff > 0 {
			time.Sleep(backoff)
		}
	}
	return err
}
