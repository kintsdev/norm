package norm

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

// withRetry executes fn with basic retry on transient errors
func (kn *KintsNorm) withRetry(ctx context.Context, fn func() error) error {
	// Circuit check is handled at executor-level; do not duplicate here
	attempts := 0
	baseBackoff := 0 * time.Millisecond
	if kn.config != nil {
		attempts = kn.config.RetryAttempts
		baseBackoff = kn.config.RetryBackoff
	}
	if attempts <= 0 {
		return fn()
	}
	var err error
	for i := 0; i < attempts; i++ {
		// allow external cancellation between attempts
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = fn()
		if err == nil {
			return nil
		}
		if i < attempts-1 && baseBackoff > 0 {
			// exponential backoff with jitter
			sleep := baseBackoff << i
			// cap to 5 seconds
			sleep = min(sleep, 5*time.Second)
			// simple jitter: +/- 20%
			jitter := time.Duration(int64(sleep) * 20 / 100)
			delay := sleep - jitter + time.Duration(int64(jitter)*int64(i%2))
			// respect context during backoff wait
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}
