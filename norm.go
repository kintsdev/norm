package norm

import (
	"context"
	"errors"
	"time"

	"github.com/kintsdev/norm/migration"

	"github.com/jackc/pgx/v5/pgxpool"
)

// KintsNorm is the main ORM entry point
type KintsNorm struct {
	pool     *pgxpool.Pool
	readPool *pgxpool.Pool
	config   *Config
	logger   Logger
	metrics  Metrics
	migrator *migration.Migrator
	breaker  *circuitBreaker
}

// New creates a new KintsNorm instance, initializing the pgx pool
func New(config *Config, opts ...Option) (*KintsNorm, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}

	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	pool, err := newPool(context.Background(), config)
	if err != nil {
		return nil, err
	}

	kn := &KintsNorm{pool: pool, config: config, logger: options.logger, metrics: options.metrics}
	// optional read-only pool
	if config.ReadOnlyConnString != "" {
		rp, rerr := newPoolFromConnString(context.Background(), config.ReadOnlyConnString)
		if rerr == nil {
			kn.readPool = rp
		}
	}
	kn.migrator = migration.NewMigrator(kn.pool)
	// initialize circuit breaker if enabled
	if config.CircuitBreakerEnabled {
		kn.breaker = newCircuitBreaker(circuitBreakerConfig{
			failureThreshold:    defaultIfZeroInt(config.CircuitFailureThreshold, 5),
			openTimeout:         defaultIfZeroDuration(config.CircuitOpenTimeout, 30*time.Second),
			halfOpenMaxInFlight: defaultIfZeroInt(config.CircuitHalfOpenMaxCalls, 1),
			onStateChange: func(state string) {
				if kn.metrics != nil {
					kn.metrics.CircuitStateChanged(state)
				}
			},
		})
	}
	return kn, nil
}

// NewWithConnString creates a new KintsNorm instance from a full pgx connection string
func NewWithConnString(connString string, opts ...Option) (*KintsNorm, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	pool, err := newPoolFromConnString(context.Background(), connString)
	if err != nil {
		return nil, err
	}
	kn := &KintsNorm{
		pool:    pool,
		config:  nil,
		logger:  options.logger,
		metrics: options.metrics,
	}
	kn.migrator = migration.NewMigrator(kn.pool)
	return kn, nil
}

// default helpers (kept here to avoid extra utils file)
func defaultIfZeroInt(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}
func defaultIfZeroDuration(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}

// AutoMigrate runs schema migrations for given models
func (kn *KintsNorm) AutoMigrate(models ...any) error {
	return kn.migrator.AutoMigrate(context.Background(), models...)
}

// Close gracefully closes the connection pool
func (kn *KintsNorm) Close() error {
	if kn.pool != nil {
		kn.pool.Close()
	}
	return nil
}

// Health performs a simple health check against the database
func (kn *KintsNorm) Health(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return healthCheck(ctx, kn.pool)
}

// Pool exposes the underlying pgx pool (read-only)
func (kn *KintsNorm) Pool() *pgxpool.Pool { return kn.pool }

// ReadPool exposes the read-only replica pool if configured, otherwise returns the primary pool
func (kn *KintsNorm) ReadPool() *pgxpool.Pool {
	if kn.readPool != nil {
		return kn.readPool
	}
	return kn.pool
}

// QueryRead uses the read pool for building queries (falls back to primary)
func (kn *KintsNorm) QueryRead() *QueryBuilder {
	qb := kn.Query()
	exec := dbExecuter(kn.ReadPool())
	if kn.breaker != nil {
		exec = breakerExecuter{kn: kn, exec: exec}
	}
	qb.exec = exec
	return qb
}
