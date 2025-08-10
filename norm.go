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
	logMode  LogMode
	metrics  Metrics
	cache    Cache
	migrator *migration.Migrator
	breaker  *circuitBreaker
	// logging enhancements
	logContextFields   func(ctx context.Context) []Field
	slowQueryThreshold time.Duration
	maskParams         bool
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

	kn := &KintsNorm{
		pool:               pool,
		config:             config,
		logger:             options.logger,
		logMode:            options.logMode,
		metrics:            options.metrics,
		cache:              options.cache,
		logContextFields:   options.logContextFields,
		slowQueryThreshold: options.slowQueryThreshold,
		maskParams:         options.maskParams,
	}
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
		pool:               pool,
		config:             nil,
		logger:             options.logger,
		logMode:            options.logMode,
		metrics:            options.metrics,
		cache:              options.cache,
		logContextFields:   options.logContextFields,
		slowQueryThreshold: options.slowQueryThreshold,
		maskParams:         options.maskParams,
	}
	kn.migrator = migration.NewMigrator(kn.pool)
	return kn, nil
}

// makeLogFields constructs structured logging fields honoring context extractors and masking options
func (kn *KintsNorm) makeLogFields(ctx context.Context, query string, args []any) []Field {
	fields := make([]Field, 0, 8)
	if kn != nil && kn.logContextFields != nil {
		if ctxFields := kn.logContextFields(ctx); len(ctxFields) > 0 {
			fields = append(fields, ctxFields...)
		}
	}
	fields = append(fields, Field{Key: "sql", Value: query})
	if kn != nil && kn.maskParams {
		fields = append(fields, Field{Key: "args", Value: "[masked]"})
	} else {
		fields = append(fields, Field{Key: "args", Value: args})
		fields = append(fields, Field{Key: "stmt", Value: inlineSQL(query, args)})
	}
	return fields
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

// AutoMigrateWithOptions allows enabling destructive ops (e.g., drop columns)
func (kn *KintsNorm) AutoMigrateWithOptions(ctx context.Context, opts migration.ApplyOptions, models ...any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return kn.migrator.AutoMigrateWithOptions(ctx, opts, models...)
}

// MigrateUpDir applies pending .up.sql migrations from a directory
func (kn *KintsNorm) MigrateUpDir(ctx context.Context, dir string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return kn.migrator.MigrateUpDir(ctx, dir)
}

// MigrateDownDir rolls back the last N migrations using .down.sql files
func (kn *KintsNorm) MigrateDownDir(ctx context.Context, dir string, steps int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return kn.migrator.MigrateDownDir(ctx, dir, steps)
}

// SetManualMigrationOptions configures safety gates for manual file-based migrations
func (kn *KintsNorm) SetManualMigrationOptions(opts migration.ManualOptions) {
	kn.migrator.SetManualOptions(opts)
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
