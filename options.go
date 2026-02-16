package norm

import (
	"context"
	"time"
)

type options struct {
	logger  Logger
	metrics Metrics
	cache   Cache
	logMode LogMode
	// logging enhancements
	logContextFields   func(ctx context.Context) []Field
	slowQueryThreshold time.Duration
	maskParams         bool
	// audit
	auditHook AuditHook
}

type Option func(*options)

func defaultOptions() options {
	return options{
		logger:             NoopLogger{},
		metrics:            NoopMetrics{},
		cache:              nil,
		logMode:            LogSilent,
		logContextFields:   nil,
		slowQueryThreshold: 0,
		maskParams:         false,
		auditHook:          nil,
	}
}

func WithLogger(l Logger) Option   { return func(o *options) { o.logger = l } }
func WithMetrics(m Metrics) Option { return func(o *options) { o.metrics = m } }
func WithCache(c Cache) Option     { return func(o *options) { o.cache = c } }

// WithLogMode sets global logging mode similar to GORM's LogMode
func WithLogMode(mode LogMode) Option { return func(o *options) { o.logMode = mode } }

// WithLogContextFields registers a function to derive structured log fields from context (e.g. correlation/request IDs)
func WithLogContextFields(fn func(ctx context.Context) []Field) Option {
	return func(o *options) { o.logContextFields = fn }
}

// WithSlowQueryThreshold enables slow query logging when duration exceeds threshold
func WithSlowQueryThreshold(threshold time.Duration) Option {
	return func(o *options) { o.slowQueryThreshold = threshold }
}

// WithLogParameterMasking masks SQL parameters in logs (hides args and avoids inlining into stmt)
func WithLogParameterMasking(mask bool) Option {
	return func(o *options) { o.maskParams = mask }
}

// WithAuditHook registers a global audit hook that is called after every CRUD operation
func WithAuditHook(hook AuditHook) Option {
	return func(o *options) { o.auditHook = hook }
}
