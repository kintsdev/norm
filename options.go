package norm

type options struct {
	logger  Logger
	metrics Metrics
	cache   Cache
	logMode LogMode
}

type Option func(*options)

func defaultOptions() options {
	return options{
		logger:  NoopLogger{},
		metrics: NoopMetrics{},
		cache:   nil,
		logMode: LogSilent,
	}
}

func WithLogger(l Logger) Option   { return func(o *options) { o.logger = l } }
func WithMetrics(m Metrics) Option { return func(o *options) { o.metrics = m } }
func WithCache(c Cache) Option     { return func(o *options) { o.cache = c } }

// WithLogMode sets global logging mode similar to GORM's LogMode
func WithLogMode(mode LogMode) Option { return func(o *options) { o.logMode = mode } }
