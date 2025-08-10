package norm

type options struct {
	logger  Logger
	metrics Metrics
}

type Option func(*options)

func defaultOptions() options {
	return options{
		logger:  NoopLogger{},
		metrics: NoopMetrics{},
	}
}

func WithLogger(l Logger) Option   { return func(o *options) { o.logger = l } }
func WithMetrics(m Metrics) Option { return func(o *options) { o.metrics = m } }
