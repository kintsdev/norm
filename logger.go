package kintsnorm

// Field represents a structured logging field
type Field struct {
    Key   string
    Value any
}

type Logger interface {
    Debug(msg string, fields ...Field)
    Info(msg string, fields ...Field)
    Warn(msg string, fields ...Field)
    Error(msg string, fields ...Field)
}

// NoopLogger is a default no-op logger
type NoopLogger struct{}

func (NoopLogger) Debug(msg string, fields ...Field) {}
func (NoopLogger) Info(msg string, fields ...Field)  {}
func (NoopLogger) Warn(msg string, fields ...Field)  {}
func (NoopLogger) Error(msg string, fields ...Field) {}


