package norm

import (
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"
)

// Field represents a structured logging field
type Field struct {
	Key   string
	Value any
}

// LogMode controls verbosity of ORM logging
type LogMode int

const (
	// LogSilent disables all logs unless a chain explicitly enables debug
	LogSilent LogMode = iota
	// LogError logs only errors
	LogError
	// LogWarn logs warnings and errors (unused for now but reserved)
	LogWarn
	// LogInfo logs queries and errors
	LogInfo
	// LogDebug logs everything at debug level
	LogDebug
)

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

// StdLogger logs to the standard library logger
type StdLogger struct{}

func (StdLogger) Debug(msg string, fields ...Field) { stdLogPrint("DEBUG", msg, fields...) }
func (StdLogger) Info(msg string, fields ...Field)  { stdLogPrint("INFO", msg, fields...) }
func (StdLogger) Warn(msg string, fields ...Field)  { stdLogPrint("WARN", msg, fields...) }
func (StdLogger) Error(msg string, fields ...Field) { stdLogPrint("ERROR", msg, fields...) }

func stdLogPrint(level string, msg string, fields ...Field) {
	for _, f := range fields {
		if f.Key == "stmt" {
			if s, ok := f.Value.(string); ok && s != "" {
				log.Printf("%s", s)
				return
			}
		}
	}
	log.Printf("[%s] %s %s", level, msg, formatFields(fields))
}

func formatFields(fields []Field) string {
	if len(fields) == 0 {
		return ""
	}
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		var val string
		if s, ok := f.Value.(string); ok {
			val = s
		} else {
			val = fmt.Sprintf("%v", f.Value)
		}
		parts = append(parts, f.Key+"="+val)
	}
	return strings.Join(parts, " ")
}

// inlineSQL returns a paste-ready SQL with all $n placeholders inlined as SQL literals and a trailing semicolon
func inlineSQL(query string, args []any) string {
	if len(args) == 0 {
		qs := strings.TrimSpace(query)
		if strings.HasSuffix(qs, ";") {
			return query
		}
		return query + ";"
	}
	inlined := query
	for i := len(args); i >= 1; i-- {
		ph := fmt.Sprintf("$%d", i)
		lit := sqlLiteral(args[i-1])
		inlined = strings.ReplaceAll(inlined, ph, lit)
	}
	qs := strings.TrimSpace(inlined)
	if strings.HasSuffix(qs, ";") {
		return inlined
	}
	return inlined + ";"
}

func sqlLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch t := v.(type) {
	case string:
		return "'" + escapeSQLString(t) + "'"
	case []byte:
		// Represent bytea as decode(hex,'hex') for easy psql paste
		return "decode('" + strings.ToUpper(hex.EncodeToString(t)) + "','hex')"
	case bool:
		if t {
			return "TRUE"
		}
		return "FALSE"
	case time.Time:
		return "'" + t.Format(time.RFC3339Nano) + "'"
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%v", v)
	}
	return "'" + escapeSQLString(fmt.Sprintf("%v", v)) + "'"
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
