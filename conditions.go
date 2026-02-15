package norm

import (
	"strings"
	"time"
)

type Condition struct {
	Expr string
	Args []any
}

func Eq(col string, v any) Condition { return Condition{Expr: col + " = ?", Args: []any{v}} }
func Ne(col string, v any) Condition { return Condition{Expr: col + " <> ?", Args: []any{v}} }
func Gt(col string, v any) Condition { return Condition{Expr: col + " > ?", Args: []any{v}} }
func Ge(col string, v any) Condition { return Condition{Expr: col + " >= ?", Args: []any{v}} }
func Lt(col string, v any) Condition { return Condition{Expr: col + " < ?", Args: []any{v}} }
func Le(col string, v any) Condition { return Condition{Expr: col + " <= ?", Args: []any{v}} }

func In(col string, vals []any) Condition {
	if len(vals) == 0 {
		return Condition{Expr: "1=0"}
	}
	args := make([]any, len(vals))
	copy(args, vals)
	// Build "col IN (?, ?, ?)" without allocating a []string for placeholders
	var sb strings.Builder
	sb.Grow(len(col) + 5 + len(vals)*3) // col + " IN (" + "?, " per val
	sb.WriteString(col)
	sb.WriteString(" IN (")
	for i := range vals {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('?')
	}
	sb.WriteByte(')')
	return Condition{Expr: sb.String(), Args: args}
}

func RawCond(expr string, args ...any) Condition { return Condition{Expr: expr, Args: args} }

// Between builds a generic BETWEEN condition inclusive of both ends
func Between(col string, start any, end any) Condition {
	return Condition{Expr: col + " BETWEEN ? AND ?", Args: []any{start, end}}
}

// DateRange returns a timestamp range condition inclusive of boundaries
func DateRange(col string, from, to time.Time) Condition {
	return Condition{Expr: col + " BETWEEN ? AND ?", Args: []any{from, to}}
}

// OnDate matches rows where timestamp column falls on the given calendar day (UTC-based start/end)
func OnDate(col string, day time.Time) Condition {
	d := day.UTC()
	start := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	return Condition{Expr: col + " >= ? AND " + col + " < ?", Args: []any{start, end}}
}

func And(conds ...Condition) Condition {
	if len(conds) == 0 {
		return Condition{Expr: "1=1"}
	}
	// Pre-count total args to avoid repeated grow
	totalArgs := 0
	for _, c := range conds {
		totalArgs += len(c.Args)
	}
	args := make([]any, 0, totalArgs)
	var sb strings.Builder
	for i, c := range conds {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteByte('(')
		sb.WriteString(c.Expr)
		sb.WriteByte(')')
		args = append(args, c.Args...)
	}
	return Condition{Expr: sb.String(), Args: args}
}

func Or(conds ...Condition) Condition {
	if len(conds) == 0 {
		return Condition{Expr: "1=0"}
	}
	// Pre-count total args to avoid repeated grow
	totalArgs := 0
	for _, c := range conds {
		totalArgs += len(c.Args)
	}
	args := make([]any, 0, totalArgs)
	var sb strings.Builder
	for i, c := range conds {
		if i > 0 {
			sb.WriteString(" OR ")
		}
		sb.WriteByte('(')
		sb.WriteString(c.Expr)
		sb.WriteByte(')')
		args = append(args, c.Args...)
	}
	return Condition{Expr: sb.String(), Args: args}
}
