package norm

import "strings"

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
	placeholders := make([]string, len(vals))
	args := make([]any, len(vals))
	for i, v := range vals {
		placeholders[i] = "?"
		args[i] = v
	}
	return Condition{Expr: col + " IN (" + strings.Join(placeholders, ", ") + ")", Args: args}
}

func RawCond(expr string, args ...any) Condition { return Condition{Expr: expr, Args: args} }

func And(conds ...Condition) Condition {
	if len(conds) == 0 {
		return Condition{Expr: "1=1"}
	}
	exprs := make([]string, 0, len(conds))
	args := make([]any, 0)
	for _, c := range conds {
		exprs = append(exprs, "("+c.Expr+")")
		args = append(args, c.Args...)
	}
	return Condition{Expr: strings.Join(exprs, " AND "), Args: args}
}

func Or(conds ...Condition) Condition {
	if len(conds) == 0 {
		return Condition{Expr: "1=0"}
	}
	exprs := make([]string, 0, len(conds))
	args := make([]any, 0)
	for _, c := range conds {
		exprs = append(exprs, "("+c.Expr+")")
		args = append(args, c.Args...)
	}
	return Condition{Expr: strings.Join(exprs, " OR "), Args: args}
}
