package norm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	core "github.com/kintsdev/norm/internal/core"
	sqlutil "github.com/kintsdev/norm/internal/sqlutil"
)

// QueryBuilder provides a fluent API for building SQL queries
type QueryBuilder struct {
	kn      *KintsNorm
	exec    dbExecuter
	table   string
	columns []string
	joins   []string
	wheres  []string
	args    []any
	orderBy string
	limit   int
	offset  int
	raw     string
	isRaw   bool
	// write ops
	op            string // "insert" | "update" | "delete"
	insertColumns []string
	insertRows    [][]any
	returningCols []string
	conflictCols  []string
	updateSetExpr string
	updateSetArgs []any
	// keyset
	afterColumn  string
	afterValue   any
	beforeColumn string
	beforeValue  any
}

// Query creates a new query builder
func (kn *KintsNorm) Query() *QueryBuilder {
	// If read pool is configured, route reads automatically using routingExecuter
	if kn.readPool != nil {
		exec := dbExecuter(routingExecuter{kn: kn})
		return &QueryBuilder{kn: kn, exec: exec}
	}
	exec := dbExecuter(kn.pool)
	if kn.breaker != nil {
		exec = breakerExecuter{kn: kn, exec: exec}
	}
	return &QueryBuilder{kn: kn, exec: exec}
}

// QuoteIdentifier safely quotes a SQL identifier by wrapping in double quotes and escaping embedded quotes
func QuoteIdentifier(identifier string) string {
	// escape existing double quotes by doubling them
	esc := strings.ReplaceAll(identifier, "\"", "\"\"")
	return fmt.Sprintf("\"%s\"", esc)
}

// quoteQualified handles schema-qualified names by quoting each part separated by '.'
func quoteQualified(name string) string {
	if strings.TrimSpace(name) == "" {
		return name
	}
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = QuoteIdentifier(strings.TrimSpace(p))
	}
	return strings.Join(parts, ".")
}

// TableQ sets the table using quoted identifier(s) (supports schema-qualified like schema.table)
func (qb *QueryBuilder) TableQ(name string) *QueryBuilder {
	qb.table = quoteQualified(name)
	return qb
}

// SelectQ appends quoted column identifiers
func (qb *QueryBuilder) SelectQ(columns ...string) *QueryBuilder {
	for _, c := range columns {
		qb.columns = append(qb.columns, quoteQualified(c))
	}
	return qb
}

// SelectQI appends fully quoted identifiers as-is (does not split on '.')
// Useful for columns that themselves contain dots in their names
func (qb *QueryBuilder) SelectQI(columns ...string) *QueryBuilder {
	for _, c := range columns {
		qb.columns = append(qb.columns, QuoteIdentifier(c))
	}
	return qb
}

func (qb *QueryBuilder) Table(name string) *QueryBuilder {
	qb.table = name
	return qb
}

func (qb *QueryBuilder) Select(columns ...string) *QueryBuilder {
	qb.columns = append(qb.columns, columns...)
	return qb
}

func (qb *QueryBuilder) Join(table, on string) *QueryBuilder {
	qb.joins = append(qb.joins, fmt.Sprintf("JOIN %s ON %s", table, on))
	return qb
}

func (qb *QueryBuilder) Where(condition string, args ...any) *QueryBuilder {
	qb.wheres = append(qb.wheres, condition)
	qb.args = append(qb.args, args...)
	return qb
}

// WhereNamed adds a WHERE clause with named parameters, converting :name to $n and appending args by map order
func (qb *QueryBuilder) WhereNamed(condition string, namedArgs map[string]any) *QueryBuilder {
	conv, ordered, err := sqlutil.ConvertNamedToPgPlaceholders(condition, namedArgs)
	if err != nil {
		// fall back to original for now; store as-is to surface error later at execution
		qb.wheres = append(qb.wheres, condition)
		return qb
	}
	qb.wheres = append(qb.wheres, conv)
	qb.args = append(qb.args, ordered...)
	return qb
}

func (qb *QueryBuilder) OrderBy(ob string) *QueryBuilder { qb.orderBy = ob; return qb }
func (qb *QueryBuilder) Limit(n int) *QueryBuilder       { qb.limit = n; return qb }
func (qb *QueryBuilder) Offset(n int) *QueryBuilder      { qb.offset = n; return qb }

// Keyset pagination helpers
func (qb *QueryBuilder) After(column string, value any) *QueryBuilder {
	qb.afterColumn = column
	qb.afterValue = value
	return qb
}
func (qb *QueryBuilder) Before(column string, value any) *QueryBuilder {
	qb.beforeColumn = column
	qb.beforeValue = value
	return qb
}

func (qb *QueryBuilder) Raw(sql string, args ...any) *QueryBuilder {
	qb.raw = sqlutil.ConvertQMarksToPgPlaceholders(sql)
	qb.args = append(qb.args, args...)
	qb.isRaw = true
	return qb
}

// RawNamed sets a raw SQL with :name placeholders
func (qb *QueryBuilder) RawNamed(sql string, namedArgs map[string]any) *QueryBuilder {
	conv, ordered, err := sqlutil.ConvertNamedToPgPlaceholders(sql, namedArgs)
	if err != nil {
		// store original; execution will likely fail which is acceptable for visibility
		qb.raw = sql
		qb.isRaw = true
		return qb
	}
	qb.raw = conv
	qb.args = append(qb.args, ordered...)
	qb.isRaw = true
	return qb
}

// WhereCond adds a typed Condition built by helpers in conditions.go
func (qb *QueryBuilder) WhereCond(c Condition) *QueryBuilder {
	return qb.Where(c.Expr, c.Args...)
}

func (qb *QueryBuilder) buildSelect() (string, []any) {
	if qb.isRaw {
		return qb.raw, qb.args
	}
	cols := "*"
	if len(qb.columns) > 0 {
		cols = strings.Join(qb.columns, ", ")
	}
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(cols)
	sb.WriteString(" FROM ")
	sb.WriteString(qb.table)
	if len(qb.joins) > 0 {
		sb.WriteString(" ")
		sb.WriteString(strings.Join(qb.joins, " "))
	}
	if len(qb.wheres) > 0 {
		sb.WriteString(" WHERE ")
		where := strings.Join(qb.wheres, " AND ")
		where = sqlutil.ConvertQMarksToPgPlaceholders(where)
		sb.WriteString(where)
	}
	// keyset
	keyset := qb.buildKeysetPredicate()
	if keyset != "" {
		if len(qb.wheres) == 0 {
			sb.WriteString(" WHERE ")
		} else {
			sb.WriteString(" AND ")
		}
		sb.WriteString(keyset)
	}
	if qb.orderBy != "" {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(qb.orderBy)
	}
	if qb.limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", qb.limit))
	}
	if qb.offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", qb.offset))
	}
	return sb.String(), qb.args
}

// Find runs the query and scans into dest (slice pointer)
func (qb *QueryBuilder) Find(ctx context.Context, dest any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	query, args := qb.buildSelect()
	started := time.Now()
	rows, err := qb.exec.Query(ctx, query, args...)
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), query)
	}
	if err != nil {
		return wrapPgError(err, query, args)
	}
	defer rows.Close()

	// Minimal generic scan: using pgx.Rows to map to map[string]any, if dest is *[]map[string]any
	switch d := dest.(type) {
	case *[]map[string]any:
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			fds := rows.FieldDescriptions()
			m := make(map[string]any, len(vals))
			for i, v := range vals {
				m[string(fds[i].Name)] = v
			}
			*d = append(*d, m)
		}
		return rows.Err()
	default:
		// reflection-based slice of structs
		rv := reflect.ValueOf(dest)
		if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
			return &ORMError{Code: ErrCodeValidation, Message: "dest must be pointer to slice"}
		}
		sliceVal := rv.Elem()
		elemType := sliceVal.Type().Elem()
		mapper := core.StructMapper(elemType)
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			fds := rows.FieldDescriptions()
			elemPtr := reflect.New(elemType)
			for i, v := range vals {
				col := strings.ToLower(string(fds[i].Name))
				if fi, ok := mapper.FieldsByColumn[col]; ok {
					core.SetFieldByIndex(elemPtr, fi.Index, v)
				}
			}
			sliceVal.Set(reflect.Append(sliceVal, elemPtr.Elem()))
		}
		return rows.Err()
	}
}

// First applies LIMIT 1 and scans the first row into dest (pointer to struct or *[]map[string]any with length 1)
func (qb *QueryBuilder) First(ctx context.Context, dest any) error {
	qb.limit = 1
	// If dest is pointer to struct, we scan into slice then copy
	rv := reflect.ValueOf(dest)
	if rv.Kind() == reflect.Ptr && rv.Elem().Kind() == reflect.Struct {
		// create a slice of the struct type
		sliceType := reflect.SliceOf(rv.Elem().Type())
		tmp := reflect.New(sliceType).Interface()
		if err := qb.Find(ctx, tmp); err != nil {
			return err
		}
		sl := reflect.ValueOf(tmp).Elem()
		if sl.Len() == 0 {
			return &ORMError{Code: ErrCodeNotFound, Message: "not found"}
		}
		rv.Elem().Set(sl.Index(0))
		return nil
	}
	// Fallback to normal find
	if err := qb.Find(ctx, dest); err != nil {
		return err
	}
	// Validate at least one row
	switch d := dest.(type) {
	case *[]map[string]any:
		if len(*d) == 0 {
			return &ORMError{Code: ErrCodeNotFound, Message: "not found"}
		}
	}
	return nil
}

// Last requires an explicit OrderBy to be set; applies LIMIT 1 and returns the last row per ordering
func (qb *QueryBuilder) Last(ctx context.Context, dest any) error {
	if strings.TrimSpace(qb.orderBy) == "" {
		return &ORMError{Code: ErrCodeValidation, Message: "Last requires OrderBy to be set"}
	}
	// Invert ordering direction by toggling ASC<->DESC for the last
	ob := strings.TrimSpace(qb.orderBy)
	lower := strings.ToLower(ob)
	if strings.HasSuffix(lower, " asc") {
		qb.orderBy = strings.TrimSpace(ob[:len(ob)-4]) + " DESC"
	} else if strings.HasSuffix(lower, " desc") {
		qb.orderBy = strings.TrimSpace(ob[:len(ob)-5]) + " ASC"
	} else {
		qb.orderBy = ob + " DESC"
	}
	qb.limit = 1
	return qb.First(ctx, dest)
}

// buildDelete builds a DELETE statement from the current builder state
func (qb *QueryBuilder) buildDelete() (string, []any) {
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(qb.table)
	if len(qb.wheres) > 0 {
		sb.WriteString(" WHERE ")
		where := strings.Join(qb.wheres, " AND ")
		where = sqlutil.ConvertQMarksToPgPlaceholders(where)
		sb.WriteString(where)
	}
	return sb.String(), qb.args
}

// Delete executes a DELETE FROM ... WHERE ... and returns rows affected
func (qb *QueryBuilder) Delete(ctx context.Context) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	query, args := qb.buildDelete()
	started := time.Now()
	tag, err := qb.exec.Exec(ctx, query, args...)
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), query)
	}
	if err != nil {
		return 0, err
	}
	return int64(tag.RowsAffected()), nil
}

// convert '?' placeholders to $1, $2... used by pgx
// moved to internal/sqlutil

// Exec executes a raw statement
func (qb *QueryBuilder) Exec(ctx context.Context) error {
	if !qb.isRaw {
		return errors.New("Exec only allowed with Raw query")
	}
	started := time.Now()
	_, err := qb.exec.Exec(ctx, qb.raw, qb.args...)
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), qb.raw)
	}
	return wrapPgError(err, qb.raw, qb.args)
}

// Insert builder
func (qb *QueryBuilder) Insert(columns ...string) *QueryBuilder {
	qb.op = "insert"
	qb.insertColumns = columns
	return qb
}
func (qb *QueryBuilder) Values(values ...any) *QueryBuilder {
	qb.insertRows = append(qb.insertRows, values)
	return qb
}
func (qb *QueryBuilder) ValuesRows(rows [][]any) *QueryBuilder {
	qb.insertRows = append(qb.insertRows, rows...)
	return qb
}
func (qb *QueryBuilder) Returning(cols ...string) *QueryBuilder  { qb.returningCols = cols; return qb }
func (qb *QueryBuilder) OnConflict(cols ...string) *QueryBuilder { qb.conflictCols = cols; return qb }
func (qb *QueryBuilder) DoUpdateSet(setExpr string, args ...any) *QueryBuilder {
	qb.updateSetExpr = setExpr
	qb.updateSetArgs = args
	return qb
}

func (qb *QueryBuilder) buildInsert() (string, []any) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(qb.table)
	if len(qb.insertColumns) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(qb.insertColumns, ", "))
		sb.WriteString(")")
	}
	sb.WriteString(" VALUES ")
	args := make([]any, 0)
	argIdx := 1
	rows := make([]string, 0, len(qb.insertRows))
	for _, r := range qb.insertRows {
		placeholders := make([]string, 0, len(r))
		for range r {
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			argIdx++
		}
		rows = append(rows, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
		args = append(args, r...)
	}
	sb.WriteString(strings.Join(rows, ", "))
	if len(qb.conflictCols) > 0 {
		sb.WriteString(" ON CONFLICT (")
		sb.WriteString(strings.Join(qb.conflictCols, ", "))
		sb.WriteString(") ")
		if qb.updateSetExpr != "" {
			sb.WriteString("DO UPDATE SET ")
			// convert ? to $n in set expr and append args
			set := sqlutil.ConvertQMarksToPgPlaceholders(qb.updateSetExpr)
			// We need to renumber placeholders to continue after current argIdx
			// Simplify: assume set uses only ? placeholders; replace $1.. with proper indexes by rebuilding
			// We'll append args and rely that set has $? style per convert function
			// Already uses $1.. relative to set-only; offset them:
			// naive replace: "$1"->fmt.Sprintf("$%d",start), "$2"->...
			// To keep it simple, rebuild with current index
			countQ := strings.Count(qb.updateSetExpr, "?")
			replaced := set
			for i := 1; i <= countQ; i++ {
				replaced = strings.ReplaceAll(replaced, fmt.Sprintf("$%d", i), fmt.Sprintf("$%d", argIdx))
				argIdx++
			}
			sb.WriteString(replaced)
			args = append(args, qb.updateSetArgs...)
		} else {
			sb.WriteString("DO NOTHING")
		}
	}
	if len(qb.returningCols) > 0 {
		sb.WriteString(" RETURNING ")
		sb.WriteString(strings.Join(qb.returningCols, ", "))
	}
	return sb.String(), args
}

func (qb *QueryBuilder) ExecInsert(ctx context.Context, dest any) (int64, error) {
	if qb.op != "insert" {
		return 0, errors.New("not an insert operation")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	query, args := qb.buildInsert()
	if len(qb.returningCols) == 0 {
		started := time.Now()
		tag, err := qb.exec.Exec(ctx, query, args...)
		if qb.kn.metrics != nil {
			qb.kn.metrics.QueryDuration(time.Since(started), query)
		}
		if err != nil {
			return 0, wrapPgError(err, query, args)
		}
		return int64(tag.RowsAffected()), nil
	}
	// RETURNING path: scan into dest like Find
	started := time.Now()
	rows, err := qb.exec.Query(ctx, query, args...)
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), query)
	}
	if err != nil {
		return 0, wrapPgError(err, query, args)
	}
	defer rows.Close()
	switch d := dest.(type) {
	case *[]map[string]any:
		var count int64
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return count, err
			}
			fds := rows.FieldDescriptions()
			m := make(map[string]any, len(vals))
			for i, v := range vals {
				m[string(fds[i].Name)] = v
			}
			*d = append(*d, m)
			count++
		}
		return count, rows.Err()
	default:
		return 0, &ORMError{Code: ErrCodeValidation, Message: "dest must be *[]map[string]any for RETURNING"}
	}
}

// Update builder (simple form): provide SET expr and args
func (qb *QueryBuilder) Set(setExpr string, args ...any) *QueryBuilder {
	qb.op = "update"
	qb.updateSetExpr = setExpr
	qb.updateSetArgs = args
	return qb
}

func (qb *QueryBuilder) buildUpdate() (string, []any) {
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(qb.table)
	sb.WriteString(" SET ")
	// convert and place args
	set := sqlutil.ConvertQMarksToPgPlaceholders(qb.updateSetExpr)
	// Build WHERE
	args := make([]any, 0)
	// Renumber $n to start at 1 for set
	countQ := strings.Count(qb.updateSetExpr, "?")
	replaced := set
	for i := 1; i <= countQ; i++ {
		replaced = strings.ReplaceAll(replaced, fmt.Sprintf("$%d", i), fmt.Sprintf("$%d", i))
	}
	sb.WriteString(replaced)
	args = append(args, qb.updateSetArgs...)
	if len(qb.wheres) > 0 {
		sb.WriteString(" WHERE ")
		where := strings.Join(qb.wheres, " AND ")
		// shift where placeholders after set args
		setCount := countQ
		whereConv := sqlutil.ConvertQMarksToPgPlaceholders(where)
		for i := 1; i <= strings.Count(where, "?"); i++ {
			whereConv = strings.ReplaceAll(whereConv, fmt.Sprintf("$%d", i), fmt.Sprintf("$%d", setCount+i))
		}
		sb.WriteString(whereConv)
		args = append(args, qb.args...)
	}
	if len(qb.returningCols) > 0 {
		sb.WriteString(" RETURNING ")
		sb.WriteString(strings.Join(qb.returningCols, ", "))
	}
	return sb.String(), args
}

func (qb *QueryBuilder) ExecUpdate(ctx context.Context, dest any) (int64, error) {
	if qb.op != "update" {
		return 0, errors.New("not an update operation")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	query, args := qb.buildUpdate()
	if len(qb.returningCols) == 0 {
		started := time.Now()
		tag, err := qb.exec.Exec(ctx, query, args...)
		if qb.kn.metrics != nil {
			qb.kn.metrics.QueryDuration(time.Since(started), query)
		}
		if err != nil {
			return 0, wrapPgError(err, query, args)
		}
		return int64(tag.RowsAffected()), nil
	}
	started := time.Now()
	rows, err := qb.exec.Query(ctx, query, args...)
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), query)
	}
	if err != nil {
		return 0, wrapPgError(err, query, args)
	}
	defer rows.Close()
	switch d := dest.(type) {
	case *[]map[string]any:
		var count int64
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return count, err
			}
			fds := rows.FieldDescriptions()
			m := make(map[string]any, len(vals))
			for i, v := range vals {
				m[string(fds[i].Name)] = v
			}
			*d = append(*d, m)
			count++
		}
		return count, rows.Err()
	default:
		return 0, &ORMError{Code: ErrCodeValidation, Message: "dest must be *[]map[string]any for RETURNING"}
	}
}

// InsertStruct inserts a struct using its `db` tags. Zero values with default: tag are skipped to allow DB defaults
func (qb *QueryBuilder) InsertStruct(ctx context.Context, entity any) (int64, error) {
	v := reflect.Indirect(reflect.ValueOf(entity))
	t := v.Type()
	cols := []string{}
	row := []any{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		col := f.Tag.Get("db")
		if col == "" {
			col = core.ToSnakeCase(f.Name)
		}
		orm := f.Tag.Get("orm")
		fv := v.Field(i)
		if strings.Contains(orm, "default:") && fv.IsZero() {
			continue
		}
		cols = append(cols, col)
		row = append(row, fv.Interface())
	}
	return qb.Insert(cols...).Values(row...).ExecInsert(ctx, nil)
}

// UpdateStructByPK updates a row by its primary key using `db` tags
func (qb *QueryBuilder) UpdateStructByPK(ctx context.Context, entity any, pkColumn string) (int64, error) {
	v := reflect.Indirect(reflect.ValueOf(entity))
	t := v.Type()
	sets := []string{}
	args := []any{}
	var id any
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		col := f.Tag.Get("db")
		if col == "" {
			col = core.ToSnakeCase(f.Name)
		}
		fv := v.Field(i).Interface()
		if strings.EqualFold(col, pkColumn) {
			id = fv
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = ?", col))
		args = append(args, fv)
	}
	if id == nil {
		return 0, &ORMError{Code: ErrCodeValidation, Message: "missing primary key value"}
	}
	qb.op = "update"
	qb.updateSetExpr = strings.Join(sets, ", ")
	qb.updateSetArgs = args
	qb.Where(""+pkColumn+" = ?", id)
	return qb.ExecUpdate(ctx, nil)
}

func (qb *QueryBuilder) buildKeysetPredicate() string {
	// Only handle when orderBy references the same column
	if qb.afterColumn == "" && qb.beforeColumn == "" {
		return ""
	}
	// detect direction
	dir := "asc"
	lower := strings.ToLower(qb.orderBy)
	if strings.HasSuffix(lower, " desc") {
		dir = "desc"
	}
	// apply predicates
	preds := []string{}
	if qb.afterColumn != "" {
		cmp := ">"
		if dir == "desc" {
			cmp = "<"
		}
		preds = append(preds, fmt.Sprintf("%s %s $%d", qb.afterColumn, cmp, len(qb.args)+1))
		qb.args = append(qb.args, qb.afterValue)
	}
	if qb.beforeColumn != "" {
		cmp := "<"
		if dir == "desc" {
			cmp = ">"
		}
		preds = append(preds, fmt.Sprintf("%s %s $%d", qb.beforeColumn, cmp, len(qb.args)+1))
		qb.args = append(qb.args, qb.beforeValue)
	}
	return strings.Join(preds, " AND ")
}
