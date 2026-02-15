package norm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
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
	deleteHard    bool   // when true, build hard DELETE instead of soft delete
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
	// cache options
	cacheKey   string
	cacheTTL   time.Duration
	invalidate []string
	// logging
	forceDebug bool
	// soft delete scoping
	qbSoftMode         qbSoftDeleteMode
	modelHasSoftDelete bool
}

// qbSoftDeleteMode controls soft-delete scoping for QueryBuilder
type qbSoftDeleteMode int

const (
	qbSoftModeDefault qbSoftDeleteMode = iota
	qbSoftModeWithTrashed
	qbSoftModeOnlyTrashed
)

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

// Model initializes a new query builder and sets its table name inferred from the provided model type.
// Usage: kn.Model(&User{}).Where("id = ?", 1).First(ctx, &u)
func (kn *KintsNorm) Model(model any) *QueryBuilder {
	qb := kn.Query()
	return qb.Model(model)
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
	// unknown model; do not assume soft-delete
	qb.modelHasSoftDelete = false
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

// UsePrimary routes subsequent calls (Query/Find/First/Last) through the primary pool (overrides auto read routing)
func (qb *QueryBuilder) UsePrimary() *QueryBuilder {
	exec := dbExecuter(qb.kn.pool)
	if qb.kn.breaker != nil {
		exec = breakerExecuter{kn: qb.kn, exec: exec}
	}
	qb.exec = exec
	return qb
}

// Debug enables debug logging for this builder chain regardless of global LogMode
func (qb *QueryBuilder) Debug() *QueryBuilder {
	qb.forceDebug = true
	return qb
}

// UseReadPool forces using the read pool for reads even if no auto routing is enabled
// Note: Do not use this for writes; Exec/insert/update/delete should go to primary
func (qb *QueryBuilder) UseReadPool() *QueryBuilder {
	exec := dbExecuter(qb.kn.ReadPool())
	if qb.kn.breaker != nil {
		exec = breakerExecuter{kn: qb.kn, exec: exec}
	}
	qb.exec = exec
	return qb
}

func (qb *QueryBuilder) Table(name string) *QueryBuilder {
	qb.table = name
	// unknown model; do not assume soft-delete
	qb.modelHasSoftDelete = false
	return qb
}

// Model sets the table name by inferring it from a provided model type/value.
// It follows the same convention used by the repository: snake_case(type name) + "s".
// Examples:
//
//	qb.Model(&User{})
//	qb.Model(User{})
func (qb *QueryBuilder) Model(model any) *QueryBuilder {
	t := reflect.TypeOf(model)
	if t == nil {
		return qb
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	qb.table = core.ToSnakeCase(t.Name()) + "s"
	qb.modelHasSoftDelete = core.ModelHasSoftDelete(t)
	return qb
}

func (qb *QueryBuilder) Select(columns ...string) *QueryBuilder {
	qb.columns = append(qb.columns, columns...)
	return qb
}

func (qb *QueryBuilder) Join(table, on string) *QueryBuilder {
	qb.joins = append(qb.joins, "JOIN "+table+" ON "+on)
	return qb
}

// InnerJoin appends an INNER JOIN clause (alias of Join)
func (qb *QueryBuilder) InnerJoin(table, on string) *QueryBuilder {
	return qb.Join(table, on)
}

// LeftJoin appends a LEFT JOIN clause
func (qb *QueryBuilder) LeftJoin(table, on string) *QueryBuilder {
	qb.joins = append(qb.joins, "LEFT JOIN "+table+" ON "+on)
	return qb
}

// RightJoin appends a RIGHT JOIN clause
func (qb *QueryBuilder) RightJoin(table, on string) *QueryBuilder {
	qb.joins = append(qb.joins, "RIGHT JOIN "+table+" ON "+on)
	return qb
}

// FullJoin appends a FULL JOIN clause
func (qb *QueryBuilder) FullJoin(table, on string) *QueryBuilder {
	qb.joins = append(qb.joins, "FULL JOIN "+table+" ON "+on)
	return qb
}

// CrossJoin appends a CROSS JOIN clause (no ON condition)
func (qb *QueryBuilder) CrossJoin(table string) *QueryBuilder {
	qb.joins = append(qb.joins, "CROSS JOIN "+table)
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

// WithCacheKey enables read-through caching for Find/First on this builder. TTL<=0 means no Set.
func (qb *QueryBuilder) WithCacheKey(key string, ttl time.Duration) *QueryBuilder {
	qb.cacheKey = key
	qb.cacheTTL = ttl
	return qb
}

// WithInvalidateKeys sets keys to invalidate after write operations (Exec/Insert/Update/Delete)
func (qb *QueryBuilder) WithInvalidateKeys(keys ...string) *QueryBuilder {
	qb.invalidate = append(qb.invalidate, keys...)
	return qb
}

// WithTrashed includes soft-deleted rows (deleted_at IS NOT NULL or NULL) in results
func (qb *QueryBuilder) WithTrashed() *QueryBuilder { qb.qbSoftMode = qbSoftModeWithTrashed; return qb }

// OnlyTrashed restricts to only soft-deleted rows (deleted_at IS NOT NULL)
func (qb *QueryBuilder) OnlyTrashed() *QueryBuilder { qb.qbSoftMode = qbSoftModeOnlyTrashed; return qb }

// Unscoped is an alias of WithTrashed (GORM-compatible naming)
func (qb *QueryBuilder) Unscoped() *QueryBuilder { return qb.WithTrashed() }

func (qb *QueryBuilder) buildSelect() (string, []any) {
	if qb.isRaw {
		// Add explicit type casts to placeholders based on Go arg types to help Postgres infer types in raw queries
		return addTypeCastsToPlaceholders(qb.raw, qb.args), qb.args
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
	// collect where clauses including default soft-delete scoping for Model-based queries
	whereClauses := make([]string, 0, len(qb.wheres)+1)
	whereClauses = append(whereClauses, qb.wheres...)
	if qb.modelHasSoftDelete {
		switch qb.qbSoftMode {
		case qbSoftModeOnlyTrashed:
			whereClauses = append(whereClauses, "deleted_at IS NOT NULL")
		case qbSoftModeWithTrashed:
			// no default filter
		default:
			whereClauses = append(whereClauses, "deleted_at IS NULL")
		}
	}
	if len(whereClauses) > 0 {
		sb.WriteString(" WHERE ")
		where := strings.Join(whereClauses, " AND ")
		where = sqlutil.ConvertQMarksToPgPlaceholders(where)
		sb.WriteString(where)
	}
	// keyset
	keyset := qb.buildKeysetPredicate()
	if keyset != "" {
		if len(whereClauses) == 0 {
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
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(qb.limit))
	}
	if qb.offset > 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(qb.offset))
	}
	return sb.String(), qb.args
}

// addTypeCastsToPlaceholders appends ::PGTYPE to $n placeholders based on arg types when not already cast
func addTypeCastsToPlaceholders(sql string, args []any) string {
	if len(args) == 0 {
		return sql
	}
	var sb strings.Builder
	sb.Grow(len(sql) + len(args)*12)
	i := 0
	for i < len(sql) {
		if sql[i] == '$' && i+1 < len(sql) && sql[i+1] >= '1' && sql[i+1] <= '9' {
			// parse placeholder number
			j := i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			num, _ := strconv.Atoi(sql[i+1 : j])
			sb.WriteString(sql[i:j])
			// check if already cast (followed by "::")
			if j+1 < len(sql) && sql[j] == ':' && sql[j+1] == ':' {
				// already has a type cast, don't add another
			} else if num >= 1 && num <= len(args) {
				sb.WriteString("::")
				sb.WriteString(pgTypeForArg(args[num-1]))
			}
			i = j
		} else {
			sb.WriteByte(sql[i])
			i++
		}
	}
	return sb.String()
}

func pgTypeForArg(a any) string {
	switch v := a.(type) {
	case int8, int16, int32:
		return "INTEGER"
	case int, int64:
		return "BIGINT"
	case uint8, uint16, uint32:
		return "INTEGER"
	case uint, uint64:
		return "BIGINT"
	case float32:
		return "REAL"
	case float64:
		return "DOUBLE PRECISION"
	case bool:
		return "BOOLEAN"
	case time.Time:
		return "TIMESTAMPTZ"
	case []byte:
		return "BYTEA"
	case string:
		return "TEXT"
	default:
		_ = v
		return "TEXT"
	}
}

// Find runs the query and scans into dest (slice pointer)
func (qb *QueryBuilder) Find(ctx context.Context, dest any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// optional read-through cache
	if qb.kn.cache != nil && qb.cacheKey != "" {
		if data, ok, _ := qb.kn.cache.Get(ctx, qb.cacheKey); ok {
			// Only support *[]map[string]any for now
			if dptr, ok2 := dest.(*[]map[string]any); ok2 {
				var cached []map[string]any
				if err := json.Unmarshal(data, &cached); err == nil {
					*dptr = append((*dptr)[:0], cached...)
					return nil
				}
			}
		}
	}
	query, args := qb.buildSelect()
	started := time.Now()
	rows, err := qb.exec.Query(ctx, query, args...)
	// logging governed by global mode or forced via Debug()
	if qb.kn != nil && qb.kn.logger != nil {
		switch qb.kn.logMode {
		case LogDebug, LogInfo:
			qb.kn.logger.Debug("query", qb.kn.makeLogFields(ctx, query, args)...)
		case LogWarn, LogError:
			// no query-level log; errors will be logged when they occur
		case LogSilent:
			if qb.forceDebug {
				qb.kn.logger.Debug("query", qb.kn.makeLogFields(ctx, query, args)...)
			}
		}
	}
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), query)
	}
	if qb.kn != nil && qb.kn.logger != nil && qb.kn.slowQueryThreshold > 0 {
		if dur := time.Since(started); dur > qb.kn.slowQueryThreshold {
			fields := qb.kn.makeLogFields(ctx, query, args)
			fields = append(fields, Field{Key: "duration_ms", Value: dur.Milliseconds()})
			qb.kn.logger.Warn("slow_query", fields...)
		}
	}
	if err != nil {
		if qb.kn != nil && qb.kn.logger != nil {
			if qb.kn.logMode != LogSilent || qb.forceDebug {
				fields := qb.kn.makeLogFields(ctx, query, args)
				fields = append(fields, Field{Key: "error", Value: err})
				qb.kn.logger.Error("query_error", fields...)
			}
		}
		return wrapPgError(err, query, args)
	}
	defer rows.Close()

	// Minimal generic scan: using pgx.Rows to map to map[string]any, if dest is *[]map[string]any
	switch d := dest.(type) {
	case *[]map[string]any:
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return wrapPgError(err, query, args)
			}
			fds := rows.FieldDescriptions()
			m := make(map[string]any, len(vals))
			for i, v := range vals {
				m[string(fds[i].Name)] = v
			}
			*d = append(*d, m)
		}
		if err := rows.Err(); err != nil {
			return wrapPgError(err, query, args)
		}
		// cache set for *[]map[string]any only for now
		if qb.kn.cache != nil && qb.cacheKey != "" && qb.cacheTTL > 0 {
			if out, err := json.Marshal(*d); err == nil {
				_ = qb.kn.cache.Set(ctx, qb.cacheKey, out, qb.cacheTTL)
			}
		}
		return nil
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
				return wrapPgError(err, query, args)
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
		if err := rows.Err(); err != nil {
			return wrapPgError(err, query, args)
		}
		// optional cache disabled for struct slices in minimal hook
		return nil
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
	// Hard delete path remains the same
	if qb.deleteHard {
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

	// Default: soft delete by setting deleted_at
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(qb.table)
	sb.WriteString(" SET deleted_at = NOW()")
	// where clauses + guard to avoid re-deleting already deleted rows
	if len(qb.wheres) > 0 {
		sb.WriteString(" WHERE ")
		where := strings.Join(qb.wheres, " AND ")
		where = sqlutil.ConvertQMarksToPgPlaceholders(where)
		sb.WriteString(where)
		sb.WriteString(" AND deleted_at IS NULL")
	} else {
		sb.WriteString(" WHERE deleted_at IS NULL")
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
	if qb.kn != nil && qb.kn.logger != nil {
		switch qb.kn.logMode {
		case LogDebug, LogInfo:
			qb.kn.logger.Debug("exec", qb.kn.makeLogFields(ctx, query, args)...)
		case LogWarn, LogError:
		case LogSilent:
			if qb.forceDebug {
				qb.kn.logger.Debug("exec", qb.kn.makeLogFields(ctx, query, args)...)
			}
		}
	}
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), query)
	}
	if qb.kn != nil && qb.kn.logger != nil && qb.kn.slowQueryThreshold > 0 {
		if dur := time.Since(started); dur > qb.kn.slowQueryThreshold {
			fields := qb.kn.makeLogFields(ctx, query, args)
			fields = append(fields, Field{Key: "duration_ms", Value: dur.Milliseconds()})
			qb.kn.logger.Warn("slow_exec", fields...)
		}
	}
	if err != nil {
		if qb.kn != nil && qb.kn.logger != nil {
			if qb.kn.logMode != LogSilent || qb.forceDebug {
				fields := qb.kn.makeLogFields(ctx, query, args)
				fields = append(fields, Field{Key: "error", Value: err})
				qb.kn.logger.Error("exec_error", fields...)
			}
		}
		return 0, wrapPgError(err, query, args)
	}
	if qb.kn.cache != nil && len(qb.invalidate) > 0 {
		_ = qb.kn.cache.Invalidate(ctx, qb.invalidate...)
	}
	return int64(tag.RowsAffected()), nil
}

// HardDelete opts into hard delete for this builder chain
func (qb *QueryBuilder) HardDelete() *QueryBuilder {
	qb.deleteHard = true
	return qb
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
	if qb.kn != nil && qb.kn.logger != nil {
		switch qb.kn.logMode {
		case LogDebug, LogInfo:
			qb.kn.logger.Debug("exec", qb.kn.makeLogFields(ctx, qb.raw, qb.args)...)
		case LogWarn, LogError:
		case LogSilent:
			if qb.forceDebug {
				qb.kn.logger.Debug("exec", qb.kn.makeLogFields(ctx, qb.raw, qb.args)...)
			}
		}
	}
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), qb.raw)
	}
	if qb.kn != nil && qb.kn.logger != nil && qb.kn.slowQueryThreshold > 0 {
		if dur := time.Since(started); dur > qb.kn.slowQueryThreshold {
			fields := qb.kn.makeLogFields(ctx, qb.raw, qb.args)
			fields = append(fields, Field{Key: "duration_ms", Value: dur.Milliseconds()})
			qb.kn.logger.Warn("slow_exec", fields...)
		}
	}
	if err != nil {
		if qb.kn != nil && qb.kn.logger != nil {
			if qb.kn.logMode != LogSilent || qb.forceDebug {
				fields := qb.kn.makeLogFields(ctx, qb.raw, qb.args)
				fields = append(fields, Field{Key: "error", Value: err})
				qb.kn.logger.Error("exec_error", fields...)
			}
		}
		return wrapPgError(err, qb.raw, qb.args)
	}
	if qb.kn.cache != nil && len(qb.invalidate) > 0 {
		_ = qb.kn.cache.Invalidate(ctx, qb.invalidate...)
	}
	return nil
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
	// Pre-count total args
	totalArgs := 0
	for _, r := range qb.insertRows {
		totalArgs += len(r)
	}
	args := make([]any, 0, totalArgs)
	argIdx := 1
	for ri, r := range qb.insertRows {
		if ri > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for ci := range r {
			if ci > 0 {
				sb.WriteString(", ")
			}
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(argIdx))
			argIdx++
		}
		sb.WriteByte(')')
		args = append(args, r...)
	}
	if len(qb.conflictCols) > 0 {
		sb.WriteString(" ON CONFLICT (")
		sb.WriteString(strings.Join(qb.conflictCols, ", "))
		sb.WriteString(") ")
		if qb.updateSetExpr != "" {
			sb.WriteString("DO UPDATE SET ")
			// convert ? to $n and renumber placeholders to continue after insert args
			set := sqlutil.ConvertQMarksToPgPlaceholders(qb.updateSetExpr)
			countQ := strings.Count(qb.updateSetExpr, "?")
			replaced := sqlutil.RenumberPlaceholders(set, argIdx-1)
			argIdx += countQ
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
		if qb.kn != nil && qb.kn.logger != nil {
			switch qb.kn.logMode {
			case LogDebug, LogInfo:
				qb.kn.logger.Debug("exec", Field{Key: "sql", Value: query}, Field{Key: "args", Value: args}, Field{Key: "stmt", Value: inlineSQL(query, args)})
			case LogWarn, LogError:
			case LogSilent:
				if qb.forceDebug {
					qb.kn.logger.Debug("exec", Field{Key: "sql", Value: query}, Field{Key: "args", Value: args}, Field{Key: "stmt", Value: inlineSQL(query, args)})
				}
			}
		}
		if qb.kn.metrics != nil {
			qb.kn.metrics.QueryDuration(time.Since(started), query)
		}
		if err != nil {
			if qb.kn != nil && qb.kn.logger != nil {
				if qb.kn.logMode != LogSilent || qb.forceDebug {
					qb.kn.logger.Error("exec_error", Field{Key: "sql", Value: query}, Field{Key: "args", Value: args}, Field{Key: "error", Value: err})
				}
			}
			return 0, wrapPgError(err, query, args)
		}
		if qb.kn.cache != nil && len(qb.invalidate) > 0 {
			_ = qb.kn.cache.Invalidate(ctx, qb.invalidate...)
		}
		return int64(tag.RowsAffected()), nil
	}
	// RETURNING path: scan into dest like Find
	started := time.Now()
	rows, err := qb.exec.Query(ctx, query, args...)
	if qb.kn != nil && qb.kn.logger != nil {
		switch qb.kn.logMode {
		case LogDebug, LogInfo:
			qb.kn.logger.Debug("query", qb.kn.makeLogFields(ctx, query, args)...)
		case LogWarn, LogError:
		case LogSilent:
			if qb.forceDebug {
				qb.kn.logger.Debug("query", qb.kn.makeLogFields(ctx, query, args)...)
			}
		}
	}
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), query)
	}
	if err != nil {
		if qb.kn != nil && qb.kn.logger != nil {
			if qb.kn.logMode != LogSilent || qb.forceDebug {
				fields := qb.kn.makeLogFields(ctx, query, args)
				fields = append(fields, Field{Key: "error", Value: err})
				qb.kn.logger.Error("query_error", fields...)
			}
		}
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
		if err := rows.Err(); err != nil {
			return count, err
		}
		if qb.kn.cache != nil && len(qb.invalidate) > 0 {
			_ = qb.kn.cache.Invalidate(ctx, qb.invalidate...)
		}
		return count, nil
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
	// convert ? placeholders in SET expression to $1, $2, ...
	set := sqlutil.ConvertQMarksToPgPlaceholders(qb.updateSetExpr)
	args := make([]any, 0)
	countQ := strings.Count(qb.updateSetExpr, "?")
	sb.WriteString(set)
	args = append(args, qb.updateSetArgs...)
	if len(qb.wheres) > 0 {
		sb.WriteString(" WHERE ")
		where := strings.Join(qb.wheres, " AND ")
		// Convert ? and renumber to continue after SET placeholders (single pass, handles >9 correctly)
		whereConv := sqlutil.RenumberPlaceholders(sqlutil.ConvertQMarksToPgPlaceholders(where), countQ)
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
		if qb.kn.cache != nil && len(qb.invalidate) > 0 {
			_ = qb.kn.cache.Invalidate(ctx, qb.invalidate...)
		}
		return int64(tag.RowsAffected()), nil
	}
	started := time.Now()
	rows, err := qb.exec.Query(ctx, query, args...)
	if qb.kn != nil && qb.kn.logger != nil {
		switch qb.kn.logMode {
		case LogDebug, LogInfo:
			qb.kn.logger.Debug("query", qb.kn.makeLogFields(ctx, query, args)...)
		case LogWarn, LogError:
		case LogSilent:
			if qb.forceDebug {
				qb.kn.logger.Debug("query", qb.kn.makeLogFields(ctx, query, args)...)
			}
		}
	}
	if qb.kn.metrics != nil {
		qb.kn.metrics.QueryDuration(time.Since(started), query)
	}
	if err != nil {
		if qb.kn != nil && qb.kn.logger != nil {
			if qb.kn.logMode != LogSilent || qb.forceDebug {
				fields := qb.kn.makeLogFields(ctx, query, args)
				fields = append(fields, Field{Key: "error", Value: err})
				qb.kn.logger.Error("query_error", fields...)
			}
		}
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
		if err := rows.Err(); err != nil {
			return count, err
		}
		if qb.kn.cache != nil && len(qb.invalidate) > 0 {
			_ = qb.kn.cache.Invalidate(ctx, qb.invalidate...)
		}
		return count, nil
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
		// Prefer `norm` tag; fallback to legacy `orm`
		orm := f.Tag.Get("norm")
		if orm == "" {
			orm = f.Tag.Get("orm")
		}
		low := strings.ToLower(orm)
		if strings.Contains(low, "-") || strings.Contains(low, "ignore") {
			continue
		}
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
	var sb strings.Builder
	if qb.afterColumn != "" {
		cmp := ">"
		if dir == "desc" {
			cmp = "<"
		}
		sb.WriteString(qb.afterColumn)
		sb.WriteByte(' ')
		sb.WriteString(cmp)
		sb.WriteString(" $")
		sb.WriteString(strconv.Itoa(len(qb.args) + 1))
		qb.args = append(qb.args, qb.afterValue)
	}
	if qb.beforeColumn != "" {
		if sb.Len() > 0 {
			sb.WriteString(" AND ")
		}
		cmp := "<"
		if dir == "desc" {
			cmp = ">"
		}
		sb.WriteString(qb.beforeColumn)
		sb.WriteByte(' ')
		sb.WriteString(cmp)
		sb.WriteString(" $")
		sb.WriteString(strconv.Itoa(len(qb.args) + 1))
		qb.args = append(qb.args, qb.beforeValue)
	}
	return sb.String()
}
