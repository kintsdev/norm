package norm

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	pgxv5 "github.com/jackc/pgx/v5"
	core "github.com/kintsdev/norm/internal/core"
)

// Condition is a placeholder for typed conditions
// moved to conditions.go

// Repository defines generic CRUD operations for type T
type Repository[T any] interface {
	Create(ctx context.Context, entity *T) error
	CreateBatch(ctx context.Context, entities []*T) error
	GetByID(ctx context.Context, id any) (*T, error)
	Update(ctx context.Context, entity *T) error
	UpdatePartial(ctx context.Context, id any, fields map[string]any) error
	Delete(ctx context.Context, id any) error
	SoftDelete(ctx context.Context, id any) error
	SoftDeleteAll(ctx context.Context) (int64, error)
	Restore(ctx context.Context, id any) error
	PurgeTrashed(ctx context.Context) (int64, error)
	Find(ctx context.Context, conditions ...Condition) ([]*T, error)
	FindOne(ctx context.Context, conditions ...Condition) (*T, error)
	Count(ctx context.Context, conditions ...Condition) (int64, error)
	Exists(ctx context.Context, conditions ...Condition) (bool, error)
	WithTrashed() Repository[T]
	OnlyTrashed() Repository[T]
	FindPage(ctx context.Context, page PageRequest, conditions ...Condition) (Page[T], error)
	CreateCopyFrom(ctx context.Context, entities []*T, columns ...string) (int64, error)
	Upsert(ctx context.Context, entity *T, conflictCols []string, updateCols []string) error
}

// repo is a minimal placeholder implementation to compile
type repo[T any] struct {
	kn   *KintsNorm
	exec dbExecuter
	mode softDeleteMode
}

type softDeleteMode int

const (
	softModeDefault softDeleteMode = iota
	softModeWithTrashed
	softModeOnlyTrashed
)

// NewRepository creates a new generic repository
func NewRepository[T any](kn *KintsNorm) Repository[T] {
	exec := dbExecuter(kn.pool)
	if kn.breaker != nil {
		exec = breakerExecuter{kn: kn, exec: exec}
	}
	return &repo[T]{kn: kn, exec: exec}
}

// NewRepositoryWithExecutor creates a repository bound to a specific executor (pool or tx)
func NewRepositoryWithExecutor[T any](kn *KintsNorm, exec dbExecuter) Repository[T] {
	return &repo[T]{kn: kn, exec: exec}
}

func (r *repo[T]) WithTrashed() Repository[T] { nr := *r; nr.mode = softModeWithTrashed; return &nr }
func (r *repo[T]) OnlyTrashed() Repository[T] { nr := *r; nr.mode = softModeOnlyTrashed; return &nr }

func (r *repo[T]) tableName() string {
	var t T
	typ := reflect.TypeOf(t)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return core.ToSnakeCase(typ.Name()) + "s"
}

func (r *repo[T]) Create(ctx context.Context, entity *T) error {
	if entity == nil {
		return &ORMError{Code: ErrCodeValidation, Message: "nil entity"}
	}
	// model hook: BeforeCreate
	if bc, ok := any(entity).(BeforeCreate); ok {
		if err := bc.BeforeCreate(ctx); err != nil {
			return err
		}
	}
	execFn := func() error {
		val := reflect.Indirect(reflect.ValueOf(entity))
		typ := val.Type()
		mapper := core.StructMapper(typ)
		cols := make([]string, 0, typ.NumField())
		placeholders := make([]string, 0, typ.NumField())
		args := make([]any, 0, typ.NumField())
		idx := 1
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.PkgPath != "" {
				continue
			}
			col := f.Tag.Get("db")
			if col == "" {
				col = core.ToSnakeCase(f.Name)
			}
			if mapper.AutoIncrement && strings.EqualFold(col, mapper.PrimaryColumn) {
				continue
			}
			// Prefer `norm` tag; fallback to legacy `orm`
			orm := f.Tag.Get("norm")
			if orm == "" {
				orm = f.Tag.Get("orm")
			}
			// skip ignored fields
			low := strings.ToLower(orm)
			if strings.Contains(low, "-") || strings.Contains(low, "ignore") {
				continue
			}
			fv := val.Field(i)
			if strings.Contains(orm, "default:") && fv.IsZero() {
				continue
			}
			cols = append(cols, col)
			placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
			args = append(args, fv.Interface())
			idx++
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.tableName(), strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		_, err := r.exec.Exec(ctx, query, args...)
		return err
	}
	if r.kn != nil {
		if err := r.kn.withRetry(ctx, execFn); err != nil {
			return err
		}
	} else {
		if err := execFn(); err != nil {
			return err
		}
	}
	// model hook: AfterCreate
	if ac, ok := any(entity).(AfterCreate); ok {
		if err := ac.AfterCreate(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo[T]) CreateBatch(ctx context.Context, entities []*T) error {
	for _, e := range entities {
		if err := r.Create(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo[T]) GetByID(ctx context.Context, id any) (*T, error) {
	var out []T
	qb := r.kn.Query().Table(r.tableName()).Where("id = ?", id).Limit(1)
	// Apply soft-delete default filter if model has deleted_at
	var t T
	if core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		switch r.mode {
		case softModeOnlyTrashed:
			qb = qb.Where("deleted_at IS NOT NULL")
		case softModeWithTrashed:
			// no filter
		default:
			qb = qb.Where("deleted_at IS NULL")
		}
	}
	if err := qb.Find(ctx, &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, &ORMError{Code: ErrCodeNotFound, Message: "not found"}
	}
	return &out[0], nil
}

func (r *repo[T]) Update(ctx context.Context, entity *T) error {
	// model hook: BeforeUpdate
	if bu, ok := any(entity).(BeforeUpdate); ok {
		if err := bu.BeforeUpdate(ctx); err != nil {
			return err
		}
	}
	val := reflect.Indirect(reflect.ValueOf(entity))
	typ := val.Type()
	mapper := core.StructMapper(typ)
	if mapper.PrimaryColumn == "" {
		return &ORMError{Code: ErrCodeValidation, Message: "no primary key"}
	}

	sets := []string{}
	args := []any{}
	idx := 1
	var id any
	// discover columns that should be set to NOW() on update
	onUpdateNow := r.onUpdateNowColumns(typ)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath != "" {
			continue
		}
		col := f.Tag.Get("db")
		if col == "" {
			col = core.ToSnakeCase(f.Name)
		}
		v := val.Field(i).Interface()
		if strings.EqualFold(col, mapper.PrimaryColumn) {
			id = v
			continue
		}
		// optimistic locking: version column gets incremented
		if strings.EqualFold(col, mapper.VersionColumn) && mapper.VersionColumn != "" {
			sets = append(sets, fmt.Sprintf("%s = %s + 1", col, col))
			continue
		}
		if onUpdateNow[col] {
			sets = append(sets, fmt.Sprintf("%s = NOW()", col))
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = $%d", col, idx))
		args = append(args, v)
		idx++
	}
	if id == nil {
		return &ORMError{Code: ErrCodeValidation, Message: "missing primary key value"}
	}
	// add conditions for optimistic locking if versionColumn present
	if mapper.VersionColumn != "" {
		// read current version value from entity
		curVersion := reflect.Indirect(reflect.ValueOf(entity)).FieldByNameFunc(func(n string) bool { return strings.EqualFold(core.ToSnakeCase(n), mapper.VersionColumn) }).Interface()
		args = append(args, id, curVersion)
		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d AND %s = $%d", r.tableName(), strings.Join(sets, ", "), mapper.PrimaryColumn, idx, mapper.VersionColumn, idx+1)
		tag, err := r.exec.Exec(ctx, query, args...)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return &ORMError{Code: ErrCodeTransaction, Message: "optimistic lock conflict"}
		}
		return nil
	}
	args = append(args, id)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d", r.tableName(), strings.Join(sets, ", "), mapper.PrimaryColumn, idx)
	_, err := r.exec.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	// model hook: AfterUpdate
	if au, ok := any(entity).(AfterUpdate); ok {
		if err := au.AfterUpdate(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo[T]) UpdatePartial(ctx context.Context, id any, fields map[string]any) error {
	// discover on_update:now() columns for T
	var t T
	typ := reflect.TypeOf(t)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	onUpdateNow := r.onUpdateNowColumns(typ)
	if len(fields) == 0 {
		if len(onUpdateNow) == 0 {
			return nil
		}
		sets := make([]string, 0, len(onUpdateNow))
		for col := range onUpdateNow {
			sets = append(sets, fmt.Sprintf("%s = NOW()", col))
		}
		query := fmt.Sprintf("UPDATE %s SET %s WHERE id = $1", r.tableName(), strings.Join(sets, ", "))
		_, err := r.exec.Exec(ctx, query, id)
		return err
	}
	idx := 1
	sets := make([]string, 0, len(fields))
	args := make([]any, 0, len(fields)+1)
	provided := map[string]struct{}{}
	for col, v := range fields {
		sets = append(sets, fmt.Sprintf("%s = $%d", col, idx))
		args = append(args, v)
		idx++
		provided[strings.ToLower(col)] = struct{}{}
	}
	// add NOW() for on_update columns not explicitly provided
	for col := range onUpdateNow {
		if _, ok := provided[strings.ToLower(col)]; !ok {
			sets = append(sets, fmt.Sprintf("%s = NOW()", col))
		}
	}
	args = append(args, id)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", r.tableName(), strings.Join(sets, ", "), idx)
	_, err := r.exec.Exec(ctx, query, args...)
	return err
}

func (r *repo[T]) Delete(ctx context.Context, id any) error {
	// dispatch hooks on zero-value model if implemented
	var t T
	if bd, ok := any(&t).(BeforeDelete); ok {
		if err := bd.BeforeDelete(ctx, id); err != nil {
			return err
		}
	} else if bdv, ok := any(t).(BeforeDelete); ok {
		if err := bdv.BeforeDelete(ctx, id); err != nil {
			return err
		}
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", r.tableName())
	_, err := r.exec.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if ad, ok := any(&t).(AfterDelete); ok {
		if err := ad.AfterDelete(ctx, id); err != nil {
			return err
		}
	} else if adv, ok := any(t).(AfterDelete); ok {
		if err := adv.AfterDelete(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo[T]) SoftDelete(ctx context.Context, id any) error {
	// ensure model supports soft delete
	var t T
	if !core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		return &ORMError{Code: ErrCodeValidation, Message: "soft delete not supported: missing deleted_at column"}
	}
	if bsd, ok := any(&t).(BeforeSoftDelete); ok {
		if err := bsd.BeforeSoftDelete(ctx, id); err != nil {
			return err
		}
	} else if bsdv, ok := any(t).(BeforeSoftDelete); ok {
		if err := bsdv.BeforeSoftDelete(ctx, id); err != nil {
			return err
		}
	}
	// expects a deleted_at column
	query := fmt.Sprintf("UPDATE %s SET deleted_at = NOW() WHERE id = $1", r.tableName())
	_, err := r.exec.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if asd, ok := any(&t).(AfterSoftDelete); ok {
		if err := asd.AfterSoftDelete(ctx, id); err != nil {
			return err
		}
	} else if asdv, ok := any(t).(AfterSoftDelete); ok {
		if err := asdv.AfterSoftDelete(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo[T]) SoftDeleteAll(ctx context.Context) (int64, error) {
	var t T
	if !core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		return 0, &ORMError{Code: ErrCodeValidation, Message: "soft delete not supported: missing deleted_at column"}
	}
	query := fmt.Sprintf("UPDATE %s SET deleted_at = NOW() WHERE deleted_at IS NULL", r.tableName())
	tag, err := r.exec.Exec(ctx, query)
	if err != nil {
		return 0, err
	}
	return int64(tag.RowsAffected()), nil
}

func (r *repo[T]) Restore(ctx context.Context, id any) error {
	var t T
	if !core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		return &ORMError{Code: ErrCodeValidation, Message: "restore not supported: missing deleted_at column"}
	}
	if br, ok := any(&t).(BeforeRestore); ok {
		if err := br.BeforeRestore(ctx, id); err != nil {
			return err
		}
	} else if brv, ok := any(t).(BeforeRestore); ok {
		if err := brv.BeforeRestore(ctx, id); err != nil {
			return err
		}
	}
	query := fmt.Sprintf("UPDATE %s SET deleted_at = NULL WHERE id = $1", r.tableName())
	_, err := r.exec.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if ar, ok := any(&t).(AfterRestore); ok {
		if err := ar.AfterRestore(ctx, id); err != nil {
			return err
		}
	} else if arv, ok := any(t).(AfterRestore); ok {
		if err := arv.AfterRestore(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo[T]) PurgeTrashed(ctx context.Context) (int64, error) {
	var t T
	if !core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		return 0, &ORMError{Code: ErrCodeValidation, Message: "purge not supported: missing deleted_at column"}
	}
	if bp, ok := any(&t).(BeforePurgeTrashed); ok {
		if err := bp.BeforePurgeTrashed(ctx); err != nil {
			return 0, err
		}
	} else if bpv, ok := any(t).(BeforePurgeTrashed); ok {
		if err := bpv.BeforePurgeTrashed(ctx); err != nil {
			return 0, err
		}
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE deleted_at IS NOT NULL", r.tableName())
	tag, err := r.exec.Exec(ctx, query)
	if err != nil {
		return 0, err
	}
	affected := int64(tag.RowsAffected())
	if ap, ok := any(&t).(AfterPurgeTrashed); ok {
		if err := ap.AfterPurgeTrashed(ctx, affected); err != nil {
			return 0, err
		}
	} else if apv, ok := any(t).(AfterPurgeTrashed); ok {
		if err := apv.AfterPurgeTrashed(ctx, affected); err != nil {
			return 0, err
		}
	}
	return affected, nil
}

func (r *repo[T]) Find(ctx context.Context, conditions ...Condition) ([]*T, error) {
	qb := r.kn.Query().Table(r.tableName())
	for _, c := range conditions {
		qb = qb.Where(c.Expr, c.Args...)
	}
	var t T
	if core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		switch r.mode {
		case softModeOnlyTrashed:
			qb = qb.Where("deleted_at IS NOT NULL")
		case softModeWithTrashed:
			// no filter
		default:
			qb = qb.Where("deleted_at IS NULL")
		}
	}
	var out []*T
	// scan to non-pointer, then take address
	var tmp []T
	if err := qb.Find(ctx, &tmp); err != nil {
		return nil, err
	}
	for i := range tmp {
		out = append(out, &tmp[i])
	}
	return out, nil
}

func (r *repo[T]) FindOne(ctx context.Context, conditions ...Condition) (*T, error) {
	qb := r.kn.Query().Table(r.tableName()).Limit(1)
	for _, c := range conditions {
		qb = qb.Where(c.Expr, c.Args...)
	}
	var t T
	if core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		switch r.mode {
		case softModeOnlyTrashed:
			qb = qb.Where("deleted_at IS NOT NULL")
		case softModeWithTrashed:
			// no filter
		default:
			qb = qb.Where("deleted_at IS NULL")
		}
	}
	var out []T
	if err := qb.Find(ctx, &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, &ORMError{Code: ErrCodeNotFound, Message: "not found"}
	}
	return &out[0], nil
}

func (r *repo[T]) Count(ctx context.Context, conditions ...Condition) (int64, error) {
	qb := r.kn.Query().Table(r.tableName()).Select("COUNT(*)")
	for _, c := range conditions {
		qb = qb.Where(c.Expr, c.Args...)
	}
	var t T
	if core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		switch r.mode {
		case softModeOnlyTrashed:
			qb = qb.Where("deleted_at IS NOT NULL")
		case softModeWithTrashed:
			// no filter
		default:
			qb = qb.Where("deleted_at IS NULL")
		}
	}
	var rows []map[string]any
	if err := qb.Find(ctx, &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	switch v := rows[0]["count"].(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	default:
		return 0, nil
	}
}

func (r *repo[T]) Exists(ctx context.Context, conditions ...Condition) (bool, error) {
	c, err := r.Count(ctx, conditions...)
	return c > 0, err
}

// PageRequest describes pagination and ordering
type PageRequest struct {
	Limit   int
	Offset  int
	OrderBy string // e.g., "id ASC" or "created_at DESC"
}

// Page represents a paginated result
type Page[T any] struct {
	Items  []*T
	Total  int64
	Limit  int
	Offset int
}

// FindPage returns a page of results and total count with the same filters
func (r *repo[T]) FindPage(ctx context.Context, page PageRequest, conditions ...Condition) (Page[T], error) {
	total, err := r.Count(ctx, conditions...)
	if err != nil {
		return Page[T]{}, err
	}
	qb := r.kn.Query().Table(r.tableName())
	for _, c := range conditions {
		qb = qb.Where(c.Expr, c.Args...)
	}
	var t T
	if core.ModelHasSoftDelete(reflect.TypeOf(t)) {
		switch r.mode {
		case softModeOnlyTrashed:
			qb = qb.Where("deleted_at IS NOT NULL")
		case softModeWithTrashed:
		default:
			qb = qb.Where("deleted_at IS NULL")
		}
	}
	if page.OrderBy != "" {
		qb = qb.OrderBy(page.OrderBy)
	}
	if page.Limit > 0 {
		qb = qb.Limit(page.Limit)
	}
	if page.Offset > 0 {
		qb = qb.Offset(page.Offset)
	}
	var tmp []T
	if err := qb.Find(ctx, &tmp); err != nil {
		return Page[T]{}, err
	}
	items := make([]*T, 0, len(tmp))
	for i := range tmp {
		items = append(items, &tmp[i])
	}
	return Page[T]{Items: items, Total: total, Limit: page.Limit, Offset: page.Offset}, nil
}

// CreateCopyFrom performs bulk insert using pgx CopyFrom for high-throughput writes.
// columns must be provided in db column names order.
func (r *repo[T]) CreateCopyFrom(ctx context.Context, entities []*T, columns ...string) (int64, error) {
	// Only works with pgx.Conn or pgxpool.Pool via CopyFrom; use a dedicated connection from pool
	pool, ok := r.exec.(interface {
		Acquire(context.Context) (*pgxv5.Conn, error)
	})
	if !ok {
		return 0, &ORMError{Code: ErrCodeValidation, Message: "CopyFrom requires pool executor"}
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	rows := make([][]any, 0, len(entities))
	for _, e := range entities {
		vals, err := r.extractValuesByColumns(e, columns)
		if err != nil {
			return 0, err
		}
		rows = append(rows, vals)
	}
	src := pgxv5.CopyFromRows(rows)
	n, err := conn.CopyFrom(ctx, pgxv5.Identifier{r.tableName()}, columns, src)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (r *repo[T]) extractValuesByColumns(entity *T, columns []string) ([]any, error) {
	val := reflect.Indirect(reflect.ValueOf(entity))
	typ := val.Type()
	mapper := core.StructMapper(typ)
	out := make([]any, len(columns))
	for i, col := range columns {
		fi, ok := mapper.FieldsByColumn[strings.ToLower(col)]
		if !ok {
			return nil, &ORMError{Code: ErrCodeValidation, Message: fmt.Sprintf("unknown column: %s", col)}
		}
		out[i] = val.FieldByIndex(fi.Index).Interface()
	}
	return out, nil
}

// Upsert performs INSERT ... ON CONFLICT (...) DO UPDATE SET col = EXCLUDED.col for given columns
func (r *repo[T]) Upsert(ctx context.Context, entity *T, conflictCols []string, updateCols []string) error {
	// model hook: BeforeUpsert
	if bu, ok := any(entity).(BeforeUpsert); ok {
		if err := bu.BeforeUpsert(ctx); err != nil {
			return err
		}
	}
	// Build from reflection
	val := reflect.Indirect(reflect.ValueOf(entity))
	typ := val.Type()
	mapper := core.StructMapper(typ)
	cols := []string{}
	placeholders := []string{}
	args := []any{}
	idx := 1
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath != "" {
			continue
		}
		col := f.Tag.Get("db")
		if col == "" {
			col = core.ToSnakeCase(f.Name)
		}
		if mapper.AutoIncrement && strings.EqualFold(col, mapper.PrimaryColumn) {
			continue
		}
		cols = append(cols, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, val.Field(i).Interface())
		idx++
	}
	setParts := make([]string, 0, len(updateCols))
	for _, c := range updateCols {
		setParts = append(setParts, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", r.tableName(), strings.Join(cols, ", "), strings.Join(placeholders, ", "), strings.Join(conflictCols, ", "), strings.Join(setParts, ", "))
	_, err := r.exec.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	// model hook: AfterUpsert
	if au, ok := any(entity).(AfterUpsert); ok {
		if err := au.AfterUpsert(ctx); err != nil {
			return err
		}
	}
	return nil
}

// onUpdateNowColumns returns a set of db column names that have orm tag on_update:now()
func (r *repo[T]) onUpdateNowColumns(typ reflect.Type) map[string]bool {
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	out := make(map[string]bool)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath != "" {
			continue
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
		if orm == "" {
			continue
		}
		parts := strings.Split(orm, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if strings.EqualFold(p, "on_update:now()") {
				col := f.Tag.Get("db")
				if col == "" {
					col = core.ToSnakeCase(f.Name)
				}
				out[col] = true
			}
		}
	}
	return out
}
