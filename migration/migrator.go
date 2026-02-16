package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrator handles migrations and schema management
type Migrator struct {
	pool *pgxpool.Pool
	// manual migration safety options
	manualOpts ManualOptions
}

func NewMigrator(pool *pgxpool.Pool) *Migrator { return &Migrator{pool: pool} }

// ManualOptions controls safety gates for manual file-based migrations
type ManualOptions struct {
	AllowTableDrop  bool // allow DROP TABLE in down migrations
	AllowColumnDrop bool // allow ALTER TABLE ... DROP COLUMN in down migrations
}

// SetManualOptions sets safety options for manual migrations
func (m *Migrator) SetManualOptions(opts ManualOptions) { m.manualOpts = opts }

// PlanResult is a preview of migration operations
type PlanResult struct {
	Statements            []string
	UnsafeStatements      []string
	Warnings              []string
	DestructiveStatements []string
	IndexDrops            []string
	ConstraintDrops       []string
	TableDrops            []string // tables in DB but not in models (explicit opt-in to apply)
	TableRenames          []string // table rename statements detected via model tag
}

// Plan computes a safe migration plan for given models (public schema)
func (m *Migrator) Plan(ctx context.Context, models ...any) (PlanResult, error) {
	plan := PlanResult{}
	// ensure migrations table exists in plan as safe
	plan.Statements = append(plan.Statements, `CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), checksum TEXT)`)

	// fetch existing tables and columns with types and nullability
	rows, err := m.pool.Query(ctx, `
        SELECT table_name, column_name, data_type, is_nullable, COALESCE(character_maximum_length, -1)
        FROM information_schema.columns
        WHERE table_schema = 'public'
    `)
	if err != nil {
		return plan, err
	}
	defer rows.Close()
	type colInfo struct {
		dataType   string
		isNullable string
	}
	existing := map[string]map[string]colInfo{}
	for rows.Next() {
		var tn, cn, dt, nn string
		var charLen int32
		if err := rows.Scan(&tn, &cn, &dt, &nn, &charLen); err != nil {
			return plan, err
		}
		if _, ok := existing[tn]; !ok {
			existing[tn] = map[string]colInfo{}
		}
		existing[tn][cn] = colInfo{dataType: canonicalPgType(dt, charLen), isNullable: nn}
	}
	if rows.Err() != nil {
		return plan, rows.Err()
	}

	// fetch existing constraints upfront to avoid re-adding
	existingConstraints := map[string]struct{}{}
	cinit, errc := m.pool.Query(ctx, `
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'public' AND c.contype IN ('f','p','u')`)
	if errc == nil {
		defer cinit.Close()
		for cinit.Next() {
			var name string
			if err := cinit.Scan(&name); err == nil {
				existingConstraints[name] = struct{}{}
			}
		}
	}

	modelTables := map[string]struct{}{}
	for _, model := range models {
		mi := parseModel(model)
		modelTables[mi.TableName] = struct{}{}

		// Handle table rename if old name exists and new doesn't
		if mi.RenameTableFrom != "" {
			_, oldExists := existing[mi.RenameTableFrom]
			_, newExists := existing[mi.TableName]
			if oldExists && !newExists {
				plan.TableRenames = append(plan.TableRenames, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quoteIdent(mi.RenameTableFrom), quoteIdent(mi.TableName)))
				// update tracking so subsequent column checks work against new name
				existing[mi.TableName] = existing[mi.RenameTableFrom]
				delete(existing, mi.RenameTableFrom)
			} else if oldExists && newExists {
				plan.Warnings = append(plan.Warnings, fmt.Sprintf("both tables %s and %s exist; manual migration likely required", mi.RenameTableFrom, mi.TableName))
			}
		}

		if _, ok := existing[mi.TableName]; !ok {
			sqls := generateCreateTableSQL(mi)
			// filter out ADD CONSTRAINT if exists already
			filtered := make([]string, 0, len(sqls.Statements))
			for _, s := range sqls.Statements {
				if strings.Contains(s, "ADD CONSTRAINT") {
					// crude extract name between ADD CONSTRAINT and next space
					seg := s[strings.Index(s, "ADD CONSTRAINT")+len("ADD CONSTRAINT"):]
					seg = strings.TrimSpace(seg)
					cname := seg
					if i := strings.Index(seg, " "); i > 0 {
						cname = seg[:i]
					}
					cname = strings.Trim(cname, `"`)
					if _, exists := existingConstraints[cname]; exists {
						continue
					}
				}
				filtered = append(filtered, s)
			}
			plan.Statements = append(plan.Statements, filtered...)
			continue
		}
		for _, f := range mi.Fields {
			// handle rename
			if f.RenameFrom != "" {
				_, oldExists := existing[mi.TableName][f.RenameFrom]
				_, newExists := existing[mi.TableName][f.DBName]
				if oldExists && !newExists {
					plan.Statements = append(plan.Statements, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", quoteIdent(mi.TableName), quoteIdent(f.RenameFrom), quoteIdent(f.DBName)))
					// treat as existing after rename for subsequent checks
					existing[mi.TableName][f.DBName] = existing[mi.TableName][f.RenameFrom]
					delete(existing[mi.TableName], f.RenameFrom)
				} else if oldExists && newExists {
					plan.Warnings = append(plan.Warnings, fmt.Sprintf("both %s and %s exist on %s; manual data migration likely required", f.RenameFrom, f.DBName, mi.TableName))
				}
			}

			if _, ok := existing[mi.TableName][f.DBName]; !ok {
				stmt := "ALTER TABLE " + quoteIdent(mi.TableName) + " ADD COLUMN IF NOT EXISTS " + quoteIdent(f.DBName) + " " + normalizeType(f)
				if f.Default != "" {
					stmt += " DEFAULT " + f.Default
				}
				plan.Statements = append(plan.Statements, stmt)
			} else {
				// type and nullability checks
				expected := strings.ToLower(normalizeType(f))
				ci := existing[mi.TableName][f.DBName]
				have := strings.ToLower(ci.dataType)
				if expected != "" && have != "" && expected != have {
					plan.Warnings = append(plan.Warnings, fmt.Sprintf("type change for %s.%s: %s -> %s", mi.TableName, f.DBName, have, expected))
					plan.UnsafeStatements = append(plan.UnsafeStatements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s",
						quoteIdent(mi.TableName), quoteIdent(f.DBName), expected, quoteIdent(f.DBName), expected))
				}
				// nullability: set NOT NULL if model requires not null and column is nullable
				if f.NotNull && strings.EqualFold(ci.isNullable, "YES") {
					plan.Warnings = append(plan.Warnings, fmt.Sprintf("nullability change for %s.%s: NULLABLE -> NOT NULL", mi.TableName, f.DBName))
					plan.UnsafeStatements = append(plan.UnsafeStatements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", quoteIdent(mi.TableName), quoteIdent(f.DBName)))
				}
			}
		}
		sqls := generateCreateTableSQL(mi)
		if len(sqls.Statements) > 1 {
			// filter out existing constraints
			filtered := make([]string, 0, len(sqls.Statements)-1)
			for _, s := range sqls.Statements[1:] {
				if strings.Contains(s, "ADD CONSTRAINT") {
					seg := s[strings.Index(s, "ADD CONSTRAINT")+len("ADD CONSTRAINT"):]
					seg = strings.TrimSpace(seg)
					cname := seg
					if i := strings.Index(seg, " "); i > 0 {
						cname = seg[:i]
					}
					cname = strings.Trim(cname, `"`)
					if _, exists := existingConstraints[cname]; exists {
						continue
					}
				}
				filtered = append(filtered, s)
			}
			plan.Statements = append(plan.Statements, filtered...)
		}
	}
	// destructive: detect tables in DB but not in any model (opt-in apply)
	// system tables like schema_migrations are excluded
	systemTables := map[string]struct{}{"schema_migrations": {}}
	for tbl := range existing {
		if _, ok := modelTables[tbl]; ok {
			continue
		}
		if _, ok := systemTables[tbl]; ok {
			continue
		}
		plan.TableDrops = append(plan.TableDrops, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdent(tbl)))
		plan.Warnings = append(plan.Warnings, fmt.Sprintf("table %s exists in database but not in models; would be dropped with opt-in", tbl))
	}

	// destructive: drop columns that exist in DB but not in model (opt-in apply)
	for tbl, cols := range existing {
		if _, ok := modelTables[tbl]; !ok {
			continue
		}
		// build set of expected columns from model
		expected := map[string]struct{}{}
		for _, model := range models {
			mi := parseModel(model)
			if mi.TableName != tbl {
				continue
			}
			for _, f := range mi.Fields {
				expected[strings.ToLower(f.DBName)] = struct{}{}
			}
		}
		for cn := range cols {
			lcn := strings.ToLower(cn)
			if _, ok := expected[lcn]; !ok {
				plan.DestructiveStatements = append(plan.DestructiveStatements, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quoteIdent(tbl), quoteIdent(cn)))
			}
		}
	}
	// Index diffing: drop indexes that are not expected by model, or with wrong uniqueness
	idxRows, err := m.pool.Query(ctx, `SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname='public'`)
	if err == nil {
		defer idxRows.Close()
		// build expected index set by name and uniqueness
		type idxSpec struct{ unique bool }
		expectedIdx := map[string]idxSpec{}
		for _, model := range models {
			mi := parseModel(model)
			for _, f := range mi.Fields {
				if f.Unique {
					expectedIdx[fmt.Sprintf("idx_%s_%s", mi.TableName, f.DBName)] = idxSpec{unique: true}
				} else if f.Index {
					expectedIdx[fmt.Sprintf("idx_%s_%s", mi.TableName, f.DBName)] = idxSpec{unique: false}
				}
			}
		}
		for idxRows.Next() {
			var tbl, name, def string
			if err := idxRows.Scan(&tbl, &name, &def); err != nil {
				continue
			}
			if !strings.HasPrefix(name, "idx_") {
				continue
			}
			if spec, ok := expectedIdx[name]; ok {
				// if uniqueness mismatch, drop so it can be recreated
				hasUnique := strings.Contains(strings.ToUpper(def), "UNIQUE INDEX")
				if hasUnique != spec.unique {
					plan.IndexDrops = append(plan.IndexDrops, fmt.Sprintf("DROP INDEX IF EXISTS %s", quoteIdent(name)))
				}
				continue
			}
			// unexpected index for this table -> drop
			plan.IndexDrops = append(plan.IndexDrops, fmt.Sprintf("DROP INDEX IF EXISTS %s", quoteIdent(name)))
		}
	}

	// Constraint diffing: drop fk_* constraints not present in model
	crows, err2 := m.pool.Query(ctx, `
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = 'public' AND c.contype IN ('f')`)
	if err2 == nil {
		defer crows.Close()
		expectedFK := map[string]struct{}{}
		for _, model := range models {
			mi := parseModel(model)
			for _, f := range mi.Fields {
				if f.FKTable != "" && f.FKColumn != "" {
					expectedFK[fmt.Sprintf("fk_%s_%s", mi.TableName, f.DBName)] = struct{}{}
				}
			}
		}
		for crows.Next() {
			var conname string
			if err := crows.Scan(&conname); err != nil {
				continue
			}
			if !strings.HasPrefix(conname, "fk_") {
				continue
			}
			if _, ok := expectedFK[conname]; !ok {
				plan.ConstraintDrops = append(plan.ConstraintDrops, fmt.Sprintf("ALTER TABLE %%s DROP CONSTRAINT %s", quoteIdent(conname)))
			}
		}
	}

	return plan, nil
}

// AutoMigrate is a placeholder implementation
func (m *Migrator) AutoMigrate(ctx context.Context, models ...any) error {
	plan, err := m.Plan(ctx, models...)
	if err != nil {
		return err
	}
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('github.com/kintsdev/norm-migrate'))`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), checksum TEXT)`); err != nil {
		return err
	}
	allStmts := make([]string, 0, len(plan.Statements)+len(plan.TableRenames))
	// apply table renames first (safe, explicit via model interface)
	for _, s := range plan.TableRenames {
		if _, err := tx.Exec(ctx, s); err != nil {
			return err
		}
		allStmts = append(allStmts, s)
	}
	for _, s := range plan.Statements {
		if _, err := tx.Exec(ctx, s); err != nil {
			return err
		}
		allStmts = append(allStmts, s)
	}
	checksum := computeChecksum(strings.Join(allStmts, ";"))
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE checksum = $1)`, checksum).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		var maxVersion int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&maxVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version, checksum) VALUES($1, $2)`, maxVersion+1, checksum); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ApplyOptions controls execution of destructive statements
type ApplyOptions struct {
	AllowDropColumns     bool
	AllowDropIndexes     bool
	AllowDropConstraints bool
	AllowDropTables      bool
}

// AutoMigrateWithOptions applies plan with additional options (e.g., allow drops)
func (m *Migrator) AutoMigrateWithOptions(ctx context.Context, opts ApplyOptions, models ...any) error {
	plan, err := m.Plan(ctx, models...)
	if err != nil {
		return err
	}
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('github.com/kintsdev/norm-migrate'))`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), checksum TEXT)`); err != nil {
		return err
	}
	allStmts := make([]string, 0, len(plan.Statements)+len(plan.DestructiveStatements)+len(plan.IndexDrops)+len(plan.ConstraintDrops)+len(plan.TableRenames)+len(plan.TableDrops))
	// apply table renames first (safe, explicit via model interface)
	for _, s := range plan.TableRenames {
		if _, err := tx.Exec(ctx, s); err != nil {
			return err
		}
		allStmts = append(allStmts, s)
	}
	for _, s := range plan.Statements {
		if _, err := tx.Exec(ctx, s); err != nil {
			return err
		}
		allStmts = append(allStmts, s)
	}
	if opts.AllowDropColumns {
		for _, s := range plan.DestructiveStatements {
			if _, err := tx.Exec(ctx, s); err != nil {
				return err
			}
			allStmts = append(allStmts, s)
		}
	}
	if opts.AllowDropIndexes {
		for _, s := range plan.IndexDrops {
			if _, err := tx.Exec(ctx, s); err != nil {
				return err
			}
			allStmts = append(allStmts, s)
		}
	}
	if opts.AllowDropConstraints {
		for _, s := range plan.ConstraintDrops {
			// unresolved %s placeholder -> skip for safety
			if strings.Contains(s, "%s") {
				continue
			}
			if _, err := tx.Exec(ctx, s); err != nil {
				return err
			}
			allStmts = append(allStmts, s)
		}
	}
	if opts.AllowDropTables {
		for _, s := range plan.TableDrops {
			if _, err := tx.Exec(ctx, s); err != nil {
				return err
			}
			allStmts = append(allStmts, s)
		}
	}
	checksum := computeChecksum(strings.Join(allStmts, ";"))
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE checksum = $1)`, checksum).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		var maxVersion int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&maxVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version, checksum) VALUES($1, $2)`, maxVersion+1, checksum); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func computeChecksum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// canonicalPgType converts information_schema types into our normalized tokens
func canonicalPgType(dataType string, charLen int32) string {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "integer":
		return "INTEGER"
	case "bigint":
		return "BIGINT"
	case "boolean":
		return "BOOLEAN"
	case "real":
		return "REAL"
	case "double precision":
		return "DOUBLE PRECISION"
	case "text":
		return "TEXT"
	case "timestamp with time zone":
		return "TIMESTAMPTZ"
	case "character varying":
		if charLen > 0 {
			return fmt.Sprintf("varchar(%d)", charLen)
		}
		return "varchar"
	default:
		return strings.ToUpper(dataType)
	}
}
