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

	modelTables := map[string]struct{}{}
	for _, model := range models {
		mi := parseModel(model)
		modelTables[mi.TableName] = struct{}{}
		if _, ok := existing[mi.TableName]; !ok {
			sqls := generateCreateTableSQL(mi)
			plan.Statements = append(plan.Statements, sqls.Statements...)
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
			plan.Statements = append(plan.Statements, sqls.Statements[1:]...)
		}
	}
	// destructive: drop columns that exist in DB but not in model (opt-in apply)
	for tbl, cols := range existing {
		if _, ok := modelTables[tbl]; !ok {
			continue // we do not drop entire tables via auto plan
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
	allStmts := make([]string, 0, len(plan.Statements))
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
	AllowDropColumns bool
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
	allStmts := make([]string, 0, len(plan.Statements)+len(plan.DestructiveStatements))
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
