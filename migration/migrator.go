package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrator handles migrations and schema management
type Migrator struct {
	pool *pgxpool.Pool
}

func NewMigrator(pool *pgxpool.Pool) *Migrator { return &Migrator{pool: pool} }

// PlanResult is a preview of migration operations
type PlanResult struct {
	Statements       []string
	UnsafeStatements []string
	Warnings         []string
}

// Plan computes a safe migration plan for given models (public schema)
func (m *Migrator) Plan(ctx context.Context, models ...interface{}) (PlanResult, error) {
	plan := PlanResult{}
	// ensure migrations table exists in plan as safe
	plan.Statements = append(plan.Statements, `CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), checksum TEXT)`)

	// fetch existing tables and columns
	rows, err := m.pool.Query(ctx, `SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'`)
	if err != nil {
		return plan, err
	}
	defer rows.Close()
	existing := map[string]map[string]struct{}{}
	for rows.Next() {
		var tn, cn string
		if err := rows.Scan(&tn, &cn); err != nil {
			return plan, err
		}
		if _, ok := existing[tn]; !ok {
			existing[tn] = map[string]struct{}{}
		}
		existing[tn][cn] = struct{}{}
	}
	if rows.Err() != nil {
		return plan, rows.Err()
	}

	for _, model := range models {
		mi := parseModel(model)
		if _, ok := existing[mi.TableName]; !ok {
			sqls := generateCreateTableSQL(mi)
			plan.Statements = append(plan.Statements, sqls.Statements...)
			continue
		}
		for _, f := range mi.Fields {
			if _, ok := existing[mi.TableName][f.DBName]; !ok {
				stmt := "ALTER TABLE " + mi.TableName + " ADD COLUMN IF NOT EXISTS " + f.DBName + " " + normalizeType(f)
				if f.Default != "" {
					stmt += " DEFAULT " + f.Default
				}
				plan.Statements = append(plan.Statements, stmt)
			}
		}
		sqls := generateCreateTableSQL(mi)
		if len(sqls.Statements) > 1 {
			plan.Statements = append(plan.Statements, sqls.Statements[1:]...)
		}
	}
	return plan, nil
}

// AutoMigrate is a placeholder implementation
func (m *Migrator) AutoMigrate(ctx context.Context, models ...interface{}) error {
	plan, err := m.Plan(ctx, models...)
	if err != nil {
		return err
	}
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('kints-norm-migrate'))`); err != nil {
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

func computeChecksum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
