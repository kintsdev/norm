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

// AutoMigrate is a placeholder implementation
func (m *Migrator) AutoMigrate(ctx context.Context, models ...interface{}) error {
	// Minimal: ensure migrations table exists
	_, err := m.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`)
	if err != nil {
		return err
	}
	// add checksum column if not exists for idempotency tracking
	if _, err := m.pool.Exec(ctx, `ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT`); err != nil {
		return err
	}

	// Parse and create table/index statements
	allStmts := make([]string, 0, len(models)*4)
	for _, model := range models {
		mi := parseModel(model)
		sqls := generateCreateTableSQL(mi)
		for _, s := range sqls.Statements {
			if _, err := m.pool.Exec(ctx, s); err != nil {
				return err
			}
			allStmts = append(allStmts, s)
		}
		// Ensure columns exist (basic alter-add)
		for _, f := range mi.Fields {
			colDef := normalizeType(f)
			stmt := "ALTER TABLE " + mi.TableName + " ADD COLUMN IF NOT EXISTS " + f.DBName + " " + colDef
			if f.Default != "" {
				stmt += " DEFAULT " + f.Default
			}
			if _, err := m.pool.Exec(ctx, stmt); err != nil {
				return err
			}
			allStmts = append(allStmts, stmt)
		}
	}
	// Compute checksum and insert schema_migrations row if not already present
	checksum := computeChecksum(strings.Join(allStmts, ";"))
	var exists bool
	if err := m.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE checksum = $1)`, checksum).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		var maxVersion int64
		if err := m.pool.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&maxVersion); err != nil {
			return err
		}
		if _, err := m.pool.Exec(ctx, `INSERT INTO schema_migrations(version, checksum) VALUES($1, $2)`, maxVersion+1, checksum); err != nil {
			return err
		}
	}
	return nil
}

func computeChecksum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
