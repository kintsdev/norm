package migration

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
)

// GoMigration represents a single Go-based migration with up/down functions.
type GoMigration struct {
	Version     int64
	Description string
	Up          func(ctx context.Context, tx pgx.Tx) error
	Down        func(ctx context.Context, tx pgx.Tx) error
}

// GoMigrationRegistry holds registered Go-based migrations.
type GoMigrationRegistry struct {
	migrations map[int64]GoMigration
}

// NewGoMigrationRegistry creates an empty migration registry.
func NewGoMigrationRegistry() *GoMigrationRegistry {
	return &GoMigrationRegistry{migrations: make(map[int64]GoMigration)}
}

// Register adds a Go migration to the registry. Returns an error if the version is already registered.
func (r *GoMigrationRegistry) Register(m GoMigration) error {
	if m.Version <= 0 {
		return errors.New("migration version must be > 0")
	}
	if m.Up == nil {
		return fmt.Errorf("migration %d: Up function is required", m.Version)
	}
	if _, exists := r.migrations[m.Version]; exists {
		return fmt.Errorf("migration %d already registered", m.Version)
	}
	r.migrations[m.Version] = m
	return nil
}

// MustRegister is like Register but panics on error.
func (r *GoMigrationRegistry) MustRegister(m GoMigration) {
	if err := r.Register(m); err != nil {
		panic(err)
	}
}

// sorted returns migrations sorted by version ascending.
func (r *GoMigrationRegistry) sorted() []GoMigration {
	out := make([]GoMigration, 0, len(r.migrations))
	for _, m := range r.migrations {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// MigrateUpGo applies pending Go-based migrations in ascending version order.
func (m *Migrator) MigrateUpGo(ctx context.Context, registry *GoMigrationRegistry) error {
	if registry == nil || len(registry.migrations) == 0 {
		return nil
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

	// fetch already-applied versions
	applied := map[int64]bool{}
	rows, err := tx.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied[v] = true
	}
	rows.Close()

	for _, mig := range registry.sorted() {
		if applied[mig.Version] {
			continue
		}
		if err := mig.Up(ctx, tx); err != nil {
			desc := mig.Description
			if desc == "" {
				desc = "unnamed"
			}
			return fmt.Errorf("go migration %d (%s) up failed: %w", mig.Version, desc, err)
		}
		checksum := computeChecksum(fmt.Sprintf("go:%d:%s", mig.Version, mig.Description))
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version, checksum) VALUES($1, $2)`, mig.Version, checksum); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// MigrateDownGo rolls back the last N applied Go-based migrations in descending version order.
func (m *Migrator) MigrateDownGo(ctx context.Context, registry *GoMigrationRegistry, steps int) error {
	if registry == nil || len(registry.migrations) == 0 {
		return nil
	}
	if steps <= 0 {
		steps = 1
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

	// get applied versions in descending order
	applied := []int64{}
	rows, err := tx.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version DESC`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return err
		}
		applied = append(applied, v)
	}
	rows.Close()

	rolled := 0
	for _, v := range applied {
		if rolled >= steps {
			break
		}
		mig, ok := registry.migrations[v]
		if !ok {
			continue // not a Go migration version, skip
		}
		if mig.Down == nil {
			return fmt.Errorf("go migration %d: Down function not provided, cannot rollback", v)
		}
		if err := mig.Down(ctx, tx); err != nil {
			desc := mig.Description
			if desc == "" {
				desc = "unnamed"
			}
			return fmt.Errorf("go migration %d (%s) down failed: %w", v, desc, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, v); err != nil {
			return err
		}
		rolled++
	}

	return tx.Commit(ctx)
}
