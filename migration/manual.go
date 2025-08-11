package migration

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var migFileRe = regexp.MustCompile(`^(\d+)_.*\.(up|down)\.sql$`)

type filePair struct {
	version  int64
	name     string
	upName   string
	downName string
	upPath   string
	downPath string
	upSQL    string
	downSQL  string
}

// MigrateUpDir applies pending .up.sql migrations from dir in ascending version order
func (m *Migrator) MigrateUpDir(ctx context.Context, dir string) error {
	if dir == "" {
		return errors.New("empty dir")
	}
	pairs, err := loadMigrationPairs(dir)
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		return nil
	}
	// ensure table
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
	// fetch applied
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
	// apply in order
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].version < pairs[j].version })
	for _, p := range pairs {
		if applied[p.version] {
			continue
		}
		if strings.TrimSpace(p.upSQL) == "" {
			return fmt.Errorf("missing up sql for version %d", p.version)
		}
		for _, stmt := range splitSQLStatements(p.upSQL) {
			if _, err := tx.Exec(ctx, stmt); err != nil {
				// include file information for easier debugging
				file := p.upPath
				if file == "" {
					file = p.upName
				}
				return fmt.Errorf("apply up %d failed in %s: %w", p.version, file, err)
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version, checksum) VALUES($1, $2)`, p.version, computeChecksum(p.upSQL)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// MigrateDownDir rolls back the last N applied migrations using .down.sql files
func (m *Migrator) MigrateDownDir(ctx context.Context, dir string, steps int) error {
	if steps <= 0 {
		steps = 1
	}
	pairs, err := loadMigrationPairs(dir)
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		return nil
	}
	byVersion := map[int64]filePair{}
	for _, p := range pairs {
		byVersion[p.version] = p
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
	// get applied versions desc
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
	// rollback
	for i := 0; i < steps && i < len(applied); i++ {
		v := applied[i]
		p, ok := byVersion[v]
		if !ok || strings.TrimSpace(p.downSQL) == "" {
			return fmt.Errorf("missing down sql for version %d", v)
		}
		for _, stmt := range splitSQLStatements(p.downSQL) {
			// safety gates: block table/column drops unless allowed
			low := strings.ToLower(strings.TrimSpace(stmt))
			if strings.HasPrefix(low, "drop table ") && !m.manualOpts.AllowTableDrop {
				return fmt.Errorf("DROP TABLE blocked by safety gate: %s", stmt)
			}
			if strings.Contains(low, " drop column ") && !m.manualOpts.AllowColumnDrop {
				return fmt.Errorf("DROP COLUMN blocked by safety gate: %s", stmt)
			}
			if _, err := tx.Exec(ctx, stmt); err != nil {
				// include file information for easier debugging
				file := p.downPath
				if file == "" {
					file = p.downName
				}
				return fmt.Errorf("apply down %d failed in %s: %w", v, file, err)
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, v); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func loadMigrationPairs(dir string) ([]filePair, error) {
	entries := map[int64]*filePair{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := filepath.Base(path)
		m := migFileRe.FindStringSubmatch(name)
		if len(m) != 3 {
			return nil
		}
		version, _ := parseInt64(m[1])
		kind := m[2]
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		p := entries[version]
		if p == nil {
			p = &filePair{version: version, name: name}
			entries[version] = p
		}
		if kind == "up" {
			p.upSQL = string(b)
			p.upName = name
			p.upPath = path
		} else {
			p.downSQL = string(b)
			p.downName = name
			p.downPath = path
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]filePair, 0, len(entries))
	for _, p := range entries {
		out = append(out, *p)
	}
	return out, nil
}

func splitSQLStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func parseInt64(s string) (int64, error) {
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid int: %s", s)
		}
		n = n*10 + int64(r-'0')
	}
	return n, nil
}
