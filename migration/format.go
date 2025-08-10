package migration

import (
	"fmt"
	"sort"
	"strings"
)

// FormatPlan returns a human-friendly summary grouped by table and severity.
func FormatPlan(plan PlanResult) string {
	// group statements by table name
	type bucket struct {
		statements  []string
		unsafe      []string
		destructive []string
	}
	byTable := map[string]*bucket{}
	add := func(tbl string) *bucket {
		b := byTable[tbl]
		if b == nil {
			b = &bucket{}
			byTable[tbl] = b
		}
		return b
	}
	for _, s := range plan.Statements {
		tbl := extractTableName(s)
		add(tbl).statements = append(add(tbl).statements, s)
	}
	for _, s := range plan.UnsafeStatements {
		tbl := extractTableName(s)
		add(tbl).unsafe = append(add(tbl).unsafe, s)
	}
	for _, s := range plan.DestructiveStatements {
		tbl := extractTableName(s)
		add(tbl).destructive = append(add(tbl).destructive, s)
	}

	// order tables
	tables := make([]string, 0, len(byTable))
	for k := range byTable {
		tables = append(tables, k)
	}
	sort.Strings(tables)

	var sb strings.Builder
	sb.WriteString("Migration Plan\n")
	// warnings
	if len(plan.Warnings) > 0 {
		sb.WriteString("Warnings:\n")
		for _, w := range plan.Warnings {
			sb.WriteString("  - ")
			sb.WriteString(w)
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}
	for _, t := range tables {
		sb.WriteString(fmt.Sprintf("[%s]\n", t))
		b := byTable[t]
		if len(b.statements) > 0 {
			sb.WriteString("  Statements:\n")
			for _, s := range b.statements {
				sb.WriteString("    - ")
				sb.WriteString(s)
				sb.WriteString("\n")
			}
		}
		if len(b.unsafe) > 0 {
			sb.WriteString("  Unsafe:\n")
			for _, s := range b.unsafe {
				sb.WriteString("    - ")
				sb.WriteString(s)
				sb.WriteString("\n")
			}
		}
		if len(b.destructive) > 0 {
			sb.WriteString("  Destructive:\n")
			for _, s := range b.destructive {
				sb.WriteString("    - ")
				sb.WriteString(s)
				sb.WriteString("\n")
			}
		}
		sb.WriteString("\n")
	}
	// global items
	if len(plan.IndexDrops) > 0 {
		sb.WriteString("[indexes]\n")
		sb.WriteString("  Drops:\n")
		for _, s := range plan.IndexDrops {
			sb.WriteString("    - ")
			sb.WriteString(s)
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}
	if len(plan.ConstraintDrops) > 0 {
		sb.WriteString("[constraints]\n")
		sb.WriteString("  Drops:\n")
		for _, s := range plan.ConstraintDrops {
			sb.WriteString("    - ")
			sb.WriteString(s)
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// extractTableName attempts to pull table identifier from SQL (CREATE/ALTER TABLE ...)
func extractTableName(sql string) string {
	s := strings.ToUpper(sql)
	idx := strings.Index(s, " TABLE ")
	if idx < 0 {
		return "global"
	}
	rest := strings.TrimSpace(sql[idx+len(" TABLE "):])
	// remove IF NOT EXISTS
	up := strings.ToUpper(rest)
	if strings.HasPrefix(up, "IF NOT EXISTS ") {
		rest = rest[len("IF NOT EXISTS "):]
	}
	// identifier ends at first space or '('
	end := len(rest)
	if i := strings.IndexAny(rest, " (\n\t"); i >= 0 {
		end = i
	}
	return strings.TrimSpace(rest[:end])
}
