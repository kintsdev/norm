package migration

import "testing"

func TestExtractTableName(t *testing.T) {
	if extractTableName("CREATE TABLE IF NOT EXISTS users (id bigint)") != "users" {
		t.Fatalf("create")
	}
	if extractTableName("ALTER TABLE public.users ADD COLUMN x int") != "public.users" {
		t.Fatalf("alter")
	}
	if extractTableName("CREATE INDEX idx ON t(x)") != "global" {
		t.Fatalf("global")
	}
}

func TestFormatPlan_Basic(t *testing.T) {
	plan := PlanResult{Statements: []string{"CREATE TABLE users(id bigint)"}, UnsafeStatements: []string{"ALTER TABLE users ALTER COLUMN x TYPE bigint"}, DestructiveStatements: []string{"ALTER TABLE users DROP COLUMN y"}, IndexDrops: []string{"DROP INDEX IF EXISTS \"idx_users_x\""}}
	out := FormatPlan(plan)
	if out == "" {
		t.Fatalf("empty")
	}
}
