package norm

import (
	"testing"
)

func TestBuildInsert_BasicAndConflict(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn}).Table("users").Insert("id", "name").Values(1, "a").Returning("id")
	sql, args := qb.buildInsert()
	if sql != "INSERT INTO users (id, name) VALUES ($1, $2) RETURNING id" {
		t.Fatalf("sql=%s", sql)
	}
	if len(args) != 2 || args[0] != 1 || args[1] != "a" {
		t.Fatalf("args")
	}

	qb2 := (&QueryBuilder{kn: kn}).Table("users").Insert("id").Values(1).OnConflict("id").DoUpdateSet("name = ?", "b")
	sql2, args2 := qb2.buildInsert()
	if sql2 != "INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET name = $2" {
		t.Fatalf("sql2=%s", sql2)
	}
	if len(args2) != 2 || args2[0] != 1 || args2[1] != "b" {
		t.Fatalf("args2")
	}
}

func TestBuildUpdate_WithWhereAndReturning(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn}).Table("users").Set("name = ?", "b").Where("id = ?", 1).Returning("id")
	sql, args := qb.buildUpdate()
	if sql != "UPDATE users SET name = $1 WHERE id = $2 RETURNING id" {
		t.Fatalf("sql=%s", sql)
	}
	if len(args) != 2 || args[0] != "b" || args[1] != 1 {
		t.Fatalf("args")
	}
}
