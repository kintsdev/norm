package norm

import (
	"fmt"
	"reflect"
	"testing"

	core "github.com/kintsdev/norm/internal/core"
	sqlutil "github.com/kintsdev/norm/internal/sqlutil"
)

type benchUser struct {
	ID        int64  `db:"id" norm:"primary_key,auto_increment"`
	Email     string `db:"email" norm:"unique,not_null,index,varchar(255)"`
	Username  string `db:"username" norm:"unique,not_null,varchar(50)"`
	Password  string `db:"password" norm:"not_null,varchar(255)"`
	IsActive  bool   `db:"is_active" norm:"default:true"`
	CreatedAt int64  `db:"created_at"`
	UpdatedAt int64  `db:"updated_at" norm:"on_update:now()"`
	Version   int64  `db:"version" norm:"version"`
}

// BenchmarkSQLUtilConvertPlaceholders measures raw '?' -> $n conversion cost on a moderately complex template.
func BenchmarkSQLUtilConvertPlaceholders(b *testing.B) {
	// Complex placeholder pattern
	tpl := "SELECT a, b, c FROM tbl WHERE x = ? AND y IN (?,?,?) AND (z BETWEEN ? AND ?) AND (m = ? OR n = ?) ORDER BY a DESC LIMIT 50 OFFSET 100"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = sqlutil.ConvertQMarksToPgPlaceholders(tpl)
	}
}

// BenchmarkStructMapper measures reflection-based struct mapping cache/build cost.
func BenchmarkStructMapper(b *testing.B) {
	tp := reflect.TypeOf(benchUser{})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = core.StructMapper(tp)
	}
}

// BenchmarkQueryBuilderBuildSelectSimple measures basic SELECT SQL string construction.
func BenchmarkQueryBuilderBuildSelectSimple(b *testing.B) {
	kn := &KintsNorm{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		qb := (&QueryBuilder{kn: kn}).Table("users").
			Select("id", "email", "username").
			Where("is_active = ?", true).
			OrderBy("id DESC").
			Limit(25).
			Offset(250)
		_, _ = qb.buildSelect()
	}
}

// BenchmarkQueryBuilderBuildInsert measures INSERT SQL building with one row of VALUES.
func BenchmarkQueryBuilderBuildInsert(b *testing.B) {
	kn := &KintsNorm{}
	cols := []string{"email", "username", "password", "is_active"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		qb := (&QueryBuilder{kn: kn}).Table("users").Insert(cols...)
		qb = qb.Values(fmt.Sprintf("u%08d@example.com", i), fmt.Sprintf("u%08d", i), "pw", i%2 == 0)
		_, _ = qb.buildInsert()
	}
}

// BenchmarkQueryBuilderKeysetPredicate measures keyset predicate assembly (After/Before) without executing.
func BenchmarkQueryBuilderKeysetPredicate(b *testing.B) {
	kn := &KintsNorm{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		qb := (&QueryBuilder{kn: kn}).Table("users").
			OrderBy("id ASC").
			After("id", 100).
			Before("id", 1000)
		_ = qb.buildKeysetPredicate()
	}
}

// BenchmarkQueryBuilderBuildUpdateSimple measures UPDATE SQL building with WHERE and RETURNING.
func BenchmarkQueryBuilderBuildUpdateSimple(b *testing.B) {
	kn := &KintsNorm{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		qb := (&QueryBuilder{kn: kn}).Table("users").
			Set("email = ?, password = ?", "u@example.com", "pw").
			Where("id = ?", i).
			Returning("id")
		_, _ = qb.buildUpdate()
	}
}

// BenchmarkConditionDSLCompose10 measures typed Condition composition overhead (no SQL execution).
func BenchmarkConditionDSLCompose10(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := And(
			Eq("a", 1),
			Or(Eq("b", 2), Eq("c", 3)),
			In("d", []any{1, 2, 3, 4, 5}),
			Ge("e", 10),
			Le("f", 100),
			Ne("g", "x"),
			Eq("h", true),
			Or(Gt("i", 5), Lt("j", 9)),
			RawCond("k is not null"),
			Eq("l", "zzz"),
		)
		_ = c
	}
}

// BenchmarkRepoOnUpdateNowColumns measures discovery of on_update:now() columns via tags.
func BenchmarkRepoOnUpdateNowColumns(b *testing.B) {
	r := &repo[benchUser]{}
	typ := reflect.TypeOf(benchUser{})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = r.onUpdateNowColumns(typ)
	}
}

// BenchmarkRepoExtractValuesByColumns measures reflection-based value extraction by column list.
func BenchmarkRepoExtractValuesByColumns(b *testing.B) {
	r := &repo[benchUser]{}
	cols := []string{"id", "email", "username", "password", "is_active", "created_at", "updated_at", "version"}
	u := &benchUser{ID: 1, Email: "e", Username: "u", Password: "p", IsActive: true, CreatedAt: 1, UpdatedAt: 2, Version: 3}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = r.extractValuesByColumns(u, cols)
	}
}

// BenchmarkSQLUtilConvertPlaceholdersHuge measures conversion cost on a much larger SQL string.
func BenchmarkSQLUtilConvertPlaceholdersHuge(b *testing.B) {
	base := "SELECT * FROM t WHERE a=? AND b=? AND c=? AND d=? AND e=? AND f=? AND g=? AND h=? AND i=? AND j=?"
	long := base
	for i := 0; i < 8; i++ { // expand to a longer query
		long += " UNION ALL " + base
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = sqlutil.ConvertQMarksToPgPlaceholders(long)
	}
}

// BenchmarkSQLUtilConvertNamed_ScalarsAndReuse stresses :name -> $n conversion with reuse and casting.
func BenchmarkSQLUtilConvertNamed_ScalarsAndReuse(b *testing.B) {
	sql := "a = :a AND b = :b AND a2 = :a AND x::text = :x"
	named := map[string]any{"a": 10, "b": "str", "x": 7}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = sqlutil.ConvertNamedToPgPlaceholders(sql, named)
	}
}

// BenchmarkSQLUtilConvertNamed_SliceExpansion measures IN (...) slice expansion and empty-slice handling.
func BenchmarkSQLUtilConvertNamed_SliceExpansion(b *testing.B) {
	sql := "id IN :ids OR status IN :statuses"
	named := map[string]any{"ids": []int{1, 2, 3, 4, 5}, "statuses": []string{"a", "b", "c"}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = sqlutil.ConvertNamedToPgPlaceholders(sql, named)
	}
}

// BenchmarkQueryBuilderBuildSelectWithJoins builds a SELECT with multiple JOINs and mixed Where/WhereNamed.
func BenchmarkQueryBuilderBuildSelectWithJoins(b *testing.B) {
	kn := &KintsNorm{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		qb := (&QueryBuilder{kn: kn}).Table("users u").
			Join("profiles p", "p.user_id = u.id").
			LeftJoin("accounts a", "a.id = u.id").
			Where("u.is_active = ?", true).
			WhereNamed("u.username <> :u AND a.slug IN :slugs", map[string]any{"u": "x", "slugs": []string{"s1", "s2", "s3"}}).
			OrderBy("u.id DESC").
			Limit(50).
			Offset(100)
		_, _ = qb.buildSelect()
	}
}

// BenchmarkQueryBuilderRawNamed benchmarks RawNamed path placeholder conversion and arg ordering.
func BenchmarkQueryBuilderRawNamed(b *testing.B) {
	kn := &KintsNorm{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		qb := (&QueryBuilder{kn: kn}).RawNamed("select (:a::int + :b::int) as s where :c = :c", map[string]any{"a": 2, "b": 3, "c": 1})
		_ = qb // ensure not optimized away
	}
}
