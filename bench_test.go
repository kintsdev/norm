package norm

import (
	"fmt"
	"reflect"
	"testing"

	core "github.com/kintsdev/norm/internal/core"
	sqlutil "github.com/kintsdev/norm/internal/sqlutil"
)

type benchUser struct {
	ID        int64  `db:"id" orm:"primary_key,auto_increment"`
	Email     string `db:"email" orm:"unique,not_null,index,varchar(255)"`
	Username  string `db:"username" orm:"unique,not_null,varchar(50)"`
	Password  string `db:"password" orm:"not_null,varchar(255)"`
	IsActive  bool   `db:"is_active" orm:"default:true"`
	CreatedAt int64  `db:"created_at"`
	UpdatedAt int64  `db:"updated_at" orm:"on_update:now()"`
	Version   int64  `db:"version" orm:"version"`
}

func BenchmarkSQLUtilConvertPlaceholders(b *testing.B) {
	// Complex placeholder pattern
	tpl := "SELECT a, b, c FROM tbl WHERE x = ? AND y IN (?,?,?) AND (z BETWEEN ? AND ?) AND (m = ? OR n = ?) ORDER BY a DESC LIMIT 50 OFFSET 100"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = sqlutil.ConvertQMarksToPgPlaceholders(tpl)
	}
}

func BenchmarkStructMapper(b *testing.B) {
	tp := reflect.TypeOf(benchUser{})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = core.StructMapper(tp)
	}
}

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

func BenchmarkRepoOnUpdateNowColumns(b *testing.B) {
	r := &repo[benchUser]{}
	typ := reflect.TypeOf(benchUser{})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = r.onUpdateNowColumns(typ)
	}
}

func BenchmarkRepoExtractValuesByColumns(b *testing.B) {
	r := &repo[benchUser]{}
	cols := []string{"id", "email", "username", "password", "is_active", "created_at", "updated_at", "version"}
	u := &benchUser{ID: 1, Email: "e", Username: "u", Password: "p", IsActive: true, CreatedAt: 1, UpdatedAt: 2, Version: 3}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = r.extractValuesByColumns(u, cols)
	}
}

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
