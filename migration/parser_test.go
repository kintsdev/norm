package migration

import (
	"reflect"
	"testing"
	"time"
)

type mUser struct {
	ID        int64      `db:"id" norm:"primary_key,auto_increment"`
	Email     string     `db:"email" norm:"unique,varchar(100)"`
	Username  string     `db:"username" norm:"index"`
	Version   int64      `db:"version" norm:"version"`
	CreatedAt time.Time  `db:"created_at" norm:"not_null,default:now(),on_update:now()"`
	DeletedAt *time.Time `db:"deleted_at"`
}

func TestParseModelAndMapType(t *testing.T) {
	mi := parseModel(mUser{})
	if mi.TableName != "m_users" {
		t.Fatalf("table: %s", mi.TableName)
	}
	if len(mi.Fields) == 0 {
		t.Fatalf("fields empty")
	}
	// check a few flags
	var email, version fieldTag
	for _, f := range mi.Fields {
		if f.DBName == "email" {
			email = f
		}
		if f.DBName == "version" {
			version = f
		}
	}
	if !email.Unique || email.DBType != "varchar(100)" {
		t.Fatalf("email flags: %+v", email)
	}
	if version.DBType != "BIGINT" {
		t.Fatalf("version type")
	}
}

func TestQuoteIdent(t *testing.T) {
	if quoteIdent("a\"b") != "\"a\"\"b\"" {
		t.Fatalf("quote")
	}
}

func TestCanonicalPgType(t *testing.T) {
	if canonicalPgType("integer", -1) != "INTEGER" {
		t.Fatalf("int")
	}
	if canonicalPgType("character varying", 50) != "varchar(50)" {
		t.Fatalf("varchar len")
	}
}

func TestGenerateCreateTableSQL(t *testing.T) {
	mi := parseModel(mUser{})
	sqls := generateCreateTableSQL(mi)
	if len(sqls.Statements) == 0 {
		t.Fatalf("no stmts")
	}
}

func TestSplitSQLStatements(t *testing.T) {
	parts := splitSQLStatements("CREATE TABLE x(a int); CREATE INDEX i ON x(a);")
	if !reflect.DeepEqual(parts, []string{"CREATE TABLE x(a int)", "CREATE INDEX i ON x(a)"}) {
		t.Fatalf("split: %v", parts)
	}
}
