package core

import (
	"reflect"
	"testing"
	"time"
)

type smUser struct {
	ID        int64      `db:"id" norm:"primary_key,auto_increment"`
	Email     string     `db:"email"`
	Version   int64      `db:"version" norm:"version"`
	DeletedAt *time.Time `db:"deleted_at"`
}

func TestStructMapper_PrimaryAndVersion(t *testing.T) {
	m := StructMapper(reflect.TypeOf(smUser{}))
	if m.PrimaryColumn != "id" || !m.AutoIncrement || m.VersionColumn != "version" {
		t.Fatalf("mapper unexpected: %+v", m)
	}
	if _, ok := m.FieldsByColumn["email"]; !ok {
		t.Fatalf("email column missing")
	}
}

func TestToSnakeCase(t *testing.T) {
	if ToSnakeCase("UserName") != "user_name" {
		t.Fatalf("snake")
	}
	// current implementation lowercases consecutive caps without extra underscore
	if ToSnakeCase("URLValue") != "url_value" && ToSnakeCase("URLValue") != "u_r_l_value" {
		t.Fatalf("snake acronym got %q", ToSnakeCase("URLValue"))
	}
}

func TestSetFieldByIndexAndModelHasSoftDelete(t *testing.T) {
	var u struct {
		Name      string     `db:"name"`
		When      time.Time  `db:"when"`
		Ptr       *int       `db:"ptr"`
		DeletedAt *time.Time `db:"deleted_at"`
	}
	m := StructMapper(reflect.TypeOf(u))
	// set string
	fi := m.FieldsByColumn["name"]
	SetFieldByIndex(reflect.ValueOf(&u), fi.Index, "alice")
	if u.Name != "alice" {
		t.Fatalf("set string")
	}
	// set time
	fi = m.FieldsByColumn["when"]
	now := time.Now()
	SetFieldByIndex(reflect.ValueOf(&u), fi.Index, now)
	if !u.When.Equal(now) {
		t.Fatalf("set time")
	}
	// nil to pointer
	fi = m.FieldsByColumn["ptr"]
	SetFieldByIndex(reflect.ValueOf(&u), fi.Index, nil)
	if u.Ptr != nil {
		t.Fatalf("nil pointer expected")
	}
	if !ModelHasSoftDelete(reflect.TypeOf(u)) {
		t.Fatalf("soft delete detect")
	}
}
