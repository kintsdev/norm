package migration

import (
	"reflect"
	"testing"
)

func TestMapGoTypeToPgType_MoreBranches(t *testing.T) {
	var (
		i8  int8
		u16 uint16
		f32 float32
		f64 float64
	)
	if got := mapGoTypeToPgType(reflect.TypeOf(i8), ""); got != "INTEGER" {
		t.Fatalf("i8 %s", got)
	}
	if got := mapGoTypeToPgType(reflect.TypeOf(u16), ""); got != "INTEGER" {
		t.Fatalf("u16 %s", got)
	}
	if got := mapGoTypeToPgType(reflect.TypeOf(f32), ""); got != "REAL" {
		t.Fatalf("f32 %s", got)
	}
	if got := mapGoTypeToPgType(reflect.TypeOf(f64), ""); got != "DOUBLE PRECISION" {
		t.Fatalf("f64 %s", got)
	}
}
