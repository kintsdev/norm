package migration

import (
	"reflect"
	"testing"
)

func TestMapGoTypeToPgType_MoreBranches(t *testing.T) {
	var ()
	if got := mapGoTypeToPgType(reflect.TypeFor[int8](), ""); got != "INTEGER" {
		t.Fatalf("i8 %s", got)
	}
	if got := mapGoTypeToPgType(reflect.TypeFor[uint16](), ""); got != "INTEGER" {
		t.Fatalf("u16 %s", got)
	}
	if got := mapGoTypeToPgType(reflect.TypeFor[float32](), ""); got != "REAL" {
		t.Fatalf("f32 %s", got)
	}
	if got := mapGoTypeToPgType(reflect.TypeFor[float64](), ""); got != "DOUBLE PRECISION" {
		t.Fatalf("f64 %s", got)
	}
}
