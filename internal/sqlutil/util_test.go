package sqlutil

import (
	"reflect"
	"testing"
)

func TestConvertQMarksToPgPlaceholders(t *testing.T) {
	in := "a = ? AND b = ?"
	want := "a = $1 AND b = $2"
	got := ConvertQMarksToPgPlaceholders(in)
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestConvertNamedToPgPlaceholders_ScalarsAndReuse(t *testing.T) {
	sql := "a = :a AND b = :b AND a2 = :a"
	out, args, err := ConvertNamedToPgPlaceholders(sql, map[string]any{"a": 10, "b": "x"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "a = $1 AND b = $2 AND a2 = $1" {
		t.Fatalf("out=%q", out)
	}
	if !reflect.DeepEqual(args, []any{10, "x"}) {
		t.Fatalf("args=%v", args)
	}
}

func TestConvertNamedToPgPlaceholders_SliceAndEmpty(t *testing.T) {
	out, args, err := ConvertNamedToPgPlaceholders("id IN :ids", map[string]any{"ids": []int{1, 2, 3}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "id IN ($1, $2, $3)" {
		t.Fatalf("out=%q", out)
	}
	if !reflect.DeepEqual(args, []any{1, 2, 3}) {
		t.Fatalf("args=%v", args)
	}

	out2, args2, err := ConvertNamedToPgPlaceholders("id IN :ids", map[string]any{"ids": []int{}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out2 != "id IN (NULL)" || len(args2) != 0 {
		t.Fatalf("empty slice handling failed: out=%q args=%v", out2, args2)
	}
}

func TestConvertNamedToPgPlaceholders_RepeatedSliceError(t *testing.T) {
	_, _, err := ConvertNamedToPgPlaceholders("x in :ids OR y in :ids", map[string]any{"ids": []int{1, 2}})
	if err == nil {
		t.Fatalf("expected error for repeated slice name")
	}
}

func TestConvertNamedToPgPlaceholders_IgnoreQuotesAndCast(t *testing.T) {
	out, args, err := ConvertNamedToPgPlaceholders("note = ':name' AND x::text = :x", map[string]any{"x": 7})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "note = ':name' AND x::text = $1" {
		t.Fatalf("out=%q", out)
	}
	if len(args) != 1 || args[0] != 7 {
		t.Fatalf("args=%v", args)
	}
}

func TestConvertNamedToPgPlaceholders_MissingParam(t *testing.T) {
	_, _, err := ConvertNamedToPgPlaceholders("x = :missing", map[string]any{"x": 1})
	if err == nil {
		t.Fatalf("expected error for missing param")
	}
}

func TestIsSliceButNotBytes(t *testing.T) {
	if !isSliceButNotBytes([]int{1}) {
		t.Fatalf("want true for []int")
	}
	if isSliceButNotBytes([]byte("a")) {
		t.Fatalf("want false for []byte")
	}
	if isSliceButNotBytes(5) {
		t.Fatalf("want false for scalar")
	}
}
