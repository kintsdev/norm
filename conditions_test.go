package norm

import (
	"reflect"
	"testing"
	"time"
)

func TestBasicConditions(t *testing.T) {
	c := Eq("id", 1)
	if c.Expr != "id = ?" || len(c.Args) != 1 || c.Args[0] != 1 {
		t.Fatalf("Eq")
	}
	c = Ne("a", 2)
	if c.Expr != "a <> ?" {
		t.Fatalf("Ne")
	}
	_ = Gt("a", 2)
	_ = Ge("a", 2)
	_ = Lt("a", 2)
	_ = Le("a", 2)
}

func TestInAndAndOr(t *testing.T) {
	c := In("id", []any{1, 2, 3})
	if c.Expr != "id IN (?, ?, ?)" || !reflect.DeepEqual(c.Args, []any{1, 2, 3}) {
		t.Fatalf("In")
	}
	c2 := And(Eq("id", 1), Ne("a", 2))
	if c2.Expr != "(id = ?) AND (a <> ?)" || len(c2.Args) != 2 {
		t.Fatalf("And")
	}
	c3 := Or()
	if c3.Expr != "1=0" {
		t.Fatalf("Or empty")
	}
	c4 := And()
	if c4.Expr != "1=1" {
		t.Fatalf("And empty")
	}
}

func TestDateHelpers(t *testing.T) {
	now := time.Now()
	br := Between("ts", 1, 2)
	if br.Expr != "ts BETWEEN ? AND ?" || len(br.Args) != 2 {
		t.Fatalf("Between")
	}
	dr := DateRange("ts", now.Add(-time.Hour), now)
	if dr.Expr != "ts BETWEEN ? AND ?" || len(dr.Args) != 2 {
		t.Fatalf("DateRange")
	}
	od := OnDate("ts", now)
	if od.Expr == "" || len(od.Args) != 2 {
		t.Fatalf("OnDate")
	}
}

func TestRawCond(t *testing.T) {
	c := RawCond("x = ?", 1)
	if c.Expr != "x = ?" || len(c.Args) != 1 || c.Args[0] != 1 {
		t.Fatalf("RawCond")
	}
}
