package norm

import "testing"

type fakeBaseRow struct {
	scanned int
	retErr  error
}

func (f *fakeBaseRow) Scan(_ ...any) error { f.scanned++; return f.retErr }

func TestRowWithAfter_CallsCallback(t *testing.T) {
	base := &fakeBaseRow{}
	called := 0
	r := rowWithAfter{Row: base, after: func(err error) { called++ }}
	_ = r.Scan()
	if called != 1 || base.scanned != 1 {
		t.Fatalf("after not called or scan not forwarded")
	}
}
