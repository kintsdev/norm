package norm

import (
	"errors"
	"testing"
)

func TestErrorRowScan(t *testing.T) {
	if err := (errorRow{err: errors.New("x")}).Scan(); err == nil {
		t.Fatalf("expected error")
	}
}
