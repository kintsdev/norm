package norm

import (
	"testing"
	"time"
)

func TestDefaultIfZeroHelpers(t *testing.T) {
	if defaultIfZeroInt(0, 5) != 5 {
		t.Fatalf("int")
	}
	if defaultIfZeroInt(3, 5) != 3 {
		t.Fatalf("int keep")
	}
	if defaultIfZeroDuration(0, time.Second) != time.Second {
		t.Fatalf("dur")
	}
	if defaultIfZeroDuration(2*time.Second, time.Second) != 2*time.Second {
		t.Fatalf("dur keep")
	}
}
