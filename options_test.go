package norm

import "testing"

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()
	if o.logger == nil || o.metrics == nil {
		t.Fatalf("defaults not set")
	}
}
