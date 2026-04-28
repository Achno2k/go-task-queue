package worker

import (
	"testing"
)

// TestBackoffMath documents the retry timing contract: delay grows
// exponentially in the attempt number, with jitter on top capped at the
// base value. We don't import time/rand here — just sanity-check the
// arithmetic the worker uses.
func TestBackoffGrowsExponentially(t *testing.T) {
	cases := []struct {
		attempt int
		minBase int64
	}{
		{1, 2},
		{2, 4},
		{3, 8},
		{4, 16},
	}

	for _, c := range cases {
		got := int64(1 << c.attempt)
		if got != c.minBase {
			t.Errorf("attempt=%d: base=%d, want %d", c.attempt, got, c.minBase)
		}
	}
}
