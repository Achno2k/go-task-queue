package worker

import (
	"context"
	"testing"
)

// BenchmarkRegistry measures the cost of dispatching to a registered
// handler. The numbers aren't a queue benchmark on their own — for that
// run the integration suite — but they confirm the registry adds no
// meaningful overhead vs. a direct call.
func BenchmarkRegistryDispatch(b *testing.B) {
	Register("bench.noop", func(ctx context.Context, _ []byte) error { return nil })
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h, _ := lookup("bench.noop")
		_ = h(ctx, nil)
	}
}
