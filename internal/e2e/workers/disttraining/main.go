// Command disttraining is a tiny worker binary that internal/e2e's
// end-to-end test builds and hands to an internal/shell/agent.Agent to exec
// once per barrier step — the owner-decided exec-once-per-step model
// (issue #94's fork D5) — the same way internal/e2e/workers/keyspace is
// (see that package's doc comment for the agent<->process contract).
//
// Wire format: stdin is internal/e2e.EncodeDTStdin's layout — the worker's
// sample-index shard [start, end) as two big-endian uint64s, the barrier
// step as a third big-endian uint64, and the incoming all-reduced gradient
// from the previous step as consecutive big-endian float64s (empty at step
// 0), decoded by internal/e2e.DecodeDTStdin. This worker computes its
// partial gradient for the step via internal/e2e.DTPartial — a
// deterministic pure function of (shard, step, incoming gradient), a
// stand-in for a real training step, not randomness — and writes it back on
// stdout via internal/e2e.EncodeGradient (exactly the byte layout
// internal/core/templates.DistTrainingCombine's sumFloat64Vectors sums),
// then exits 0. Because DTPartial is deterministic and exported, a test can
// compute the exact same numbers without spawning this binary at all.
package main

import (
	"io"
	"os"

	"github.com/msivraj/swarm/internal/e2e"
)

func main() {
	os.Exit(run())
}

func run() int {
	in, err := io.ReadAll(os.Stdin)
	if err != nil {
		return 1
	}
	start, end, step, incoming, ok := e2e.DecodeDTStdin(in)
	if !ok {
		return 1
	}

	partial := e2e.DTPartial(start, end, step, incoming)
	if _, err := os.Stdout.Write(e2e.EncodeGradient(partial)); err != nil {
		return 1
	}
	return 0
}
