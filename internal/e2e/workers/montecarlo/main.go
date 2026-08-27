// Command montecarlo is a tiny worker binary that internal/e2e's end-to-end
// test builds and hands to an internal/shell/agent.Agent to exec, the same
// way internal/e2e/workers/keyspace is (see that package's doc comment for
// the agent<->process contract).
//
// Wire format: stdin is exactly 16 bytes, the big-endian int64 Seed and
// big-endian int64 Trials a monte-carlo task's Input carries (see
// internal/e2e.DecodeMCTaskInput, mirroring
// internal/core/templates/montecarlo.go's unexported mcTaskInput). This
// worker "runs" Trials trials by evaluating internal/e2e.NextValue at each
// trial index — a deterministic stand-in for a real Monte-Carlo draw, not
// randomness — and writes back the block's count, sum, and sum-of-squares
// via internal/e2e.EncodeMCResult (mirroring
// internal/core/templates.mcResult, the layout
// internal/core/templates.MonteCarloMerge decodes), then exits 0. Because
// NextValue is deterministic and exported, a test can compute the exact
// same numbers without spawning this binary at all.
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
	seed, trials, ok := e2e.DecodeMCTaskInput(in)
	if !ok || trials <= 0 {
		return 1
	}

	var sum, sumSq float64
	for i := int64(0); i < trials; i++ {
		v := e2e.NextValue(seed, i)
		sum += v
		sumSq += v * v
	}

	if _, err := os.Stdout.Write(e2e.EncodeMCResult(trials, sum, sumSq)); err != nil {
		return 1
	}
	return 0
}
