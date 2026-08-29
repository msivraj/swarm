package e2e

// NextValue is the deterministic stand-in for a Monte-Carlo trial that the
// montecarlo worker (internal/e2e/workers/montecarlo) runs, and that the
// test computes directly to get an expected aggregate without spawning that
// worker. It draws no randomness — the same (seed, i) pair always yields the
// same value, a splitmix64-style hash scaled into [0, 1) — which is exactly
// what lets a test independently predict the sum/mean/variance a real
// process running this same formula will report back over Task.Output.
func NextValue(seed, i int64) float64 {
	x := uint64(seed) + uint64(i)*0x9E3779B97F4A7C15
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return float64(x>>11) / float64(uint64(1)<<53)
}

// dtDimension is the small, fixed model-parameter count the dist-training
// worker's toy gradient uses when there is no incoming gradient yet to take
// the dimension from (step 0's incoming is empty).
const dtDimension = 4

// DTPartial is the deterministic, pure function of (shard, step, incoming
// gradient) the dist-training worker (internal/e2e/workers/disttraining)
// computes as its partial gradient for one barrier step — and that a test
// calls directly to compute the expected result without spawning the
// worker, the same "recompute, don't re-run the code under test" pattern as
// NextValue.
//
// Each parameter p's value is a splitmix64-style hash of (start, end, step,
// p) scaled into [0, 1), plus a tenth of the incoming combined gradient at
// p — a toy computation that still reacts to the previous step's result
// without any randomness or clock. Its dimension is len(incoming) if that
// is nonzero (an incoming gradient from a prior step exists), otherwise the
// fixed dtDimension.
func DTPartial(start, end, step uint64, incoming []float64) []float64 {
	dim := len(incoming)
	if dim == 0 {
		dim = dtDimension
	}

	out := make([]float64, dim)
	for p := 0; p < dim; p++ {
		x := start*0x9E3779B97F4A7C15 + end*0xBF58476D1CE4E5B9 + step*0x94D049BB133111EB + uint64(p)*0xD6E8FEB86659FD93
		x ^= x >> 30
		x *= 0xBF58476D1CE4E5B9
		x ^= x >> 27
		x *= 0x94D049BB133111EB
		x ^= x >> 31
		hash := float64(x>>11) / float64(uint64(1)<<53)

		var in float64
		if p < len(incoming) {
			in = incoming[p]
		}
		out[p] = hash + 0.1*in
	}
	return out
}

// ExpectedAllReducedGradient independently computes what a barrier step's
// all-reduced gradient should be: DTPartial for each [start, end) shard in
// shards, summed elementwise — the same reduction
// internal/core/templates.DistTrainingCombine performs over the workers'
// real stdout — so a test can assert the disttraining worker binary and the
// pure combine core agree without re-implementing either.
func ExpectedAllReducedGradient(shards [][2]uint64, step uint64, incoming []float64) []float64 {
	dim := len(incoming)
	if dim == 0 {
		dim = dtDimension
	}

	sum := make([]float64, dim)
	for _, shard := range shards {
		partial := DTPartial(shard[0], shard[1], step, incoming)
		for i, v := range partial {
			sum[i] += v
		}
	}
	return sum
}
