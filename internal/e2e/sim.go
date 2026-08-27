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
