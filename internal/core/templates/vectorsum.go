package templates

import (
	"encoding/binary"
	"math"
)

// sumFloat64Vectors elementwise-sums a set of equal-length float64 vectors,
// each encoded as consecutive big-endian float64s (8 bytes per element) —
// the shared reducer DistTrainingCombine (all-reduce gradients) and
// AgentSimCombine (aggregate state) both use, since both are, at heart, "add
// up N workers' same-shaped numeric vectors."
//
// The vector's dimension is taken from the first well-formed entry (a
// nonzero length that is a multiple of 8); any entry whose length does not
// match that dimension is skipped as malformed rather than aborting the
// whole reduction, mirroring MonteCarloMerge's "a bad block contributes
// nothing" precedent in montecarlo.go. Because addition is both commutative
// and associative, sumFloat64Vectors gives the same result regardless of the
// order vectors arrive in or how they are grouped into sub-sums — the law
// the property tests in disttraining_test.go and agentsim_test.go check —
// modulo the usual floating-point rounding from reordering additions, which
// the property tests avoid by using exactly-representable small integers.
//
// A nil or empty input, or one with no well-formed entry, returns nil (the
// identity: no vector to report).
func sumFloat64Vectors(vectors [][]byte) []byte {
	dim := 0
	for _, v := range vectors {
		if len(v) > 0 && len(v)%8 == 0 {
			dim = len(v) / 8
			break
		}
	}
	if dim == 0 {
		return nil
	}

	sums := make([]float64, dim)
	for _, v := range vectors {
		if len(v) != dim*8 {
			continue // malformed, or a different dimension than the rest: skip
		}
		for i := 0; i < dim; i++ {
			sums[i] += math.Float64frombits(binary.BigEndian.Uint64(v[i*8 : i*8+8]))
		}
	}

	out := make([]byte, dim*8)
	for i, s := range sums {
		binary.BigEndian.PutUint64(out[i*8:i*8+8], math.Float64bits(s))
	}
	return out
}

// decodeFloat64Vector decodes b, a sequence of big-endian float64s, back
// into a []float64 — the test-facing inverse of the encoding
// sumFloat64Vectors expects its inputs in and produces its output in.
func decodeFloat64Vector(b []byte) ([]float64, bool) {
	if len(b) == 0 || len(b)%8 != 0 {
		return nil, false
	}
	out := make([]float64, len(b)/8)
	for i := range out {
		out[i] = math.Float64frombits(binary.BigEndian.Uint64(b[i*8 : i*8+8]))
	}
	return out, true
}

// encodeFloat64Vector is decodeFloat64Vector's inverse, used by tests to
// build well-formed per-worker vectors.
func encodeFloat64Vector(vs []float64) []byte {
	out := make([]byte, len(vs)*8)
	for i, v := range vs {
		binary.BigEndian.PutUint64(out[i*8:i*8+8], math.Float64bits(v))
	}
	return out
}
