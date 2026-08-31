// Package verification is a pure core: it decides which K machines an
// open-tier task is dispatched to, how much redundancy a runner's tier and
// reputation demand, and which answer a quorum of K results agrees on. It
// performs no I/O and reads no clock or randomness of its own — the shell
// supplies a seed (for Assign) and collects Results (for Verdict); this
// package only ever returns data. See
// docs/phases/swarm-p3-components.txt §02 (RESULT VERIFICATION) and §03 (the
// `assign` and `verdict` properties).
package verification

import (
	"hash/fnv"

	"github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/model"
)

// Assign deterministically selects k distinct machines from pool to run task
// t. Randomness is derived entirely from the caller-supplied seed via an
// in-package splitMix64 PRNG — never math/rand or crypto/rand, both of which
// fcischeck forbids in core because they would make Assign's output depend
// on process state instead of its inputs. Same (t, pool, k, seed) always
// produces byte-identical output; distinct seeds (or distinct tasks sharing
// a seed, since t is mixed into the PRNG's seed below) generally produce
// distinct selections.
//
// Selection uses a partial Fisher-Yates shuffle: walk pool front-to-back,
// and at each position i swap in a uniformly-chosen element from the
// remaining unshuffled suffix [i, len(work)), stopping after k swaps. This
// is the standard algorithm for drawing a uniform sample of size k without
// replacement from n items in O(k) time; because every swap draws from
// exactly the elements not yet chosen, the k selected machines are always
// distinct and always a subset of pool.
//
// k <= 0 or an empty pool returns nil — nothing to assign. k >= len(pool)
// returns the whole pool, in seeded (not input) order, so a full-pool draw
// leaks no more positional information than a partial one.
func Assign(t model.TaskID, pool []model.MachineID, k int, seed uint64) []model.MachineID {
	if k <= 0 || len(pool) == 0 {
		return nil
	}
	if k > len(pool) {
		k = len(pool)
	}

	work := make([]model.MachineID, len(pool))
	copy(work, pool)

	rng := newSplitMix64(seed ^ fnv1a64(string(t)))
	for i := 0; i < k; i++ {
		j := i + int(rng.next()%uint64(len(work)-i))
		work[i], work[j] = work[j], work[i]
	}
	return work[:k]
}

// Redundancy returns how many replicas an open-tier task needs given the
// runner's tier and reputation. It delegates to reputation.NeedsK — the
// canonical replica-count function (see internal/core/reputation) — rather
// than re-deriving the tier/score bounds here, so the two packages can never
// silently diverge on how much redundancy a given (tier, rep) requires.
func Redundancy(tier model.Tier, rep model.Reputation) int {
	return reputation.NeedsK(rep, tier)
}

// Verdict tallies K results into a quorum decision: Agreed{Value} when a
// strict majority of rs (more than half) share the same Value; Disputed
// when rs is non-empty but no Value reaches a strict majority (including an
// exact even split); Insufficient when no results were collected at all.
// The zero-value Verdict returned in every non-Agreed case has Value == nil,
// so a caller can never mistake a Disputed or Insufficient verdict for an
// accepted answer.
//
// This is the safety property P3 stands on ("verdict", §03): because the
// tally only ever accepts a value more than half of rs share, a minority of
// liars — no matter what they claim — can never out-count an honest
// majority. K=5 with 3 honest results agreeing and 2 arbitrary, differing
// lies still yields Agreed on the honest value, since 3 > 5/2 and no lie
// value can also reach 3 without agreeing with another liar (which would
// only grow the minority, never overtake the majority).
func Verdict(rs []model.Result) model.Verdict {
	if len(rs) == 0 {
		return model.Verdict{Kind: model.Insufficient}
	}

	counts := make(map[string]int, len(rs))
	for _, r := range rs {
		counts[string(r.Value)]++
	}

	// Walk rs (not the map) to find the best-count value, so the result
	// never depends on Go's randomized map iteration order — ties (no
	// value reaches a strict majority) fall through to Disputed regardless
	// of which tied value this loop happens to keep.
	var bestValue []byte
	var bestCount int
	for _, r := range rs {
		if c := counts[string(r.Value)]; c > bestCount {
			bestCount = c
			bestValue = r.Value
		}
	}

	if bestCount*2 > len(rs) {
		return model.Verdict{Kind: model.Agreed, Value: bestValue}
	}
	return model.Verdict{Kind: model.Disputed}
}

// fnv1a64 hashes s with FNV-1a (stdlib, non-cryptographic) so distinct task
// IDs mixed into the same base seed still drive independent PRNG streams in
// Assign, instead of every task under one seed landing on the same K-set.
func fnv1a64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // hash.Hash.Write never errors
	return h.Sum64()
}

// splitMix64 is a small, fast, deterministic PRNG (Vigna's SplitMix64)
// seeded entirely from data the caller supplies. It is not
// cryptographically secure and is not meant to be — Assign only needs a
// reproducible, roughly-uniform stream, and a stdlib-only generator keeps
// this package free of math/rand and crypto/rand, both of which fcischeck
// forbids in core because they read hidden process/OS state instead of the
// function's declared inputs.
type splitMix64 struct {
	state uint64
}

func newSplitMix64(seed uint64) *splitMix64 {
	return &splitMix64{state: seed}
}

// next advances the generator and returns the next pseudo-random uint64.
func (s *splitMix64) next() uint64 {
	s.state += 0x9E3779B97F4A7C15
	z := s.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
