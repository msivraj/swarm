// Package reputation is a pure core: it accounts for how much an identity has
// earned an open tier's trust and turns that trust into two decisions the
// rest of P3 relies on — how much a vote is worth (Weight) and how many
// redundant replicas a task needs before its result can be trusted (NeedsK).
// It performs no I/O and reads no clock or randomness: the shell persists the
// reputation store (keyed by SPIFFE identity) and calls Update after each
// verdict.
//
// Every fresh identity starts at model.Reputation{} — the zero value — which
// this package treats as the trust floor. A machine that lies its way to a
// bad reputation gains nothing by discarding its identity and re-enrolling:
// the best a fresh Sybil can do is the same floor a caught liar is clamped
// to (see Update). That is the "zero-start" property #03 of the phase doc
// names.
package reputation

import "github.com/msivraj/swarm/internal/model"

const (
	// maxScore is the cap Update clamps Score to. A cap keeps Weight bounded
	// and NeedsK's step function well-defined instead of requiring ever more
	// honest observations to matter less and less.
	maxScore int64 = 1000

	// honestGain is how much Score rises for one agreed == true verdict.
	honestGain int64 = 10

	// liePenalty is how much Score falls for one agreed == false verdict.
	// It is larger than honestGain: a single lie should cost more trust than
	// a single honest answer earns, so patient honest behavior is required
	// to recover from a lie and Sybil-and-relie is never profitable.
	liePenalty int64 = 50
)

// Update returns the reputation that results from one verdict about an
// identity: it rises by honestGain when the identity agreed with the honest
// quorum (agreed == true) and falls by liePenalty on a detected lie
// (agreed == false). Score is clamped to [0, maxScore] on every call, so:
//
//   - Update(rep, true) never lowers Score (only raises it, up to the cap).
//   - Update(rep, false) never raises Score (only lowers it, down to the
//     floor of 0 — the same floor a brand-new identity starts at). A lie
//     can never push Score below zero, so an identity cannot "buy" a worse
//     score than a fresh Sybil already has.
//
// Observations always increments by one, honest or not, so the shell (and a
// future refinement of Weight/NeedsK) can tell an identity with a long
// honest history from one that has barely participated.
func Update(rep model.Reputation, agreed bool) model.Reputation {
	next := rep
	next.Observations = rep.Observations + 1
	if agreed {
		next.Score = clamp(rep.Score+honestGain, 0, maxScore)
	} else {
		next.Score = clamp(rep.Score-liePenalty, 0, maxScore)
	}
	return next
}

// Weight maps a reputation to a trust weight in [0, 1]: 0 for the zero-value
// (brand-new/untrusted) reputation, rising to 1 as Score approaches the cap
// Update enforces. It is monotonic non-decreasing in Score and always
// finite, so callers may sum or compare Weights (e.g. to weight a vote)
// without guarding against NaN or Inf.
func Weight(rep model.Reputation) float64 {
	score := clamp(rep.Score, 0, maxScore)
	return float64(score) / float64(maxScore)
}

// Tier-specific replica-count bounds NeedsK steps between. Open-tier
// identities have no account and can be re-created for free, so even a
// maximally trusted Open identity is still checked against openMinK peers;
// Core-tier identities are provisioned/trusted out of band, so a maximally
// trusted Core identity needs no redundancy beyond itself (coreMinK == 1).
// Both bounds are odd: verdict() (the quorum core) needs an odd K so a
// verdict can never tie — an even split would be undecidable.
const (
	openMinK = 3
	openMaxK = 9
	coreMinK = 1
	coreMaxK = 5

	// repStep is the Score needed to drop NeedsK by one quorum step (2, to
	// keep K odd at every step).
	repStep int64 = 200
)

// NeedsK returns how many redundant replicas an identity of this reputation
// needs at this tier before its result can be trusted: more for a low- or
// no-reputation identity, fewer as trust is earned, down to a floor that
// never reaches zero. It is the canonical replica-count function — P3's
// verification core (redundancy()) delegates to it rather than duplicating
// the bounds below.
//
// K is always odd (see the const block above) and always >= 1: an odd count
// guarantees a quorum verdict can never tie, and Open's floor (openMinK == 3)
// is always >= Core's floor (coreMinK == 1) for the same reputation, since
// Open identities are unaccountable in a way Core identities are not.
func NeedsK(rep model.Reputation, tier model.Tier) int {
	minK, maxK := coreMinK, coreMaxK
	if tier == model.Open {
		minK, maxK = openMinK, openMaxK
	}

	steps := clamp(rep.Score, 0, maxScore) / repStep
	k := maxK - int(2*steps)
	if k < minK {
		return minK
	}
	return k
}

// clamp bounds v to [lo, hi].
func clamp(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
