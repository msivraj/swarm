// Package reputation is a pure core: it accounts for how much an identity has
// earned an open tier's trust and turns that trust into decisions the rest
// of P3 relies on — how much a vote is worth (Weight), how many redundant
// replicas a task needs before its result can be trusted (NeedsK), and
// whether a chronic quorum-loser should be soft-frozen out of work
// (Eligible). It performs no I/O and reads no clock or randomness: the shell
// persists the reputation store (keyed by SPIFFE identity) and calls Update
// after each verdict.
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

// minObservations is the participation floor below which an identity is
// still "fresh": Eligible never freezes it, so zero-start is preserved. A
// brand-new machine — including a fresh Sybil — has not yet earned or lost
// enough trust to judge, so it stays eligible for work.
const minObservations = 4

// freezeFloor is the Score at or below which a non-fresh identity is frozen.
// Score clamps to [0, maxScore] (see Update), so a chronic liar bottoms out
// at exactly freezeFloor after repeated lies — the same floor a brand-new
// identity starts at.
const freezeFloor int64 = 0

// Eligible reports whether an identity of this reputation may still be
// assigned open-tier work. It is FALSE — frozen — iff the identity has
// participated enough to be judged (Observations >= minObservations) yet has
// earned ~nothing (Score <= freezeFloor). A FRESH identity (Observations <
// minObservations) is always eligible: zero-start is preserved, so a
// brand-new zero-value Reputation is eligible, never frozen.
//
// Because Update never lets Score go negative, a chronic liar and a fresh
// Sybil clamp to the same Score — freezeFloor. Re-minting a fresh identity
// to dodge a freeze buys nothing but a brief eligible window before the new
// identity's own Observations climb past minObservations, and costs a fresh
// proof-of-work to even get there.
func Eligible(rep model.Reputation) bool {
	if rep.Observations < minObservations {
		return true
	}
	return rep.Score > freezeFloor
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

// decayUnit and decayPerUnit calibrate Decay's fade rate: a linear decay
// (not a half-life curve), expressed in whole days so it is exact integer
// arithmetic with no floating point to round unpredictably. A once-good but
// ABSENT identity loses decayPerUnit Score for every full decayUnit it goes
// unseen. decayPerUnit is chosen so repStep (200) — the Score needed to drop
// NeedsK by one quorum step — is exactly 10 decayUnit apart, so a machine
// that stops participating fades one NeedsK step roughly every 10 days.
const (
	decayUnit          = model.Duration(24 * 60 * 60 * 1_000_000_000) // one day, in ns
	decayPerUnit int64 = 20
)

// Decay fades a reputation's Score toward the zero floor as elapsed grows,
// modeling stale trust: an identity that has stopped participating should
// not coast forever on trust it earned in the past. Score falls linearly by
// decayPerUnit for every full decayUnit of elapsed and is clamped to
// [0, maxScore] — the same floor Update enforces — so Decay can never push
// Score below zero. That preserves the P3 Sybil-floor property: a decayed
// veteran's Score is never worse than a fresh identity's own floor.
//
// Observations is left untouched: participation count is a historical fact,
// not trust that goes stale, and Eligible needs it undisturbed to tell a
// once-active veteran apart from a brand-new identity.
//
// elapsed <= 0 returns rep unchanged — nothing has elapsed to fade. elapsed
// is data the shell passes in from its own clock (a periodic decay pass);
// Decay itself never reads a clock.
//
// This is the intended decay x freeze interaction: as Score fades toward 0,
// an ABSENT identity with Observations >= minObservations eventually
// crosses Eligible's freeze floor (Observations >= minObservations &&
// Score <= freezeFloor) and is soft-frozen out of open-tier work, exactly
// as a chronic liar would be. That is deliberate — stale trust must be
// re-earned by participating again, not merely remembered. A fresh identity
// (Observations < minObservations) never freezes this way: Eligible always
// treats it as fresh regardless of Score, so zero-start is preserved even
// under decay.
func Decay(rep model.Reputation, elapsed model.Duration) model.Reputation {
	if elapsed <= 0 {
		return rep
	}
	units := int64(elapsed / decayUnit)
	next := rep
	next.Score = clamp(rep.Score-units*decayPerUnit, 0, maxScore)
	return next
}

// repTierLoBand and repTierHiBand are the Score cutoffs Tier buckets on,
// expressed as repStep multiples so Tier reads NeedsK's own step function
// instead of inventing an unrelated scale. repTierHiBand (3*repStep == 600)
// is exactly the Score at which NeedsK reaches its floor for the stricter
// Open tier (steps == 3, so maxK-2*steps == openMinK): RepTrusted starts
// exactly where NeedsK stops asking for fewer replicas as Score rises
// further.
const (
	repTierLoBand = repStep
	repTierHiBand = 3 * repStep
)

// Tier buckets a reputation into a coarse verification-maturity RepTier. It
// is a read of the same signals Weight and NeedsK already use (Score and
// Observations) and is monotonic non-decreasing over each: raising either
// Score or Observations can only raise or hold the tier, never lower it. It
// does not feed back into Weight, NeedsK, or Eligible — those are entirely
// unchanged; Tier is an additional coarse view for a caller that wants
// three buckets instead of a continuous score.
//
//   - RepUntrusted: fresh (Observations < minObservations — zero-start is
//     preserved here too) OR low Score (< repTierLoBand). This also covers
//     every frozen (!Eligible) identity, since Eligible's freeze requires
//     Score <= freezeFloor == 0, which is always < repTierLoBand.
//   - RepTrusted: mature (Observations >= minObservations) AND high Score
//     (>= repTierHiBand).
//   - RepProvisional: everything else — some honest history, a mid Score.
func Tier(rep model.Reputation) model.RepTier {
	score := clamp(rep.Score, 0, maxScore)

	if rep.Observations < minObservations || score < repTierLoBand {
		return model.RepUntrusted
	}
	if score >= repTierHiBand {
		return model.RepTrusted
	}
	return model.RepProvisional
}
