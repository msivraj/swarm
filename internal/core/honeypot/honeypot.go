// Package honeypot is a pure core: it decides when to hand an identity a
// known-answer spot-check, compares a claimed answer against the known one,
// and emits the blacklist action when the claim is a lie. It performs no
// I/O and reads no clock or randomness — the shell draws the uniform sample
// `rng` and injects it as data. See docs/phases/swarm-p3-components.txt §02
// (HONEYPOT SPOT-CHECKS) and §03 (the `honeypot` property).
package honeypot

import (
	"bytes"

	"github.com/msivraj/swarm/internal/model"
)

// Probe-rate band: a fresh or distrusted identity (Score <= 0) is probed at
// maxProbeRate; a well-established identity (Score >= trustedScore) is
// probed at minProbeRate — the floor every identity retains no matter how
// trusted, so accumulated trust can never be banked to disable spot-checks
// entirely. Between the two, the rate falls linearly in Score.
const (
	maxProbeRate = 0.5
	minProbeRate = 0.05
	trustedScore = 100
)

// ShouldProbe decides whether to hand this identity a known-answer honeypot
// task. `rng` is a uniform [0,1) sample supplied by the shell — the core
// never draws randomness itself. The identity is probed when rng falls
// below its probe rate: rng == 0.0 always probes (every rate is positive);
// rng at or above the identity's rate never does. Lower-reputation
// identities have a higher rate and are therefore probed for a wider rng
// range than higher-reputation ones (monotonic in Score).
func ShouldProbe(rep model.Reputation, rng float64) bool {
	return rng < probeRate(rep)
}

// probeRate maps a Reputation's Score to a probe rate in [minProbeRate,
// maxProbeRate]. Score <= 0 — including the zero value every fresh identity
// starts at, and any identity that has been caught lying — is clamped to
// maxProbeRate. Score >= trustedScore is clamped to minProbeRate. Between
// the two bounds the rate decreases linearly, so probeRate is monotonically
// non-increasing in Score.
func probeRate(rep model.Reputation) float64 {
	switch {
	case rep.Score <= 0:
		return maxProbeRate
	case rep.Score >= trustedScore:
		return minProbeRate
	default:
		frac := float64(rep.Score) / float64(trustedScore)
		return maxProbeRate - frac*(maxProbeRate-minProbeRate)
	}
}

// Check compares a claimed honeypot result against the known-good result
// and returns Match or Lie. A Match requires BOTH OK to match AND Value to
// be byte-equal (bytes.Equal, so a nil and an empty slice compare equal);
// anything else is a Lie. In particular, a Value that differs from the
// known one is unconditionally a Lie, regardless of OK — this is what makes
// "a lie on a known answer is always caught" hold with no escape hatch. A
// claimed OK == false is treated the same as any other claim: it is a Lie
// unless the known result is also OK == false with an equal Value — i.e.
// claiming failure on a task the known answer completed successfully (or
// vice versa) is itself a lie, not a free pass.
func Check(claimed model.Result, known model.Result) model.Probe {
	if claimed.OK == known.OK && bytes.Equal(claimed.Value, known.Value) {
		return model.Match
	}
	return model.Lie
}

// OnLie returns the action to take when identity id lied on a known-answer
// probe: Blacklist that identity. This is a pure decision — the shell
// performs the actual blacklisting.
//
// OnLie is one-strike — it blacklists on every lie, with no tolerance for
// a single transient fault. OnRepeatedLie below is the strike-aware
// replacement; OnLie is kept unchanged here (signature and behavior) so
// its existing caller (internal/shell/honeypot's ProbingDispatcher) keeps
// compiling until that shell is rewired to OnRepeatedLie in a follow-up
// ticket.
func OnLie(id model.SpiffeID) model.Action {
	return model.Action{Kind: model.Blacklist, ID: id}
}

// DefaultStrikeLimit is the number of honeypot lies an identity may
// accumulate before OnRepeatedLie blacklists it. Two-strike (default 2): a
// single transient fault on a honeypot task is tolerated, but a repeat
// offender is evicted. A documented constant so the threshold is tunable
// later without touching call sites that pass it explicitly.
const DefaultStrikeLimit = 2

// OnRepeatedLie is the strike-aware, pure blacklist decision: given that
// identity id has now accumulated `strikes` honeypot lies (the shell's
// per-identity counter, taken post-increment — i.e. `strikes` includes the
// lie that just triggered this call) against a blacklist threshold
// `limit`, it returns Blacklist iff strikes >= limit, else the inert
// model.Action{} (NoAction). This is what makes a single transient fault
// on a honeypot task survivable while a repeat offender is still evicted.
//
// A non-positive limit is nonsensical as a strike threshold and must not
// silently disable eviction, so limit <= 0 falls back to
// DefaultStrikeLimit rather than being treated as "never blacklist".
func OnRepeatedLie(id model.SpiffeID, strikes, limit int) model.Action {
	if limit <= 0 {
		limit = DefaultStrikeLimit
	}
	if strikes >= limit {
		return model.Action{Kind: model.Blacklist, ID: id}
	}
	return model.Action{}
}
