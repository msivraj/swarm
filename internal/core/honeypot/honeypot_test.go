package honeypot

import (
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// -----------------------------------------------------------------------
// Check — table-driven
// -----------------------------------------------------------------------

func TestCheck(t *testing.T) {
	tests := []struct {
		name    string
		claimed model.Result
		known   model.Result
		want    model.Probe
	}{
		{
			name:    "matching value and OK => Match",
			claimed: model.Result{ID: "m1", Value: []byte("42"), OK: true},
			known:   model.Result{ID: "known", Value: []byte("42"), OK: true},
			want:    model.Match,
		},
		{
			name:    "differing value, both OK => Lie",
			claimed: model.Result{ID: "m1", Value: []byte("43"), OK: true},
			known:   model.Result{ID: "known", Value: []byte("42"), OK: true},
			want:    model.Lie,
		},
		{
			name:    "claimed OK false, known OK true => Lie",
			claimed: model.Result{ID: "m1", Value: []byte("42"), OK: false},
			known:   model.Result{ID: "known", Value: []byte("42"), OK: true},
			want:    model.Lie,
		},
		{
			name:    "claimed OK true, known OK false => Lie",
			claimed: model.Result{ID: "m1", Value: []byte("42"), OK: true},
			known:   model.Result{ID: "known", Value: []byte("42"), OK: false},
			want:    model.Lie,
		},
		{
			name:    "both OK false, same value => Match",
			claimed: model.Result{ID: "m1", Value: []byte("err"), OK: false},
			known:   model.Result{ID: "known", Value: []byte("err"), OK: false},
			want:    model.Match,
		},
		{
			name:    "both OK false, differing value => Lie",
			claimed: model.Result{ID: "m1", Value: []byte("err-a"), OK: false},
			known:   model.Result{ID: "known", Value: []byte("err-b"), OK: false},
			want:    model.Lie,
		},
		{
			name:    "nil vs empty slice both OK => Match (byte-equal)",
			claimed: model.Result{ID: "m1", Value: nil, OK: true},
			known:   model.Result{ID: "known", Value: []byte{}, OK: true},
			want:    model.Match,
		},
		{
			name:    "empty claimed value vs nonempty known => Lie",
			claimed: model.Result{ID: "m1", Value: []byte{}, OK: true},
			known:   model.Result{ID: "known", Value: []byte("42"), OK: true},
			want:    model.Lie,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Check(tt.claimed, tt.known); got != tt.want {
				t.Errorf("Check(%+v, %+v) = %v, want %v", tt.claimed, tt.known, got, tt.want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// honeypot property (§03): a lie on a known answer is always caught, and
// always blacklists the lying identity. No input escapes.
// -----------------------------------------------------------------------

// candidateValues is a deterministic sweep of byte-slice payloads (no
// randomness — a pure core test stays reproducible by enumeration), used to
// exercise Check/OnLie across many claimed-vs-known combinations.
func candidateValues() [][]byte {
	return [][]byte{
		nil,
		{},
		[]byte("a"),
		[]byte("known-answer"),
		[]byte("Known-Answer"), // case differs
		[]byte("known-answer-longer"),
		{0x00},
		{0x00, 0x01, 0x02},
		[]byte("known-answe"), // one byte short
	}
}

func TestHoneypotProperty_LieAlwaysCaughtAlwaysBlacklists(t *testing.T) {
	known := model.Result{ID: "known", Value: []byte("known-answer"), OK: true}
	ids := []model.SpiffeID{"spiffe://open/a", "spiffe://open/b", "spiffe://open/c"}

	for _, id := range ids {
		for _, v := range candidateValues() {
			for _, ok := range []bool{true, false} {
				claimed := model.Result{ID: id, Value: v, OK: ok}
				differs := !bytesEqual(v, known.Value) || ok != known.OK

				probe := Check(claimed, known)

				if differs {
					if probe != model.Lie {
						t.Fatalf("Check(%+v, %+v) = %v, want Lie (claim differs from known)", claimed, known, probe)
					}
					action := OnLie(id)
					if action != (model.Action{Kind: model.Blacklist, ID: id}) {
						t.Fatalf("OnLie(%q) = %+v, want Blacklist action for exactly that id", id, action)
					}
					continue
				}

				if probe != model.Match {
					t.Fatalf("Check(%+v, %+v) = %v, want Match (claim equals known)", claimed, known, probe)
				}
			}
		}
	}
}

// TestOnLie_AlwaysBlacklistsExactlyTheGivenID confirms OnLie never targets
// any identity other than the one it was called with, and never returns the
// inert zero value (NoAction).
func TestOnLie_AlwaysBlacklistsExactlyTheGivenID(t *testing.T) {
	ids := []model.SpiffeID{"", "spiffe://open/x", "spiffe://open/y-longer-id"}
	for _, id := range ids {
		got := OnLie(id)
		want := model.Action{Kind: model.Blacklist, ID: id}
		if got != want {
			t.Errorf("OnLie(%q) = %+v, want %+v", id, got, want)
		}
		if got.Kind != model.Blacklist {
			t.Errorf("OnLie(%q).Kind = %v, want Blacklist", id, got.Kind)
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------
// ShouldProbe — table-driven, monotonicity, and boundaries
// -----------------------------------------------------------------------

func TestShouldProbe(t *testing.T) {
	fresh := model.Reputation{Score: 0, Observations: 0}
	mid := model.Reputation{Score: 50, Observations: 10}
	trusted := model.Reputation{Score: 100, Observations: 100}
	distrusted := model.Reputation{Score: -10, Observations: 5}

	tests := []struct {
		name string
		rep  model.Reputation
		rng  float64
		want bool
	}{
		{"fresh identity, rng 0.0 => probe", fresh, 0.0, true},
		{"fresh identity, rng just under max rate => probe", fresh, maxProbeRate - 0.001, true},
		{"fresh identity, rng at max rate => no probe (boundary)", fresh, maxProbeRate, false},
		{"fresh identity, rng near 1.0 => no probe", fresh, 0.999999, false},

		{"distrusted (negative score) clamps to max rate, rng 0.0 => probe", distrusted, 0.0, true},
		{"distrusted (negative score) at max rate boundary => no probe", distrusted, maxProbeRate, false},

		{"trusted identity, rng 0.0 => probe (floor keeps probing)", trusted, 0.0, true},
		{"trusted identity, rng just under min rate => probe", trusted, minProbeRate - 0.001, true},
		{"trusted identity, rng at min rate => no probe (boundary)", trusted, minProbeRate, false},
		{"trusted identity, rng near 1.0 => no probe", trusted, 0.999999, false},

		{"mid reputation, rng 0.0 => probe", mid, 0.0, true},
		{"mid reputation, rng at its rate => no probe (boundary)", mid, probeRate(mid), false},
		{"mid reputation, rng just under its rate => probe", mid, probeRate(mid) - 0.0001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldProbe(tt.rep, tt.rng); got != tt.want {
				t.Errorf("ShouldProbe(%+v, %v) = %v, want %v", tt.rep, tt.rng, got, tt.want)
			}
		})
	}
}

// TestShouldProbe_MonotonicInReputation checks that a lower-reputation
// identity is probed for at least as wide an rng range as a
// higher-reputation one: probeRate is non-increasing in Score, so every rng
// that probes a higher-Score identity must also probe a lower-Score one.
func TestShouldProbe_MonotonicInReputation(t *testing.T) {
	scores := []int64{-50, -1, 0, 1, 25, 50, 75, 99, 100, 200}

	for i := 0; i+1 < len(scores); i++ {
		lo := model.Reputation{Score: scores[i]}
		hi := model.Reputation{Score: scores[i+1]}

		rateLo := probeRate(lo)
		rateHi := probeRate(hi)
		if rateLo < rateHi {
			t.Fatalf("probeRate(Score=%d) = %v < probeRate(Score=%d) = %v; want non-increasing in Score", lo.Score, rateLo, hi.Score, rateHi)
		}

		// Sweep a deterministic set of rng samples: any rng that probes the
		// higher-Score (more trusted) identity must also probe the
		// lower-Score one, since rateLo >= rateHi.
		for _, rng := range []float64{0, 0.02, 0.04, 0.06, 0.1, 0.2, 0.3, 0.4, 0.49, 0.5, 0.6, 0.9, 0.999} {
			if ShouldProbe(hi, rng) && !ShouldProbe(lo, rng) {
				t.Fatalf("rng=%v probed higher-reputation Score=%d but not lower-reputation Score=%d", rng, hi.Score, lo.Score)
			}
		}
	}
}

// TestShouldProbe_Deterministic confirms the pure-core guarantee: the same
// (rep, rng) input always yields the same decision.
func TestShouldProbe_Deterministic(t *testing.T) {
	reps := []model.Reputation{
		{Score: 0, Observations: 0},
		{Score: -25, Observations: 3},
		{Score: 50, Observations: 12},
		{Score: 150, Observations: 500},
	}
	rngs := []float64{0, 0.01, 0.049, 0.05, 0.2, 0.499, 0.5, 0.75, 0.999}

	for _, rep := range reps {
		for _, rng := range rngs {
			first := ShouldProbe(rep, rng)
			for i := 0; i < 5; i++ {
				if got := ShouldProbe(rep, rng); got != first {
					t.Fatalf("ShouldProbe(%+v, %v) is nondeterministic: got %v then %v", rep, rng, first, got)
				}
			}
		}
	}
}

// TestProbeRate_Bounds confirms probeRate never leaves [minProbeRate,
// maxProbeRate] across a wide sweep of scores, including extremes.
func TestProbeRate_Bounds(t *testing.T) {
	scores := []int64{-1000, -1, 0, 1, 10, 50, 99, 100, 101, 1000}
	for _, s := range scores {
		rate := probeRate(model.Reputation{Score: s})
		if rate < minProbeRate || rate > maxProbeRate {
			t.Errorf("probeRate(Score=%d) = %v, out of bounds [%v, %v]", s, rate, minProbeRate, maxProbeRate)
		}
	}
}
