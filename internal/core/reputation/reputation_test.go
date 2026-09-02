package reputation

import (
	"math"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// TestUpdate is a table-driven test of the honest-raise / lie-lowers
// asymmetry Update documents: agreed == true only ever raises Score (up to
// the cap), agreed == false only ever lowers it (down to the zero floor),
// and Observations always advances by one either way.
func TestUpdate(t *testing.T) {
	tests := []struct {
		name      string
		rep       model.Reputation
		agreed    bool
		wantScore int64
		wantObs   int
	}{
		{"honest from zero raises", model.Reputation{}, true, honestGain, 1},
		{"lie from zero stays at floor", model.Reputation{}, false, 0, 1},
		{"honest from mid raises", model.Reputation{Score: 100, Observations: 4}, true, 100 + honestGain, 5},
		{"lie from mid lowers", model.Reputation{Score: 100, Observations: 4}, false, 100 - liePenalty, 5},
		{"lie below penalty clamps to floor", model.Reputation{Score: 10, Observations: 1}, false, 0, 2},
		{"honest at cap stays at cap", model.Reputation{Score: maxScore, Observations: 50}, true, maxScore, 51},
		{"honest near cap clamps to cap", model.Reputation{Score: maxScore - 1, Observations: 9}, true, maxScore, 10},
		{"lie at floor stays at floor", model.Reputation{Score: 0, Observations: 9}, false, 0, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Update(tt.rep, tt.agreed)
			if got.Score != tt.wantScore {
				t.Errorf("Update(%+v, %v).Score = %d, want %d", tt.rep, tt.agreed, got.Score, tt.wantScore)
			}
			if got.Observations != tt.wantObs {
				t.Errorf("Update(%+v, %v).Observations = %d, want %d", tt.rep, tt.agreed, got.Observations, tt.wantObs)
			}
			if got.Score < 0 {
				t.Errorf("Update(%+v, %v).Score = %d, below the zero-value floor", tt.rep, tt.agreed, got.Score)
			}
			if got.Score > maxScore {
				t.Errorf("Update(%+v, %v).Score = %d, above the cap %d", tt.rep, tt.agreed, got.Score, maxScore)
			}
		})
	}
}

// TestUpdateMonotonicAndZeroStart is the §03 "update" property test: from
// any starting reputation, honest agreement never lowers Score and a lie
// never raises it, and the zero value is always the floor no lie can push
// below — so a caught liar gains nothing by discarding its identity and
// re-enrolling as a fresh Sybil (the best a fresh identity can do is the
// same floor a repeat liar is clamped to).
func TestUpdateMonotonicAndZeroStart(t *testing.T) {
	starts := []model.Reputation{
		{},
		{Score: 1},
		{Score: 30, Observations: 3},
		{Score: 250, Observations: 25},
		{Score: maxScore, Observations: 500},
		{Score: maxScore - 5, Observations: 9},
	}

	for _, start := range starts {
		// Honest agreement never lowers Score.
		rep := start
		for i := 0; i < 5; i++ {
			next := Update(rep, true)
			if next.Score < rep.Score {
				t.Fatalf("start=%+v: Update(rep, true) lowered Score: %d -> %d", start, rep.Score, next.Score)
			}
			if next.Score < 0 {
				t.Fatalf("start=%+v: Score went below the zero floor: %d", start, next.Score)
			}
			rep = next
		}

		// A run of honest updates strictly raises Score until it hits the
		// cap, then holds there.
		rep = model.Reputation{}
		prev := rep.Score
		reachedCap := false
		for i := 0; i < 200; i++ {
			rep = Update(rep, true)
			if prev == maxScore {
				if rep.Score != maxScore {
					t.Fatalf("Score moved after reaching the cap: %d -> %d", prev, rep.Score)
				}
				reachedCap = true
			} else if rep.Score <= prev {
				t.Fatalf("honest update did not strictly raise Score below the cap: %d -> %d", prev, rep.Score)
			}
			prev = rep.Score
		}
		if !reachedCap {
			t.Fatalf("start=%+v: 200 honest updates never reached the cap %d (got %d)", start, maxScore, rep.Score)
		}

		// A lie never raises Score, and never pushes it below the zero
		// floor. Enough consecutive lies always settle Score at the floor.
		rep = start
		for i := 0; i < 50; i++ {
			next := Update(rep, false)
			if next.Score > rep.Score {
				t.Fatalf("start=%+v: Update(rep, false) raised Score: %d -> %d", start, rep.Score, next.Score)
			}
			if next.Score < 0 {
				t.Fatalf("start=%+v: lie pushed Score below the zero floor: %d", start, next.Score)
			}
			rep = next
		}
		if rep.Score != 0 {
			t.Fatalf("start=%+v: repeated lies did not settle at the zero floor, got %d", start, rep.Score)
		}
	}

	// Every fresh identity starts at the zero-value floor: a brand-new
	// Sybil's very first lie cannot leave it any worse off than a
	// long-lived liar already clamped to zero — and no better off either.
	fresh := model.Reputation{}
	liedDown := Update(model.Reputation{Score: 5, Observations: 1}, false)
	if fresh.Score != 0 {
		t.Fatalf("zero-value Reputation.Score = %d, want 0", fresh.Score)
	}
	if Update(fresh, false).Score != liedDown.Score {
		t.Fatalf("a fresh identity's first lie (%d) and a caught liar's floor (%d) diverge",
			Update(fresh, false).Score, liedDown.Score)
	}
}

func TestWeight(t *testing.T) {
	tests := []struct {
		name string
		rep  model.Reputation
		want float64
	}{
		{"zero value is minimal", model.Reputation{}, 0},
		{"half of cap", model.Reputation{Score: maxScore / 2}, 0.5},
		{"at cap", model.Reputation{Score: maxScore}, 1},
		{"beyond cap clamps to 1", model.Reputation{Score: maxScore * 2}, 1},
		{"negative clamps to zero-minimal", model.Reputation{Score: -10}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Weight(tt.rep)
			if got != tt.want {
				t.Errorf("Weight(%+v) = %v, want %v", tt.rep, got, tt.want)
			}
		})
	}
}

// TestWeightMonotonicNonDecreasing checks Weight is non-decreasing in Score
// across the full clamped range, always finite, and that the zero value is
// the minimum over that range.
func TestWeightMonotonicNonDecreasing(t *testing.T) {
	zero := Weight(model.Reputation{})

	prev := math.Inf(-1)
	for score := int64(0); score <= maxScore; score += 10 {
		rep := model.Reputation{Score: score}
		w := Weight(rep)

		if math.IsNaN(w) || math.IsInf(w, 0) {
			t.Fatalf("Weight(%+v) = %v, not finite", rep, w)
		}
		if w < prev {
			t.Fatalf("Weight not monotonic non-decreasing: score=%d weight=%v < previous %v", score, w, prev)
		}
		if w < zero {
			t.Fatalf("Weight(%+v) = %v, below the zero-value minimum %v", rep, w, zero)
		}
		prev = w
	}
}

func TestNeedsK(t *testing.T) {
	tests := []struct {
		name string
		rep  model.Reputation
		tier model.Tier
		want int
	}{
		{"fresh open identity gets the ceiling", model.Reputation{}, model.Open, openMaxK},
		{"fresh core identity gets the ceiling", model.Reputation{}, model.Core, coreMaxK},
		{"max-rep open identity gets the floor", model.Reputation{Score: maxScore}, model.Open, openMinK},
		{"max-rep core identity gets the floor", model.Reputation{Score: maxScore}, model.Core, coreMinK},
		{"one step earned lowers K by two", model.Reputation{Score: repStep}, model.Open, openMaxK - 2},
		{"negative score treated as zero", model.Reputation{Score: -50}, model.Open, openMaxK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NeedsK(tt.rep, tt.tier)
			if got != tt.want {
				t.Errorf("NeedsK(%+v, %v) = %d, want %d", tt.rep, tt.tier, got, tt.want)
			}
		})
	}
}

// TestNeedsKMonotonicAndValid is a property test over a dense sweep of
// Score values and both tiers: NeedsK is non-increasing as reputation rises,
// Open never needs fewer replicas than Core at the same reputation, and
// every returned K is a positive odd quorum size.
func TestNeedsKMonotonicAndValid(t *testing.T) {
	tiers := []model.Tier{model.Core, model.Open}

	for _, tier := range tiers {
		prevK := math.MaxInt64
		for score := int64(0); score <= maxScore+repStep; score += 5 {
			rep := model.Reputation{Score: score}
			k := NeedsK(rep, tier)

			if k <= 0 {
				t.Fatalf("tier=%v score=%d: NeedsK = %d, not positive", tier, score, k)
			}
			if k%2 == 0 {
				t.Fatalf("tier=%v score=%d: NeedsK = %d, not odd (a tie would be undecidable)", tier, score, k)
			}
			if k > prevK {
				t.Fatalf("tier=%v score=%d: NeedsK rose from %d to %d as reputation increased", tier, score, prevK, k)
			}
			prevK = k
		}
	}

	// Open never needs fewer replicas than Core at the same reputation.
	for score := int64(-10); score <= maxScore+repStep; score += 5 {
		rep := model.Reputation{Score: score}
		coreK := NeedsK(rep, model.Core)
		openK := NeedsK(rep, model.Open)
		if openK < coreK {
			t.Fatalf("score=%d: Open K=%d < Core K=%d, Open must never need fewer replicas", score, openK, coreK)
		}
	}
}

// TestDeterministic asserts all three functions are pure: identical inputs
// always produce identical outputs, repeatedly and regardless of call order.
func TestDeterministic(t *testing.T) {
	reps := []model.Reputation{
		{},
		{Score: 5, Observations: 1},
		{Score: 300, Observations: 40},
		{Score: maxScore, Observations: 9999},
	}
	tiers := []model.Tier{model.Core, model.Open}

	for _, rep := range reps {
		for _, agreed := range []bool{true, false} {
			first := Update(rep, agreed)
			for i := 0; i < 10; i++ {
				got := Update(rep, agreed)
				if got != first {
					t.Fatalf("Update(%+v, %v) not deterministic: %+v vs %+v", rep, agreed, first, got)
				}
			}
		}

		wFirst := Weight(rep)
		for i := 0; i < 10; i++ {
			if got := Weight(rep); got != wFirst {
				t.Fatalf("Weight(%+v) not deterministic: %v vs %v", rep, wFirst, got)
			}
		}

		for _, tier := range tiers {
			kFirst := NeedsK(rep, tier)
			for i := 0; i < 10; i++ {
				if got := NeedsK(rep, tier); got != kFirst {
					t.Fatalf("NeedsK(%+v, %v) not deterministic: %d vs %d", rep, tier, kFirst, got)
				}
			}
		}
	}
}

// TestEligible is a table-driven test of the Eligible boundary: frozen iff
// Observations >= minObservations AND Score <= freezeFloor, with the exact
// boundaries pinned.
func TestEligible(t *testing.T) {
	tests := []struct {
		name string
		rep  model.Reputation
		want bool
	}{
		{"zero value is fresh, eligible", model.Reputation{}, true},
		{"fresh with a few observations, still eligible", model.Reputation{Observations: minObservations - 1, Score: 0}, true},
		{"observations just below floor, eligible regardless of score", model.Reputation{Observations: minObservations - 1, Score: maxScore}, true},
		{"observations at floor, score at freeze floor: frozen", model.Reputation{Observations: minObservations, Score: freezeFloor}, false},
		{"observations at floor, score just above freeze floor: eligible", model.Reputation{Observations: minObservations, Score: freezeFloor + 1}, true},
		{"observations well past floor, score at freeze floor: frozen", model.Reputation{Observations: minObservations + 100, Score: freezeFloor}, false},
		{"observations well past floor, high score: eligible", model.Reputation{Observations: minObservations + 100, Score: maxScore}, true},
		{"observations at floor, high score: eligible", model.Reputation{Observations: minObservations, Score: maxScore}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Eligible(tt.rep)
			if got != tt.want {
				t.Errorf("Eligible(%+v) = %v, want %v", tt.rep, got, tt.want)
			}
		})
	}
}

// TestEligibleFreezeFloorWithoutBreakingZeroStart is the "freeze floor
// without breaking zero-start" property: a fresh identity (few Observations)
// stays eligible no matter how low its Score, a chronic loser (enough
// Observations, Score clamped to the floor) is frozen, and a well-behaved
// identity (enough Observations, high Score) stays eligible. Together these
// pin the shape #03 of the phase doc names — a soft-freeze that never
// hard-boots an unlucky-but-honest node and never relies on negative scores.
func TestEligibleFreezeFloorWithoutBreakingZeroStart(t *testing.T) {
	t.Run("freshIdentityEligible", func(t *testing.T) {
		for obs := 0; obs < minObservations; obs++ {
			rep := model.Reputation{Observations: obs, Score: 0}
			if !Eligible(rep) {
				t.Fatalf("fresh identity %+v not eligible, zero-start broken", rep)
			}
		}
	})

	t.Run("chronicLoserFrozen", func(t *testing.T) {
		for obs := minObservations; obs <= minObservations+50; obs++ {
			rep := model.Reputation{Observations: obs, Score: freezeFloor}
			if Eligible(rep) {
				t.Fatalf("chronic loser %+v eligible, want frozen", rep)
			}
		}
	})

	t.Run("wellBehavedEligible", func(t *testing.T) {
		for obs := minObservations; obs <= minObservations+50; obs++ {
			rep := model.Reputation{Observations: obs, Score: maxScore}
			if !Eligible(rep) {
				t.Fatalf("well-behaved identity %+v not eligible", rep)
			}
		}
	})

	t.Run("freshLiarStillEligible", func(t *testing.T) {
		// A would-be Sybil/liar clamped to the freeze floor but with too
		// few Observations to be judged yet is still eligible — it can't
		// be frozen before it has participated, but it also earns nothing
		// by re-minting, since the floor is the same one a chronic liar
		// lands on.
		for obs := 0; obs < minObservations; obs++ {
			rep := model.Reputation{Observations: obs, Score: freezeFloor}
			if !Eligible(rep) {
				t.Fatalf("fresh liar %+v not eligible, zero-start broken", rep)
			}
		}
	})

	t.Run("updateNeverNegative", func(t *testing.T) {
		reps := []model.Reputation{
			{},
			{Score: 1, Observations: 1},
			{Score: freezeFloor, Observations: minObservations},
			{Score: maxScore, Observations: 500},
		}
		for _, rep := range reps {
			for _, agreed := range []bool{true, false} {
				if got := Update(rep, agreed).Score; got < 0 {
					t.Fatalf("Update(%+v, %v).Score = %d, went negative — freeze must rely on Observations, not negatives", rep, agreed, got)
				}
			}
		}
	})

	t.Run("eligibleDeterministic", func(t *testing.T) {
		reps := []model.Reputation{
			{},
			{Observations: minObservations - 1, Score: 0},
			{Observations: minObservations, Score: freezeFloor},
			{Observations: minObservations, Score: maxScore},
		}
		for _, rep := range reps {
			first := Eligible(rep)
			for i := 0; i < 10; i++ {
				if got := Eligible(rep); got != first {
					t.Fatalf("Eligible(%+v) not deterministic: %v vs %v", rep, first, got)
				}
			}
		}
	})
}

// TestFreshSybilAndChronicLiarClampToSameScore re-derives the guard the
// ticket calls out: because Update never lets Score go negative, a fresh
// identity's first lie and a long-lived chronic liar's Score both settle at
// the exact same floor — freezeFloor — so re-minting a fresh identity to
// escape a freeze gains nothing on Score (only a brief eligible window
// before its own Observations climb past minObservations).
func TestFreshSybilAndChronicLiarClampToSameScore(t *testing.T) {
	freshSybil := model.Reputation{}
	for i := 0; i < 20; i++ {
		freshSybil = Update(freshSybil, false)
	}

	chronicLiar := model.Reputation{Score: maxScore, Observations: 1000}
	for i := 0; i < 20; i++ {
		chronicLiar = Update(chronicLiar, false)
	}

	if freshSybil.Score != freezeFloor {
		t.Fatalf("fresh sybil's Score = %d after repeated lies, want freezeFloor %d", freshSybil.Score, freezeFloor)
	}
	if chronicLiar.Score != freezeFloor {
		t.Fatalf("chronic liar's Score = %d after repeated lies, want freezeFloor %d", chronicLiar.Score, freezeFloor)
	}
	if freshSybil.Score != chronicLiar.Score {
		t.Fatalf("fresh sybil (%d) and chronic liar (%d) did not clamp to the same score", freshSybil.Score, chronicLiar.Score)
	}
}
