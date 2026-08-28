package detection

import (
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// -----------------------------------------------------------------------
// Deadline
// -----------------------------------------------------------------------

func TestDeadline(t *testing.T) {
	tests := []struct {
		name string
		tier model.Tier
		c    model.Coupling
		want model.Duration
	}{
		{"core barrier", model.Core, model.Barrier, coreBarrier},
		{"core leader", model.Core, model.Leader, coreLeader},
		{"core message passing", model.Core, model.MessagePassing, coreMessagePassing},
		{"core independent", model.Core, model.Independent, coreIndependent},
		{"open barrier", model.Open, model.Barrier, openBarrier},
		{"open leader", model.Open, model.Leader, openLeader},
		{"open message passing", model.Open, model.MessagePassing, openMessagePassing},
		{"open independent", model.Open, model.Independent, openIndependent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Deadline(tt.tier, tt.c); got != tt.want {
				t.Fatalf("Deadline(%v, %v) = %v, want %v", tt.tier, tt.c, got, tt.want)
			}
		})
	}
}

// TestDeadlineHeadline is the acceptance criterion's named property: a
// core-tier barrier deadline is on the order of seconds, strictly less than
// the open-tier independent deadline, which is on the order of tens of
// seconds — "a barrier straggler is evicted in seconds" is provable with no
// cluster.
func TestDeadlineHeadline(t *testing.T) {
	fast := Deadline(model.Core, model.Barrier)
	patient := Deadline(model.Open, model.Independent)

	if fast <= 0 || fast > 10*second {
		t.Fatalf("Deadline(Core, Barrier) = %v, want a positive, seconds-scale duration", fast)
	}
	if patient < 10*second || patient > 100*second {
		t.Fatalf("Deadline(Open, Independent) = %v, want a tens-of-seconds-scale duration", patient)
	}
	if fast >= patient {
		t.Fatalf("Deadline(Core, Barrier) = %v is not strictly less than Deadline(Open, Independent) = %v", fast, patient)
	}
}

// TestDeadlineEveryCombinationIsPositive asserts every Tier x Coupling
// combination returns a positive, documented duration — no zero-value gaps
// in the table that would make a member instantly "dead".
func TestDeadlineEveryCombinationIsPositive(t *testing.T) {
	tiers := []model.Tier{model.Core, model.Open}
	couplings := []model.Coupling{model.Independent, model.Barrier, model.Leader, model.MessagePassing}

	for _, tier := range tiers {
		for _, c := range couplings {
			if got := Deadline(tier, c); got <= 0 {
				t.Fatalf("Deadline(%v, %v) = %v, want > 0", tier, c, got)
			}
		}
	}
}

// TestDeadlineOrdering is the acceptance criterion's matrix ordering: within
// a tier, tighter coupling never yields a longer deadline than looser
// coupling, and every Core-tier deadline is strictly less than every
// Open-tier deadline for the same coupling.
func TestDeadlineOrdering(t *testing.T) {
	// Tightest-to-loosest, per the documented rule.
	tightestToLoosest := []model.Coupling{model.Barrier, model.Leader, model.MessagePassing, model.Independent}

	for _, tier := range []model.Tier{model.Core, model.Open} {
		for i := 0; i+1 < len(tightestToLoosest); i++ {
			tighter := Deadline(tier, tightestToLoosest[i])
			looser := Deadline(tier, tightestToLoosest[i+1])
			if tighter >= looser {
				t.Fatalf("tier %v: Deadline(%v)=%v is not strictly less than Deadline(%v)=%v",
					tier, tightestToLoosest[i], tighter, tightestToLoosest[i+1], looser)
			}
		}
	}

	for _, c := range tightestToLoosest {
		core := Deadline(model.Core, c)
		open := Deadline(model.Open, c)
		if core >= open {
			t.Fatalf("coupling %v: Deadline(Core)=%v is not strictly less than Deadline(Open)=%v", c, core, open)
		}
	}
}

// TestDeadlineUnknownFallsBackToPatient guards the documented fallback: an
// unrecognized Tier or Coupling value never yields a zero-length deadline —
// it falls back to the most patient (Open, Independent) entry.
func TestDeadlineUnknownFallsBackToPatient(t *testing.T) {
	unknownTier := model.Tier(99)
	unknownCoupling := model.Coupling(99)

	if got := Deadline(unknownTier, model.Independent); got != openIndependent {
		t.Fatalf("Deadline(unknown tier, Independent) = %v, want fallback %v", got, openIndependent)
	}
	if got := Deadline(model.Open, unknownCoupling); got != openIndependent {
		t.Fatalf("Deadline(Open, unknown coupling) = %v, want fallback %v", got, openIndependent)
	}
	if got := Deadline(model.Core, unknownCoupling); got != coreIndependent {
		t.Fatalf("Deadline(Core, unknown coupling) = %v, want fallback %v", got, coreIndependent)
	}
}

// TestDeadlineIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestDeadlineIsDeterministic(t *testing.T) {
	first := Deadline(model.Core, model.Barrier)
	for i := 0; i < 100; i++ {
		if got := Deadline(model.Core, model.Barrier); got != first {
			t.Fatalf("non-deterministic output on run %d: %v vs %v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// IsDead
// -----------------------------------------------------------------------

func TestIsDead(t *testing.T) {
	tests := []struct {
		name     string
		lastSeen model.Instant
		dl       model.Instant
		now      model.Instant
		want     bool
	}{
		{"now well before deadline: alive", 0, 100, 50, false},
		{"now exactly at deadline: alive (exclusive boundary)", 0, 100, 100, false},
		{"now one tick past deadline: dead", 0, 100, 101, true},
		{"now well past deadline: dead", 0, 100, 1_000, true},
		{"deadline in the past relative to lastSeen, now still before it: alive", 50, 60, 55, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDead(tt.lastSeen, tt.dl, tt.now); got != tt.want {
				t.Fatalf("IsDead(%v, %v, %v) = %v, want %v", tt.lastSeen, tt.dl, tt.now, got, tt.want)
			}
		})
	}
}

// TestIsDeadBoundaryIsExact property-tests the exclusive boundary across a
// spread of deadline instants: now == dl is always alive, now == dl+1 is
// always dead.
func TestIsDeadBoundaryIsExact(t *testing.T) {
	for dl := model.Instant(-1000); dl <= 1000; dl += 137 {
		if IsDead(0, dl, dl) {
			t.Fatalf("IsDead(0, %v, %v) = true, want false at the exact boundary", dl, dl)
		}
		if !IsDead(0, dl, dl+1) {
			t.Fatalf("IsDead(0, %v, %v) = false, want true one tick past the boundary", dl, dl+1)
		}
	}
}

// TestIsDeadIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestIsDeadIsDeterministic(t *testing.T) {
	first := IsDead(0, 100, 150)
	for i := 0; i < 100; i++ {
		if got := IsDead(0, 100, 150); got != first {
			t.Fatalf("non-deterministic output on run %d: %v vs %v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// Straggler property: barrier deadline expires within the deadline window
// -----------------------------------------------------------------------

// TestBarrierStragglerDeclaredDeadInSeconds is the ticket's named property:
// a barrier straggler on the core tier is declared dead in seconds — feed
// the shell's own composition (dl = lastSeen + Deadline) through IsDead and
// confirm it flips exactly at the seconds-scale boundary Deadline set, with
// no cluster involved.
func TestBarrierStragglerDeclaredDeadInSeconds(t *testing.T) {
	lastSeen := model.Instant(1_000)
	dl := lastSeen + model.Instant(Deadline(model.Core, model.Barrier))

	if IsDead(lastSeen, dl, dl) {
		t.Fatalf("straggler declared dead exactly at its deadline instant, want alive until past it")
	}
	if !IsDead(lastSeen, dl, dl+1) {
		t.Fatalf("straggler not declared dead one tick past its deadline instant")
	}

	elapsed := model.Duration(dl - lastSeen)
	if elapsed <= 0 || elapsed > 10*second {
		t.Fatalf("core-tier barrier straggler deadline elapsed = %v, want a positive, seconds-scale span", elapsed)
	}
}
