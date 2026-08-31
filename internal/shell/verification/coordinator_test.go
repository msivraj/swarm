package verification

import (
	"context"
	"testing"

	coreverification "github.com/msivraj/swarm/internal/core/verification"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/enrollment"
	"github.com/msivraj/swarm/internal/shell/reputation"
)

// idSet turns a slice of MachineID into a set for order-independent
// comparison — concurrent dispatch goroutines land in FakeDispatcher.Calls
// in a nondeterministic order within a round.
func idSet(ids []model.MachineID) map[model.MachineID]struct{} {
	set := make(map[model.MachineID]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func setsEqual(a, b map[model.MachineID]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if _, ok := b[id]; !ok {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------
// K=5, 3 honest + 2 liars => Agreed on the honest value; reputation moves.
// -----------------------------------------------------------------------

func TestVerify_HonestMajorityAcceptedAndReputationUpdated(t *testing.T) {
	pool := []model.MachineID{"m1", "m2", "m3", "m4", "m5"}
	honestValue := []byte("the-true-answer")

	disp := NewFakeDispatcher()
	disp.Honest("m1", honestValue)
	disp.Honest("m2", honestValue)
	disp.Honest("m3", honestValue)
	disp.Lying("m4", []byte("wrong-a"))
	disp.Lying("m5", []byte("wrong-b"))

	repStore := reputation.NewMemStore()
	// A requester at Score 400 needs exactly K=5 at the Open tier (see
	// internal/core/reputation.NeedsK: openMaxK=9, repStep=200, two steps
	// earned drops K by 4, to 5).
	const requester = model.SpiffeID("req-1")
	repStore.Put(requester, model.Reputation{Score: 400})

	// Give every machine a nonzero baseline so a lie's fall is visibly
	// distinct from a fresh (already-zero) floor.
	for _, m := range pool {
		repStore.Put(identityOf(m), model.Reputation{Score: 50, Observations: 2})
	}

	c := New(Config{
		Dispatcher:  disp,
		Reputation:  repStore,
		Clock:       NewFakeClock(0),
		Timeout:     1000,
		MaxAttempts: 1,
	})

	task := model.Task{ID: "t1"}
	v, err := c.Verify(context.Background(), task, model.Open, requester, pool, 1)
	if err != nil {
		t.Fatalf("Verify returned error %v, want nil", err)
	}
	if v.Kind != model.Agreed {
		t.Fatalf("Verify Kind = %v, want Agreed", v.Kind)
	}
	if string(v.Value) != string(honestValue) {
		t.Fatalf("Verify Value = %q, want %q", v.Value, honestValue)
	}

	// Honest machines rose; liars fell.
	for _, m := range []model.MachineID{"m1", "m2", "m3"} {
		got := repStore.Get(identityOf(m))
		if got.Score != 60 {
			t.Errorf("honest machine %s Score = %d, want 60 (50 + honestGain)", m, got.Score)
		}
		if got.Observations != 3 {
			t.Errorf("honest machine %s Observations = %d, want 3", m, got.Observations)
		}
	}
	for _, m := range []model.MachineID{"m4", "m5"} {
		got := repStore.Get(identityOf(m))
		if got.Score != 0 {
			t.Errorf("lying machine %s Score = %d, want 0 (50 - liePenalty, clamped)", m, got.Score)
		}
		if got.Observations != 3 {
			t.Errorf("lying machine %s Observations = %d, want 3", m, got.Observations)
		}
	}
}

// -----------------------------------------------------------------------
// Disputed => re-assign with a fresh seed; the second K-set differs.
// -----------------------------------------------------------------------

func TestVerify_DisputedRetriesWithFreshSeedDifferentKSet(t *testing.T) {
	pool := []model.MachineID{"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"}

	disp := NewFakeDispatcher()
	// Every machine claims a mutually distinct value, so any 3-machine
	// K-set (round 1 or round 2) is Disputed: no value reaches a strict
	// majority of 3.
	for i, m := range pool {
		disp.Honest(m, []byte{byte('a' + i)})
	}

	const requester = model.SpiffeID("req-1")
	repStore := reputation.NewMemStore()
	// Max reputation floors K to openMinK == 3.
	repStore.Put(requester, model.Reputation{Score: 1000})

	c := New(Config{
		Dispatcher:  disp,
		Reputation:  repStore,
		Clock:       NewFakeClock(0),
		Timeout:     1000,
		MaxAttempts: 2,
	})

	task := model.Task{ID: "t1"}
	const baseSeed = uint64(1)
	v, err := c.Verify(context.Background(), task, model.Open, requester, pool, baseSeed)
	if err != ErrNoQuorum {
		t.Fatalf("Verify err = %v, want ErrNoQuorum (both rounds Disputed)", err)
	}
	if v.Kind != model.Disputed {
		t.Fatalf("last Verdict Kind = %v, want Disputed", v.Kind)
	}

	// Independently compute the K-set Assign would choose for each
	// attempt's seed, over the same (task, pool, k) Verify used.
	k := coreverification.Redundancy(model.Open, model.Reputation{Score: 1000})
	wantRound1 := idSet(coreverification.Assign(task.ID, pool, k, baseSeed+0))
	wantRound2 := idSet(coreverification.Assign(task.ID, pool, k, baseSeed+1))
	if setsEqual(wantRound1, wantRound2) {
		t.Fatalf("test setup: round1/round2 K-sets coincide (%v), fix the seeds/pool so they differ", wantRound1)
	}

	calls := disp.Calls()
	if len(calls) != 2*k {
		t.Fatalf("dispatcher saw %d calls, want %d (two rounds of K=%d)", len(calls), 2*k, k)
	}
	gotRound1 := idSet(calls[:k])
	gotRound2 := idSet(calls[k:])

	if !setsEqual(gotRound1, wantRound1) {
		t.Errorf("round1 dispatched to %v, want %v (Assign with seed %d)", gotRound1, wantRound1, baseSeed)
	}
	if !setsEqual(gotRound2, wantRound2) {
		t.Errorf("round2 dispatched to %v, want %v (Assign with seed %d)", gotRound2, wantRound2, baseSeed+1)
	}
	if setsEqual(gotRound1, gotRound2) {
		t.Errorf("round2 K-set (%v) equals round1's (%v) — a fresh seed did not change the assignment", gotRound2, gotRound1)
	}
}

// -----------------------------------------------------------------------
// Minimum-response floor: most of K time out => not accepted as Agreed.
// -----------------------------------------------------------------------

func TestVerify_MinimumResponseFloorRejectsLoneResponse(t *testing.T) {
	pool := []model.MachineID{"m1", "m2", "m3", "m4", "m5"}
	honestValue := []byte("the-true-answer")

	disp := NewFakeDispatcher()
	// Only one of the five assigned machines ever answers; the rest hang
	// until the round's context is canceled. Verdict would trivially call
	// this Agreed-of-one on the lone response alone — the floor must
	// override that.
	disp.Honest("m1", honestValue)
	disp.Hanging("m2")
	disp.Hanging("m3")
	disp.Hanging("m4")
	disp.Hanging("m5")

	const requester = model.SpiffeID("req-1")
	repStore := reputation.NewMemStore()
	repStore.Put(requester, model.Reputation{Score: 400}) // K == 5

	clock := NewFakeClock(0)
	const roundTimeout = model.Duration(1000)
	const maxAttempts = 2

	c := New(Config{
		Dispatcher:  disp,
		Reputation:  repStore,
		Clock:       clock,
		Timeout:     roundTimeout,
		MaxAttempts: maxAttempts,
	})

	task := model.Task{ID: "t1"}
	type outcome struct {
		v   model.Verdict
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		v, err := c.Verify(context.Background(), task, model.Open, requester, pool, 1)
		done <- outcome{v, err}
	}()

	// Fire every round's collection deadline without any real sleep: each
	// round registers exactly one Clock.After waiter, which BlockUntil(1)
	// waits for before the test advances the clock past it.
	for i := 0; i < maxAttempts; i++ {
		clock.BlockUntil(1)
		clock.Advance(roundTimeout)
	}

	got := <-done
	if got.err != ErrNoQuorum {
		t.Fatalf("Verify err = %v, want ErrNoQuorum (the lone response never met the floor)", got.err)
	}
	if got.v.Kind == model.Agreed {
		t.Fatalf("Verify accepted Agreed(%q) from a single response out of K=5 — the minimum-response floor was not enforced", got.v.Value)
	}
}

// -----------------------------------------------------------------------
// Blacklisted machines are excluded from the K-set.
// -----------------------------------------------------------------------

func TestVerify_BlacklistedMachinesExcluded(t *testing.T) {
	pool := []model.MachineID{"m1", "m2", "m3", "m4", "m5"}
	honestValue := []byte("the-true-answer")

	disp := NewFakeDispatcher()
	disp.Honest("m1", honestValue)
	disp.Honest("m2", honestValue)
	// m3 is blacklisted and deliberately left unconfigured: if the
	// coordinator ever dispatched to it, Dispatch would return an error
	// for lack of a configured behavior.
	disp.Honest("m4", honestValue)
	disp.Honest("m5", honestValue)

	blacklist := enrollment.NewFakeBlacklist(identityOf("m3"))

	const requester = model.SpiffeID("req-1")
	repStore := reputation.NewMemStore() // zero-value reputation => K = openMaxK = 9, clamped to the eligible pool.

	c := New(Config{
		Dispatcher:  disp,
		Reputation:  repStore,
		Blacklist:   blacklist,
		Clock:       NewFakeClock(0),
		Timeout:     1000,
		MaxAttempts: 1,
	})

	task := model.Task{ID: "t1"}
	v, err := c.Verify(context.Background(), task, model.Open, requester, pool, 1)
	if err != nil {
		t.Fatalf("Verify returned error %v, want nil", err)
	}
	if v.Kind != model.Agreed {
		t.Fatalf("Verify Kind = %v, want Agreed", v.Kind)
	}

	for _, m := range disp.Calls() {
		if m == "m3" {
			t.Fatalf("dispatcher was called for blacklisted machine m3: calls=%v", disp.Calls())
		}
	}
	if len(disp.Calls()) != 4 {
		t.Fatalf("dispatcher saw %d calls, want 4 (the 5-machine pool minus the one blacklisted machine)", len(disp.Calls()))
	}
}

// -----------------------------------------------------------------------
// Every machine blacklisted => ErrEmptyPool, no dispatch at all.
// -----------------------------------------------------------------------

func TestVerify_AllBlacklistedReturnsErrEmptyPool(t *testing.T) {
	pool := []model.MachineID{"m1", "m2"}
	disp := NewFakeDispatcher()
	blacklist := enrollment.NewFakeBlacklist(identityOf("m1"), identityOf("m2"))

	c := New(Config{
		Dispatcher: disp,
		Blacklist:  blacklist,
		Clock:      NewFakeClock(0),
		Timeout:    1000,
	})

	_, err := c.Verify(context.Background(), model.Task{ID: "t1"}, model.Open, "req-1", pool, 1)
	if err != ErrEmptyPool {
		t.Fatalf("Verify err = %v, want ErrEmptyPool", err)
	}
	if calls := disp.Calls(); len(calls) != 0 {
		t.Fatalf("dispatcher was called (%v) with an entirely blacklisted pool", calls)
	}
}

// -----------------------------------------------------------------------
// Config validation.
// -----------------------------------------------------------------------

func TestVerify_MissingDispatcherReturnsError(t *testing.T) {
	c := New(Config{Clock: NewFakeClock(0)})
	_, err := c.Verify(context.Background(), model.Task{ID: "t1"}, model.Open, "req-1", []model.MachineID{"m1"}, 1)
	if err != ErrNoDispatcher {
		t.Fatalf("Verify err = %v, want ErrNoDispatcher", err)
	}
}

func TestVerify_MissingClockReturnsError(t *testing.T) {
	c := New(Config{Dispatcher: NewFakeDispatcher()})
	_, err := c.Verify(context.Background(), model.Task{ID: "t1"}, model.Open, "req-1", []model.MachineID{"m1"}, 1)
	if err != ErrNoClock {
		t.Fatalf("Verify err = %v, want ErrNoClock", err)
	}
}

// TestVerify_NilReputationSkipsSizingAndRecording exercises the two nil
// Config.Reputation branches: sizing falls back to the zero-value
// Reputation (max redundancy) and, since a nil store has nowhere to write,
// recordVerdict is a no-op rather than a panic.
func TestVerify_NilReputationSkipsSizingAndRecording(t *testing.T) {
	pool := []model.MachineID{"m1", "m2", "m3"}
	honestValue := []byte("v")

	disp := NewFakeDispatcher()
	disp.Honest("m1", honestValue)
	disp.Honest("m2", honestValue)
	disp.Honest("m3", honestValue)

	c := New(Config{
		Dispatcher:  disp,
		Clock:       NewFakeClock(0),
		Timeout:     1000,
		MaxAttempts: 1,
	})

	v, err := c.Verify(context.Background(), model.Task{ID: "t1"}, model.Open, "req-1", pool, 1)
	if err != nil {
		t.Fatalf("Verify returned error %v, want nil", err)
	}
	if v.Kind != model.Agreed {
		t.Fatalf("Verify Kind = %v, want Agreed", v.Kind)
	}
}
