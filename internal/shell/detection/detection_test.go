package detection

import (
	"sync"
	"testing"
	"time"

	coredetection "github.com/msivraj/swarm/internal/core/detection"
	"github.com/msivraj/swarm/internal/model"
)

// fakeClock is a controllable clock for deterministic Detector.Run tests: no
// wall-clock sleeps drive the eviction *decision*, only the ticker's cadence
// is real time.
type fakeClock struct {
	mu  sync.Mutex
	now model.Instant
}

func (c *fakeClock) Now() model.Instant {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(at model.Instant) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
}

// -----------------------------------------------------------------------
// Seen / Register bookkeeping
// -----------------------------------------------------------------------

func TestHeartbeatingMemberNeverEvicted(t *testing.T) {
	var evictions []Member
	d := New(func() model.Instant { return 0 }, func(m Member, at model.Instant) {
		evictions = append(evictions, m)
	})

	dl := coredetection.Deadline(model.Core, model.Barrier)
	d.Register("m1", model.Core, model.Barrier, 0)

	// Heartbeat well inside every deadline window, many windows in a row.
	step := model.Instant(dl) / 2
	now := model.Instant(0)
	for i := 0; i < 20; i++ {
		now += step
		d.Seen("m1", now)
		if got := d.Sweep(now); len(got) != 0 {
			t.Fatalf("Sweep at tick %d evicted %v, want none (heartbeat kept it alive)", i, got)
		}
	}

	if len(evictions) != 0 {
		t.Fatalf("evictions = %v, want none for a member that kept heartbeating", evictions)
	}
	if dead := d.Dead(now); len(dead) != 0 {
		t.Fatalf("Dead(%v) = %v, want none", now, dead)
	}
}

func TestSilentMemberEvictedAfterDeadline(t *testing.T) {
	d := New(func() model.Instant { return 0 }, nil)
	d.Register("m1", model.Core, model.Barrier, 0)

	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))

	if got := d.Sweep(dl); len(got) != 0 {
		t.Fatalf("Sweep(dl) = %v, want none evicted at the exact deadline instant (exclusive boundary)", got)
	}
	if got := d.Dead(dl); len(got) != 0 {
		t.Fatalf("Dead(dl) = %v, want none at the exact deadline instant", got)
	}

	got := d.Sweep(dl + 1)
	if len(got) != 1 || got[0] != "m1" {
		t.Fatalf("Sweep(dl+1) = %v, want [m1] evicted one tick past its deadline", got)
	}
}

func TestResumedHeartbeatCancelsEviction(t *testing.T) {
	d := New(func() model.Instant { return 0 }, nil)
	d.Register("m1", model.Core, model.Barrier, 0)

	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))

	// Heartbeat arrives just before the deadline elapses.
	d.Seen("m1", dl-1)
	if got := d.Sweep(dl - 1 + dl); len(got) != 0 {
		// New deadline is (dl-1)+dl-worth of budget from the refreshed
		// lastSeen, so this instant is still well within it.
		t.Fatalf("Sweep after refreshed heartbeat = %v, want none evicted", got)
	}
}

func TestSeenAfterEvictionUnEvicts(t *testing.T) {
	d := New(func() model.Instant { return 0 }, nil)
	d.Register("m1", model.Core, model.Barrier, 0)
	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))

	if got := d.Sweep(dl + 1); len(got) != 1 {
		t.Fatalf("Sweep(dl+1) = %v, want m1 evicted", got)
	}

	// m1 comes back to life.
	d.Seen("m1", dl+1)

	if got := d.Sweep(dl + 2); len(got) != 0 {
		t.Fatalf("Sweep after resumed heartbeat = %v, want no re-eviction", got)
	}
	if dead := d.Dead(dl + 2); len(dead) != 0 {
		t.Fatalf("Dead after resumed heartbeat = %v, want none", dead)
	}
}

func TestSeenOnUnregisteredMemberIsNoop(t *testing.T) {
	d := New(func() model.Instant { return 0 }, nil)
	d.Seen("ghost", 100)

	if got := d.Dead(1_000_000); len(got) != 0 {
		t.Fatalf("Dead() = %v, want none — Seen on an unregistered member must not start tracking it", got)
	}
}

func TestForgetStopsTracking(t *testing.T) {
	var evictions int
	d := New(func() model.Instant { return 0 }, func(Member, model.Instant) { evictions++ })
	d.Register("m1", model.Core, model.Barrier, 0)
	d.Forget("m1")

	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))
	if got := d.Sweep(dl + 100); len(got) != 0 {
		t.Fatalf("Sweep after Forget = %v, want none — forgotten members are not evaluated", got)
	}
	if evictions != 0 {
		t.Fatalf("evictions = %d, want 0 for a forgotten member", evictions)
	}
}

// -----------------------------------------------------------------------
// Eviction fires exactly once
// -----------------------------------------------------------------------

func TestEvictionFiresExactlyOnce(t *testing.T) {
	var mu sync.Mutex
	var evictions []Member
	d := New(func() model.Instant { return 0 }, func(m Member, at model.Instant) {
		mu.Lock()
		defer mu.Unlock()
		evictions = append(evictions, m)
	})
	d.Register("m1", model.Core, model.Barrier, 0)

	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))

	// Repeated sweeps well past the deadline must only fire the callback once.
	for i := 0; i < 5; i++ {
		d.Sweep(dl + 1 + model.Instant(i))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(evictions) != 1 || evictions[0] != "m1" {
		t.Fatalf("evictions = %v, want exactly one eviction of m1", evictions)
	}
}

// -----------------------------------------------------------------------
// Adaptive-by-tier: barrier straggler vs. independent member (real core)
// -----------------------------------------------------------------------

func TestAdaptiveDeadlineDiffersByTier(t *testing.T) {
	var mu sync.Mutex
	evicted := map[Member]model.Instant{}
	d := New(func() model.Instant { return 0 }, func(m Member, at model.Instant) {
		mu.Lock()
		defer mu.Unlock()
		evicted[m] = at
	})

	start := model.Instant(0)
	d.Register("straggler", model.Core, model.Barrier, start)
	d.Register("independent", model.Open, model.Independent, start)

	coreDL := model.Instant(coredetection.Deadline(model.Core, model.Barrier))
	openDL := model.Instant(coredetection.Deadline(model.Open, model.Independent))
	if coreDL >= openDL {
		t.Fatalf("test setup: core barrier deadline %v must be strictly shorter than open independent deadline %v", coreDL, openDL)
	}

	// Just past the core-tier barrier deadline: the straggler is evicted,
	// the open-tier independent member is not — it hasn't hit its deadline.
	d.Sweep(coreDL + 1)

	mu.Lock()
	_, stragglerEvicted := evicted["straggler"]
	_, independentEvictedEarly := evicted["independent"]
	mu.Unlock()

	if !stragglerEvicted {
		t.Fatalf("core-tier barrier straggler was not evicted at coreDL+1 (%v)", coreDL+1)
	}
	if independentEvictedEarly {
		t.Fatalf("open-tier independent member was evicted at coreDL+1 (%v), before its own deadline (%v)", coreDL+1, openDL)
	}

	// Advance past the open-tier deadline: now it is evicted too.
	d.Sweep(openDL + 1)

	mu.Lock()
	_, independentEvicted := evicted["independent"]
	mu.Unlock()

	if !independentEvicted {
		t.Fatalf("open-tier independent member was not evicted at openDL+1 (%v)", openDL+1)
	}
}

// -----------------------------------------------------------------------
// Dead() is a pure query
// -----------------------------------------------------------------------

func TestDeadIsPureQuery(t *testing.T) {
	var evictions int
	d := New(func() model.Instant { return 0 }, func(Member, model.Instant) { evictions++ })
	d.Register("m1", model.Core, model.Barrier, 0)

	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))

	for i := 0; i < 5; i++ {
		dead := d.Dead(dl + 1)
		if len(dead) != 1 || dead[0] != "m1" {
			t.Fatalf("Dead(dl+1) call %d = %v, want [m1] every time", i, dead)
		}
	}

	if evictions != 0 {
		t.Fatalf("evictions = %d, want 0 — Dead must never invoke onEvict", evictions)
	}

	// A subsequent Sweep must still fire exactly once, unaffected by the
	// repeated Dead polling above.
	got := d.Sweep(dl + 1)
	if len(got) != 1 || got[0] != "m1" {
		t.Fatalf("Sweep(dl+1) after Dead polling = %v, want [m1]", got)
	}
	if evictions != 1 {
		t.Fatalf("evictions = %d, want 1 after the first Sweep", evictions)
	}
}

// -----------------------------------------------------------------------
// Determinism
// -----------------------------------------------------------------------

func TestSweepIsDeterministic(t *testing.T) {
	build := func() *Detector {
		d := New(func() model.Instant { return 0 }, nil)
		d.Register("m1", model.Core, model.Barrier, 0)
		d.Register("m2", model.Open, model.Independent, 0)
		return d
	}

	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))

	first := build()
	firstResult := first.Sweep(dl + 1)

	for i := 0; i < 50; i++ {
		d := build()
		got := d.Sweep(dl + 1)
		if len(got) != len(firstResult) {
			t.Fatalf("run %d: Sweep = %v, want same shape as %v", i, got, firstResult)
		}
		for j := range got {
			if got[j] != firstResult[j] {
				t.Fatalf("run %d: Sweep = %v, want %v", i, got, firstResult)
			}
		}
	}
}

// -----------------------------------------------------------------------
// Run: ticker-driven loop reads the injected clock, not wall-clock, for the
// decision itself. The ticker's cadence is real time (that's what a timer
// loop is); the *decision* is entirely governed by the fake clock.
// -----------------------------------------------------------------------

func TestRunEvictsUsingInjectedClockNotWallClock(t *testing.T) {
	clock := &fakeClock{now: 0}
	evicted := make(chan Member, 1)
	d := New(clock.Now, func(m Member, at model.Instant) {
		evicted <- m
	})

	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))
	d.Register("m1", model.Core, model.Barrier, 0)

	stop := make(chan struct{})
	defer close(stop)
	go d.Run(time.Millisecond, stop)

	// Keep the injected clock well within the deadline for several ticks:
	// no eviction should fire no matter how many times the ticker fires.
	clock.Set(dl / 2)
	select {
	case m := <-evicted:
		t.Fatalf("unexpected eviction of %v while injected clock is still within the deadline", m)
	case <-time.After(20 * time.Millisecond):
		// expected: no eviction while the injected clock is inside budget.
	}

	// Now push the injected clock (not wall-clock) past the deadline; the
	// next tick must evict using that instant.
	clock.Set(dl + 1)
	select {
	case m := <-evicted:
		if m != "m1" {
			t.Fatalf("evicted %v, want m1", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Run's ticker to evict m1 after the injected clock passed its deadline")
	}
}

func TestRunStopsOnCloseAndStopsFiring(t *testing.T) {
	dl := model.Instant(coredetection.Deadline(model.Core, model.Barrier))
	clock := &fakeClock{now: dl + 1}
	fired := make(chan struct{}, 1)
	d := New(clock.Now, func(Member, model.Instant) {
		select {
		case fired <- struct{}{}:
		default:
		}
	})
	d.Register("m1", model.Core, model.Barrier, 0)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		d.Run(time.Millisecond, stop)
		close(done)
	}()

	// Wait for confirmation the loop actually ran at least once (its
	// callback fired) before asking it to stop — synchronized on the
	// callback signal, not a fixed sleep.
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("Run never fired an eviction before the stop signal")
	}
	close(stop)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop was closed")
	}
}
