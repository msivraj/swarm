package verification

import (
	"sync"
	"time"

	"github.com/msivraj/swarm/internal/model"
)

// Clock is the coordinator's time source for a collection round's timeout.
// It mirrors the shape of internal/shell/agent.Clock (Now) plus a
// time.After-style seam (After) so a round's deadline can be driven
// deterministically in tests instead of by a real wall-clock sleep — the
// core itself never sees a Clock at all; only this shell does.
type Clock interface {
	// Now returns the current instant.
	Now() model.Instant
	// After returns a channel that receives the instant at which d has
	// elapsed, once it has. A production Clock schedules this off the real
	// wall clock; a fake Clock in tests fires it only when explicitly
	// advanced, so a round that would otherwise wait out a real timeout
	// resolves instantly and deterministically.
	After(d model.Duration) <-chan model.Instant
}

// RealClock is the Clock the shell uses in production: the wall clock, with
// After backed by time.AfterFunc.
type RealClock struct{}

// Now returns the current wall-clock instant, in nanoseconds.
func (RealClock) Now() model.Instant {
	return model.Instant(time.Now().UnixNano())
}

// After returns a channel that fires once d has really elapsed.
func (RealClock) After(d model.Duration) <-chan model.Instant {
	ch := make(chan model.Instant, 1)
	time.AfterFunc(time.Duration(d), func() {
		ch <- model.Instant(time.Now().UnixNano())
	})
	return ch
}

// fakeWaiter is one pending After call on a FakeClock: it fires ch once the
// clock reaches deadline.
type fakeWaiter struct {
	deadline model.Instant
	ch       chan model.Instant
}

// FakeClock is a controllable Clock for deterministic tests: Now never
// advances on its own, and After's channel only fires when a test calls
// Advance (or Set) past the requested deadline — never on a real sleep.
// Safe for concurrent use.
type FakeClock struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     model.Instant
	waiters []fakeWaiter
}

// NewFakeClock returns a FakeClock starting at start.
func NewFakeClock(start model.Instant) *FakeClock {
	c := &FakeClock{now: start}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// Now returns the clock's current (fake) instant.
func (c *FakeClock) Now() model.Instant {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After registers a waiter for d beyond the clock's current instant and
// returns its channel. A non-positive d fires immediately — the deadline
// has already "elapsed" relative to now.
func (c *FakeClock) After(d model.Duration) <-chan model.Instant {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan model.Instant, 1)
	deadline := c.now + model.Instant(d)
	if deadline <= c.now {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, fakeWaiter{deadline: deadline, ch: ch})
	c.cond.Broadcast()
	return ch
}

// BlockUntil blocks until at least n After calls are currently pending
// (i.e. registered but not yet fired). Tests use this to synchronize with a
// goroutine that calls After before the test calls Advance — so the round's
// timeout can be driven deterministically with no real sleep on either
// side.
func (c *FakeClock) BlockUntil(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.waiters) < n {
		c.cond.Wait()
	}
}

// Advance moves the clock forward by d, firing (and clearing) every pending
// waiter whose deadline is now due.
func (c *FakeClock) Advance(d model.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now += model.Instant(d)
	c.fireDueLocked()
}

// Set moves the clock directly to now, firing every pending waiter whose
// deadline is due.
func (c *FakeClock) Set(now model.Instant) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
	c.fireDueLocked()
}

// fireDueLocked fires and removes every waiter whose deadline is <= the
// clock's current instant. Callers must hold c.mu.
func (c *FakeClock) fireDueLocked() {
	remaining := c.waiters[:0]
	for _, w := range c.waiters {
		if w.deadline <= c.now {
			w.ch <- c.now
		} else {
			remaining = append(remaining, w)
		}
	}
	c.waiters = remaining
}
