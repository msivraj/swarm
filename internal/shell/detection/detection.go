// Package detection is the imperative shell that drives the pure
// internal/core/detection core: it tracks each member's lastSeen instant and,
// on a timer, evaluates detection.IsDead against a deadline computed by
// detection.Deadline, firing an eviction callback the first time a member is
// found dead.
//
// All timing math (the deadline table and the alive/dead decision) lives in
// internal/core/detection; this package only tracks state and drives it.
// Every decision consumes an injected model.Instant — never time.Now — so it
// stays deterministic under a fake clock even though the eviction loop's
// ticker fires on real wall-clock time.
package detection

import (
	"sort"
	"sync"
	"time"

	coredetection "github.com/msivraj/swarm/internal/core/detection"
	"github.com/msivraj/swarm/internal/model"
)

// Member identifies a tracked participant to the detector — an agent, task,
// or barrier participant, depending on what the caller (control plane / cell
// leader) is watching for liveness.
type Member string

// EvictFunc is called exactly once per member the first time it is found
// dead, with the instant the eviction was decided at. It is the downstream
// signal (a driver Lost/Evict event, a re-schedule trigger, …) — this
// package does not interpret it further.
type EvictFunc func(member Member, at model.Instant)

// state is one tracked member's bookkeeping: the tier/coupling it was
// registered with (which set its Deadline), its lastSeen instant, and
// whether an eviction has already fired for it.
type state struct {
	tier     model.Tier
	coupling model.Coupling
	lastSeen model.Instant
	evicted  bool
}

// Detector tracks per-member lastSeen instants and evaluates their liveness
// against internal/core/detection on demand (Dead) or on a timer (Run /
// Sweep). It is safe for concurrent use.
type Detector struct {
	mu      sync.Mutex
	now     func() model.Instant
	onEvict EvictFunc
	members map[Member]*state
}

// New builds a Detector. now supplies the injected clock Run's ticker loop
// reads to make its Sweep decisions; onEvict is invoked (outside the
// Detector's lock, so it may safely call back into Detector methods) exactly
// once per member the first time it is found dead. onEvict may be nil if the
// caller only ever polls via Dead.
func New(now func() model.Instant, onEvict EvictFunc) *Detector {
	return &Detector{
		now:     now,
		onEvict: onEvict,
		members: make(map[Member]*state),
	}
}

// Register begins tracking member with the given tier/coupling (which fixes
// its detection.Deadline) and an initial lastSeen of at. Calling Register
// again for an already-tracked member updates its tier/coupling and lastSeen
// and clears any prior eviction — this is the re-admission path for a member
// that rejoins after a new job assignment.
func (d *Detector) Register(member Member, tier model.Tier, coupling model.Coupling, at model.Instant) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.members[member] = &state{tier: tier, coupling: coupling, lastSeen: at}
}

// Seen records a heartbeat/report from member at instant at: it refreshes
// lastSeen and clears eviction status if the member was previously found
// dead — a resumed heartbeat cancels a pending or already-fired eviction. It
// is a no-op for a member that was never Registered.
func (d *Detector) Seen(member Member, at model.Instant) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, ok := d.members[member]
	if !ok {
		return
	}
	st.lastSeen = at
	st.evicted = false
}

// Forget stops tracking member entirely, e.g. because its job completed or
// it left cleanly. A forgotten member fires no further evictions.
func (d *Detector) Forget(member Member) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.members, member)
}

// Dead returns every currently tracked member whose deadline — lastSeen +
// coredetection.Deadline(tier, coupling), evaluated by coredetection.IsDead —
// has passed as of now. It is a pure read: it does not mark members evicted
// or invoke onEvict, so it is safe to poll repeatedly (e.g. from a leader
// query) without affecting Sweep's exactly-once firing.
func (d *Detector) Dead(now model.Instant) []Member {
	d.mu.Lock()
	defer d.mu.Unlock()

	var dead []Member
	for m, st := range d.members {
		if coredetection.IsDead(st.lastSeen, deadline(st), now) {
			dead = append(dead, m)
		}
	}
	sort.Slice(dead, func(i, j int) bool { return dead[i] < dead[j] })
	return dead
}

// Sweep evaluates every tracked member against now and, for each one newly
// found dead (i.e. not already evicted), marks it evicted and invokes
// onEvict exactly once. It returns the members newly evicted by this call —
// a member already evicted by a prior Sweep is not returned again unless a
// Register/Seen call cleared its evicted status in between.
func (d *Detector) Sweep(now model.Instant) []Member {
	d.mu.Lock()
	var newlyDead []Member
	for m, st := range d.members {
		if st.evicted {
			continue
		}
		if coredetection.IsDead(st.lastSeen, deadline(st), now) {
			st.evicted = true
			newlyDead = append(newlyDead, m)
		}
	}
	sort.Slice(newlyDead, func(i, j int) bool { return newlyDead[i] < newlyDead[j] })
	onEvict := d.onEvict
	d.mu.Unlock()

	if onEvict != nil {
		for _, m := range newlyDead {
			onEvict(m, now)
		}
	}
	return newlyDead
}

// Run drives Sweep on a wall-clock ticker of period interval until stop is
// closed. The ticker itself fires on real time, but every Sweep call reads
// the instant from d.now (the injected clock), so the eviction decision
// stays deterministic and reproducible under a fake clock in tests — only
// the *cadence* of checks is real time, never the *decision*.
func (d *Detector) Run(interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.Sweep(d.now())
		case <-stop:
			return
		}
	}
}

// deadline computes the absolute eviction instant for st: its lastSeen plus
// the span coredetection.Deadline assigns its tier/coupling pair. This is
// the shell's only arithmetic — the deadline table and the alive/dead
// comparison both stay in internal/core/detection.
func deadline(st *state) model.Instant {
	return st.lastSeen + model.Instant(coredetection.Deadline(st.tier, st.coupling))
}
