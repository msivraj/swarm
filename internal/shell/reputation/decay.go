// This file adds a periodic decay pass on top of the reputation Store: a
// second in-memory implementation, DecayingStore, that additionally tracks
// each identity's lastTouched Instant (set on every RecordVerdict and on
// every DecayPass) so a periodic pass can compute, as data, how long an
// identity has gone unseen and fade its Score via the pure
// internal/core/reputation.Decay. The P3 verdict path (RecordVerdict) is
// unchanged in behavior — it still just calls reputation.Update and writes
// the result back — DecayPass is purely additive.
package reputation

import (
	"sync"

	"github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/model"
)

// DecayingStore is a Store that also runs a periodic decay pass over every
// identity it holds. It satisfies the same Store interface as memStore (so
// it is a drop-in replacement anywhere a Store is wanted) and additionally
// exposes DecayPass for a background loop — or a test — to drive.
//
// A single mutex guards reps and touched together: Get/Put/RecordVerdict/
// DecayPass all take it for their full operation, so a decay pass racing a
// concurrent verdict for the same identity can never interleave or lose an
// update — the same atomicity memStore's RecordVerdict promises.
type DecayingStore struct {
	// now supplies the clock RecordVerdict stamps a fresh verdict's
	// lastTouched with. It is an injected seam (like controlplane.Server's
	// own now func) — DecayingStore never reads a real clock itself, so
	// tests can drive RecordVerdict's timestamping with a fake clock.
	now func() model.Instant

	mu      sync.Mutex
	reps    map[model.SpiffeID]model.Reputation
	touched map[model.SpiffeID]model.Instant
}

// NewDecayingStore returns an empty, ready-to-use DecayingStore. Any id not
// yet Put/RecordVerdict reads back the zero-value model.Reputation{} — the
// same zero-start floor memStore guarantees.
func NewDecayingStore(now func() model.Instant) *DecayingStore {
	return &DecayingStore{
		now:     now,
		reps:    make(map[model.SpiffeID]model.Reputation),
		touched: make(map[model.SpiffeID]model.Instant),
	}
}

func (s *DecayingStore) Get(id model.SpiffeID) model.Reputation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reps[id]
}

func (s *DecayingStore) Put(id model.SpiffeID, rep model.Reputation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reps[id] = rep
}

// RecordVerdict applies one verdict's worth of trust update via the pure
// core (reputation.Update, exactly as memStore does) and stamps id's
// lastTouched with the injected clock's current Instant, so a later
// DecayPass measures elapsed idle time from this verdict rather than from
// whenever the identity was first seen.
func (s *DecayingStore) RecordVerdict(id model.SpiffeID, agreed bool) model.Reputation {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := reputation.Update(s.reps[id], agreed)
	s.reps[id] = next
	s.touched[id] = s.now()
	return next
}

// DecayPass applies the pure reputation.Decay to every stored identity,
// fading each one's Score toward the zero floor by the elapsed span since it
// was last touched (by a verdict or by a previous DecayPass), and persists
// the faded result. now is the current Instant, supplied by the caller — a
// periodic loop's own injected clock, or a test driving decay directly — as
// data; DecayPass itself never reads a clock.
//
// An identity that has never been touched (Put without a prior RecordVerdict
// or DecayPass) is treated as touched at now: nothing has elapsed yet, so
// its first pass is a no-op, matching Decay's own "elapsed <= 0 returns rep
// unchanged" contract.
//
// Every decayed identity's lastTouched is advanced to now, so a second
// DecayPass at a later Instant measures a fresh elapsed span from this pass
// rather than compounding against the original lastTouched — repeated
// passes drive a stale Score toward, and clamp at, the zero floor instead of
// overshooting it.
func (s *DecayingStore) DecayPass(now model.Instant) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, rep := range s.reps {
		last, ok := s.touched[id]
		if !ok {
			last = now
		}
		elapsed := model.Duration(now - last)
		s.reps[id] = reputation.Decay(rep, elapsed)
		s.touched[id] = now
	}
}
