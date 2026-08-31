// Package reputation is a shell package: it persists each open-tier
// identity's model.Reputation, keyed by model.SpiffeID. It performs no trust
// math of its own — every score change is computed by the pure core
// internal/core/reputation (Update); this package only reads the value it
// has stored, calls the core, and writes the result back.
//
// Location note (ticket #139): the ticket allowed either a new
// internal/shell/reputation package or an internal/shell/store delta. This
// implementation adds a new sibling package, matching how Swarm organizes
// shells by component (mitosis, placement, driver, reputation, …) rather
// than folding an unrelated key space into the P0 control-plane Store.
//
// Zero-start: Get on an identity that has never been stored returns the
// zero-value model.Reputation{} — never a defaulted-trusted value. That
// mirrors the core's zero-start property: every fresh SPIFFE identity, and
// every identity this store has never seen, starts at the trust floor.
package reputation

import (
	"sync"

	"github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/model"
)

// Store persists an open-tier identity's reputation across verdicts. The
// intended read-update-write cycle after a verdict is:
//
//	rep := store.Get(id)
//	rep = reputation.Update(rep, agreed)
//	store.Put(id, rep)
//
// or, atomically, RecordVerdict(id, agreed). A minimal interface is kept
// here on purpose — persisting a durable backend later (e.g. FoundationDB,
// per P4) only needs a second implementation of this surface.
type Store interface {
	// Get returns the reputation stored for id. An id that has never been
	// stored (or never Put) returns the zero-value model.Reputation{} — the
	// zero-start floor, never a defaulted-trusted value.
	Get(id model.SpiffeID) model.Reputation
	// Put durably writes rep under id, overwriting any prior value.
	Put(id model.SpiffeID, rep model.Reputation)
	// RecordVerdict atomically applies one verdict's worth of trust update
	// to id's reputation: it reads the current value, calls the pure core's
	// reputation.Update(rep, agreed), writes the result back, and returns
	// it. Callers that already hold rep may instead call Get/Put by hand;
	// RecordVerdict exists so concurrent verdicts about the SAME id cannot
	// race and lose an update (see the package's read-modify-write lock).
	RecordVerdict(id model.SpiffeID, agreed bool) model.Reputation
}

// memStore is an in-memory Store, safe for concurrent use. A single mutex
// guards the whole map: Get/Put/RecordVerdict all take it for their full
// operation, so RecordVerdict's read-modify-write is atomic per call even
// when multiple goroutines race to record verdicts for the same id.
type memStore struct {
	mu   sync.Mutex
	reps map[model.SpiffeID]model.Reputation
}

// NewMemStore returns an empty, ready-to-use in-memory Store. Any id not yet
// Put reads back as the zero-value model.Reputation{}.
func NewMemStore() Store {
	return &memStore{
		reps: make(map[model.SpiffeID]model.Reputation),
	}
}

func (s *memStore) Get(id model.SpiffeID) model.Reputation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reps[id]
}

func (s *memStore) Put(id model.SpiffeID, rep model.Reputation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reps[id] = rep
}

func (s *memStore) RecordVerdict(id model.SpiffeID, agreed bool) model.Reputation {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := reputation.Update(s.reps[id], agreed)
	s.reps[id] = next
	return next
}
