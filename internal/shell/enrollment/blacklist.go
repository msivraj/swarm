package enrollment

import (
	"sync"

	"github.com/msivraj/swarm/internal/model"
)

// Blacklist is the fork-b seam (#132): a set of identities refused at
// enrollment/dispatch admission, kept distinct from the P2 reason-agnostic
// liveness-eviction path. This shell only CONSULTS a Blacklist; the
// concrete, populated blacklist is owned and written by the honeypot shell
// (#141), which turns a model.Action{Kind: model.Blacklist, ID: id} —
// produced by the pure honeypot core on a caught lie — into a write against
// whatever Blacklist implementation is deployed (in-memory, a replicated
// store, ...). Keeping the interface to a single read method lets #141
// implement it without any churn here.
type Blacklist interface {
	// IsBlacklisted reports whether id must be refused at admission or
	// dispatch. A blacklist implementation must never panic — an unknown id
	// simply reports false.
	IsBlacklisted(id model.SpiffeID) bool
}

// FakeBlacklist is an in-memory Blacklist for tests: some ids can be
// pre-blacklisted (or blacklisted later, mirroring how #141 would call Add
// in response to a honeypot Action). Safe for concurrent use.
type FakeBlacklist struct {
	mu  sync.RWMutex
	set map[model.SpiffeID]struct{}
}

// NewFakeBlacklist returns a Blacklist pre-populated with ids.
func NewFakeBlacklist(ids ...model.SpiffeID) *FakeBlacklist {
	b := &FakeBlacklist{set: make(map[model.SpiffeID]struct{}, len(ids))}
	for _, id := range ids {
		b.set[id] = struct{}{}
	}
	return b
}

// Add blacklists id. Idempotent.
func (b *FakeBlacklist) Add(id model.SpiffeID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.set[id] = struct{}{}
}

// IsBlacklisted implements Blacklist.
func (b *FakeBlacklist) IsBlacklisted(id model.SpiffeID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.set[id]
	return ok
}
