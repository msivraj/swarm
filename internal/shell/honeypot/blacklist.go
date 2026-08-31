package honeypot

import (
	"sync"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/enrollment"
)

// Blacklist is the concrete, populated blacklist store the design's fork b
// (#132) calls for: a set of identities refused at enrollment/dispatch
// admission, kept distinct from the P2 reason-agnostic liveness-eviction
// path. It implements internal/shell/enrollment.Blacklist's read side
// (IsBlacklisted) — so both the enrollment shell (admission) and the
// verification coordinator (K-set filtering, internal/shell/verification's
// Config.Blacklist) can consult the very same store — and adds the write
// side (Apply/Add) that ProbingDispatcher calls when the pure honeypot core
// catches a lie. Safe for concurrent use: dispatch happens concurrently
// across the K assigned machines.
type Blacklist struct {
	mu  sync.RWMutex
	set map[model.SpiffeID]struct{}
}

// NewBlacklist returns an empty, ready-to-use Blacklist.
func NewBlacklist() *Blacklist {
	return &Blacklist{set: make(map[model.SpiffeID]struct{})}
}

// Add blacklists id directly. Idempotent.
func (b *Blacklist) Add(id model.SpiffeID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.set[id] = struct{}{}
}

// Apply performs the effect described by act: a model.Action{Kind:
// Blacklist, ID: id} — the pure honeypot core's OnLie output — blacklists
// id. Any other Kind (including the zero-value NoAction) is a no-op, so
// applying a zero-value Action never blacklists anyone.
func (b *Blacklist) Apply(act model.Action) {
	if act.Kind != model.Blacklist {
		return
	}
	b.Add(act.ID)
}

// IsBlacklisted implements internal/shell/enrollment.Blacklist.
func (b *Blacklist) IsBlacklisted(id model.SpiffeID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.set[id]
	return ok
}

// compile-time assertion: Blacklist implements enrollment.Blacklist's read
// side, so the enrollment shell's admission path and the verification
// coordinator's K-set filtering can both consult the very same store this
// package writes to on a caught lie.
var _ enrollment.Blacklist = (*Blacklist)(nil)
