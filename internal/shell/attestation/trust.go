package attestation

import (
	"sync"

	"github.com/msivraj/swarm/internal/model"
)

// TrustRegistry is the integration point with P3 enrollment (#142): it
// maps an already-enrolled model.SpiffeID to the model.TrustTier its
// attestation evidence earned, so the enrollment/dispatch path can consult
// an identity's trust tier without this package editing
// internal/shell/enrollment at all. Attestor.Attest is the only writer;
// Get is the read side any later shell (dispatch, placement, ...) can call.
//
// Get on an identity that was never attested (or that attested and failed)
// returns model.BaselineTrust — the registry's zero value for an absent
// entry — so consulting it for an identity attestation was never run
// against still reads as the safe default, never a false boost. This is
// the same "fail closed to the zero value" shape as
// model.AttResult{}/TrustFromAttestation in the core.
type TrustRegistry struct {
	mu    sync.RWMutex
	tiers map[model.SpiffeID]model.TrustTier
}

// NewTrustRegistry returns an empty TrustRegistry.
func NewTrustRegistry() *TrustRegistry {
	return &TrustRegistry{tiers: make(map[model.SpiffeID]model.TrustTier)}
}

// Set records id's trust tier, overwriting any previous entry. Safe for
// concurrent use.
func (r *TrustRegistry) Set(id model.SpiffeID, tier model.TrustTier) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tiers[id] = tier
}

// Get returns id's recorded trust tier, or model.BaselineTrust if id has no
// entry (never attested) — attestation is a boost, never a gate, so an
// identity this registry has no opinion on is never worse off than
// baseline. Safe for concurrent use.
func (r *TrustRegistry) Get(id model.SpiffeID) model.TrustTier {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tiers[id]
}
