package attestation

import (
	coreattestation "github.com/msivraj/swarm/internal/core/attestation"
	"github.com/msivraj/swarm/internal/model"
)

// Attestor runs the P5 attestation flow for an already-enrolled identity:
// obtain a quote via the AttestationProvider seam (if configured), verify
// it against a policy through the pure core, and bind the resulting
// model.TrustTier to the identity in a TrustRegistry. Attestor adds NO
// verification logic of its own — VerifyAttestation and
// TrustFromAttestation (internal/core/attestation) are the sole source of
// the verdict and the tier; this type only fetches evidence and records
// the outcome.
//
// Integration point with P3 enrollment (#142, documented per #191's scope
// boundary — internal/shell/enrollment is NOT edited): call Attestor.Attest
// with the model.SpiffeID an enrollment.Enroller.Enroll call just admitted
// (result.Admit.ID on enrollment.StatusAccepted), then consult the
// identity's tier later via TrustRegistry.Get wherever dispatch/placement
// needs it. Composition happens entirely through Enroller's exported
// surface (EnrollResult.Admit.ID) — nothing in internal/shell/enrollment
// changes.
type Attestor struct {
	provider   AttestationProvider
	expected   [][]byte
	trustedKey model.PubKey
	trust      *TrustRegistry
}

// NewAttestor builds an Attestor. provider may be nil — the hardware-
// agnostic guarantee: a node with no AttestationProvider configured still
// runs, Attest binds it to model.BaselineTrust without attempting any I/O.
// expected and trustedKey describe the policy a non-nil provider's quotes
// must satisfy (the measurements and the one trusted signer); trust must
// not be nil — Attest always records a tier there.
func NewAttestor(provider AttestationProvider, expected [][]byte, trustedKey model.PubKey, trust *TrustRegistry) *Attestor {
	return &Attestor{provider: provider, expected: expected, trustedKey: trustedKey, trust: trust}
}

// Attest binds id's trust tier from a fresh attestation challenge over
// nonce (the shell-chosen freshness challenge — the caller picks a new
// nonce per call, e.g. a counter or random value; the core rejects any
// quote that doesn't echo it, defeating replay of a stale quote):
//
//   - No provider configured (nil): no I/O is attempted, id is bound to
//     model.BaselineTrust immediately — a machine with no attestation
//     hardware/agent still runs.
//   - provider.Quote fails: treated the same as a failed verification —
//     id is bound to model.BaselineTrust, never AttestedTrust, and the
//     node still runs (attestation never gates entry).
//   - provider.Quote succeeds: the quote is verified against a policy
//     built from (expected, trustedKey, nonce) via
//     internal/core/attestation.VerifyAttestation, and the resulting
//     model.TrustTier — AttestedTrust only if Valid, BaselineTrust
//     otherwise — is bound to id.
//
// The tier bound is always returned, and the same value is what
// TrustRegistry.Get(id) will report afterward.
func (a *Attestor) Attest(id model.SpiffeID, nonce []byte) model.TrustTier {
	tier := a.evaluate(nonce)
	a.trust.Set(id, tier)
	return tier
}

// evaluate computes the trust tier a fresh challenge over nonce earns,
// without touching the registry — split out so Attest's single
// responsibility is "fetch evidence, then record."
func (a *Attestor) evaluate(nonce []byte) model.TrustTier {
	if a.provider == nil {
		return coreattestation.TrustFromAttestation(model.AttResult{})
	}
	quote, err := a.provider.Quote(nonce)
	if err != nil {
		return coreattestation.TrustFromAttestation(model.AttResult{})
	}
	policy := model.AttPolicy{
		Expected:      a.expected,
		TrustedKey:    a.trustedKey,
		ExpectedNonce: nonce,
	}
	result := coreattestation.VerifyAttestation(quote, policy)
	return coreattestation.TrustFromAttestation(result)
}
