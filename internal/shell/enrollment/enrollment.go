// Package enrollment is the imperative shell for anonymous open-tier
// enrollment (P3, docs/phases/swarm-p3-components.txt §02): it calls the
// pure internal/core/enrollment decisions (AdmitOpen, VerifySignature) and
// performs the I/O those decisions license — issuing a SPIRE/mTLS identity
// on Accept (behind the IdentityIssuer seam, design fork c), consulting a
// Blacklist (design fork b, distinct from P2's liveness eviction) before
// issuance, and distributing/consulting signing public keys so a workload
// can be verified before dispatch.
//
// This package never reimplements the PoW or signature math — every
// admission/verification DECISION comes from internal/core/enrollment; this
// package only wires I/O around those decisions.
package enrollment

import (
	coreenrollment "github.com/msivraj/swarm/internal/core/enrollment"
	"github.com/msivraj/swarm/internal/model"
)

// Status is the outcome of an Enroll call, distinguishing the stage at
// which a join request was refused (if it was).
type Status int

const (
	// StatusRejected means the pure core refused admission (bad/missing
	// proof-of-work) — the zero value, so a zero-value EnrollResult reads
	// as "not admitted" rather than silently as some other status.
	StatusRejected Status = iota
	// StatusBlacklisted means the core accepted the proof-of-work but the
	// resulting identity is on the Blacklist (fork b) — refused before any
	// identity is issued.
	StatusBlacklisted
	// StatusIssuerError means the core accepted and the identity is not
	// blacklisted, but the IdentityIssuer seam failed to mint a
	// certificate (e.g. a real SPIRE server unreachable).
	StatusIssuerError
	// StatusAccepted means the join request was fully admitted: PoW valid,
	// not blacklisted, and a certificate was issued.
	StatusAccepted
)

// EnrollResult is what Enroll returns: the admission Status, the core's
// Admit decision (for the SpiffeID, when accepted), and the issued Cert
// when Status == StatusAccepted.
type EnrollResult struct {
	Status Status
	Admit  model.Admit
	Cert   Cert
}

// Enroller runs the anonymous-enrollment flow end to end: verify
// proof-of-work via the pure core, consult the Blacklist seam, issue an
// identity via the IdentityIssuer seam, and register the joiner's signing
// key in the Keyring so a later workload can be verified before dispatch.
type Enroller struct {
	cfg       model.PowCfg
	issuer    IdentityIssuer
	blacklist Blacklist
	keys      *Keyring
}

// NewEnroller builds an Enroller. cfg configures the PoW difficulty the
// pure core enforces; issuer and blacklist are the two P3 seams (forks c
// and b); keys is the signing-key registry VerifyWorkload consults.
// blacklist may be nil (no blacklist consulted, e.g. before #141 is
// wired); issuer and keys must not be nil.
func NewEnroller(cfg model.PowCfg, issuer IdentityIssuer, blacklist Blacklist, keys *Keyring) *Enroller {
	return &Enroller{cfg: cfg, issuer: issuer, blacklist: blacklist, keys: keys}
}

// Enroll admits req/pow through the pure core, then performs the I/O the
// decision licenses:
//
//   - Kind != Accept (bad/missing PoW): StatusRejected, no blacklist
//     consult, no identity issued.
//   - Accept but ID is blacklisted: StatusBlacklisted, no identity issued.
//   - Accept and not blacklisted: the IdentityIssuer mints a Cert for
//     admit.ID and the joiner's PubKey is registered in the Keyring for
//     future VerifyWorkload calls; StatusAccepted.
//   - Accept, not blacklisted, but Issue fails: StatusIssuerError, and the
//     error is returned so the caller can retry/log — no key is
//     registered.
func (e *Enroller) Enroll(req model.JoinReq, pow model.PowProof) (EnrollResult, error) {
	admit := coreenrollment.AdmitOpen(req, pow, e.cfg)
	if admit.Kind != model.Accept {
		return EnrollResult{Status: StatusRejected, Admit: admit}, nil
	}
	if e.blacklist != nil && e.blacklist.IsBlacklisted(admit.ID) {
		return EnrollResult{Status: StatusBlacklisted, Admit: admit}, nil
	}
	cert, err := e.issuer.Issue(admit.ID)
	if err != nil {
		return EnrollResult{Status: StatusIssuerError, Admit: admit}, err
	}
	if e.keys != nil {
		e.keys.Register(admit.ID, req.PubKey)
	}
	return EnrollResult{Status: StatusAccepted, Admit: admit, Cert: cert}, nil
}

// VerifyWorkload reports whether wl is validly signed by id's registered
// signing key, and MUST be called (and return true) before dispatching any
// workload attributed to id. It refuses — never dispatch on false — both
// when id has no registered key (never enrolled, or its key was revoked,
// e.g. after a blacklist) and when the signature fails the pure core's
// enrollment.VerifySignature check.
func (e *Enroller) VerifyWorkload(id model.SpiffeID, wl []byte, sig model.Sig) bool {
	if e.keys == nil {
		return false
	}
	key, ok := e.keys.Lookup(id)
	if !ok {
		return false
	}
	return coreenrollment.VerifySignature(wl, sig, key)
}
