package enrollment

import (
	"errors"
	"sync"

	"github.com/msivraj/swarm/internal/model"
)

// ErrEmptyID is returned by an IdentityIssuer when asked to issue a
// certificate for the zero-value SpiffeID — that never happens for a real
// model.Admit{Kind: model.Accept, ...}, since the core only accepts with a
// non-empty ID, but the seam guards against a caller skipping the admission
// check.
var ErrEmptyID = errors.New("enrollment: cannot issue a certificate for an empty SpiffeID")

// Cert is the identity credential an IdentityIssuer hands back on Issue. It
// is deliberately minimal and opaque: real SPIRE/mTLS issuance produces an
// X.509-SVID plus private key material the shell would hold instead — this
// type only carries what the rest of the enrollment shell needs to reason
// about (which identity, which issuance).
type Cert struct {
	// ID is the SpiffeID this certificate was issued for.
	ID model.SpiffeID
	// Serial distinguishes issuances (monotonically increasing per issuer
	// instance); it is not a security property, only a debugging/audit aid.
	Serial uint64
}

// IdentityIssuer is the SPIRE/mTLS issuance seam (design fork c, #132): it
// is consulted by the enrollment shell only after the pure core
// (enrollment.AdmitOpen) has already decided to Accept a join request — the
// issuer never makes an admission decision, it only turns an already-issued
// SpiffeID into a certificate. A real implementation talks to a SPIRE
// server over gRPC/mTLS (owner-infra, not built here); FakeIssuer below is
// an in-memory stand-in that satisfies the gate and -race.
//
// This is the seam #141 (honeypot) and #140 (verification coordinator) can
// depend on without needing a real SPIRE deployment: construct a
// FakeIssuer (or any IdentityIssuer) and wire it into an Enroller.
type IdentityIssuer interface {
	// Issue mints a certificate for id. Called only for identities the pure
	// core has already Accept-ed; implementations may still fail (e.g. a
	// real SPIRE server unreachable) — callers must not dispatch a job
	// under an identity whose Issue call returned a non-nil error.
	Issue(id model.SpiffeID) (Cert, error)
}

// FakeIssuer is an in-memory, deterministic IdentityIssuer for tests and
// dev — no network, no real SPIRE. Re-issuing the same SpiffeID returns the
// certificate already on file rather than minting a fresh serial, mirroring
// how a real SPIRE server would hand back the same SVID for an
// already-enrolled workload identity.
type FakeIssuer struct {
	mu     sync.Mutex
	serial uint64
	issued map[model.SpiffeID]Cert
}

// NewFakeIssuer returns an empty in-memory IdentityIssuer.
func NewFakeIssuer() *FakeIssuer {
	return &FakeIssuer{issued: make(map[model.SpiffeID]Cert)}
}

// Issue mints (or replays) a Cert for id. Safe for concurrent use.
func (f *FakeIssuer) Issue(id model.SpiffeID) (Cert, error) {
	if id == "" {
		return Cert{}, ErrEmptyID
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.issued[id]; ok {
		return c, nil
	}
	f.serial++
	c := Cert{ID: id, Serial: f.serial}
	f.issued[id] = c
	return c, nil
}

// Issued reports whether id currently holds an issued certificate — a test
// helper for asserting "no identity was issued" on a rejected/blacklisted
// join.
func (f *FakeIssuer) Issued(id model.SpiffeID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.issued[id]
	return ok
}
