// Package attestation is the imperative shell for P5 core-tier attestation
// (docs/phases/swarm-p5-components.txt §02): it OBTAINS a quote via the
// AttestationProvider seam, calls the pure internal/core/attestation
// decisions (VerifyAttestation, TrustFromAttestation) to turn that quote
// into a model.TrustTier, and binds the tier to an already-enrolled
// identity. It performs no verification logic of its own — every verdict
// comes from the core; this package only fetches evidence and records the
// result.
//
// This shell is HARDWARE-AGNOSTIC and NOT TPM-specific, mirroring P3's
// internal/shell/enrollment IdentityIssuer seam (#132/#142) exactly: a
// real AttestationProvider (SPIRE's node-attestation plugin against a TPM)
// is owner-infra, deferred to #194, and NOTHING here assumes a TPM exists.
// A node configured with NO provider (nil) still enrolls and runs, at
// BaselineTrust — see Attestor.Attest and TrustRegistry.Get.
package attestation

import (
	"crypto/ed25519"

	"github.com/msivraj/swarm/internal/model"
)

// AttestationProvider is the quote-obtaining seam (design fork d, #183): it
// is consulted by the attestation shell to fetch a fresh quote over a
// shell-chosen challenge nonce. A real implementation talks to SPIRE's
// node-attestation plugin against a TPM (owner-infra, #194, NOT built
// here); FakeProvider below is an in-memory, deterministic stand-in that
// satisfies the gate and -race.
//
// AttestationProvider never makes a trust decision — it only produces
// evidence. internal/core/attestation.VerifyAttestation is the sole
// authority on whether that evidence is valid.
type AttestationProvider interface {
	// Quote obtains a fresh attestation quote over nonce. Implementations
	// may fail (e.g. a real TPM/attester unreachable) — a non-nil error is
	// treated the same as a failed verification: BaselineTrust, never a
	// gate to entry.
	Quote(nonce []byte) (model.AttQuote, error)
}

// FakeProvider is an in-memory, deterministic AttestationProvider for tests
// and dev — NO TPM, NO real attester, NO network. It signs generic
// ed25519-over-(Measurements||Nonce) evidence with a fixed
// ed25519.NewKeyFromSeed key, over a configurable, fixed set of
// measurements — nothing TPM-format-specific. Mirrors P3's FakeIssuer.
type FakeProvider struct {
	priv         ed25519.PrivateKey
	pub          ed25519.PublicKey
	measurements [][]byte
}

// NewFakeProvider derives a deterministic ed25519 keypair from seed (via
// ed25519.NewKeyFromSeed — never crypto/rand) and configures the provider
// to sign measurements on every Quote call. Distinct seeds yield distinct
// but always-reproducible keypairs, so tests are deterministic.
func NewFakeProvider(seed [ed25519.SeedSize]byte, measurements [][]byte) *FakeProvider {
	priv := ed25519.NewKeyFromSeed(seed[:])
	return &FakeProvider{
		priv:         priv,
		pub:          priv.Public().(ed25519.PublicKey),
		measurements: measurements,
	}
}

// PublicKey returns the provider's signing key, e.g. to populate an
// AttPolicy.TrustedKey for a policy expecting this provider's quotes.
func (f *FakeProvider) PublicKey() model.PubKey {
	return model.PubKey(f.pub)
}

// Quote implements AttestationProvider: it signs signedBody(measurements,
// nonce) with the provider's fixed key and returns a well-formed
// model.AttQuote. It never fails.
func (f *FakeProvider) Quote(nonce []byte) (model.AttQuote, error) {
	body := signedBody(f.measurements, nonce)
	return model.AttQuote{
		Measurements: f.measurements,
		Nonce:        nonce,
		Signature:    model.Sig(ed25519.Sign(f.priv, body)),
		Signer:       model.PubKey(f.pub),
	}, nil
}

// signedBody reproduces, byte-for-byte, the canonical evidence encoding
// documented on internal/core/attestation.VerifyAttestation (a private
// function there, signedBody): length-prefixed measurements followed by a
// length-prefixed nonce, each length a fixed-width 8-byte big-endian
// count, so no choice of measurement contents can shift a byte from one
// field into another.
//
// This is NOT verification logic — it is the shared evidence-encoding
// convention any attester (a real TPM/SPIRE plugin, or this fake) must
// produce for internal/core/attestation.VerifyAttestation to accept it.
// Duplicating the documented layout here (rather than reimplementing the
// pass/fail DECISION, which stays solely in the core) keeps the seam
// symmetric: this package plays the part of the external attester, the
// core plays the part of the verifier, exactly as a real deployment would
// split the two.
//
// Layout: uint64(len(measurements)) ||
//
//	for each measurement m: uint64(len(m)) || m ||
//	uint64(len(nonce)) || nonce
func signedBody(measurements [][]byte, nonce []byte) []byte {
	size := 8
	for _, m := range measurements {
		size += 8 + len(m)
	}
	size += 8 + len(nonce)

	out := make([]byte, 0, size)
	out = appendUint64(out, uint64(len(measurements)))
	for _, m := range measurements {
		out = appendUint64(out, uint64(len(m)))
		out = append(out, m...)
	}
	out = appendUint64(out, uint64(len(nonce)))
	out = append(out, nonce...)
	return out
}

// appendUint64 appends v to out as 8 fixed-width big-endian bytes.
func appendUint64(out []byte, v uint64) []byte {
	return append(out,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v),
	)
}
