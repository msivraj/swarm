package model

// P5 boundary types: CORE-TIER ATTESTATION. verifyAttestation() binds
// identity to hardware on the core tier by checking a signed quote against
// a policy — pure crypto, format-agnostic and HARDWARE-AGNOSTIC (no TPM
// assumption anywhere in this package). A machine with no attestation
// provider simply supplies no quote and still runs at baseline trust — see
// docs/phases/swarm-p5-components.txt §02.

// AttQuote is a generic, signed attestation quote: opaque measurement
// digests plus a freshness nonce, signed by the attesting key. NOT a
// TPM-specific structure — any provider that can produce this shape works.
type AttQuote struct {
	// Measurements are opaque digests (e.g. boot/binary hashes).
	Measurements [][]byte
	// Nonce is the freshness challenge the quote signs over.
	Nonce []byte
	// Signature is the ed25519 signature over (Measurements || Nonce).
	Signature Sig
	// Signer is the key that produced Signature.
	Signer PubKey
}

// AttPolicy is what a valid quote must satisfy: the expected measurements,
// the one trusted signer, and the expected nonce. Format-agnostic.
type AttPolicy struct {
	// Expected lists the required measurements (order-independent set
	// match).
	Expected [][]byte
	// TrustedKey is the only key whose signature is accepted.
	TrustedKey PubKey
	// ExpectedNonce is the challenge the quote must echo.
	ExpectedNonce []byte
}

// AttResult is Valid{Measurements} | Invalid. The zero value has Valid ==
// false, i.e. Invalid — an absent, failed, or un-set attestation result is
// never silently trusted (fail-closed).
type AttResult struct {
	Valid bool
	// Measurements is echoed on Valid, for audit.
	Measurements [][]byte
}

// TrustTier is the trust BOOST attestation grants on the core tier. It is
// distinct from Tier (Core|Open): attestation raises assurance WITHIN the
// core tier, it never gates entry and never touches the open tier's model.
type TrustTier int

const (
	// BaselineTrust means no/absent/invalid attestation — the zero value,
	// so a node with no attestation provider still runs.
	BaselineTrust TrustTier = iota
	// AttestedTrust means a valid quote was verified against policy —
	// higher assurance.
	AttestedTrust
)
