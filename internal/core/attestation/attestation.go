// Package attestation is a pure core: it verifies a generic, signed
// attestation quote against a policy and maps the result to a trust tier.
// It performs no I/O and reads no clock. Obtaining the quote is the shell's
// job (the AttestationProvider seam) — this package only verifies.
//
// This core is HARDWARE-AGNOSTIC and NOT TPM-specific: it verifies an
// ed25519 signature over opaque measurement digests plus a freshness nonce.
// Any provider that can produce model.AttQuote's shape works — no TPM quote
// structure is assumed anywhere in this package. A machine with no
// attestation (the zero model.AttResult) still runs at BaselineTrust:
// attestation is a trust BOOST on the core tier, never a gate to entry.
package attestation

import (
	"crypto/ed25519"
	"crypto/subtle"

	"github.com/msivraj/swarm/internal/model"
)

// VerifyAttestation checks quote q against policy p and returns
// Valid{Measurements} iff ALL of the following hold (fail-closed — any
// failure, including malformed input, yields the zero AttResult, i.e.
// Invalid, and never panics):
//
//  1. q.Signer byte-for-byte equals p.TrustedKey (only the trusted key is
//     accepted — a well-formed signature from any other key is refused);
//  2. the ed25519 signature q.Signature verifies over the canonical signed
//     body signedBody(q.Measurements, q.Nonce) — see that function for the
//     exact, unambiguous encoding;
//  3. q.Nonce echoes p.ExpectedNonce byte-for-byte (freshness — rejects
//     replay of a stale quote);
//  4. q.Measurements SET-MATCH p.Expected: every expected measurement is
//     present, in any order, with none missing and none extra (a duplicate
//     measurement standing in for a missing one is also rejected — see
//     measurementsMatch).
//
// Key and signature lengths are guarded before any ed25519 call, so a
// short/nil key or signature is Invalid rather than a panic.
func VerifyAttestation(q model.AttQuote, p model.AttPolicy) model.AttResult {
	if !signerTrusted(q.Signer, p.TrustedKey) {
		return model.AttResult{}
	}
	if !signatureValid(q, p.TrustedKey) {
		return model.AttResult{}
	}
	if !bytesEqual(q.Nonce, p.ExpectedNonce) {
		return model.AttResult{}
	}
	if !measurementsMatch(q.Measurements, p.Expected) {
		return model.AttResult{}
	}
	return model.AttResult{Valid: true, Measurements: q.Measurements}
}

// TrustFromAttestation maps a verification result to a trust tier: Valid
// yields AttestedTrust, the boost on the core tier. Anything else —
// Invalid, or the zero AttResult from no/absent attestation — yields
// BaselineTrust. A node that never attested still runs at BaselineTrust;
// attestation never gates entry, it only raises assurance.
func TrustFromAttestation(r model.AttResult) model.TrustTier {
	if r.Valid {
		return model.AttestedTrust
	}
	return model.BaselineTrust
}

// signerTrusted reports whether signer is exactly the policy's trusted key,
// guarding the ed25519 public-key length first so a short/nil key is simply
// refused rather than treated specially.
func signerTrusted(signer, trusted model.PubKey) bool {
	if len(signer) != ed25519.PublicKeySize || len(trusted) != ed25519.PublicKeySize {
		return false
	}
	return bytesEqual(signer, trusted)
}

// signatureValid reports whether q.Signature is a valid ed25519 signature
// over signedBody(q.Measurements, q.Nonce) under key. Malformed input
// (wrong-length signature) is refused rather than panicking — the stdlib
// ed25519.Verify panics on a wrong-length key, so lengths are checked
// first.
func signatureValid(q model.AttQuote, key model.PubKey) bool {
	if len(key) != ed25519.PublicKeySize {
		return false
	}
	if len(q.Signature) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(key), signedBody(q.Measurements, q.Nonce), q.Signature)
}

// signedBody is the canonical, unambiguous encoding of (Measurements ||
// Nonce) that a quote's signature covers. Each field is length-prefixed
// with a fixed-width 8-byte big-endian count/length before its bytes, so
// no choice of measurement contents can shift a byte from one field (or
// one measurement) into another — e.g. measurements ["ab","c"] and
// ["a","bc"] never collide, and appending an extra empty measurement never
// forges the same body as a shorter list padded differently.
//
// Layout: uint64(len(measurements)) ||
//
//	for each measurement m: uint64(len(m)) || m ||
//	uint64(len(nonce)) || nonce
//
// Measurements are encoded in the order given in q.Measurements — the
// prover's chosen order is part of what is signed, even though
// VerifyAttestation later compares them to the policy order-independently
// (constraint 4, applied after the signature is confirmed to cover exactly
// this quote's data).
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

// measurementsMatch reports whether got and expected hold the same
// multiset of measurements, independent of order: every expected
// measurement is present exactly as many times as it appears in expected,
// and got has no extra entries. Comparing as a multiset (rather than a
// unique set) closes the gap a naive set comparison would leave open — a
// quote can't duplicate one measurement to paper over a missing one, since
// a duplicate that isn't matched by an equal duplicate in expected leaves
// counts unequal.
func measurementsMatch(got, expected [][]byte) bool {
	if len(got) != len(expected) {
		return false
	}
	sortedGot := sortedCopy(got)
	sortedExpected := sortedCopy(expected)
	for i := range sortedGot {
		if !bytesEqual(sortedGot[i], sortedExpected[i]) {
			return false
		}
	}
	return true
}

// sortedCopy returns a new slice holding the elements of in, ordered by
// lexicographic byte comparison, without mutating in.
func sortedCopy(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	copy(out, in)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && bytesLess(out[j], out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// bytesLess reports whether a sorts strictly before b, lexicographically by
// byte value with a shorter equal-prefix sorting first.
func bytesLess(a, b []byte) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// bytesEqual reports whether a and b hold identical bytes, in constant time
// for equal-length inputs (subtle.ConstantTimeCompare); differing lengths
// are never equal and are rejected before the constant-time comparison.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare(a, b) == 1
}
