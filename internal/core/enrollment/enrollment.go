// Package enrollment is a pure core: it decides whether an anonymous
// open-tier join request is admitted, and whether a workload's signature is
// valid. It performs no I/O and reads no clock — proof-of-work verification
// is a deterministic hash comparison against a difficulty supplied as data
// (model.PowCfg), never a randomness draw. Certificate issuance, signing-key
// distribution, and verify-before-dispatch are the enrollment SHELL's job;
// this package only decides.
package enrollment

import (
	"crypto/ed25519"
	"crypto/sha256"

	"github.com/msivraj/swarm/internal/model"
)

// AdmitOpen verifies the proof-of-work `pow` against `cfg`'s required
// leading-zero-bit difficulty for the join request `req`, and returns
// Accept{open SpiffeID} on a valid proof or Reject otherwise.
//
// The proof is well-formed only when req.PubKey is non-empty and pow.Nonce
// echoes req.Nonce byte-for-byte — a proof solved against a different
// request's nonce is not a proof of this request's work and is rejected
// regardless of its hash. A well-formed proof's digest is
// sha256(req.PubKey || req.Nonce || pow.Solution); it is accepted when the
// digest's leading zero bits are >= cfg.DifficultyBits.
//
// When cfg.DifficultyBits == 0, proof-of-work is disabled: any well-formed
// request (as defined above) is admitted regardless of Solution — the
// difficulty floor of zero bits is trivially met by any digest.
//
// On acceptance the returned SpiffeID is derived deterministically from
// req.PubKey alone (see openSpiffeID) — the same joiner (by key) always maps
// to the same open-tier identity, so re-solving the PoW under a fresh nonce
// buys a Sybil no new identity.
func AdmitOpen(req model.JoinReq, pow model.PowProof, cfg model.PowCfg) model.Admit {
	if !wellFormed(req, pow) {
		return model.Admit{}
	}
	digest := powDigest(req, pow)
	if leadingZeroBits(digest) < cfg.DifficultyBits {
		return model.Admit{}
	}
	return model.Admit{Kind: model.Accept, ID: openSpiffeID(req.PubKey)}
}

// wellFormed reports whether req/pow carry the minimum shape AdmitOpen
// requires: a non-empty PubKey to derive an identity from, and a PowProof
// nonce that echoes the request's own nonce.
func wellFormed(req model.JoinReq, pow model.PowProof) bool {
	if len(req.PubKey) == 0 {
		return false
	}
	return bytesEqual(req.Nonce, pow.Nonce)
}

// powDigest is the deterministic hash a proof-of-work solution must meet
// cfg's difficulty against: sha256 over PubKey, Nonce, and Solution.
func powDigest(req model.JoinReq, pow model.PowProof) [sha256.Size]byte {
	h := sha256.New()
	h.Write(req.PubKey)
	h.Write(req.Nonce)
	h.Write(pow.Solution)
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

// leadingZeroBits counts the number of leading zero bits in digest, from the
// most significant bit of digest[0] onward.
func leadingZeroBits(digest [sha256.Size]byte) int {
	count := 0
	for _, b := range digest {
		if b == 0 {
			count += 8
			continue
		}
		for mask := byte(0x80); mask != 0; mask >>= 1 {
			if b&mask != 0 {
				return count
			}
			count++
		}
	}
	return count
}

// openSpiffeID deterministically derives the open-tier identity for a
// joiner's public key: the same PubKey always yields the same SpiffeID, and
// distinct keys yield (with overwhelming probability) distinct ids — a
// Sybil buys no shortcut by re-enrolling the same key.
func openSpiffeID(pub model.PubKey) model.SpiffeID {
	sum := sha256.Sum256(pub)
	return model.SpiffeID("spiffe://open/" + hexEncode(sum[:]))
}

// VerifySignature reports whether sig is a valid ed25519 signature of
// workload bytes wl under key. Malformed input (wrong-length key or
// signature, or either nil) is refused (false) rather than panicking — the
// stdlib ed25519.Verify panics on a wrong-length key, so lengths are
// checked first.
func VerifySignature(wl []byte, sig model.Sig, key model.PubKey) bool {
	if len(key) != ed25519.PublicKeySize {
		return false
	}
	if len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(key), wl, sig)
}

// bytesEqual reports whether a and b hold identical bytes.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

const hexDigits = "0123456789abcdef"

// hexEncode renders b as a lowercase hex string, avoiding an
// encoding/hex import for one call site.
func hexEncode(b []byte) string {
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexDigits[v>>4]
		out[i*2+1] = hexDigits[v&0x0f]
	}
	return string(out)
}
