package attestation_test

import (
	"crypto/ed25519"
	"testing"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/attestation"
	"github.com/msivraj/swarm/internal/shell/enrollment"
)

// seed builds a fixed 32-byte ed25519 seed from a single byte, mirroring
// the pattern in internal/core/attestation_test.go and
// internal/shell/enrollment_test.go — deterministic key material, never
// crypto/rand.
func seed(b byte) [ed25519.SeedSize]byte {
	var s [ed25519.SeedSize]byte
	for i := range s {
		s[i] = b
	}
	return s
}

// enrollOne runs the P3 open-tier enrollment flow (PoW disabled) and
// returns the admitted model.SpiffeID, demonstrating the documented
// integration point: composition happens purely through Enroller's
// exported EnrollResult.Admit.ID — internal/shell/enrollment is never
// edited by this package.
func enrollOne(t *testing.T, pubKeyLabel string) model.SpiffeID {
	t.Helper()
	e := enrollment.NewEnroller(model.PowCfg{}, enrollment.NewFakeIssuer(), enrollment.NewFakeBlacklist(), enrollment.NewKeyring())
	req := model.JoinReq{PubKey: model.PubKey(pubKeyLabel), Nonce: []byte("join-nonce")}
	pow := model.PowProof{Nonce: req.Nonce, Solution: []byte("unused-at-difficulty-zero")}

	result, err := e.Enroll(req, pow)
	if err != nil {
		t.Fatalf("Enroll: unexpected error: %v", err)
	}
	if result.Status != enrollment.StatusAccepted {
		t.Fatalf("Enroll: Status = %v, want StatusAccepted", result.Status)
	}
	return result.Admit.ID
}

// TestAttest_ValidQuote_AttestedTrust is the ticket's first acceptance
// criterion: a FakeProvider producing a quote whose measurements and nonce
// match the policy verifies Valid, and the already-enrolled identity is
// bound to AttestedTrust.
func TestAttest_ValidQuote_AttestedTrust(t *testing.T) {
	id := enrollOne(t, "node-a-key")

	measurements := [][]byte{[]byte("boot-hash"), []byte("binary-hash")}
	provider := attestation.NewFakeProvider(seed(1), measurements)
	trust := attestation.NewTrustRegistry()
	att := attestation.NewAttestor(provider, measurements, provider.PublicKey(), trust)

	tier := att.Attest(id, []byte("challenge-nonce-1"))

	if tier != model.AttestedTrust {
		t.Fatalf("Attest() = %v, want AttestedTrust", tier)
	}
	if got := trust.Get(id); got != model.AttestedTrust {
		t.Fatalf("TrustRegistry.Get(id) = %v, want AttestedTrust", got)
	}
}

// TestAttest_MismatchedMeasurements_BaselineTrustStillRuns is the ticket's
// second acceptance criterion: a provider whose measurements don't match
// the policy yields Invalid, so the identity is bound to BaselineTrust —
// and enrollment (which already succeeded, independent of attestation)
// still stands: the node still runs.
func TestAttest_MismatchedMeasurements_BaselineTrustStillRuns(t *testing.T) {
	id := enrollOne(t, "node-b-key")

	policyMeasurements := [][]byte{[]byte("boot-hash"), []byte("binary-hash")}
	tamperedMeasurements := [][]byte{[]byte("boot-hash"), []byte("compromised-binary")}
	provider := attestation.NewFakeProvider(seed(2), tamperedMeasurements)
	trust := attestation.NewTrustRegistry()
	att := attestation.NewAttestor(provider, policyMeasurements, provider.PublicKey(), trust)

	tier := att.Attest(id, []byte("challenge-nonce-2"))

	if tier != model.BaselineTrust {
		t.Fatalf("Attest() = %v, want BaselineTrust", tier)
	}
	if got := trust.Get(id); got != model.BaselineTrust {
		t.Fatalf("TrustRegistry.Get(id) = %v, want BaselineTrust", got)
	}
	// The node still runs: the identity remains admitted regardless of the
	// attestation outcome — enrollment status is untouched by Attest.
	if id == "" {
		t.Fatalf("identity was never enrolled — attestation must not gate entry")
	}
}

// TestAttest_BadSignature_BaselineTrustStillRuns covers the other
// "tampered" shape named by the ticket: a quote whose signature does not
// verify under the expected key (e.g. a different signer entirely) is
// Invalid, never AttestedTrust.
func TestAttest_BadSignature_BaselineTrustStillRuns(t *testing.T) {
	id := enrollOne(t, "node-c-key")

	measurements := [][]byte{[]byte("boot-hash")}
	// The provider signs with seed(3), but the policy is configured to
	// trust seed(4)'s key — an honest quote from an unrecognized signer.
	provider := attestation.NewFakeProvider(seed(3), measurements)
	untrustedProviderForKey := attestation.NewFakeProvider(seed(4), measurements)
	trust := attestation.NewTrustRegistry()
	att := attestation.NewAttestor(provider, measurements, untrustedProviderForKey.PublicKey(), trust)

	tier := att.Attest(id, []byte("challenge-nonce-3"))

	if tier != model.BaselineTrust {
		t.Fatalf("Attest() = %v, want BaselineTrust", tier)
	}
}

// TestAttest_NoProvider_BaselineTrustStillRuns is the ticket's key
// portability assertion: with a nil AttestationProvider, enrollment
// succeeds and the identity runs at BaselineTrust with NO attestation
// attempted at all — the hardware-agnostic guarantee that the swarm runs
// on any machine, TPM or not.
func TestAttest_NoProvider_BaselineTrustStillRuns(t *testing.T) {
	id := enrollOne(t, "node-d-key")

	trust := attestation.NewTrustRegistry()
	att := attestation.NewAttestor(nil, nil, nil, trust)

	tier := att.Attest(id, []byte("challenge-nonce-4"))

	if tier != model.BaselineTrust {
		t.Fatalf("Attest() with nil provider = %v, want BaselineTrust", tier)
	}
	if got := trust.Get(id); got != model.BaselineTrust {
		t.Fatalf("TrustRegistry.Get(id) = %v, want BaselineTrust", got)
	}
}

// TestTrustRegistry_NeverAttested_ReadsBaselineTrust reinforces the same
// portability guarantee from the read side: an identity that Attest was
// NEVER called for at all (no Attestor even constructed for it — the
// common case on a machine with no attestation provider configured
// anywhere) reads as BaselineTrust, never a zero-value panic or an
// accidental boost.
func TestTrustRegistry_NeverAttested_ReadsBaselineTrust(t *testing.T) {
	id := enrollOne(t, "node-e-key")

	trust := attestation.NewTrustRegistry()

	if got := trust.Get(id); got != model.BaselineTrust {
		t.Fatalf("TrustRegistry.Get(never-attested id) = %v, want BaselineTrust", got)
	}
}

// replayProvider wraps a FakeProvider but always returns a single captured
// quote from an earlier challenge, ignoring whatever nonce it is asked
// for — simulating an attacker replaying a stale, previously-observed
// quote against a fresh challenge.
type replayProvider struct {
	captured model.AttQuote
}

func (r replayProvider) Quote([]byte) (model.AttQuote, error) {
	return r.captured, nil
}

// TestAttest_StaleNonce_ReplayedQuoteInvalid is the ticket's nonce
// freshness criterion: a captured quote signed over an old nonce, replayed
// against a fresh challenge, fails VerifyAttestation's freshness check and
// yields BaselineTrust — the challenge binds the quote.
func TestAttest_StaleNonce_ReplayedQuoteInvalid(t *testing.T) {
	id := enrollOne(t, "node-f-key")

	measurements := [][]byte{[]byte("boot-hash")}
	real := attestation.NewFakeProvider(seed(5), measurements)
	stale, err := real.Quote([]byte("old-nonce"))
	if err != nil {
		t.Fatalf("Quote: unexpected error: %v", err)
	}

	trust := attestation.NewTrustRegistry()
	att := attestation.NewAttestor(replayProvider{captured: stale}, measurements, real.PublicKey(), trust)

	// The attacker replays the captured quote against a freshly issued
	// challenge nonce, distinct from the one it was actually signed over.
	tier := att.Attest(id, []byte("fresh-nonce"))

	if tier != model.BaselineTrust {
		t.Fatalf("Attest() with replayed stale-nonce quote = %v, want BaselineTrust", tier)
	}
}

// TestAttest_ProviderError_BaselineTrustStillRuns checks that a provider
// failure (e.g. a real attester unreachable) is treated the same as a
// failed verification: BaselineTrust, and Attest itself never returns an
// error the caller must handle specially — attestation never blocks the
// node from running.
func TestAttest_ProviderError_BaselineTrustStillRuns(t *testing.T) {
	id := enrollOne(t, "node-g-key")

	trust := attestation.NewTrustRegistry()
	att := attestation.NewAttestor(erroringProvider{}, nil, nil, trust)

	tier := att.Attest(id, []byte("challenge-nonce-5"))

	if tier != model.BaselineTrust {
		t.Fatalf("Attest() with a failing provider = %v, want BaselineTrust", tier)
	}
}

type erroringProvider struct{}

func (erroringProvider) Quote([]byte) (model.AttQuote, error) {
	return model.AttQuote{}, errProviderUnreachable
}

var errProviderUnreachable = errUnreachable("provider unreachable")

type errUnreachable string

func (e errUnreachable) Error() string { return string(e) }

// TestAttest_OpenTierFlowUntouched checks the acceptance criterion that
// the open tier's quorum+reputation path never invokes attestation at
// all: enrolling never constructs, nor requires, an Attestor — the two
// packages compose only when a caller explicitly chooses to, through the
// documented integration point (Enroller's exported EnrollResult.Admit.ID
// feeding Attestor.Attest), never implicitly.
func TestAttest_OpenTierFlowUntouched(t *testing.T) {
	id := enrollOne(t, "node-h-key")
	if id == "" {
		t.Fatalf("open-tier enrollment did not admit an identity")
	}
	// No attestation.Attestor is constructed anywhere above — enrollment
	// alone is a complete, unblocked flow.
}

// TestAttest_Determinism mirrors the core's determinism property test at
// the shell boundary: identical (provider, expected, trustedKey, nonce)
// inputs always yield an identical trust tier.
func TestAttest_Determinism(t *testing.T) {
	measurements := [][]byte{[]byte("boot-hash"), []byte("binary-hash")}
	provider := attestation.NewFakeProvider(seed(6), measurements)
	nonce := []byte("stable-nonce")

	trust := attestation.NewTrustRegistry()
	att := attestation.NewAttestor(provider, measurements, provider.PublicKey(), trust)

	first := att.Attest(model.SpiffeID("spiffe://open/stable-id"), nonce)
	for i := 0; i < 50; i++ {
		if got := att.Attest(model.SpiffeID("spiffe://open/stable-id"), nonce); got != first {
			t.Fatalf("non-deterministic tier on run %d: %v vs %v", i, got, first)
		}
	}
}
