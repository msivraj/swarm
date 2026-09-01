package attestation

import (
	"crypto/ed25519"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// testKey deterministically derives an ed25519 keypair from a fixed 32-byte
// seed — fcischeck bans crypto/rand (and math/rand) from every .go file
// under internal/core, tests included, so key material here is derived,
// never drawn. Distinct seed bytes yield distinct (but always reproducible)
// keypairs. Mirrors internal/core/enrollment/enrollment_test.go.
func testKey(seedByte byte) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	priv := ed25519.NewKeyFromSeed(seed)
	return priv.Public().(ed25519.PublicKey), priv
}

var (
	trustedPub, trustedPriv = testKey(1)
	otherPub, _             = testKey(2)
)

// signQuote builds a well-formed AttQuote: it signs signedBody(measurements,
// nonce) with priv and stamps signer as the claimed signing key.
func signQuote(priv ed25519.PrivateKey, signer ed25519.PublicKey, measurements [][]byte, nonce []byte) model.AttQuote {
	return model.AttQuote{
		Measurements: measurements,
		Nonce:        nonce,
		Signature:    model.Sig(ed25519.Sign(priv, signedBody(measurements, nonce))),
		Signer:       model.PubKey(signer),
	}
}

func basePolicy() model.AttPolicy {
	return model.AttPolicy{
		Expected:      [][]byte{[]byte("boot-hash"), []byte("binary-hash")},
		TrustedKey:    model.PubKey(trustedPub),
		ExpectedNonce: []byte("nonce-123"),
	}
}

func baseQuote() model.AttQuote {
	p := basePolicy()
	return signQuote(trustedPriv, trustedPub, p.Expected, p.ExpectedNonce)
}

// TestVerifyAttestation_validQuote is the ticket's named validQuote table
// test: a quote correctly signed by the trusted key, over the expected
// measurements and nonce, verifies as Valid and boosts trust.
func TestVerifyAttestation_validQuote(t *testing.T) {
	q := baseQuote()
	p := basePolicy()

	got := VerifyAttestation(q, p)
	want := model.AttResult{Valid: true, Measurements: q.Measurements}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("VerifyAttestation() = %+v, want %+v", got, want)
	}
	if tier := TrustFromAttestation(got); tier != model.AttestedTrust {
		t.Fatalf("TrustFromAttestation(valid) = %v, want AttestedTrust", tier)
	}
}

// TestVerifyAttestation_measurementOrderIndependent checks that measurement
// SET-MATCH (constraint 4) is order-independent, per §02/#183 fork (d) —
// the quote's measurements need not be listed in the policy's order, only
// the signed body's own encoding order matters for the signature.
func TestVerifyAttestation_measurementOrderIndependent(t *testing.T) {
	p := basePolicy()
	reordered := [][]byte{[]byte("binary-hash"), []byte("boot-hash")}
	q := signQuote(trustedPriv, trustedPub, reordered, p.ExpectedNonce)

	got := VerifyAttestation(q, p)
	if !got.Valid {
		t.Fatalf("VerifyAttestation() with reordered measurements = %+v, want Valid", got)
	}
}

// TestVerifyAttestation_tamperInvalid is the ticket's named tamperInvalid
// property test: every enumerated adversarial mutation of a validly-signed
// quote must fail closed to Invalid, and never boost trust above baseline.
// A minority-tampered quote can never reach AttestedTrust.
func TestVerifyAttestation_tamperInvalid(t *testing.T) {
	p := basePolicy()

	tests := []struct {
		name  string
		build func() model.AttQuote
	}{
		{
			name: "flipped measurement byte",
			build: func() model.AttQuote {
				q := baseQuote()
				tampered := append([]byte(nil), q.Measurements[0]...)
				tampered[0] ^= 0xFF
				q.Measurements = [][]byte{tampered, q.Measurements[1]}
				return q
			},
		},
		{
			name: "wrong expected measurement",
			build: func() model.AttQuote {
				return signQuote(trustedPriv, trustedPub, [][]byte{[]byte("wrong-hash"), []byte("binary-hash")}, p.ExpectedNonce)
			},
		},
		{
			name: "missing measurement",
			build: func() model.AttQuote {
				return signQuote(trustedPriv, trustedPub, [][]byte{[]byte("boot-hash")}, p.ExpectedNonce)
			},
		},
		{
			name: "extra measurement",
			build: func() model.AttQuote {
				return signQuote(trustedPriv, trustedPub, [][]byte{[]byte("boot-hash"), []byte("binary-hash"), []byte("extra-hash")}, p.ExpectedNonce)
			},
		},
		{
			name: "duplicate measurement masking a missing one",
			build: func() model.AttQuote {
				return signQuote(trustedPriv, trustedPub, [][]byte{[]byte("boot-hash"), []byte("boot-hash")}, p.ExpectedNonce)
			},
		},
		{
			name: "wrong nonce (replay of stale quote)",
			build: func() model.AttQuote {
				return signQuote(trustedPriv, trustedPub, p.Expected, []byte("stale-nonce"))
			},
		},
		{
			name: "wrong signer key",
			build: func() model.AttQuote {
				return signQuote(trustedPriv, otherPub, p.Expected, p.ExpectedNonce)
			},
		},
		{
			name: "corrupted signature",
			build: func() model.AttQuote {
				q := baseQuote()
				sig := append([]byte(nil), q.Signature...)
				sig[0] ^= 0xFF
				q.Signature = model.Sig(sig)
				return q
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VerifyAttestation(tt.build(), p)
			if got.Valid {
				t.Fatalf("VerifyAttestation() = %+v, want Invalid", got)
			}
			if !reflect.DeepEqual(got, model.AttResult{}) {
				t.Fatalf("Invalid result should be the zero value, got %+v", got)
			}
			if tier := TrustFromAttestation(got); tier != model.BaselineTrust {
				t.Fatalf("TrustFromAttestation(invalid) = %v, want BaselineTrust", tier)
			}
		})
	}
}

// TestTrustFromAttestation_absentBaseline is the ticket's named
// absentBaseline test: the zero AttResult (no provider / absent
// attestation) maps to BaselineTrust — the node still runs; attestation is
// a boost, never a gate.
func TestTrustFromAttestation_absentBaseline(t *testing.T) {
	tests := []struct {
		name string
		r    model.AttResult
	}{
		{"zero value (no attestation provider)", model.AttResult{}},
		{"explicit invalid", model.AttResult{Valid: false, Measurements: [][]byte{[]byte("stale")}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TrustFromAttestation(tt.r); got != model.BaselineTrust {
				t.Fatalf("TrustFromAttestation(%+v) = %v, want BaselineTrust", tt.r, got)
			}
		})
	}
}

// TestVerifyAttestation_malformedInput checks that malformed key/signature
// material — wrong length or nil — yields Invalid, never a panic.
func TestVerifyAttestation_malformedInput(t *testing.T) {
	p := basePolicy()
	q := baseQuote()

	tests := []struct {
		name string
		q    model.AttQuote
		p    model.AttPolicy
	}{
		{
			name: "nil signer",
			q:    model.AttQuote{Measurements: q.Measurements, Nonce: q.Nonce, Signature: q.Signature, Signer: nil},
			p:    p,
		},
		{
			name: "short signer key",
			q:    model.AttQuote{Measurements: q.Measurements, Nonce: q.Nonce, Signature: q.Signature, Signer: model.PubKey(trustedPub[:8])},
			p:    p,
		},
		{
			name: "nil signature",
			q:    model.AttQuote{Measurements: q.Measurements, Nonce: q.Nonce, Signature: nil, Signer: q.Signer},
			p:    p,
		},
		{
			name: "truncated signature",
			q:    model.AttQuote{Measurements: q.Measurements, Nonce: q.Nonce, Signature: model.Sig(q.Signature[:10]), Signer: q.Signer},
			p:    p,
		},
		{
			name: "nil trusted key in policy",
			q:    q,
			p:    model.AttPolicy{Expected: p.Expected, TrustedKey: nil, ExpectedNonce: p.ExpectedNonce},
		},
		{
			name: "short trusted key in policy",
			q:    q,
			p:    model.AttPolicy{Expected: p.Expected, TrustedKey: model.PubKey(trustedPub[:4]), ExpectedNonce: p.ExpectedNonce},
		},
		{
			name: "entirely empty quote and policy",
			q:    model.AttQuote{},
			p:    model.AttPolicy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("VerifyAttestation panicked: %v", r)
				}
			}()
			got := VerifyAttestation(tt.q, tt.p)
			if got.Valid {
				t.Fatalf("VerifyAttestation() = %+v, want Invalid", got)
			}
		})
	}
}

// TestVerifyAttestation_determinism is a property test mirroring
// mitosis's TestGateIsDeterministic: identical (quote, policy) inputs
// always yield an identical AttResult.
func TestVerifyAttestation_determinism(t *testing.T) {
	q := baseQuote()
	p := basePolicy()

	first := VerifyAttestation(q, p)
	for i := 0; i < 100; i++ {
		if got := VerifyAttestation(q, p); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// TestTrustFromAttestation_determinism checks the second pure mapping is
// likewise deterministic for both branches.
func TestTrustFromAttestation_determinism(t *testing.T) {
	tests := []model.AttResult{
		{},
		{Valid: true, Measurements: [][]byte{[]byte("a")}},
	}
	for _, r := range tests {
		first := TrustFromAttestation(r)
		for i := 0; i < 100; i++ {
			if got := TrustFromAttestation(r); got != first {
				t.Fatalf("non-deterministic output on run %d for %+v: %v vs %v", i, r, got, first)
			}
		}
	}
}

// TestSignedBody_noBoundarySmuggling checks the canonicalization used to
// build the signed body is unambiguous: two different measurement splits
// whose naive concatenation would collide must produce distinct encodings.
func TestSignedBody_noBoundarySmuggling(t *testing.T) {
	a := signedBody([][]byte{[]byte("ab"), []byte("c")}, []byte("n"))
	b := signedBody([][]byte{[]byte("a"), []byte("bc")}, []byte("n"))
	if reflect.DeepEqual(a, b) {
		t.Fatalf("signedBody collided across a measurement boundary: %v == %v", a, b)
	}
}

// TestMeasurementsMatch_tableTest is a direct table-driven test of the
// order-independent multiset comparison measurementsMatch performs.
func TestMeasurementsMatch_tableTest(t *testing.T) {
	tests := []struct {
		name          string
		got, expected [][]byte
		want          bool
	}{
		{"exact match, same order", [][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte("a"), []byte("b")}, true},
		{"exact match, reordered", [][]byte{[]byte("b"), []byte("a")}, [][]byte{[]byte("a"), []byte("b")}, true},
		{"missing", [][]byte{[]byte("a")}, [][]byte{[]byte("a"), []byte("b")}, false},
		{"extra", [][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte("a"), []byte("b")}, false},
		{"duplicate masking missing", [][]byte{[]byte("a"), []byte("a")}, [][]byte{[]byte("a"), []byte("b")}, false},
		{"both empty", nil, nil, true},
		{"wrong content, same length", [][]byte{[]byte("x")}, [][]byte{[]byte("a")}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := measurementsMatch(tt.got, tt.expected); got != tt.want {
				t.Fatalf("measurementsMatch(%v, %v) = %v, want %v", tt.got, tt.expected, got, tt.want)
			}
		})
	}
}
