package enrollment

import (
	"crypto/ed25519"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// testKey deterministically derives an ed25519 keypair from a fixed 32-byte
// seed — fcischeck bans crypto/rand from every .go file under
// internal/core, tests included, so key material here is derived, not
// drawn. Distinct seed bytes yield distinct (but always reproducible)
// keypairs.
func testKey(seedByte byte) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	priv := ed25519.NewKeyFromSeed(seed)
	return priv.Public().(ed25519.PublicKey), priv
}

// findSolution brute-force searches (deterministically — a plain counter,
// never a random draw) for the first Solution whose powDigest against req
// meets at least minBits leading zero bits. It exists only to build fixed,
// reproducible test fixtures; the core itself never searches for a proof,
// only verifies one.
func findSolution(t *testing.T, req model.JoinReq, minBits int) []byte {
	t.Helper()
	for i := uint64(0); ; i++ {
		sol := make([]byte, 8)
		binary.BigEndian.PutUint64(sol, i)
		pow := model.PowProof{Nonce: req.Nonce, Solution: sol}
		if leadingZeroBits(powDigest(req, pow)) >= minBits {
			return sol
		}
		if i > 5_000_000 {
			t.Fatalf("no solution found under %d bits within search budget", minBits)
		}
	}
}

func TestAdmitOpen(t *testing.T) {
	baseReq := model.JoinReq{PubKey: model.PubKey("joiner-key"), Nonce: []byte("nonce-1")}
	sol8 := findSolution(t, baseReq, 8) // meets >=8 leading zero bits

	tests := []struct {
		name string
		req  model.JoinReq
		pow  model.PowProof
		cfg  model.PowCfg
		want model.Admit
	}{
		{
			name: "solution meets difficulty accepts with non-empty SpiffeID",
			req:  baseReq,
			pow:  model.PowProof{Nonce: baseReq.Nonce, Solution: sol8},
			cfg:  model.PowCfg{DifficultyBits: 8},
			want: model.Admit{Kind: model.Accept, ID: openSpiffeID(baseReq.PubKey)},
		},
		{
			name: "one bit short of difficulty rejects",
			req:  baseReq,
			pow:  model.PowProof{Nonce: baseReq.Nonce, Solution: sol8},
			cfg:  model.PowCfg{DifficultyBits: leadingZeroBits(powDigest(baseReq, model.PowProof{Nonce: baseReq.Nonce, Solution: sol8})) + 1},
			want: model.Admit{},
		},
		{
			name: "tampered solution rejects",
			req:  baseReq,
			pow:  model.PowProof{Nonce: baseReq.Nonce, Solution: tamper(sol8)},
			cfg:  model.PowCfg{DifficultyBits: 8},
			want: model.Admit{},
		},
		{
			name: "tampered nonce rejects regardless of digest",
			req:  baseReq,
			pow:  model.PowProof{Nonce: []byte("wrong-nonce"), Solution: sol8},
			cfg:  model.PowCfg{DifficultyBits: 0},
			want: model.Admit{},
		},
		{
			name: "difficulty zero accepts any well-formed request regardless of solution",
			req:  baseReq,
			pow:  model.PowProof{Nonce: baseReq.Nonce, Solution: []byte("garbage-unsolved")},
			cfg:  model.PowCfg{DifficultyBits: 0},
			want: model.Admit{Kind: model.Accept, ID: openSpiffeID(baseReq.PubKey)},
		},
		{
			name: "empty PubKey is never well-formed, even at difficulty zero",
			req:  model.JoinReq{PubKey: nil, Nonce: baseReq.Nonce},
			pow:  model.PowProof{Nonce: baseReq.Nonce, Solution: []byte("x")},
			cfg:  model.PowCfg{DifficultyBits: 0},
			want: model.Admit{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AdmitOpen(tt.req, tt.pow, tt.cfg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("AdmitOpen() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// tamper flips the low bit of the last byte of a solution, producing a
// distinct candidate whose sha256 digest (by the hash's avalanche property)
// no longer clears the difficulty the original solution cleared. Both
// solution and cfg are fixed test data, so this is fully deterministic —
// not a probabilistic assertion — even though it isn't proven a priori.
func tamper(sol []byte) []byte {
	out := append([]byte(nil), sol...)
	out[len(out)-1] ^= 0x01
	return out
}

// TestAdmitOpenIsDeterministic mirrors TestGateIsDeterministic in
// internal/core/mitosis: identical inputs must yield identical output on
// every call, since the core reads no clock and draws no randomness.
func TestAdmitOpenIsDeterministic(t *testing.T) {
	req := model.JoinReq{PubKey: model.PubKey("stable-key"), Nonce: []byte("stable-nonce")}
	sol := findSolution(t, req, 10)
	pow := model.PowProof{Nonce: req.Nonce, Solution: sol}
	cfg := model.PowCfg{DifficultyBits: 10}

	first := AdmitOpen(req, pow, cfg)
	for i := 0; i < 100; i++ {
		if got := AdmitOpen(req, pow, cfg); !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d: non-deterministic output: %+v vs %+v", i, got, first)
		}
	}
}

// TestOpenSpiffeIDDeterministic asserts the identity derivation law the
// ticket names directly: the same PubKey always maps to the same SpiffeID,
// and distinct keys map to distinct ids — re-enrolling under the same key
// buys a Sybil no new identity.
func TestOpenSpiffeIDDeterministic(t *testing.T) {
	keyA := model.PubKey("key-a")
	keyB := model.PubKey("key-b")

	idA1 := openSpiffeID(keyA)
	idA2 := openSpiffeID(keyA)
	idB := openSpiffeID(keyB)

	if idA1 != idA2 {
		t.Fatalf("openSpiffeID(keyA) not stable: %q vs %q", idA1, idA2)
	}
	if idA1 == "" {
		t.Fatal("openSpiffeID returned empty id")
	}
	if idA1 == idB {
		t.Fatalf("distinct PubKeys collided: keyA and keyB both mapped to %q", idA1)
	}
}

// TestAdmitOpenSpiffeIDTable is the table test the acceptance criteria name
// directly: distinct AdmitOpen calls for the same joiner (same PubKey) —
// even under different nonces/solutions — always accept with the same
// SpiffeID.
func TestAdmitOpenSpiffeIDTable(t *testing.T) {
	pub := model.PubKey("same-joiner")
	cfg := model.PowCfg{DifficultyBits: 0}

	req1 := model.JoinReq{PubKey: pub, Nonce: []byte("n1")}
	req2 := model.JoinReq{PubKey: pub, Nonce: []byte("n2")}

	got1 := AdmitOpen(req1, model.PowProof{Nonce: req1.Nonce, Solution: []byte("s1")}, cfg)
	got2 := AdmitOpen(req2, model.PowProof{Nonce: req2.Nonce, Solution: []byte("s2")}, cfg)

	if got1.Kind != model.Accept || got2.Kind != model.Accept {
		t.Fatalf("expected both accepted at difficulty 0: got1=%+v got2=%+v", got1, got2)
	}
	if got1.ID != got2.ID {
		t.Fatalf("same joiner PubKey issued different SpiffeIDs: %q vs %q", got1.ID, got2.ID)
	}

	other := AdmitOpen(model.JoinReq{PubKey: model.PubKey("different-joiner"), Nonce: []byte("n3")},
		model.PowProof{Nonce: []byte("n3"), Solution: []byte("s3")}, cfg)
	if other.ID == got1.ID {
		t.Fatalf("different joiner PubKey collided with %q", got1.ID)
	}
}

func TestVerifySignature(t *testing.T) {
	pub, priv := testKey(0x01)
	wl := []byte("workload bytes to sign")
	sig := ed25519.Sign(priv, wl)

	otherPub, _ := testKey(0x02)

	flippedSig := append([]byte(nil), sig...)
	flippedSig[0] ^= 0x01

	tests := []struct {
		name string
		wl   []byte
		sig  model.Sig
		key  model.PubKey
		want bool
	}{
		{
			name: "genuine signature verifies",
			wl:   wl,
			sig:  model.Sig(sig),
			key:  model.PubKey(pub),
			want: true,
		},
		{
			name: "wrong key rejects",
			wl:   wl,
			sig:  model.Sig(sig),
			key:  model.PubKey(otherPub),
			want: false,
		},
		{
			name: "flipped signature byte rejects",
			wl:   wl,
			sig:  model.Sig(flippedSig),
			key:  model.PubKey(pub),
			want: false,
		},
		{
			name: "tampered payload rejects",
			wl:   append([]byte(nil), append(wl, 'x')...),
			sig:  model.Sig(sig),
			key:  model.PubKey(pub),
			want: false,
		},
		{
			name: "truncated signature rejects without panic",
			sig:  model.Sig(sig[:len(sig)-1]),
			key:  model.PubKey(pub),
			wl:   wl,
			want: false,
		},
		{
			name: "nil signature rejects without panic",
			wl:   wl,
			sig:  nil,
			key:  model.PubKey(pub),
			want: false,
		},
		{
			name: "nil key rejects without panic",
			wl:   wl,
			sig:  model.Sig(sig),
			key:  nil,
			want: false,
		},
		{
			name: "oversized key rejects without panic",
			wl:   wl,
			sig:  model.Sig(sig),
			key:  append(model.PubKey(pub), 0x00),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := VerifySignature(tt.wl, tt.sig, tt.key); got != tt.want {
				t.Fatalf("VerifySignature() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestVerifySignatureIsDeterministic mirrors TestGateIsDeterministic: the
// same (wl, sig, key) always verifies the same way.
func TestVerifySignatureIsDeterministic(t *testing.T) {
	pub, priv := testKey(0x03)
	wl := []byte("deterministic payload")
	sig := ed25519.Sign(priv, wl)

	first := VerifySignature(wl, model.Sig(sig), model.PubKey(pub))
	for i := 0; i < 100; i++ {
		if got := VerifySignature(wl, model.Sig(sig), model.PubKey(pub)); got != first {
			t.Fatalf("run %d: non-deterministic output: %v vs %v", i, got, first)
		}
	}
}

func TestLeadingZeroBits(t *testing.T) {
	tests := []struct {
		name   string
		digest [32]byte
		want   int
	}{
		{name: "all zero", digest: [32]byte{}, want: 256},
		{name: "msb set", digest: [32]byte{0x80}, want: 0},
		{name: "one zero byte then set bit", digest: [32]byte{0x00, 0x40}, want: 9},
		{name: "single leading zero bit", digest: [32]byte{0x7f}, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := leadingZeroBits(tt.digest); got != tt.want {
				t.Fatalf("leadingZeroBits() = %d, want %d", got, tt.want)
			}
		})
	}
}
