package enrollment_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"testing"

	coreenrollment "github.com/msivraj/swarm/internal/core/enrollment"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/enrollment"
)

// findSolution deterministically brute-forces (a plain counter, never
// crypto/rand) the first Solution whose sha256(PubKey||Nonce||Solution)
// meets at least minBits leading zero bits — mirroring the fixture builder
// in internal/core/enrollment's own tests, since AdmitOpen's digest
// construction is package-private there and this is a shell test calling
// the core through its public API only.
func findSolution(t *testing.T, req model.JoinReq, minBits int) []byte {
	t.Helper()
	for i := uint64(0); i < 5_000_000; i++ {
		sol := make([]byte, 8)
		binary.BigEndian.PutUint64(sol, i)
		if leadingZeroBits(digest(req.PubKey, req.Nonce, sol)) >= minBits {
			return sol
		}
	}
	t.Fatalf("no PoW solution found under %d bits within search budget", minBits)
	return nil
}

func digest(pub model.PubKey, nonce, sol []byte) [sha256.Size]byte {
	h := sha256.New()
	h.Write(pub)
	h.Write(nonce)
	h.Write(sol)
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func leadingZeroBits(d [sha256.Size]byte) int {
	count := 0
	for _, b := range d {
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

func testKey(t *testing.T, seedByte byte) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	priv := ed25519.NewKeyFromSeed(seed)
	return priv.Public().(ed25519.PublicKey), priv
}

func newEnroller(t *testing.T, difficulty int) (*enrollment.Enroller, *enrollment.FakeIssuer, *enrollment.FakeBlacklist) {
	t.Helper()
	issuer := enrollment.NewFakeIssuer()
	blacklist := enrollment.NewFakeBlacklist()
	keys := enrollment.NewKeyring()
	e := enrollment.NewEnroller(model.PowCfg{DifficultyBits: difficulty}, issuer, blacklist, keys)
	return e, issuer, blacklist
}

// TestEnroll_ValidPoW_AdmitsAndIssuesIdentity is the ticket's first
// acceptance criterion: a join request with a valid PoW is admitted and the
// IdentityIssuer fake issues an identity for admit.ID.
func TestEnroll_ValidPoW_AdmitsAndIssuesIdentity(t *testing.T) {
	e, issuer, _ := newEnroller(t, 8)

	req := model.JoinReq{PubKey: model.PubKey("joiner-key"), Nonce: []byte("nonce-1")}
	sol := findSolution(t, req, 8)
	pow := model.PowProof{Nonce: req.Nonce, Solution: sol}

	result, err := e.Enroll(req, pow)
	if err != nil {
		t.Fatalf("Enroll: unexpected error: %v", err)
	}
	if result.Status != enrollment.StatusAccepted {
		t.Fatalf("Status = %v, want StatusAccepted", result.Status)
	}
	if result.Admit.Kind != model.Accept || result.Admit.ID == "" {
		t.Fatalf("Admit = %+v, want Accept with a non-empty SpiffeID", result.Admit)
	}
	if result.Cert.ID != result.Admit.ID {
		t.Fatalf("Cert.ID = %q, want %q", result.Cert.ID, result.Admit.ID)
	}
	if !issuer.Issued(result.Admit.ID) {
		t.Fatalf("issuer did not record an issuance for %q", result.Admit.ID)
	}
}

// TestEnroll_InvalidPoW_RejectsAndIssuesNothing is the ticket's first
// acceptance criterion's other half: an invalid/short PoW is rejected and
// no identity is issued.
func TestEnroll_InvalidPoW_RejectsAndIssuesNothing(t *testing.T) {
	e, issuer, _ := newEnroller(t, 16)

	req := model.JoinReq{PubKey: model.PubKey("joiner-key"), Nonce: []byte("nonce-1")}
	// A solution meeting only 8 bits is short of the required 16.
	short := findSolution(t, req, 8)
	shortDigestBits := leadingZeroBits(digest(req.PubKey, req.Nonce, short))
	if shortDigestBits >= 16 {
		t.Fatalf("test fixture bug: solution accidentally meets 16 bits (%d)", shortDigestBits)
	}
	pow := model.PowProof{Nonce: req.Nonce, Solution: short}

	result, err := e.Enroll(req, pow)
	if err != nil {
		t.Fatalf("Enroll: unexpected error: %v", err)
	}
	if result.Status != enrollment.StatusRejected {
		t.Fatalf("Status = %v, want StatusRejected", result.Status)
	}
	if result.Admit.Kind != model.Reject {
		t.Fatalf("Admit.Kind = %v, want Reject", result.Admit.Kind)
	}

	// No identity for the (deterministic) SpiffeID this PubKey would have
	// gotten had it been accepted.
	wouldBeID := coreenrollment.AdmitOpen(req, model.PowProof{Nonce: req.Nonce, Solution: findSolution(t, req, 0)}, model.PowCfg{}).ID
	if issuer.Issued(wouldBeID) {
		t.Fatalf("issuer recorded an issuance despite a rejected PoW")
	}
}

// TestEnroll_Blacklisted_RefusedBeforeIssuance is the ticket's third
// acceptance criterion: a blacklisted identity is refused at
// enrollment/admission — no identity is issued even though the PoW itself
// is valid.
func TestEnroll_Blacklisted_RefusedBeforeIssuance(t *testing.T) {
	e, issuer, blacklist := newEnroller(t, 8)

	req := model.JoinReq{PubKey: model.PubKey("bad-actor-key"), Nonce: []byte("nonce-2")}
	sol := findSolution(t, req, 8)
	pow := model.PowProof{Nonce: req.Nonce, Solution: sol}

	// Discover the SpiffeID this PubKey maps to (deterministic, per the
	// core's openSpiffeID law) and pre-blacklist it — mirroring how #141's
	// honeypot would have already blacklisted this identity from a prior
	// caught lie.
	wouldBeID := coreenrollment.AdmitOpen(req, pow, model.PowCfg{DifficultyBits: 8}).ID
	if wouldBeID == "" {
		t.Fatalf("test fixture bug: PoW fixture itself did not admit")
	}
	blacklist.Add(wouldBeID)

	result, err := e.Enroll(req, pow)
	if err != nil {
		t.Fatalf("Enroll: unexpected error: %v", err)
	}
	if result.Status != enrollment.StatusBlacklisted {
		t.Fatalf("Status = %v, want StatusBlacklisted", result.Status)
	}
	if issuer.Issued(wouldBeID) {
		t.Fatalf("issuer recorded an issuance for a blacklisted identity")
	}
}

// TestVerifyWorkload_UnsignedOrWrongKey_RefusedBeforeDispatch is the
// ticket's second acceptance criterion: an unsigned or wrong-key workload is
// refused before dispatch; a correctly-signed one proceeds.
func TestVerifyWorkload_UnsignedOrWrongKey_RefusedBeforeDispatch(t *testing.T) {
	e, _, _ := newEnroller(t, 0) // PoW disabled: any well-formed request admits.

	pub, priv := testKey(t, 0x01)
	_, otherPriv := testKey(t, 0x02)

	req := model.JoinReq{PubKey: model.PubKey(pub), Nonce: []byte("nonce-3")}
	pow := model.PowProof{Nonce: req.Nonce, Solution: []byte("unused-at-difficulty-zero")}

	result, err := e.Enroll(req, pow)
	if err != nil {
		t.Fatalf("Enroll: unexpected error: %v", err)
	}
	if result.Status != enrollment.StatusAccepted {
		t.Fatalf("Status = %v, want StatusAccepted", result.Status)
	}
	id := result.Admit.ID

	wl := []byte("some workload bytes to dispatch")
	genuineSig := model.Sig(ed25519.Sign(priv, wl))
	wrongKeySig := model.Sig(ed25519.Sign(otherPriv, wl))

	if e.VerifyWorkload(id, wl, nil) {
		t.Fatalf("VerifyWorkload accepted an unsigned (nil sig) workload")
	}
	if e.VerifyWorkload(id, wl, wrongKeySig) {
		t.Fatalf("VerifyWorkload accepted a workload signed under a different key")
	}
	if !e.VerifyWorkload(id, wl, genuineSig) {
		t.Fatalf("VerifyWorkload refused a correctly-signed workload")
	}
	// An identity never enrolled has no key to verify against.
	if e.VerifyWorkload(model.SpiffeID("spiffe://open/never-enrolled"), wl, genuineSig) {
		t.Fatalf("VerifyWorkload accepted a workload for an unenrolled identity")
	}
}

// TestEnroll_Concurrent stresses Enroll/VerifyWorkload under -race: many
// goroutines enroll distinct joiners concurrently, and every accepted
// identity must end up both issued and verifiable.
func TestEnroll_Concurrent(t *testing.T) {
	e, issuer, _ := newEnroller(t, 0)

	const n = 32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pub, priv := testKey(t, byte(i))
			req := model.JoinReq{PubKey: model.PubKey(pub), Nonce: []byte("nonce")}
			pow := model.PowProof{Nonce: req.Nonce, Solution: []byte("x")}

			result, err := e.Enroll(req, pow)
			if err != nil {
				t.Errorf("Enroll(%d): %v", i, err)
				return
			}
			if result.Status != enrollment.StatusAccepted {
				t.Errorf("Enroll(%d): Status = %v, want StatusAccepted", i, result.Status)
				return
			}
			wl := []byte("workload")
			sig := model.Sig(ed25519.Sign(priv, wl))
			if !e.VerifyWorkload(result.Admit.ID, wl, sig) {
				t.Errorf("VerifyWorkload(%d): refused a correctly-signed workload", i)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		pub, _ := testKey(t, byte(i))
		id := coreenrollment.AdmitOpen(
			model.JoinReq{PubKey: model.PubKey(pub), Nonce: []byte("nonce")},
			model.PowProof{Nonce: []byte("nonce"), Solution: []byte("x")},
			model.PowCfg{},
		).ID
		if !issuer.Issued(id) {
			t.Errorf("issuer never recorded issuance for goroutine %d's identity", i)
		}
	}
}
