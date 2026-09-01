// p5_security_capstone_test.go is the P5 security-audit capstone (issue
// #193, design ruling #183 fork (e)): a consolidated ADVERSARIAL property
// suite over ALL of the pure security cores spanning P3 and P5.
//
// Every property here composes the REAL, already-merged pure cores as black
// boxes — internal/core/verification, internal/core/reputation,
// internal/core/enrollment, internal/core/attestation, and
// internal/core/tenancy — calling only their EXPORTED functions. Nothing is
// reimplemented; where a fixture needs to be built against a documented
// contract (e.g. the PoW digest formula, or an attestation quote's signed
// body encoding), the doc comment on the relevant exported function is the
// only source of truth, and the real function is what actually decides.
//
// Per docs/phases/swarm-p5-components.txt §03, the security audit is
// tractable exactly because these are pure, property-tested functions: an
// auditor can answer "can a minority flip a verdict?" or "can an over-quota
// tenant sneak a job through?" by reading one function and its tests. This
// file is the single artifact that asks every one of §03's named questions
// at once, hermetically and deterministically — no network, no clock, no
// crypto/rand or math/rand anywhere below (fixed ed25519.NewKeyFromSeed keys
// and enumerated/swept vectors only), so it is stable under
// `-race -count=N`.
package e2e

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/msivraj/swarm/internal/core/attestation"
	"github.com/msivraj/swarm/internal/core/enrollment"
	"github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/core/tenancy"
	"github.com/msivraj/swarm/internal/core/verification"
	"github.com/msivraj/swarm/internal/model"
)

// p5secKey deterministically derives an ed25519 keypair from seedByte —
// mirroring internal/core/enrollment's and internal/core/attestation's own
// test helpers. fcischeck bans crypto/rand from core (tests included); this
// suite holds itself to the same fixed-seed discipline even though
// internal/e2e is shell-scoped, so every fixture here is bit-for-bit
// reproducible across runs.
func p5secKey(seedByte byte) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	priv := ed25519.NewKeyFromSeed(seed)
	return priv.Public().(ed25519.PublicKey), priv
}

// -----------------------------------------------------------------------
// minorityCannotFlip — verification.Verdict
// -----------------------------------------------------------------------

// TestSecurityCapstone_minorityCannotFlip is the §03 "verdict" property,
// consolidated: whenever honest machines hold a strict majority of K
// collected results, an adversarial minority of liars — no matter what
// values they claim — can never change the Agreed value away from the
// honest one, and can never force Disputed. Swept across many K sizes, every
// liar count up to the largest that still leaves honest machines a strict
// majority, and several distinct adversarial liar-value strategies.
func TestSecurityCapstone_minorityCannotFlip(t *testing.T) {
	honestValue := []byte("the-one-honest-answer")

	// Adversarial strategies for how the minority spends its votes: united
	// behind a single rival value (the best chance of building a
	// competing count), scattered across many distinct values (noise), or
	// crafted to be a near-miss of the honest value itself (never an exact
	// collision, since Verdict compares by byte value).
	strategies := []struct {
		name string
		gen  func(n int) [][]byte
	}{
		{"liars unite behind one rival value", func(n int) [][]byte {
			out := make([][]byte, n)
			for i := range out {
				out[i] = []byte("colluding-rival-value")
			}
			return out
		}},
		{"each liar claims a distinct value", func(n int) [][]byte {
			out := make([][]byte, n)
			for i := range out {
				out[i] = []byte(fmt.Sprintf("distinct-lie-%d", i))
			}
			return out
		}},
		{"liars claim near-misses of the honest value", func(n int) [][]byte {
			out := make([][]byte, n)
			for i := range out {
				out[i] = append(append([]byte{}, honestValue...), byte('0'+i%10))
			}
			return out
		}},
	}

	ks := []int{3, 5, 7, 9, 15, 21, 31, 51}
	for _, k := range ks {
		maxLiars := (k - 1) / 2 // largest liar count that still leaves honest a strict majority
		for liars := 0; liars <= maxLiars; liars++ {
			honestCount := k - liars
			for _, strategy := range strategies {
				name := fmt.Sprintf("K=%d/liars=%d/%s", k, liars, strategy.name)
				t.Run(name, func(t *testing.T) {
					var rs []model.Result
					for i := 0; i < honestCount; i++ {
						rs = append(rs, model.Result{
							ID:    model.SpiffeID(fmt.Sprintf("honest-%d", i)),
							Value: honestValue,
							OK:    true,
						})
					}
					for i, v := range strategy.gen(liars) {
						rs = append(rs, model.Result{
							ID:    model.SpiffeID(fmt.Sprintf("liar-%d", i)),
							Value: v,
							OK:    true,
						})
					}

					got := verification.Verdict(rs)
					if got.Kind != model.Agreed {
						t.Fatalf("Verdict.Kind = %v, want Agreed (honest majority %d of %d) — a minority forced a non-Agreed verdict", got.Kind, honestCount, k)
					}
					if !bytes.Equal(got.Value, honestValue) {
						t.Fatalf("Verdict.Value = %q, want the honest value %q — a minority flipped the verdict", got.Value, honestValue)
					}
				})
			}
		}
	}
}

// -----------------------------------------------------------------------
// sybilEarnsNothing — reputation.Update / Weight / NeedsK
// -----------------------------------------------------------------------

// TestSecurityCapstone_sybilEarnsNothing is the §03 "zero-start" +
// "update"/"weight"/"needsK" properties, consolidated: every fresh identity
// starts at the trust floor, a lie never raises reputation, honest work
// never lowers it, and — the Sybil-specific consolidation — minting many
// fresh identities buys an adversary nothing better than a single identity
// that has already been caught lying down to the same floor. Only sustained
// honest behavior earns a better standing.
func TestSecurityCapstone_sybilEarnsNothing(t *testing.T) {
	fresh := model.Reputation{}

	if w := reputation.Weight(fresh); w != 0 {
		t.Fatalf("Weight(zero-value Reputation) = %v, want 0 (the trust floor)", w)
	}

	// Discover the cap Update climbs Score to under repeated honest
	// agreement, dynamically — never hardcoded — so this test does not
	// depend on reputation's private constants.
	capRep := fresh
	for i := 0; i < 10_000; i++ {
		next := reputation.Update(capRep, true)
		if next == capRep {
			break
		}
		capRep = next
	}
	if capRep == fresh {
		t.Fatal("repeated honest agreement never moved Score off the zero-value floor — reputation cannot be earned at all")
	}

	// A maximally-earned identity never needs MORE redundancy than a fresh
	// one, at either tier.
	if got, fr := reputation.NeedsK(capRep, model.Open), reputation.NeedsK(fresh, model.Open); got > fr {
		t.Fatalf("max-earned identity needs MORE open-tier redundancy (%d) than a fresh one (%d)", got, fr)
	}
	if got, fr := reputation.NeedsK(capRep, model.Core), reputation.NeedsK(fresh, model.Core); got > fr {
		t.Fatalf("max-earned identity needs MORE core-tier redundancy (%d) than a fresh one (%d)", got, fr)
	}

	// Monotonicity, swept over a dense range of starting reputations
	// (including the discovered cap and several points either side of a
	// quorum step boundary): a lie never raises Score, honest agreement
	// never lowers it, and Score never leaves the [0, cap] range.
	starts := []model.Reputation{
		{},
		{Score: 1, Observations: 1},
		{Score: 50, Observations: 3},
		{Score: 199, Observations: 3},
		{Score: 200, Observations: 10},
		{Score: 201, Observations: 10},
		{Score: 999, Observations: 500},
		capRep,
	}
	for _, start := range starts {
		honest := reputation.Update(start, true)
		lied := reputation.Update(start, false)
		if honest.Score < start.Score {
			t.Errorf("start=%+v: honest agreement LOWERED Score: %d -> %d", start, start.Score, honest.Score)
		}
		if lied.Score > start.Score {
			t.Errorf("start=%+v: a lie RAISED Score: %d -> %d", start, start.Score, lied.Score)
		}
		if lied.Score < 0 {
			t.Errorf("start=%+v: a lie pushed Score below the zero floor: %d", start, lied.Score)
		}
		if honest.Score > capRep.Score {
			t.Errorf("start=%+v: honest agreement pushed Score above the discovered cap %d: got %d", start, capRep.Score, honest.Score)
		}
	}

	// Sybil-mints-nothing: N freshly-minted identities, each caught lying
	// exactly once, land on IDENTICAL reputation — being newly minted buys
	// none of them a better (or worse) outcome than any other.
	const nSybils = 50
	sybilOutcomes := make([]model.Reputation, nSybils)
	for i := range sybilOutcomes {
		sybilOutcomes[i] = reputation.Update(model.Reputation{}, false)
	}
	for i, r := range sybilOutcomes {
		if r != sybilOutcomes[0] {
			t.Fatalf("sybil %d landed on reputation %+v, want the same as sybil 0's %+v", i, r, sybilOutcomes[0])
		}
	}

	// The floor a fresh Sybil's very first lie lands on is EXACTLY the
	// floor a long-lived, repeatedly-caught liar is clamped to — minting a
	// new identity buys no escape from an existing bad reputation, and no
	// worse outcome either.
	chronicLiar := model.Reputation{Score: 500, Observations: 200}
	for i := 0; i < 20; i++ {
		chronicLiar = reputation.Update(chronicLiar, false)
	}
	if chronicLiar.Score != sybilOutcomes[0].Score {
		t.Fatalf("chronic liar's floor Score (%d) != a fresh Sybil's first-lie Score (%d)", chronicLiar.Score, sybilOutcomes[0].Score)
	}
	if reputation.NeedsK(chronicLiar, model.Open) != reputation.NeedsK(sybilOutcomes[0], model.Open) {
		t.Fatal("chronic liar and fresh Sybil demand different Open redundancy despite equal Score")
	}
	if reputation.Weight(chronicLiar) != reputation.Weight(sybilOutcomes[0]) {
		t.Fatal("chronic liar and fresh Sybil have different Weight despite equal Score")
	}

	// Contrast: only SUSTAINED honest behavior earns a better standing than
	// a fresh mint — an honestly-earned identity's Weight is strictly
	// higher, and its Open NeedsK strictly lower, than every fresh Sybil's.
	earned := model.Reputation{}
	for i := 0; i < 50; i++ {
		earned = reputation.Update(earned, true)
	}
	if reputation.Weight(earned) <= reputation.Weight(fresh) {
		t.Fatalf("50 rounds of honest agreement did not raise Weight above a fresh identity's: earned=%v fresh=%v", reputation.Weight(earned), reputation.Weight(fresh))
	}
	if reputation.NeedsK(earned, model.Open) >= reputation.NeedsK(fresh, model.Open) {
		t.Fatalf("50 rounds of honest agreement did not lower Open NeedsK below a fresh identity's: earned=%d fresh=%d", reputation.NeedsK(earned, model.Open), reputation.NeedsK(fresh, model.Open))
	}
}

// -----------------------------------------------------------------------
// admissionHolds — enrollment.AdmitOpen / VerifySignature
// -----------------------------------------------------------------------

// p5secPowDigest mirrors, for fixture construction ONLY, the digest formula
// enrollment.AdmitOpen's doc comment documents: sha256(PubKey || Nonce ||
// Solution). The admission decision under test always goes through the
// real, exported AdmitOpen — this only lets the test search for/confirm
// valid proof-of-work fixtures deterministically.
func p5secPowDigest(req model.JoinReq, pow model.PowProof) [sha256.Size]byte {
	h := sha256.New()
	h.Write(req.PubKey)
	h.Write(req.Nonce)
	h.Write(pow.Solution)
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

// p5secLeadingZeroBits counts leading zero bits from the digest's most
// significant bit, matching AdmitOpen's documented difficulty rule.
func p5secLeadingZeroBits(digest [sha256.Size]byte) int {
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

// p5secFindSolution deterministically brute-forces (a plain incrementing
// counter, never a random draw) the first Solution whose digest against req
// clears at least minBits leading zero bits.
func p5secFindSolution(t *testing.T, req model.JoinReq, minBits int) []byte {
	t.Helper()
	for i := uint64(0); ; i++ {
		sol := make([]byte, 8)
		binary.BigEndian.PutUint64(sol, i)
		pow := model.PowProof{Nonce: req.Nonce, Solution: sol}
		if p5secLeadingZeroBits(p5secPowDigest(req, pow)) >= minBits {
			return sol
		}
		if i > 5_000_000 {
			t.Fatalf("no PoW solution found under %d bits within search budget", minBits)
		}
	}
}

// TestSecurityCapstone_admissionHolds is the §03 Sybil/PoW + signature
// integrity property, consolidated: enrollment.AdmitOpen rejects a proof
// that falls short of the configured difficulty, rejects a proof solved
// against a different request's nonce (no replay), and
// enrollment.VerifySignature rejects every unsigned/wrong-key/tampered
// workload.
func TestSecurityCapstone_admissionHolds(t *testing.T) {
	t.Run("a proof below difficulty is rejected", func(t *testing.T) {
		req := model.JoinReq{PubKey: model.PubKey("attacker-key"), Nonce: []byte("attacker-nonce")}
		sol := p5secFindSolution(t, req, 8)
		pow := model.PowProof{Nonce: req.Nonce, Solution: sol}
		achieved := p5secLeadingZeroBits(p5secPowDigest(req, pow))

		if got := enrollment.AdmitOpen(req, pow, model.PowCfg{DifficultyBits: achieved}); got.Kind != model.Accept {
			t.Fatalf("AdmitOpen at exactly the achieved difficulty (%d) = %+v, want Accept", achieved, got)
		}
		for _, extra := range []int{1, 2, 5, 20, 100} {
			cfg := model.PowCfg{DifficultyBits: achieved + extra}
			if got := enrollment.AdmitOpen(req, pow, cfg); got.Kind != model.Reject {
				t.Errorf("AdmitOpen(difficulty=achieved+%d=%d) = %+v, want Reject — a below-difficulty proof was admitted", extra, achieved+extra, got)
			}
		}
	})

	t.Run("a proof solved against a different request's nonce is rejected (no replay)", func(t *testing.T) {
		pubKey := model.PubKey("replay-attacker-key")
		reqA := model.JoinReq{PubKey: pubKey, Nonce: []byte("nonce-A")}
		reqB := model.JoinReq{PubKey: pubKey, Nonce: []byte("nonce-B-different")}

		solA := p5secFindSolution(t, reqA, 8)
		cfg := model.PowCfg{DifficultyBits: 8}

		// Sanity: the honestly-solved proof is admitted against its OWN
		// request.
		if got := enrollment.AdmitOpen(reqA, model.PowProof{Nonce: reqA.Nonce, Solution: solA}, cfg); got.Kind != model.Accept {
			t.Fatalf("AdmitOpen(reqA, its own solved proof) = %+v, want Accept", got)
		}

		// Replay: submit reqB (a fresh join request) but attach reqA's old
		// PoW nonce unchanged. The proof no longer echoes reqB's OWN nonce,
		// so it is rejected outright, regardless of what the digest is.
		replayed := model.PowProof{Nonce: reqA.Nonce, Solution: solA}
		if got := enrollment.AdmitOpen(reqB, replayed, cfg); got.Kind != model.Reject {
			t.Fatalf("AdmitOpen(reqB, reqA's proof replayed unmodified) = %+v, want Reject", got)
		}

		// Even if the attacker forges pow.Nonce to reqB's nonce (passing
		// the echo check) while recycling reqA's solution, the nonce is
		// mixed into the digest, so the recycled solution's digest against
		// reqB is a DIFFERENT hash than the one it was actually solved
		// for — proven structurally here, not merely asserted by a
		// fixed-difficulty accept/reject that could get lucky.
		forgedNonce := model.PowProof{Nonce: reqB.Nonce, Solution: solA}
		origDigest := p5secPowDigest(reqA, model.PowProof{Nonce: reqA.Nonce, Solution: solA})
		forgedDigest := p5secPowDigest(reqB, forgedNonce)
		if origDigest == forgedDigest {
			t.Fatal("recycled solution produced an IDENTICAL digest under a different request's nonce — the nonce is not actually bound into the proof")
		}
	})

	t.Run("unsigned or wrong-key workload fails VerifySignature", func(t *testing.T) {
		pub, priv := p5secKey(0x10)
		wrongPub, _ := p5secKey(0x11)
		wl := []byte("open-tier workload payload bytes")
		sig := ed25519.Sign(priv, wl)

		if !enrollment.VerifySignature(wl, model.Sig(sig), model.PubKey(pub)) {
			t.Fatal("VerifySignature(genuine signature, correct key) = false, want true")
		}
		if enrollment.VerifySignature(wl, nil, model.PubKey(pub)) {
			t.Fatal("VerifySignature(no signature at all) = true, want false")
		}
		if enrollment.VerifySignature(wl, model.Sig(sig), model.PubKey(wrongPub)) {
			t.Fatal("VerifySignature(genuine signature, WRONG verification key) = true, want false")
		}
		tampered := append([]byte(nil), wl...)
		tampered[0] ^= 0xFF
		if enrollment.VerifySignature(tampered, model.Sig(sig), model.PubKey(pub)) {
			t.Fatal("VerifySignature(tampered payload, original signature) = true, want false")
		}
		flipped := append([]byte(nil), sig...)
		flipped[0] ^= 0xFF
		if enrollment.VerifySignature(wl, model.Sig(flipped), model.PubKey(pub)) {
			t.Fatal("VerifySignature(flipped signature byte) = true, want false")
		}
	})
}

// -----------------------------------------------------------------------
// noForgedTrust — attestation.VerifyAttestation / TrustFromAttestation
// -----------------------------------------------------------------------

// p5secSignedBody mirrors, for fixture construction ONLY, the canonical
// signed-body encoding attestation.VerifyAttestation's doc comment
// documents: uint64(len(measurements)) || for each m: uint64(len(m)) || m
// || uint64(len(nonce)) || nonce. The verification decision under test
// always goes through the real, exported VerifyAttestation.
func p5secSignedBody(measurements [][]byte, nonce []byte) []byte {
	var buf []byte
	appendU64 := func(v uint64) {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], v)
		buf = append(buf, b[:]...)
	}
	appendU64(uint64(len(measurements)))
	for _, m := range measurements {
		appendU64(uint64(len(m)))
		buf = append(buf, m...)
	}
	appendU64(uint64(len(nonce)))
	buf = append(buf, nonce...)
	return buf
}

// p5secSignQuote builds a well-formed model.AttQuote: it signs
// p5secSignedBody(measurements, nonce) with priv and stamps signer as the
// claimed signing key (which need not equal priv's own public key — that
// mismatch is exactly the "wrong signer" tamper vector below).
func p5secSignQuote(priv ed25519.PrivateKey, signer ed25519.PublicKey, measurements [][]byte, nonce []byte) model.AttQuote {
	return model.AttQuote{
		Measurements: measurements,
		Nonce:        nonce,
		Signature:    model.Sig(ed25519.Sign(priv, p5secSignedBody(measurements, nonce))),
		Signer:       model.PubKey(signer),
	}
}

// p5secAttScenario is one independent (trusted key, expected measurements,
// expected nonce) attestation policy the tamper sweep runs against, so the
// property is checked across more than one fixed fixture.
type p5secAttScenario struct {
	name        string
	trustedPub  ed25519.PublicKey
	trustedPriv ed25519.PrivateKey
	otherPub    ed25519.PublicKey
	expected    [][]byte
	nonce       []byte
}

// TestSecurityCapstone_noForgedTrust is the §03 attestation property,
// consolidated: no tampered quote — bad signature, wrong signer, wrong,
// missing, extra, or duplicate-masking measurement, or a replayed
// (stale-nonce) quote — ever verifies as Valid or yields AttestedTrust; only
// a genuine, matching quote does. Swept across several independent policy
// scenarios, not one fixed fixture.
func TestSecurityCapstone_noForgedTrust(t *testing.T) {
	seedPairs := [][2]byte{{0x20, 0x21}, {0x30, 0x31}, {0x40, 0x41}}
	scenarios := make([]p5secAttScenario, len(seedPairs))
	for i, pair := range seedPairs {
		tp, tpriv := p5secKey(pair[0])
		op, _ := p5secKey(pair[1])
		scenarios[i] = p5secAttScenario{
			name:        fmt.Sprintf("scenario-%d", i),
			trustedPub:  tp,
			trustedPriv: tpriv,
			otherPub:    op,
			expected:    [][]byte{[]byte(fmt.Sprintf("boot-hash-%d", i)), []byte(fmt.Sprintf("binary-hash-%d", i))},
			nonce:       []byte(fmt.Sprintf("nonce-%d", i)),
		}
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			policy := model.AttPolicy{
				Expected:      sc.expected,
				TrustedKey:    model.PubKey(sc.trustedPub),
				ExpectedNonce: sc.nonce,
			}
			genuine := p5secSignQuote(sc.trustedPriv, sc.trustedPub, sc.expected, sc.nonce)

			// Positive control: exactly one genuine, matching quote
			// verifies and boosts trust.
			res := attestation.VerifyAttestation(genuine, policy)
			if !res.Valid {
				t.Fatalf("genuine quote failed to verify: %+v", res)
			}
			if attestation.TrustFromAttestation(res) != model.AttestedTrust {
				t.Fatal("genuine quote did not grant AttestedTrust")
			}

			badSig := genuine
			flipped := append([]byte(nil), genuine.Signature...)
			flipped[0] ^= 0xFF
			badSig.Signature = model.Sig(flipped)

			vectors := map[string]model.AttQuote{
				"bad signature":                   badSig,
				"wrong signer":                    p5secSignQuote(sc.trustedPriv, sc.otherPub, sc.expected, sc.nonce),
				"wrong measurement":               p5secSignQuote(sc.trustedPriv, sc.trustedPub, [][]byte{[]byte("forged-hash"), sc.expected[1]}, sc.nonce),
				"missing measurement":             p5secSignQuote(sc.trustedPriv, sc.trustedPub, sc.expected[:1], sc.nonce),
				"extra measurement":               p5secSignQuote(sc.trustedPriv, sc.trustedPub, append(append([][]byte{}, sc.expected...), []byte("extra-hash")), sc.nonce),
				"duplicate-masking measurement":   p5secSignQuote(sc.trustedPriv, sc.trustedPub, [][]byte{sc.expected[0], sc.expected[0]}, sc.nonce),
				"replayed (stale) nonce":          p5secSignQuote(sc.trustedPriv, sc.trustedPub, sc.expected, []byte("stale-"+string(sc.nonce))),
				"wrong signer AND replayed nonce": p5secSignQuote(sc.trustedPriv, sc.otherPub, sc.expected, []byte("stale-"+string(sc.nonce))),
			}

			for name, q := range vectors {
				t.Run(name, func(t *testing.T) {
					got := attestation.VerifyAttestation(q, policy)
					if got.Valid {
						t.Fatalf("tamper vector %q verified as Valid — trust was forged", name)
					}
					if attestation.TrustFromAttestation(got) != model.BaselineTrust {
						t.Fatalf("tamper vector %q did not fall back to BaselineTrust", name)
					}
				})
			}
		})
	}

	// Absent attestation (no provider at all) is likewise never trusted
	// above baseline — attestation is a boost, never a gate, and never
	// forgeable by simply omitting a quote.
	if got := attestation.TrustFromAttestation(model.AttResult{}); got != model.BaselineTrust {
		t.Fatalf("TrustFromAttestation(zero AttResult) = %v, want BaselineTrust", got)
	}
}

// -----------------------------------------------------------------------
// overQuotaCannotSneak — tenancy.WithinQuota
// -----------------------------------------------------------------------

// TestSecurityCapstone_overQuotaCannotSneak is the §03 quota property,
// consolidated: an adversarial tenant probing every resource dimension —
// including an undeclared or explicit-zero-ceiling resource — can never get
// WithinQuota to admit an over-quota usage vector; a within-quota one is
// always admitted.
func TestSecurityCapstone_overQuotaCannotSneak(t *testing.T) {
	limits := model.ResourceVec{"cpu": 0.5, "mem": 0.4, "gpu": 0.25, "disk": 0.9}
	quota := model.Quota{Limit: limits}
	tenant := model.Tenant{ID: "adversarial-tenant"}
	kinds := []model.ResourceKind{"cpu", "mem", "gpu", "disk"}

	// Probe every declared dimension individually: exceed exactly one,
	// keep every other comfortably under its own limit.
	for _, over := range kinds {
		t.Run("over on "+string(over), func(t *testing.T) {
			consumed := model.ResourceVec{}
			for _, k := range kinds {
				if k == over {
					consumed[k] = limits[k] + 0.01
				} else {
					consumed[k] = limits[k] / 2
				}
			}
			usage := model.Usage{Consumed: consumed}
			if tenancy.WithinQuota(tenant, usage, quota) {
				t.Fatalf("WithinQuota admitted usage over quota on %q: usage=%+v quota=%+v", over, usage, quota)
			}
		})
	}

	// A cartesian sweep of every over/under combination across all four
	// declared dimensions: admitted iff and only if NO dimension is over.
	for mask := 0; mask < 1<<len(kinds); mask++ {
		consumed := model.ResourceVec{}
		anyOver := false
		for i, k := range kinds {
			if mask&(1<<i) != 0 {
				consumed[k] = limits[k] + 0.01
				anyOver = true
			} else {
				consumed[k] = limits[k] / 2
			}
		}
		usage := model.Usage{Consumed: consumed}
		got := tenancy.WithinQuota(tenant, usage, quota)
		want := !anyOver
		if got != want {
			t.Fatalf("mask=%04b usage=%+v: WithinQuota = %v, want %v", mask, consumed, got, want)
		}
	}

	// An UNDECLARED resource (absent from the quota's Limit entirely) is a
	// zero ceiling: any nonzero use of it is over quota, even with every
	// declared dimension at zero.
	t.Run("undeclared resource is a zero ceiling", func(t *testing.T) {
		usage := model.Usage{Consumed: model.ResourceVec{"exotic-undeclared-resource": 0.0001}}
		if tenancy.WithinQuota(tenant, usage, quota) {
			t.Fatal("WithinQuota admitted nonzero use of a resource entirely undeclared in the quota")
		}
	})

	// An EXPLICIT zero-ceiling resource (present in Limit at exactly 0)
	// likewise rejects any nonzero use.
	t.Run("explicit zero-ceiling resource", func(t *testing.T) {
		zq := model.Quota{Limit: model.ResourceVec{"cpu": 0.5, "banned-resource": 0}}
		usage := model.Usage{Consumed: model.ResourceVec{"cpu": 0.1, "banned-resource": 0.001}}
		if tenancy.WithinQuota(tenant, usage, zq) {
			t.Fatal("WithinQuota admitted nonzero use of an explicit zero-ceiling resource")
		}
	})

	// Positive control: a within-quota job (every dimension strictly under
	// its limit, plus an untouched-but-zero undeclared resource) is
	// admitted.
	t.Run("within-quota job is admitted", func(t *testing.T) {
		usage := model.Usage{Consumed: model.ResourceVec{
			"cpu": 0.1, "mem": 0.1, "gpu": 0.1, "disk": 0.1, "untouched-resource": 0,
		}}
		if !tenancy.WithinQuota(tenant, usage, quota) {
			t.Fatal("WithinQuota rejected a legitimately within-quota job")
		}
	})

	// A zero-value JobSpec.Demand (untenanted/undemanded job, matching
	// pre-P5 JobSpecs) never itself pushes a tenant over quota.
	t.Run("zero-value JobSpec.Demand never sneaks in over quota", func(t *testing.T) {
		job := model.JobSpec{ID: "j", Tenant: tenant.ID}
		usage := model.Usage{Consumed: job.Demand}
		if !tenancy.WithinQuota(tenant, usage, quota) {
			t.Fatal("WithinQuota rejected a zero-value Demand — an undemanded job must trivially fit")
		}
	})
}
