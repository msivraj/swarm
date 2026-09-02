// p3_hardening_capstone_test.go extends the P3 exit-criterion capstone
// (p3_capstone_test.go, #143) with the graduated-eviction hardening pass
// (issues #208-#212): the honeypot's blacklist decision is now two-strike
// (#208/#210), and the reputation store soft-freezes a chronic quorum-loser
// out of future K-sets without ever hard-booting a fresh identity
// (#209/#211).
//
// Like its sibling, this composes the REAL, already-merged shells over an
// in-memory pool — no reimplementation of any decision, no edits to any
// shipped component. It reuses p3_capstone_test.go's helpers (p3Pool,
// p3ConstRNG, p3EnrollMachine, the embedded WASM fixture and its signing
// keys) rather than duplicating them.
package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"

	corereputation "github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/enrollment"
	"github.com/msivraj/swarm/internal/shell/honeypot"
	"github.com/msivraj/swarm/internal/shell/reputation"
	"github.com/msivraj/swarm/internal/shell/sandbox"
	"github.com/msivraj/swarm/internal/shell/verification"
)

// p3HardeningHonestModule builds the same signed WASM fixture
// p3_capstone_test.go's TestP3Capstone_UntrustedPoolWithPlantedLiars does,
// and runs it once through the real wazero sandbox to obtain the "honest
// value" every honest machine in these tests converges on. Kept as a small
// per-test helper (rather than a shared package-level var) so each hardening
// scenario below is independently hermetic and self-contained.
func p3HardeningHonestModule(t *testing.T) (sandbox.Module, []byte) {
	t.Helper()

	module := sandbox.Module{
		Bytes: p3CapstoneWasm,
		Sig:   model.Sig(ed25519.Sign(p3ModuleSignerKey, p3CapstoneWasm)),
	}
	task := model.Task{ID: "hardening-honest-probe", Declared: model.WasiCaps{Stdio: true}}

	run, err := (sandbox.Runner{}).Run(context.Background(), task, module, p3ModuleSignerPub)
	if err != nil {
		t.Fatalf("Run(correctly-signed module) returned error: %v", err)
	}
	if !run.OK {
		t.Fatalf("Run(correctly-signed module).OK = false, want true")
	}
	if len(run.Output) == 0 {
		t.Fatalf("real wazero run captured no output — test fixture is broken")
	}
	return module, run.Output
}

// TestP3Hardening_TwoStrikesToBoot is the two-strike honeypot exit criterion
// (#208/#210, driven end to end per #212): a machine that lies on ONE
// injected known-answer probe is NOT blacklisted and is still dispatched to
// in the following round; only a SECOND honeypot lie blacklists it, and it
// is excluded from every round after that (and refused at enrollment,
// StatusBlacklisted). ProbingDispatcher's StrikeLimit is left at its
// unset/zero value so this exercises the DEFAULT limit
// (corehoneypot.DefaultStrikeLimit == 2) — this is deliberately distinct
// from p3_capstone_test.go's scenario, which sets StrikeLimit: 1 to
// preserve its own single-lie-catch assertion; that existing test is left
// unmodified and still passes.
func TestP3Hardening_TwoStrikesToBoot(t *testing.T) {
	ctx := context.Background()
	honestModule, honestValue := p3HardeningHonestModule(t)
	liarValue := []byte("two-strike-liar-claims-this")

	issuer := enrollment.NewFakeIssuer()
	blacklist := honeypot.NewBlacklist()
	keys := enrollment.NewKeyring()
	enroller := enrollment.NewEnroller(model.PowCfg{DifficultyBits: 0}, issuer, blacklist, keys)

	honestIDs := []model.SpiffeID{
		p3EnrollMachine(t, enroller, 0x11),
		p3EnrollMachine(t, enroller, 0x12),
		p3EnrollMachine(t, enroller, 0x13),
	}
	liarID := p3EnrollMachine(t, enroller, 0x14)

	pool := make([]model.MachineID, 0, 4)
	isHonest := make(map[model.MachineID]bool, 4)
	for _, id := range honestIDs {
		m := model.MachineID(id)
		pool = append(pool, m)
		isHonest[m] = true
	}
	liarMachine := model.MachineID(liarID)
	pool = append(pool, liarMachine)

	repStore := reputation.NewMemStore()
	pd := &p3Pool{isHonest: isHonest, lieValue: liarValue, module: honestModule, key: p3ModuleSignerPub}

	probeTask := model.Task{ID: "hardening-two-strike-probe", Declared: model.WasiCaps{Stdio: true}}
	probeResult := model.Result{Value: honestValue, OK: true}

	// StrikeLimit is deliberately left unset (zero value) so ProbingDispatcher
	// falls back to corehoneypot.DefaultStrikeLimit (2) — the default
	// two-strike policy this test proves end to end.
	dispatcher := honeypot.NewProbingDispatcher(honeypot.Config{
		Dispatcher:  pd,
		Reputation:  repStore,
		Blacklist:   blacklist,
		RNG:         p3ConstRNG(0), // force a probe on every dispatch, every round
		ProbeTask:   probeTask,
		ProbeResult: probeResult,
	})

	coord := verification.New(verification.Config{
		Dispatcher:  dispatcher,
		Reputation:  repStore,
		Blacklist:   blacklist,
		Clock:       verification.NewFakeClock(0),
		Timeout:     1_000_000_000, // never consulted: nothing in this pool hangs
		MaxAttempts: 1,
	})

	task := model.Task{ID: "hardening-two-strike-task", JobID: "hardening-job", Declared: model.WasiCaps{Stdio: true}}
	const requester = model.SpiffeID("hardening-requester")

	// ---------------------------------------------------------------------
	// Round 1: the liar lies on its FIRST forced honeypot probe. One strike
	// is below the default limit (2), so it must NOT be blacklisted yet, and
	// the honest majority (3 of 4) still carries the quorum.
	// ---------------------------------------------------------------------
	v1, err := coord.Verify(ctx, task, model.Open, requester, pool, 1)
	if err != nil {
		t.Fatalf("round 1 Verify returned error: %v", err)
	}
	if v1.Kind != model.Agreed || !bytes.Equal(v1.Value, honestValue) {
		t.Fatalf("round 1 Verdict = %+v, want Agreed(%q)", v1, honestValue)
	}
	if blacklist.IsBlacklisted(liarID) {
		t.Fatalf("liar %s was blacklisted after a SINGLE honeypot lie — two-strike policy requires a second lie first", liarID)
	}

	round1Calls := pd.Calls()
	if !containsMachine(round1Calls, liarMachine) {
		t.Fatalf("round 1 never dispatched to the liar %s at all — fixture is broken", liarID)
	}

	// ---------------------------------------------------------------------
	// Round 2 (a DIFFERENT seed, so the assignment isn't a rerun of round
	// 1's exact call): still not blacklisted, so the liar is STILL
	// dispatched to. Its second forced probe lie crosses the default
	// two-strike limit and it is blacklisted.
	// ---------------------------------------------------------------------
	v2, err := coord.Verify(ctx, task, model.Open, requester, pool, 2)
	if err != nil {
		t.Fatalf("round 2 Verify returned error: %v", err)
	}
	if v2.Kind != model.Agreed || !bytes.Equal(v2.Value, honestValue) {
		t.Fatalf("round 2 Verdict = %+v, want Agreed(%q)", v2, honestValue)
	}

	round2Calls := pd.Calls()[len(round1Calls):]
	if !containsMachine(round2Calls, liarMachine) {
		t.Fatalf("round 2 never dispatched to the liar %s — it must still be assigned work after only ONE strike", liarID)
	}
	if !blacklist.IsBlacklisted(liarID) {
		t.Fatalf("liar %s lied on a SECOND honeypot probe but was not blacklisted — two-strike policy requires eviction at the limit", liarID)
	}

	// ---------------------------------------------------------------------
	// Round 3: now blacklisted, the liar must be excluded from the K-set
	// entirely — never dispatched to again.
	// ---------------------------------------------------------------------
	callsBeforeRound3 := len(pd.Calls())
	v3, err := coord.Verify(ctx, task, model.Open, requester, pool, 3)
	if err != nil {
		t.Fatalf("round 3 Verify returned error: %v", err)
	}
	if v3.Kind != model.Agreed || !bytes.Equal(v3.Value, honestValue) {
		t.Fatalf("round 3 Verdict = %+v, want Agreed(%q)", v3, honestValue)
	}
	round3Calls := pd.Calls()[callsBeforeRound3:]
	if containsMachine(round3Calls, liarMachine) {
		t.Fatalf("round 3 dispatched to blacklisted liar %s — it must be dropped from every future K-set", liarID)
	}

	// ---------------------------------------------------------------------
	// Refused at enrollment admission too, consulting the SAME Blacklist:
	// StatusBlacklisted, no certificate issued.
	// ---------------------------------------------------------------------
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = 0x14
	}
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	req := model.JoinReq{PubKey: model.PubKey(pub), Nonce: []byte{0x14}}
	pow := model.PowProof{Nonce: req.Nonce}

	res, err := enroller.Enroll(req, pow)
	if err != nil {
		t.Fatalf("re-enrolling blacklisted liar %s: unexpected error: %v", liarID, err)
	}
	if res.Admit.ID != liarID {
		t.Fatalf("re-enrolling under the same key derived a different identity: got %s, want %s", res.Admit.ID, liarID)
	}
	if res.Status != enrollment.StatusBlacklisted {
		t.Fatalf("Enroll status for blacklisted liar %s = %v, want StatusBlacklisted", liarID, res.Status)
	}
	if res.Cert != (enrollment.Cert{}) {
		t.Fatalf("Enroll issued a certificate for blacklisted liar %s: %+v", liarID, res.Cert)
	}
}

// TestP3Hardening_ChronicLoserFrozenOutFreshGetsWork is the reputation
// soft-freeze exit criterion (#209/#211, driven end to end per #212): an
// identity driven to a chronic-loser reputation (Observations >= the
// freeze's participation floor, Score clamped at the floor of 0 — reached
// here the way the ticket suggests, via repeated real RecordVerdict(id,
// false) calls on the real reputation.Store) is excluded from every future
// K-set the Coordinator assigns, while a FRESH identity — added to the very
// same pool, never previously seen by the store — still gets dispatched
// work. Honeypot probing is disabled (RNG always >= every probe rate) so
// this scenario isolates the reputation-freeze mechanism from the two-strike
// honeypot mechanism TestP3Hardening_TwoStrikesToBoot already covers.
func TestP3Hardening_ChronicLoserFrozenOutFreshGetsWork(t *testing.T) {
	ctx := context.Background()
	honestModule, honestValue := p3HardeningHonestModule(t)

	issuer := enrollment.NewFakeIssuer()
	blacklist := honeypot.NewBlacklist()
	keys := enrollment.NewKeyring()
	enroller := enrollment.NewEnroller(model.PowCfg{DifficultyBits: 0}, issuer, blacklist, keys)

	honestA := p3EnrollMachine(t, enroller, 0x21)
	honestB := p3EnrollMachine(t, enroller, 0x22)
	freshID := p3EnrollMachine(t, enroller, 0x23) // never touches the reputation store below
	chronicLoserID := p3EnrollMachine(t, enroller, 0x24)

	repStore := reputation.NewMemStore()

	// Drive chronicLoserID below the freeze floor with real verdicts: four
	// losses (Observations >= the freeze's participation floor) each clamp
	// Score at 0 (reputation.Update never lets Score go negative, per the
	// locked zero-start design constraint) — the exact "chronic quorum-loser"
	// shape reputation.Eligible freezes.
	for i := 0; i < 4; i++ {
		repStore.RecordVerdict(chronicLoserID, false)
	}
	chronicRep := repStore.Get(chronicLoserID)
	if chronicRep.Observations < 4 || chronicRep.Score > 0 {
		t.Fatalf("chronicLoserID reputation = %+v after 4 losses, want Observations >= 4 and Score == 0 — fixture is broken", chronicRep)
	}
	if corereputation.Eligible(chronicRep) {
		t.Fatalf("corereputation.Eligible(%+v) = true, want false (frozen) — fixture is broken", chronicRep)
	}

	// freshID is left untouched: Get returns the zero-value Reputation{},
	// and reputation.Eligible is true for any identity below the
	// participation floor — zero-start intact.
	freshRep := repStore.Get(freshID)
	if !corereputation.Eligible(freshRep) {
		t.Fatalf("corereputation.Eligible(%+v) = false for a never-seen fresh identity, want true (zero-start)", freshRep)
	}

	pool := []model.MachineID{
		model.MachineID(honestA),
		model.MachineID(honestB),
		model.MachineID(freshID),
		model.MachineID(chronicLoserID),
	}
	isHonest := map[model.MachineID]bool{
		model.MachineID(honestA):        true,
		model.MachineID(honestB):        true,
		model.MachineID(freshID):        true,
		model.MachineID(chronicLoserID): true, // never actually dispatched to below
	}

	pd := &p3Pool{isHonest: isHonest, lieValue: []byte("unused"), module: honestModule, key: p3ModuleSignerPub}

	dispatcher := honeypot.NewProbingDispatcher(honeypot.Config{
		Dispatcher:  pd,
		Reputation:  repStore,
		Blacklist:   blacklist,
		RNG:         p3ConstRNG(1), // 1 is never below any positive probe rate: never probes
		ProbeTask:   model.Task{ID: "unused-probe", Declared: model.WasiCaps{Stdio: true}},
		ProbeResult: model.Result{Value: honestValue, OK: true},
	})

	coord := verification.New(verification.Config{
		Dispatcher:  dispatcher,
		Reputation:  repStore,
		Blacklist:   blacklist,
		Clock:       verification.NewFakeClock(0),
		Timeout:     1_000_000_000,
		MaxAttempts: 1,
	})

	task := model.Task{ID: "hardening-freeze-task", JobID: "hardening-freeze-job", Declared: model.WasiCaps{Stdio: true}}
	const requester = model.SpiffeID("hardening-freeze-requester")

	// Two rounds, two different seeds — the chronic loser must be excluded
	// from every future K-set, not just the first one filtered.
	for round, seed := range []uint64{10, 20} {
		v, err := coord.Verify(ctx, task, model.Open, requester, pool, seed)
		if err != nil {
			t.Fatalf("round %d Verify returned error: %v", round+1, err)
		}
		if v.Kind != model.Agreed || !bytes.Equal(v.Value, honestValue) {
			t.Fatalf("round %d Verdict = %+v, want Agreed(%q)", round+1, v, honestValue)
		}
	}

	calls := pd.Calls()
	if len(calls) == 0 {
		t.Fatal("no machine was ever dispatched to at all — fixture is broken")
	}
	if containsMachine(calls, model.MachineID(chronicLoserID)) {
		t.Fatalf("the chronic quorum-loser %s was dispatched to — it must be frozen out of every K-set once Observations >= floor and Score == 0", chronicLoserID)
	}
	if !containsMachine(calls, model.MachineID(freshID)) {
		t.Fatalf("the fresh identity %s was never dispatched to — zero-start must keep a never-seen identity eligible for work", freshID)
	}

	// Cross-check against the store directly too, not just the dispatcher's
	// observed calls.
	if corereputation.Eligible(repStore.Get(chronicLoserID)) {
		t.Fatalf("chronic loser %s reputation.Eligible == true after the freeze, want false", chronicLoserID)
	}
	if !corereputation.Eligible(repStore.Get(freshID)) {
		t.Fatalf("fresh identity %s reputation.Eligible == false, want true (zero-start intact)", freshID)
	}
}

// containsMachine reports whether m appears anywhere in calls.
func containsMachine(calls []model.MachineID, m model.MachineID) bool {
	for _, c := range calls {
		if c == m {
			return true
		}
	}
	return false
}
