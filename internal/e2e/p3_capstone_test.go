// p3_capstone_test.go is the P3 exit-criterion capstone (issue #143, design
// ruling #132 fork d): an untrusted machine pool with planted liars proves
// the whole open tier holds together end to end.
//
// This composes the REAL, already-merged P3 shells over an in-memory pool —
// no reimplementation of any decision, and no edits to any shipped
// component:
//
//   - internal/shell/verification.Coordinator drives the assign -> dispatch
//     -> collect -> quorum-verdict -> reputation-write loop.
//   - internal/shell/honeypot.ProbingDispatcher decorates the dispatch path
//     with known-answer spot-checks and writes caught lies into the shared
//     Blacklist.
//   - internal/shell/reputation.Store persists trust, read back after the
//     verdict.
//   - internal/shell/enrollment.Enroller derives each pool machine's
//     open-tier SpiffeID (real PoW-gated admission, PoW disabled via
//     DifficultyBits: 0 for a deterministic test) and is later used to prove
//     a blacklisted identity is refused at admission.
//   - internal/shell/sandbox.Runner is the REAL wazero runner: every honest
//     machine in the pool executes a real, ed25519-signed WASM module
//     (testdata/echo.wasm, the same fixture #138 built) and its captured
//     stdout is the "honest value" the quorum must agree on. Liar machines
//     never touch wazero at all — they simply claim a divergent value.
//
// Only the clock (verification.FakeClock) and randomness (a fixed-value RNG
// seam) are faked, per CLAUDE.md's core-purity contract — there is no real
// network, no real SPIRE, and no real sleep anywhere in this test.
package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	_ "embed"
	"errors"
	"sync"
	"testing"

	corehoneypot "github.com/msivraj/swarm/internal/core/honeypot"
	coreverification "github.com/msivraj/swarm/internal/core/verification"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/enrollment"
	"github.com/msivraj/swarm/internal/shell/honeypot"
	"github.com/msivraj/swarm/internal/shell/reputation"
	"github.com/msivraj/swarm/internal/shell/sandbox"
	"github.com/msivraj/swarm/internal/shell/verification"
)

// p3CapstoneWasm is testdata/echo.wasm — the exact fixture-building recipe
// #138's internal/shell/sandbox/testdata/echo.rs documents (rustc --target
// wasm32-wasip1 ...), copied here so this suite embeds committed bytes and
// depends on no wasm toolchain at test time. It ignores its arguments/stdin
// and always writes the fixed line "sandbox-ok\n" to stdout before exiting
// 0 — deterministic, so a real wazero run of it is a stable "honest value"
// every honest pool machine converges on.
//
//go:embed testdata/echo.wasm
var p3CapstoneWasm []byte

// Deterministic ed25519 keys for signing the WASM module under test —
// crypto/ed25519 seeded from fixed bytes, never crypto/rand, so the fixture
// is stable across runs (mirroring internal/shell/sandbox's own tests).
var (
	p3ModuleSignerSeed = [ed25519.SeedSize]byte{0xC1, 0xA5, 0x70, 0x0E, 0x01, 0x02, 0x03, 0x04}
	p3ModuleSignerKey  = ed25519.NewKeyFromSeed(p3ModuleSignerSeed[:])
	p3ModuleSignerPub  = model.PubKey(p3ModuleSignerKey.Public().(ed25519.PublicKey))

	p3ModuleWrongSeed = [ed25519.SeedSize]byte{0xBA, 0xD0, 0x70, 0x0E, 0x05, 0x06, 0x07, 0x08}
	p3ModuleWrongKey  = ed25519.NewKeyFromSeed(p3ModuleWrongSeed[:])
	p3ModuleWrongPub  = model.PubKey(p3ModuleWrongKey.Public().(ed25519.PublicKey))
)

// p3ConstRNG returns a func() float64 seam that always returns v — the
// shell-side randomness seam corehoneypot.ShouldProbe consumes as data, per
// CLAUDE.md ("the clock and any randomness are passed in as data"). v == 0
// is always below every positive probe rate, so it forces a probe on every
// dispatch — enough to guarantee this test observes a planted liar's lie on
// the very round it is dispatched, with no reliance on a randomness draw.
func p3ConstRNG(v float64) func() float64 {
	return func() float64 { return v }
}

// p3EnrollMachine derives a deterministic ed25519 identity key from seedByte
// and runs it through the REAL enrollment core (PoW disabled via
// DifficultyBits: 0 in the caller's Enroller, so admission is deterministic)
// to obtain the open-tier SpiffeID a real anonymous joiner would be issued.
// Using an actually-enrolled identity — rather than an arbitrary test string
// — for every pool machine matches the documented MachineID<->SpiffeID
// assumption every P3 shell encodes: a pool's MachineID names the exact
// identity that machine enrolled as.
func p3EnrollMachine(t *testing.T, enroller *enrollment.Enroller, seedByte byte) model.SpiffeID {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)

	req := model.JoinReq{PubKey: model.PubKey(pub), Nonce: []byte{seedByte}}
	pow := model.PowProof{Nonce: req.Nonce} // difficulty 0: Solution is unused

	res, err := enroller.Enroll(req, pow)
	if err != nil {
		t.Fatalf("Enroll: unexpected error: %v", err)
	}
	if res.Status != enrollment.StatusAccepted {
		t.Fatalf("Enroll status = %v, want StatusAccepted", res.Status)
	}
	return res.Admit.ID
}

// p3Pool is the untrusted machine pool's verification.Dispatcher: an honest
// machine runs the real, signed WASM module through the real
// internal/shell/sandbox.Runner (real wazero) and reports its captured
// output; a liar machine never touches wazero at all and simply claims
// lieValue for every task, probe included. It records every machine it was
// asked to run, in call order, so the test can prove a blacklisted liar is
// never dispatched to again.
type p3Pool struct {
	mu       sync.Mutex
	calls    []model.MachineID
	isHonest map[model.MachineID]bool
	lieValue []byte
	module   sandbox.Module
	key      model.PubKey
}

func (p *p3Pool) Dispatch(ctx context.Context, machine model.MachineID, task model.Task) (model.Result, error) {
	p.mu.Lock()
	p.calls = append(p.calls, machine)
	p.mu.Unlock()

	if !p.isHonest[machine] {
		return model.Result{Value: p.lieValue, OK: true}, nil
	}

	result, err := (sandbox.Runner{}).Run(ctx, task, p.module, p.key)
	if err != nil {
		return model.Result{}, err
	}
	return model.Result{Value: result.Output, OK: result.OK}, nil
}

// Calls returns every machine Dispatch was invoked for so far, in call order.
func (p *p3Pool) Calls() []model.MachineID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]model.MachineID, len(p.calls))
	copy(out, p.calls)
	return out
}

var _ verification.Dispatcher = (*p3Pool)(nil)

// TestP3Capstone_UntrustedPoolWithPlantedLiars is the P3 exit criterion
// (#143): five machines — three honest, two planted liars — are dispatched
// an open-tier task through the real Coordinator/ProbingDispatcher/
// reputation.Store/enrollment stack. All 5 of the design ruling's (#132
// fork d) required assertions are made below, each labeled in place.
func TestP3Capstone_UntrustedPoolWithPlantedLiars(t *testing.T) {
	ctx := context.Background()

	honestModule := sandbox.Module{
		Bytes: p3CapstoneWasm,
		Sig:   model.Sig(ed25519.Sign(p3ModuleSignerKey, p3CapstoneWasm)),
	}

	// -------------------------------------------------------------------
	// Assertion 5 (part a): the real sandbox.Runner refuses an unsigned or
	// wrong-key module before ever instantiating it (ErrUnsigned, no wazero
	// runtime created) — the signature gate holds even before any pool
	// machine is involved.
	// -------------------------------------------------------------------
	sigTask := model.Task{ID: "capstone-sig-probe", Declared: model.WasiCaps{Stdio: true}}

	if _, err := (sandbox.Runner{}).Run(ctx, sigTask, sandbox.Module{Bytes: p3CapstoneWasm, Sig: nil}, p3ModuleSignerPub); !errors.Is(err, sandbox.ErrUnsigned) {
		t.Fatalf("Run(unsigned module) error = %v, want ErrUnsigned", err)
	}
	if _, err := (sandbox.Runner{}).Run(ctx, sigTask, honestModule, p3ModuleWrongPub); !errors.Is(err, sandbox.ErrUnsigned) {
		t.Fatalf("Run(correctly-signed module, wrong verification key) error = %v, want ErrUnsigned", err)
	}
	if _, err := (sandbox.Runner{}).Run(ctx, sigTask, sandbox.Module{Bytes: p3CapstoneWasm, Sig: model.Sig(ed25519.Sign(p3ModuleWrongKey, p3CapstoneWasm))}, p3ModuleSignerPub); !errors.Is(err, sandbox.ErrUnsigned) {
		t.Fatalf("Run(module signed by the wrong key) error = %v, want ErrUnsigned", err)
	}

	// -------------------------------------------------------------------
	// Assertion 5 (part b): a correctly-signed module runs to completion in
	// the real wazero sandbox and its captured output becomes the value the
	// rest of this test treats as "honest".
	// -------------------------------------------------------------------
	honestRun, err := (sandbox.Runner{}).Run(ctx, sigTask, honestModule, p3ModuleSignerPub)
	if err != nil {
		t.Fatalf("Run(correctly-signed module) returned error: %v", err)
	}
	if !honestRun.OK {
		t.Fatalf("Run(correctly-signed module).OK = false, want true")
	}
	honestValue := honestRun.Output
	if len(honestValue) == 0 {
		t.Fatalf("real wazero run captured no output — test fixture is broken")
	}
	liarValue := []byte("i-am-lying-about-the-task-result")

	// -------------------------------------------------------------------
	// Enroll five real open-tier identities (3 honest, 2 liars) through the
	// REAL enrollment core (PoW disabled for determinism) — the pool's
	// MachineIDs below are exactly these issued SpiffeIDs, matching the
	// documented MachineID<->SpiffeID assumption every P3 shell relies on.
	// -------------------------------------------------------------------
	issuer := enrollment.NewFakeIssuer()
	blacklist := honeypot.NewBlacklist() // implements both enrollment.Blacklist and honeypot.BlacklistWriter
	keys := enrollment.NewKeyring()
	enroller := enrollment.NewEnroller(model.PowCfg{DifficultyBits: 0}, issuer, blacklist, keys)

	honestIDs := []model.SpiffeID{
		p3EnrollMachine(t, enroller, 0x01),
		p3EnrollMachine(t, enroller, 0x02),
		p3EnrollMachine(t, enroller, 0x03),
	}
	liarIDs := []model.SpiffeID{
		p3EnrollMachine(t, enroller, 0x04),
		p3EnrollMachine(t, enroller, 0x05),
	}

	pool := make([]model.MachineID, 0, 5)
	isHonest := make(map[model.MachineID]bool, 5)
	for _, id := range honestIDs {
		m := model.MachineID(id)
		pool = append(pool, m)
		isHonest[m] = true
	}
	for _, id := range liarIDs {
		pool = append(pool, model.MachineID(id))
	}

	// Pre-seed every pool identity's reputation at a shared, non-floor
	// baseline: the floor (Score 0) is already clamped on a lie, so proving
	// reputation actually FALLS for a liar (assertion 2) needs headroom to
	// fall from.
	const baselineScore = int64(200)
	repStore := reputation.NewMemStore()
	for _, id := range append(append([]model.SpiffeID{}, honestIDs...), liarIDs...) {
		repStore.Put(id, model.Reputation{Score: baselineScore, Observations: 5})
	}

	pd := &p3Pool{isHonest: isHonest, lieValue: liarValue, module: honestModule, key: p3ModuleSignerPub}

	probeTask := model.Task{ID: "capstone-honeypot-probe", Declared: model.WasiCaps{Stdio: true}}
	probeResult := model.Result{Value: honestValue, OK: true}

	dispatcher := honeypot.NewProbingDispatcher(honeypot.Config{
		Dispatcher:  pd,
		Reputation:  repStore,
		Blacklist:   blacklist,
		RNG:         p3ConstRNG(0), // force a probe on every dispatch this round
		ProbeTask:   probeTask,
		ProbeResult: probeResult,
		// StrikeLimit: 1 — each liar is only ever dispatched (and so only
		// ever probed) once per round in this fixture, so this capstone
		// exercises the honeypot's catch-and-exclude path, not the
		// strike-aware threshold itself (see #210's internal/shell/honeypot
		// tests for the two-strike-tolerance property directly).
		StrikeLimit: 1,
	})

	coord := verification.New(verification.Config{
		Dispatcher:  dispatcher,
		Reputation:  repStore,
		Blacklist:   blacklist,
		Clock:       verification.NewFakeClock(0),
		Timeout:     1_000_000_000, // never consulted: nothing in this pool hangs
		MaxAttempts: 1,
	})

	realTask := model.Task{ID: "capstone-open-task", JobID: "capstone-job", Declared: model.WasiCaps{Stdio: true}}
	const requester = model.SpiffeID("capstone-requester")

	// -------------------------------------------------------------------
	// Assertion 1: quorum beats liars. The pool has exactly 5 machines, so
	// the requester's zero-reputation redundancy (which asks for far more
	// than 5) clamps to the whole pool — K=5, 3 honest + 2 liars — and the
	// coordinator accepts Verdict.Agreed on the HONEST value. The minority
	// of liars cannot flip it.
	// -------------------------------------------------------------------
	v1, err := coord.Verify(ctx, realTask, model.Open, requester, pool, 42)
	if err != nil {
		t.Fatalf("round 1 Verify returned error: %v", err)
	}
	if v1.Kind != model.Agreed {
		t.Fatalf("round 1 Verdict.Kind = %v, want Agreed", v1.Kind)
	}
	if !bytes.Equal(v1.Value, honestValue) {
		t.Fatalf("round 1 Verdict.Value = %q, want the honest value %q", v1.Value, honestValue)
	}

	// -------------------------------------------------------------------
	// Assertion 2: reputation moves correctly. Read back from the real
	// reputation.Store — the 3 honest identities rose above baseline, the 2
	// liars fell below it.
	// -------------------------------------------------------------------
	for _, id := range honestIDs {
		got := repStore.Get(id)
		if got.Score <= baselineScore {
			t.Errorf("honest identity %s reputation = %+v, want Score > baseline %d", id, got, baselineScore)
		}
	}
	for _, id := range liarIDs {
		got := repStore.Get(id)
		if got.Score >= baselineScore {
			t.Errorf("liar identity %s reputation = %+v, want Score < baseline %d", id, got, baselineScore)
		}
	}

	// -------------------------------------------------------------------
	// Assertion 3 (part a): the honeypot catches + blacklists. Round 1 force-
	// probed every dispatched machine, so both liars were already caught
	// lying about the injected known-answer probe during round 1's own
	// dispatch phase (never blacklisting an honest machine that answered it
	// correctly). Independently replaying the probe against the pool and
	// checking it with the pure honeypot core cross-validates Check==Lie for
	// the wired path, not just the blacklist's end state.
	// -------------------------------------------------------------------
	for _, id := range liarIDs {
		if !blacklist.IsBlacklisted(id) {
			t.Errorf("liar %s lied on the injected honeypot probe but was not blacklisted", id)
		}
		claimed, err := pd.Dispatch(ctx, model.MachineID(id), probeTask)
		if err != nil {
			t.Fatalf("replaying the probe against liar %s: %v", id, err)
		}
		if corehoneypot.Check(claimed, probeResult) != model.Lie {
			t.Errorf("corehoneypot.Check(liar %s's claim, known answer) != Lie", id)
		}
	}
	for _, id := range honestIDs {
		if blacklist.IsBlacklisted(id) {
			t.Errorf("honest identity %s was wrongly blacklisted", id)
		}
	}

	callsAfterRound1 := len(pd.Calls())

	// -------------------------------------------------------------------
	// Assertion 4: a fresh-seed re-run still returns the honest value. The
	// coordinator's blacklist filter now excludes both liars from
	// eligibility, so round 2 assigns only the 3 honest survivors — still
	// Agreed on the honest value, with a base seed different from round 1's.
	// (A direct pure-core check further down shows Assign itself picks a
	// different subset under a different seed, independent of this round's
	// dispatch — see "different Assign K-set" below.)
	// -------------------------------------------------------------------
	v2, err := coord.Verify(ctx, realTask, model.Open, requester, pool, 999)
	if err != nil {
		t.Fatalf("round 2 Verify returned error: %v", err)
	}
	if v2.Kind != model.Agreed {
		t.Fatalf("round 2 Verdict.Kind = %v, want Agreed", v2.Kind)
	}
	if !bytes.Equal(v2.Value, honestValue) {
		t.Fatalf("round 2 Verdict.Value = %q, want the honest value %q", v2.Value, honestValue)
	}

	// -------------------------------------------------------------------
	// Assertion 3 (part b): blacklisted liars are dropped from future
	// K-sets — never re-dispatched, round 2 onward.
	// -------------------------------------------------------------------
	round2Calls := pd.Calls()[callsAfterRound1:]
	if len(round2Calls) == 0 {
		t.Fatal("round 2 dispatched to no machines at all")
	}
	for _, m := range round2Calls {
		for _, id := range liarIDs {
			if m == model.MachineID(id) {
				t.Fatalf("round 2 dispatched to blacklisted liar %s — it must be dropped from every future K-set", id)
			}
		}
	}

	// -------------------------------------------------------------------
	// Assertion 3 (part c): a blacklisted identity is also refused at
	// enrollment admission — StatusBlacklisted, no certificate issued —
	// consulting the SAME Blacklist ProbingDispatcher wrote into.
	// -------------------------------------------------------------------
	for i, id := range liarIDs {
		seed := make([]byte, ed25519.SeedSize)
		seedByte := byte(0x04 + i)
		for j := range seed {
			seed[j] = seedByte
		}
		pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
		req := model.JoinReq{PubKey: model.PubKey(pub), Nonce: []byte{seedByte}}
		pow := model.PowProof{Nonce: req.Nonce}

		res, err := enroller.Enroll(req, pow)
		if err != nil {
			t.Fatalf("re-enrolling blacklisted liar %s: unexpected error: %v", id, err)
		}
		if res.Admit.ID != id {
			t.Fatalf("re-enrolling under the same key derived a different identity: got %s, want %s", res.Admit.ID, id)
		}
		if res.Status != enrollment.StatusBlacklisted {
			t.Fatalf("Enroll status for blacklisted liar %s = %v, want StatusBlacklisted", id, res.Status)
		}
		if res.Cert != (enrollment.Cert{}) {
			t.Fatalf("Enroll issued a certificate for blacklisted liar %s: %+v", id, res.Cert)
		}
	}

	// -------------------------------------------------------------------
	// Assertion 4 (pure-core cross-check): Assign itself picks a different
	// K-set for a different seed over the same pool — the safety in
	// assertion 4 above isn't an accident of round 2's pool happening to
	// equal K.
	// -------------------------------------------------------------------
	assignA := coreverification.Assign(realTask.ID, pool, 3, 42)
	assignB := coreverification.Assign(realTask.ID, pool, 3, 999)
	if sameMachineSet(assignA, assignB) {
		t.Fatalf("Assign(seed=42) and Assign(seed=999) picked the same K-set %v — fixture no longer demonstrates seed-dependent assignment", assignA)
	}
}

// sameMachineSet reports whether a and b contain exactly the same machines,
// ignoring order.
func sameMachineSet(a, b []model.MachineID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[model.MachineID]bool, len(a))
	for _, m := range a {
		set[m] = true
	}
	for _, m := range b {
		if !set[m] {
			return false
		}
	}
	return true
}
