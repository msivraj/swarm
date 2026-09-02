// p6_capstone_test.go is the P6 exit-criterion capstone (issue #225, owner
// fork (e) of #218): THE DESIGN REALIZED. It is the final composed
// self-tuning capstone over all six phases — the last of the series'
// hermetic capstones (p3_capstone_test.go #143, p4_capstone_test.go #168,
// p5_chaos_capstone_test.go #192) — proving that "self-tuning is the same
// core, better input" (docs/phases/swarm-p6-components.txt §01) holds when
// the REAL, already-merged P6 shells/cores are composed together and driven
// end to end:
//
//  1. Signal-based mitosis divides on a MEASURED p99 crossing its coupling's
//     SLO-derived threshold — not the P0 size/count proxy — via the real
//     control-plane mitosis loop reading a fake CellSignalSource
//     (Config.CellSignals, #231) into mitosis.DecideSignal.
//  2. Locality-ranked placement lands a task on the network-closer capable
//     cell via the real control-plane placement layer (placeLocked,
//     Config.Locality, #234) reading a fake LocalitySource into
//     placement.BestFit — not merely the first-fit cell plain Place would
//     pick.
//  3. Reputation decay fades a once-good but long-absent identity's trust
//     into the P3 freeze via the real periodic decay pass
//     (reputation.DecayingStore.DecayPass, #230), while a fresh identity
//     decayed the same way stays eligible; composed further through the
//     real P3 verification coordinator to show the frozen identity is
//     excluded from a dispatch K-set while the fresh one still gets work.
//
// This composes the REAL, already-merged shells and cores over a SIMULATED
// fleet — no reimplementation of any decision, and no edits to any shipped
// component. Every id is a deterministic function of a loop index or a
// fixed literal (never math/rand); every clock is the same injected
// testClock (internal/e2e/e2e_test.go) driven by explicit advances (or, for
// the mitosis mechanism, deliberately left frozen so cooldown windows never
// need real time to hold — see TestP6Capstone_SignalMitosisSplitsOnMeasuredP99's
// doc); there is no real network beyond the same in-process bufconn dial
// e2e_test.go's own newControlPlane already uses.
//
// Deferred owner-infra (NOT this ticket, matching #167/#179/#194's own
// "OUT of scope" notes): the literal sustained ~1M-node cloud run, over a
// real FoundationDB backend and real SPIRE/TPM attestation, is the true GA
// gate — an owner activity, not something a hermetic `go test` can prove.
// This suite proves the MECHANISM: the same three pure decisions
// (DecideSignal, Rank/BestFit, Decay/Eligible) driving real shells at a
// synthetic scale fast and deterministically enough for make gate-full.
package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/msivraj/swarm/internal/core/placement"
	corereputation "github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/controlplane"
	shellreputation "github.com/msivraj/swarm/internal/shell/reputation"
	"github.com/msivraj/swarm/internal/shell/transport"
	"github.com/msivraj/swarm/internal/shell/verification"
)

// =============================================================================
// Mechanism 1 — signal-based mitosis divides on MEASURED p99, not the count
// proxy
// =============================================================================

// p6CellSignals is a fake controlplane.CellSignalSource: a plain,
// concurrency-safe map from CellID to a measured (p99, tput) reading, set by
// the test rather than fed from a real P4 observability pipeline. A cell
// with no entry reports ok==false — "not measured" — exactly the contract
// Config.CellSignals documents.
type p6CellSignals struct {
	mu  sync.Mutex
	p99 map[model.CellID]model.Duration
}

func newP6CellSignals() *p6CellSignals {
	return &p6CellSignals{p99: make(map[model.CellID]model.Duration)}
}

func (s *p6CellSignals) set(cell model.CellID, p99 model.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.p99[cell] = p99
}

// CellSignal implements controlplane.CellSignalSource.
func (s *p6CellSignals) CellSignal(cell model.CellID) (model.Duration, float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p99, ok := s.p99[cell]
	if !ok {
		return 0, 0, false
	}
	return p99, 0, true
}

// TestP6Capstone_SignalMitosisSplitsOnMeasuredP99 is mechanism 1, the §01
// headline: a cell well UNDER the P0 size target — count alone would NEVER
// split it — is split by the REAL control-plane mitosis loop because its
// fake CellSignalSource reports a measured p99 OVER its coupling's
// SLO-derived split threshold, while a CONTROL cell of the identical size
// but a LOW measured p99 does not split. This is the self-tuning
// differentiator: the same pure mitosis.DecideSignal core, now driven by a
// richer, MEASURED input instead of the P0 count proxy.
//
// The fake CellSignalSource is pre-populated with this fresh server's next
// two deterministic cell IDs ("cell-1", "cell-2" — Server.newCellIDLocked's
// documented counter, asserted below as a fixture guard) BEFORE either
// agent joins, so every mitosis tick — including the very first, which
// fires on a 10ms fastConfig ticker — already sees the correct measured
// signal for each cell. Without this, an early tick could observe P99==0
// (no measurement yet) and take the count-fallback path instead, racing the
// test's own signal setup.
//
// The clock is deliberately left frozen at its zero value for this whole
// test (never advanced): the mitosis loop's cooldown check is
// now-lastResizeTime < window, so a frozen clock keeps every already-resized
// cell in cooldown forever, making the post-split fleet shape stable however
// many further ticks the real 10ms ticker fires during the test — no sleep
// tuning, no flakiness, and no need to touch the ticker's own real-time
// cadence at all.
func TestP6Capstone_SignalMitosisSplitsOnMeasuredP99(t *testing.T) {
	const (
		hotCellID  = model.CellID("cell-1")
		ctrlCellID = model.CellID("cell-2")
	)

	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 1 // every JoinAgent below forces a brand-new cell (Free hits 0 immediately)

	signals := newP6CellSignals()
	cfg.CellSignals = signals

	// cfg.SLO (DefaultConfig: Objective 0.999, AtRisk 0.5) derives, for an
	// Independent cell (baseSplitIndependent == 200ms, the loosest band):
	// SplitP99 == 100ms, MergeP99 == 50ms (see mitosis.signalThreshold). The
	// hot cell's measured p99 sits well over the split line; the control
	// cell's sits well under the merge line — same size, opposite
	// directions, so any split/no-split difference is attributable ONLY to
	// the measured signal.
	const hotP99 = 150 * time.Millisecond
	const ctrlP99 = 10 * time.Millisecond
	signals.set(hotCellID, model.Duration(hotP99))
	signals.set(ctrlCellID, model.Duration(ctrlP99))

	client, _, teardown := newControlPlane(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	hotResp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "hot-agent", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent(hot): %v", err)
	}
	if got := model.CellID(hotResp.GetCellId()); got != hotCellID {
		t.Fatalf("fixture bug: hot agent landed on cell %q, want %q (newCellIDLocked's counter assumption broke)", got, hotCellID)
	}

	ctrlResp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "ctrl-agent", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent(control): %v", err)
	}
	if got := model.CellID(ctrlResp.GetCellId()); got != ctrlCellID {
		t.Fatalf("fixture bug: control agent landed on cell %q, want %q (newCellIDLocked's counter assumption broke)", got, ctrlCellID)
	}

	// Both cells are size 1 — MASSIVELY under DefaultConfig's count target
	// (4): 2*Target == 8, so mitosis.Decide (count-only) would NEVER split
	// either of these cells at this size, at any tick. Any split that
	// happens below is attributable only to the measured p99 signal.
	psBefore, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps (before): %v", err)
	}
	if psBefore.GetCells() != 2 || psBefore.GetMachines() != 2 {
		t.Fatalf("Ps (before) = {Cells:%d Machines:%d}, want {2 2}", psBefore.GetCells(), psBefore.GetMachines())
	}

	// The real mitosis loop, on its own 10ms ticker, must split the hot
	// cell — driven purely by its measured p99, not its (well-under-target)
	// size.
	waitFor(t, 5*time.Second, func() bool {
		resp, err := client.Ps(ctx, &transport.PsRequest{})
		if err != nil {
			t.Fatalf("Ps (poll): %v", err)
		}
		return resp.GetCells() == 3
	})

	psAfter, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps (after): %v", err)
	}
	if psAfter.GetCells() != 3 {
		t.Fatalf("Cells = %d, want exactly 3 (control cell split too — it must not, its measured p99 is well under the split line)", psAfter.GetCells())
	}
	if psAfter.GetMachines() != 2 {
		t.Fatalf("Machines = %d, want 2 (no agent lost across the split)", psAfter.GetMachines())
	}

	// Hold across several more real ticks (the frozen clock keeps the split
	// cells in cooldown forever, and the control cell — alone, with no
	// merge partner — never merges): the fleet shape is stable, not a
	// one-tick fluke.
	time.Sleep(80 * time.Millisecond)
	psStable, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps (stable): %v", err)
	}
	if psStable.GetCells() != 3 || psStable.GetMachines() != 2 {
		t.Fatalf("Ps (stable, after further ticks) = {Cells:%d Machines:%d}, want {3 2} — the control cell must never split, and the split halves must never re-merge",
			psStable.GetCells(), psStable.GetMachines())
	}
}

// =============================================================================
// Mechanism 2 — locality-ranked placement lands work on the closer capable
// cell
// =============================================================================

// p6FakeLocality is a fake controlplane.LocalitySource: origin is the fixed
// topology every task in the test should be placed near, and zone maps a
// cell to its own fixed topology. A cell absent from zone (or a task the
// test never registers an origin for, though this fake never returns
// ok==false for TaskOrigin) is simply left for placement.Rank's own
// max-distance fallback to handle.
type p6FakeLocality struct {
	origin model.Topology
	zone   map[model.CellID]model.Topology
}

func (l *p6FakeLocality) CellTopology(cell model.CellID) (model.Topology, bool) {
	topo, ok := l.zone[cell]
	return topo, ok
}

func (l *p6FakeLocality) TaskOrigin(model.Task) (model.Topology, bool) {
	return l.origin, true
}

var _ controlplane.LocalitySource = (*p6FakeLocality)(nil)

// TestP6Capstone_LocalityPlacementPrefersCloserCell is mechanism 2: two
// cells are both capability-capable (the control plane's own registry
// snapshot carries no CapSet, so every task with no declared Requires
// trivially satisfies both — see placement.Satisfies) and both have room,
// but one ("far") is cross-region from the task's declared origin and the
// other ("near") is exactly at it. A REAL SubmitJob against a REAL
// controlplane.Server wired with a fake LocalitySource (Config.Locality)
// lands the task's one task on the NEAR cell via placeLocked's
// placement.BestFit — not the FAR cell, which is what plain first-fit
// placement.Place would have picked (asserted directly against the pure
// core below, over the exact same two-cell snapshot, as the concrete
// "would not necessarily under plain Place" comparison).
func TestP6Capstone_LocalityPlacementPrefersCloserCell(t *testing.T) {
	near := model.Topology{Region: "us-east", AZ: "az1", Rack: "rack1"}
	far := model.Topology{Region: "eu-west", AZ: "az9", Rack: "rack9"}

	loc := &p6FakeLocality{origin: near, zone: map[model.CellID]model.Topology{}}

	clock := &testClock{}
	cfg := controlplane.DefaultConfig()
	cfg.HeartbeatSweep = time.Hour  // no reaping during this short test
	cfg.MitosisInterval = time.Hour // no split/merge interference
	cfg.DefaultCellCapacity = 2
	cfg.Locality = loc

	client, _, teardown := newControlPlane(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	// far-agent's JoinAgent(Caps:1) forms a brand-new cell (capacity 2,
	// Free 1 after joining) — Server.newCellIDLocked's counter makes this
	// "cell-1" in a fresh server, asserted below as a fixture guard.
	farResp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "far-agent", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent(far): %v", err)
	}
	farCell := model.CellID(farResp.GetCellId())
	if farCell != "cell-1" {
		t.Fatalf("fixture bug: far agent landed on cell %q, want \"cell-1\"", farCell)
	}

	// near-agent requests Caps:2 — farCell's Free (1) cannot satisfy that,
	// so rendezvous.AdmitAgent forms a second brand-new cell ("cell-2").
	nearResp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "near-agent", Region: "us", Caps: 2})
	if err != nil {
		t.Fatalf("JoinAgent(near): %v", err)
	}
	nearCell := model.CellID(nearResp.GetCellId())
	if nearCell != "cell-2" {
		t.Fatalf("fixture bug: near agent landed on cell %q, want \"cell-2\"", nearCell)
	}

	loc.zone[farCell] = far
	loc.zone[nearCell] = near

	// Concrete divergence check: plain first-fit placement.Place, called
	// directly against the exact same two-cell snapshot registry.Snapshot
	// would hand drainPendingLocked, picks the FAR cell (lower CellID,
	// first in slice order) — locality plays no part in Place at all.
	snapshot := []model.CellView{{ID: farCell, Free: 1}, {ID: nearCell, Free: 1}}
	plain := placement.Place(model.Task{}, snapshot)
	if plain.Kind != placement.Assign || plain.Cell != farCell {
		t.Fatalf("fixture bug: placement.Place(same snapshot) = %+v, want Assign{%s} (the plain first-fit pick this test contrasts against)", plain, farCell)
	}

	// A single-task job (keyspace-search, shards=1) submitted over the REAL
	// gRPC control plane. SubmitJob's handler synchronously drains the
	// pending buffer (drainPendingLocked -> placeLocked -> placement.BestFit
	// against loc) before it returns, so by the time this call returns the
	// task has already landed.
	if _, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"start": "0", "end": "1", "shards": "1"},
	}); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	nearPull, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "near-agent"})
	if err != nil {
		t.Fatalf("PullTask(near-agent): %v", err)
	}
	if !nearPull.GetHasTask() {
		t.Fatalf("PullTask(near-agent).HasTask = false, want true — the task should have landed on the locality-closer near cell")
	}

	farPull, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "far-agent"})
	if err != nil {
		t.Fatalf("PullTask(far-agent): %v", err)
	}
	if farPull.GetHasTask() {
		t.Fatalf("PullTask(far-agent).HasTask = true, want false — locality-ranked placement must have preferred the near cell, not the far one plain first-fit would pick")
	}
}

// =============================================================================
// Mechanism 3 — reputation decay fades stale trust into the freeze
// =============================================================================

// TestP6Capstone_ReputationDecayFadesIntoFreeze is mechanism 3: a once-good
// identity that then goes ABSENT for a long simulated span has its Score
// decayed by the REAL periodic decay pass (reputation.DecayingStore.
// DecayPass) all the way to the P3 freeze floor, so corereputation.Eligible
// reads it as frozen — while a fresh, barely-participated identity decayed
// over the exact same span stays eligible (zero-start is preserved under
// decay), and neither identity's Score ever goes negative (the P3 Sybil
// floor DecayPass must never breach).
func TestP6Capstone_ReputationDecayFadesIntoFreeze(t *testing.T) {
	const (
		veteran = model.SpiffeID("veteran-once-good")
		fresh   = model.SpiffeID("fresh-newcomer")
	)

	clock := &testClock{}
	store := shellreputation.NewDecayingStore(clock.now)

	// veteran earns real trust: 50 honest verdicts (Score 500,
	// Observations 50) — comfortably past the freeze participation floor
	// (Observations >= 4) and a Score decay must fully fade before the
	// freeze can engage.
	for i := 0; i < 50; i++ {
		store.RecordVerdict(veteran, true)
	}
	// fresh has barely participated: one honest verdict (Observations 1),
	// under the freeze's participation floor.
	store.RecordVerdict(fresh, true)

	before := store.Get(veteran)
	if before.Score != 500 || before.Observations != 50 {
		t.Fatalf("fixture bug: veteran before decay = %+v, want {Score:500 Observations:50}", before)
	}
	if !corereputation.Eligible(before) {
		t.Fatalf("fixture bug: veteran before decay is already ineligible — the freeze this test drives via decay would be meaningless")
	}

	// A long simulated absence: 60 days is comfortably more than the 25
	// days (500 Score / 20-per-day) veteran's Score needs to fully decay to
	// the floor.
	clock.advance(60 * 24 * time.Hour)
	store.DecayPass(clock.now())

	veteranAfter := store.Get(veteran)
	if veteranAfter.Score != 0 {
		t.Fatalf("veteran Score after a long absence = %d, want 0 (fully decayed to the floor)", veteranAfter.Score)
	}
	if veteranAfter.Score < 0 {
		t.Fatalf("veteran Score after decay = %d, want >= 0 (the P3 Sybil floor must never be breached)", veteranAfter.Score)
	}
	if veteranAfter.Observations != 50 {
		t.Fatalf("veteran Observations after decay = %d, want unchanged 50 (Decay never touches participation history)", veteranAfter.Observations)
	}
	if corereputation.Eligible(veteranAfter) {
		t.Fatalf("veteran is still Eligible after decaying to the freeze floor with Observations=%d — want frozen (Eligible==false)", veteranAfter.Observations)
	}

	freshAfter := store.Get(fresh)
	if freshAfter.Score < 0 {
		t.Fatalf("fresh Score after decay = %d, want >= 0 (the Sybil floor)", freshAfter.Score)
	}
	if !corereputation.Eligible(freshAfter) {
		t.Fatalf("fresh (Observations=%d, under the freeze floor) is ineligible after the same decay pass — zero-start must be preserved under decay", freshAfter.Observations)
	}

	// Compose further: the REAL P3 verification coordinator's eligiblePool
	// filter (fed by this same DecayingStore) must exclude the now-frozen
	// veteran from a live dispatch K-set, while fresh still gets
	// dispatched to and completes a round.
	dispatcher := verification.NewFakeDispatcher()
	dispatcher.Honest(model.MachineID(fresh), []byte("answer"))

	coord := verification.New(verification.Config{
		Dispatcher:  dispatcher,
		Reputation:  store,
		Clock:       verification.NewFakeClock(0),
		Timeout:     model.Duration(time.Second),
		MaxAttempts: 1,
	})

	pool := []model.MachineID{model.MachineID(veteran), model.MachineID(fresh)}
	task := model.Task{ID: "p6-decay-freeze-task"}
	verdict, err := coord.Verify(context.Background(), task, model.Open, model.SpiffeID("requester"), pool, 0xC0FFEE)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if verdict.Kind != model.Agreed {
		t.Fatalf("Verdict.Kind = %v, want Agreed", verdict.Kind)
	}

	calls := dispatcher.Calls()
	sawFresh := false
	for _, m := range calls {
		if m == model.MachineID(veteran) {
			t.Fatalf("dispatcher was called for the FROZEN veteran identity %q — eligiblePool must exclude it from every K-set", veteran)
		}
		if m == model.MachineID(fresh) {
			sawFresh = true
		}
	}
	if !sawFresh {
		t.Fatalf("dispatcher was never called for the fresh identity %q — it must still get work", fresh)
	}
}
