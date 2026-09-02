package controlplane

import (
	"context"
	"sync"
	"testing"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// fakeCellSignals is a CellSignalSource test double: it lets a test inject a
// measured p99/throughput reading for a specific cell without wiring the
// real observability.Reporter (see CellSignalSource's doc — the whole point
// of the seam is that a fake can stand in for it in a hermetic test). A cell
// with no set() call reads ok == false, exactly a real source's "never
// measured this cell" case.
type fakeCellSignals struct {
	mu      sync.Mutex
	signals map[model.CellID]cellReading
}

type cellReading struct {
	p99  model.Duration
	tput float64
}

func newFakeCellSignals() *fakeCellSignals {
	return &fakeCellSignals{signals: make(map[model.CellID]cellReading)}
}

func (f *fakeCellSignals) set(cell model.CellID, p99 model.Duration, tput float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signals[cell] = cellReading{p99: p99, tput: tput}
}

func (f *fakeCellSignals) CellSignal(cell model.CellID) (model.Duration, float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.signals[cell]
	return r.p99, r.tput, ok
}

// TestMitosisSignalSplitsUndersizedCellOnMeasuredP99 is the self-tuning
// differentiator: a cell well UNDER the size target (so mitosis.Decide's
// count rule would never touch it) is Split anyway because its injected
// measured p99 crosses the SLO-derived latency band for its (Independent,
// the default) coupling. DefaultConfig's SLO (Objective 0.999, AtRisk 0.5)
// derives a 100ms Independent SplitP99 (200ms base * 0.5 — see
// mitosis.signalThreshold's doc); 150ms is comfortably over it.
func TestMitosisSignalSplitsUndersizedCellOnMeasuredP99(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 10
	// Target stays the DefaultConfig 4: 2*Target == 8, so this cell's Size
	// (3, joined below) is nowhere near the count-based split line — if a
	// split still happens, it can only be the measured signal driving it.
	fake := newFakeCellSignals()
	cfg.CellSignals = fake
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	joinResp := joinAgent(t, ctx, client, "agent-1", 1)
	cell := model.CellID(joinResp.GetCellId())
	joinAgent(t, ctx, client, "agent-2", 1)
	joinAgent(t, ctx, client, "agent-3", 1)

	psResp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if psResp.GetCells() != 1 {
		t.Fatalf("Ps before mitosis: Cells = %d, want 1 (all three agents share one cell)", psResp.GetCells())
	}

	// 150ms measured p99, well over the 100ms Independent SplitP99 the
	// default SLO derives, for a cell whose Size (3) is nowhere near
	// 2*Target (8) — a count-based decision alone would never split it.
	fake.set(cell, 150*1_000_000, 42.0)

	srv.mitosisOnce()

	psResp, err = client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if psResp.GetCells() != 2 {
		t.Fatalf("Ps after mitosisOnce with a high measured p99: Cells = %d, want 2 (the undersized cell should have split on the MEASURED signal)", psResp.GetCells())
	}
	if psResp.GetMachines() != 3 {
		t.Fatalf("Ps after signal-driven split: Machines = %d, want 3 (no agent lost across the split)", psResp.GetMachines())
	}
}

// TestMitosisSignalCooldownSuppressesSplit confirms a cell within its resize
// cooldown window is left alone even though its injected measured p99 is
// well over the split threshold — the cooldown check in DecideSignal runs
// before the p99 comparison, exactly as it does for the count-based path.
func TestMitosisSignalCooldownSuppressesSplit(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 10
	fake := newFakeCellSignals()
	cfg.CellSignals = fake
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	joinResp := joinAgent(t, ctx, client, "agent-1", 1)
	cell := model.CellID(joinResp.GetCellId())
	joinAgent(t, ctx, client, "agent-2", 1)
	joinAgent(t, ctx, client, "agent-3", 1)

	fake.set(cell, 150*1_000_000, 42.0)

	// Put the cell in cooldown as of "now": DefaultConfig's 30s
	// CooldownNS means an immediate mitosisOnce call sees it as freshly
	// resized, regardless of how high the measured p99 reads.
	srv.mu.Lock()
	srv.cooldowns[cell] = clock.now()
	srv.mu.Unlock()

	srv.mitosisOnce()

	psResp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if psResp.GetCells() != 1 {
		t.Fatalf("Ps after mitosisOnce during cooldown: Cells = %d, want 1 (cooldown must suppress the resize despite the high measured p99)", psResp.GetCells())
	}
}

// TestMitosisSignalCountFallbackUnchangedWithNoSource is a regression check
// for the transparency guarantee at the config layer: leaving
// Config.CellSignals nil (its zero value, exactly what DefaultConfig and
// every P0-P5 caller already does) reproduces the prior count-based split —
// mirrors TestMitosisSplitsOversizedCell, but exercised through
// mitosisOnce's DecideSignal rewiring rather than the pre-#222 Decide call.
func TestMitosisSignalCountFallbackUnchangedWithNoSource(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 10
	cfg.MitosisThresholds.Target = 2
	cfg.MitosisThresholds.CooldownNS = 0
	// cfg.CellSignals left nil — no measured signal wired anywhere.
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	joinAgent(t, ctx, client, "agent-1", 1)
	joinAgent(t, ctx, client, "agent-2", 1)
	joinAgent(t, ctx, client, "agent-3", 1)
	joinAgent(t, ctx, client, "agent-4", 1)
	joinAgent(t, ctx, client, "agent-5", 1)
	// Size(5) > 2*Target(2): the exact count-based split condition
	// TestMitosisSplitsOversizedCell already covers via the background
	// ticker; this asserts the same outcome via a direct mitosisOnce call.

	srv.mitosisOnce()

	psResp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if psResp.GetCells() != 2 {
		t.Fatalf("Ps after mitosisOnce with nil CellSignals: Cells = %d, want 2 (count-based fallback unchanged)", psResp.GetCells())
	}
	if psResp.GetMachines() != 5 {
		t.Fatalf("Ps after count-based split: Machines = %d, want 5 (no agent lost across the split)", psResp.GetMachines())
	}
}

// TestCellCouplingsLockedTightestWins is the #232 regression: a single cell
// co-reserved by a Barrier gang AND an Independent gang must resolve to
// Barrier (the tightest coupling, rank 0), deterministically — never
// last-writer-wins over s.gangJobs' map iteration order. It seeds s.gangJobs
// directly (rather than driving two real overlapping gang admissions, which
// admission.AdmitGang would ordinarily prevent) with both possible job-ID
// insertion orders, and repeats the call across many map instances: Go
// deliberately randomizes map iteration order per range, so a flaky result
// here would expose a real order dependence rather than a testing artifact.
func TestCellCouplingsLockedTightestWins(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	joinResp := joinAgent(t, ctx, client, "agent-1", 1)
	cell := model.CellID(joinResp.GetCellId())

	barrierJob := model.JobSpec{ID: "job-barrier", Coupling: model.Barrier}
	independentJob := model.JobSpec{ID: "job-independent", Coupling: model.Independent}
	assignment := []admission.Assignment{{Cell: cell, Members: 1}}

	for _, order := range [][2]model.JobSpec{
		{barrierJob, independentJob},
		{independentJob, barrierJob},
	} {
		for i := 0; i < 50; i++ {
			srv.mu.Lock()
			srv.gangJobs = map[model.JobID]gangReservation{
				order[0].ID: {job: order[0], assignments: assignment},
				order[1].ID: {job: order[1], assignments: assignment},
			}
			couplings := srv.cellCouplingsLocked()
			srv.mu.Unlock()

			if got := couplings[cell]; got != model.Barrier {
				t.Fatalf("cellCouplingsLocked() insertion order %v, iteration %d: coupling = %v, want Barrier (tightest of Barrier+Independent)", order, i, got)
			}
		}
	}
}

// TestCouplingRankOrdersTightestToLoosest is the table-driven check for
// couplingRank's total order — Barrier < Leader < MessagePassing <
// Independent — that cellCouplingsLocked's tightest-wins comparison depends
// on.
func TestCouplingRankOrdersTightestToLoosest(t *testing.T) {
	tests := []struct {
		coupling model.Coupling
		rank     int
	}{
		{model.Barrier, 0},
		{model.Leader, 1},
		{model.MessagePassing, 2},
		{model.Independent, 3},
	}
	for _, tt := range tests {
		if got := couplingRank(tt.coupling); got != tt.rank {
			t.Errorf("couplingRank(%v) = %d, want %d", tt.coupling, got, tt.rank)
		}
	}

	if couplingRank(model.Barrier) >= couplingRank(model.Leader) ||
		couplingRank(model.Leader) >= couplingRank(model.MessagePassing) ||
		couplingRank(model.MessagePassing) >= couplingRank(model.Independent) {
		t.Fatal("couplingRank must strictly order Barrier < Leader < MessagePassing < Independent")
	}
}
