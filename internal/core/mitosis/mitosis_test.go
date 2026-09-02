package mitosis

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestDecide(t *testing.T) {
	cfg := Thresholds{Target: 100, CooldownNS: 1000}
	cell := func(id string, size int) model.CellView {
		return model.CellView{ID: model.CellID(id), Size: size}
	}

	tests := []struct {
		name      string
		cells     []model.CellView
		cooldowns map[model.CellID]model.Instant
		now       model.Instant
		want      []Command
	}{
		{
			name:  "in band is a no-op",
			cells: []model.CellView{cell("a", 100), cell("b", 150)},
			want:  nil,
		},
		{
			name:  "over 2T splits",
			cells: []model.CellView{cell("a", 201)},
			want:  []Command{{Op: Split, Cell: "a"}},
		},
		{
			name:      "split suppressed during cooldown",
			cells:     []model.CellView{cell("a", 201)},
			cooldowns: map[model.CellID]model.Instant{"a": 500},
			now:       900, // 900-500 = 400 < 1000 window
			want:      nil,
		},
		{
			name:  "two under-full cells merge",
			cells: []model.CellView{cell("a", 20), cell("b", 30)},
			want:  []Command{{Op: Merge, Cell: "a", Other: "b"}},
		},
		{
			name:  "under-full but combined >= T does not merge",
			cells: []model.CellView{cell("a", 60), cell("b", 60)},
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Decide(tt.cells, cfg, tt.cooldowns, tt.now)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Decide() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestDecideIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestDecideIsDeterministic(t *testing.T) {
	cfg := Thresholds{Target: 50, CooldownNS: 0}
	cells := []model.CellView{{ID: "x", Size: 200}, {ID: "y", Size: 10}, {ID: "z", Size: 10}}
	first := Decide(cells, cfg, nil, 0)
	for i := 0; i < 100; i++ {
		if got := Decide(cells, cfg, nil, 0); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

func TestResizeSafe(t *testing.T) {
	tests := []struct {
		name   string
		c      model.Coupling
		atCkpt bool
		want   bool
	}{
		{"independent, not at checkpoint", model.Independent, false, true},
		{"independent, at checkpoint", model.Independent, true, true},
		{"barrier, not at checkpoint", model.Barrier, false, false},
		{"barrier, at checkpoint", model.Barrier, true, true},
		{"leader, not at checkpoint", model.Leader, false, false},
		{"leader, at checkpoint", model.Leader, true, true},
		{"message passing, not at checkpoint", model.MessagePassing, false, false},
		{"message passing, at checkpoint", model.MessagePassing, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResizeSafe(tt.c, tt.atCkpt); got != tt.want {
				t.Fatalf("ResizeSafe(%v, %v) = %v, want %v", tt.c, tt.atCkpt, got, tt.want)
			}
		})
	}
}

func TestGate(t *testing.T) {
	cmds := []Command{
		{Op: Split, Cell: "a"},
		{Op: Merge, Cell: "b", Other: "c"},
	}

	tests := []struct {
		name   string
		cmds   []Command
		c      model.Coupling
		atCkpt bool
		want   []Command
	}{
		{
			name:   "independent, not at checkpoint, passes through unchanged",
			cmds:   cmds,
			c:      model.Independent,
			atCkpt: false,
			want: []Command{
				{Op: Split, Cell: "a"},
				{Op: Merge, Cell: "b", Other: "c"},
			},
		},
		{
			name:   "independent, at checkpoint, passes through unchanged",
			cmds:   cmds,
			c:      model.Independent,
			atCkpt: true,
			want: []Command{
				{Op: Split, Cell: "a"},
				{Op: Merge, Cell: "b", Other: "c"},
			},
		},
		{
			name:   "barrier, not at checkpoint, all deferred, none dropped",
			cmds:   cmds,
			c:      model.Barrier,
			atCkpt: false,
			want: []Command{
				{Op: Split, Cell: "a", Deferred: true},
				{Op: Merge, Cell: "b", Other: "c", Deferred: true},
			},
		},
		{
			name:   "barrier, at checkpoint, passes through unchanged",
			cmds:   cmds,
			c:      model.Barrier,
			atCkpt: true,
			want: []Command{
				{Op: Split, Cell: "a"},
				{Op: Merge, Cell: "b", Other: "c"},
			},
		},
		{
			name:   "leader, not at checkpoint, all deferred",
			cmds:   cmds,
			c:      model.Leader,
			atCkpt: false,
			want: []Command{
				{Op: Split, Cell: "a", Deferred: true},
				{Op: Merge, Cell: "b", Other: "c", Deferred: true},
			},
		},
		{
			name:   "message passing, not at checkpoint, all deferred",
			cmds:   cmds,
			c:      model.MessagePassing,
			atCkpt: false,
			want: []Command{
				{Op: Split, Cell: "a", Deferred: true},
				{Op: Merge, Cell: "b", Other: "c", Deferred: true},
			},
		},
		{
			name:   "nil commands stay nil",
			cmds:   nil,
			c:      model.Barrier,
			atCkpt: false,
			want:   nil,
		},
		{
			name:   "empty commands stay empty",
			cmds:   []Command{},
			c:      model.Barrier,
			atCkpt: false,
			want:   []Command{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Gate(tt.cmds, tt.c, tt.atCkpt)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Gate() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestGateTimeline follows the §04 doc example: steps N..N+4, checkpoints at
// N and N+3. A Split requested at N+2 (not a checkpoint) is deferred; at the
// next checkpoint (N+3) it passes through unchanged — the command survives
// deferral and is emitted at the boundary.
func TestGateTimeline(t *testing.T) {
	requested := Command{Op: Split, Cell: "cell-1"}

	atN2 := Gate([]Command{requested}, model.Barrier, false)
	want := []Command{{Op: Split, Cell: "cell-1", Deferred: true}}
	if !reflect.DeepEqual(atN2, want) {
		t.Fatalf("at N+2 (non-checkpoint): Gate() = %+v, want %+v", atN2, want)
	}

	// The shell re-offers the (still original) command at the next checkpoint.
	atN3 := Gate([]Command{requested}, model.Barrier, true)
	wantAtBoundary := []Command{{Op: Split, Cell: "cell-1"}}
	if !reflect.DeepEqual(atN3, wantAtBoundary) {
		t.Fatalf("at N+3 (checkpoint): Gate() = %+v, want %+v", atN3, wantAtBoundary)
	}
}

// TestGateNeverResizesCoupledCellOffCheckpoint is the B3 property test: a
// coupled cell's mitosis command is NEVER allowed through unless atCkpt is
// true — it is always deferred off the checkpoint boundary, regardless of
// coupling kind or command shape.
func TestGateNeverResizesCoupledCellOffCheckpoint(t *testing.T) {
	couplings := []model.Coupling{model.Barrier, model.Leader, model.MessagePassing}
	ops := []Op{Split, Merge}
	cellIDs := []model.CellID{"a", "b", "c", "d"}

	for _, c := range couplings {
		for _, op := range ops {
			for _, id := range cellIDs {
				for _, other := range cellIDs {
					cmd := Command{Op: op, Cell: id, Other: other}
					got := Gate([]Command{cmd}, c, false)

					if len(got) != 1 {
						t.Fatalf("Gate(%+v, %v, false) dropped a command: got %+v", cmd, c, got)
					}
					if !got[0].Deferred {
						t.Fatalf("Gate(%+v, %v, false) = %+v, want Deferred true (B3 violated)", cmd, c, got[0])
					}
					if got[0].Op != cmd.Op || got[0].Cell != cmd.Cell || got[0].Other != cmd.Other {
						t.Fatalf("Gate(%+v, %v, false) mutated the command: got %+v", cmd, c, got[0])
					}
				}
			}
		}
	}
}

// TestGateIsDeterministic guards Gate's own determinism, matching the P0
// Decide guarantee: identical inputs always produce identical output.
func TestGateIsDeterministic(t *testing.T) {
	cmds := []Command{{Op: Split, Cell: "x"}, {Op: Merge, Cell: "y", Other: "z"}}
	first := Gate(cmds, model.Barrier, false)
	for i := 0; i < 100; i++ {
		if got := Gate(cmds, model.Barrier, false); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// --- P6 §03 upgrade: signal-based adaptive mitosis ---------------------

// TestSignalThreshold pins the SLO -> latency-band derivation: base bands
// tightest to loosest (Barrier < Leader < MessagePassing < Independent),
// tightened by AtRisk (the SLO's remaining-budget fraction), floored at
// minBudgetFraction and clamped at 1 (never loosened past the base band).
func TestSignalThreshold(t *testing.T) {
	tests := []struct {
		name      string
		c         model.Coupling
		slo       model.SLO
		wantSplit model.Duration
		wantMerge model.Duration
	}{
		{"barrier, full budget", model.Barrier, model.SLO{AtRisk: 1}, 20_000_000, 10_000_000},
		{"barrier, half budget tightens by half", model.Barrier, model.SLO{AtRisk: 0.5}, 10_000_000, 5_000_000},
		{"barrier, zero AtRisk floors at min fraction", model.Barrier, model.SLO{AtRisk: 0}, 2_000_000, 1_000_000},
		{"barrier, AtRisk over 1 clamps to base", model.Barrier, model.SLO{AtRisk: 2}, 20_000_000, 10_000_000},
		{"leader, full budget", model.Leader, model.SLO{AtRisk: 1}, 30_000_000, 15_000_000},
		{"message passing, full budget", model.MessagePassing, model.SLO{AtRisk: 1}, 50_000_000, 25_000_000},
		{"independent, full budget", model.Independent, model.SLO{AtRisk: 1}, 200_000_000, 100_000_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := signalThreshold(tt.c, tt.slo)
			want := model.Threshold{SplitP99: tt.wantSplit, MergeP99: tt.wantMerge}
			if got != want {
				t.Fatalf("signalThreshold(%v, %+v) = %+v, want %+v", tt.c, tt.slo, got, want)
			}
		})
	}
}

// TestSignalThresholdCouplingOrdering is the per-coupling property: at a
// fixed SLO, coupled cells (Barrier, Leader, MessagePassing) always get a
// band at least as tight as Independent's, and every band keeps the
// split/merge hysteresis gap (MergeP99 strictly below SplitP99).
func TestSignalThresholdCouplingOrdering(t *testing.T) {
	slo := model.SLO{AtRisk: 1}
	barrier := signalThreshold(model.Barrier, slo)
	leader := signalThreshold(model.Leader, slo)
	messagePassing := signalThreshold(model.MessagePassing, slo)
	independent := signalThreshold(model.Independent, slo)

	if !(barrier.SplitP99 <= leader.SplitP99 &&
		leader.SplitP99 <= messagePassing.SplitP99 &&
		messagePassing.SplitP99 <= independent.SplitP99) {
		t.Fatalf("coupling ordering violated: barrier=%d leader=%d messagePassing=%d independent=%d",
			barrier.SplitP99, leader.SplitP99, messagePassing.SplitP99, independent.SplitP99)
	}
	for _, th := range []model.Threshold{barrier, leader, messagePassing, independent} {
		if th.MergeP99 >= th.SplitP99 {
			t.Fatalf("hysteresis gap violated: %+v", th)
		}
	}
}

// TestDecideSignal exercises the signal-based decision itself: split above
// the SLO-derived band, merge well under it, neither in the hysteresis dead
// zone, and cooldown suppression exactly as Decide applies it.
func TestDecideSignal(t *testing.T) {
	// Barrier @ AtRisk=1 (see TestSignalThreshold): SplitP99=20ms, MergeP99=10ms.
	cfg := model.SignalThresholds{Target: 100, CooldownNS: 1000, SLO: model.SLO{AtRisk: 1}}
	cell := func(id string, size int, p99 model.Duration) model.CellSignal {
		return model.CellSignal{Cell: model.CellID(id), Coupling: model.Barrier, Size: size, P99: p99}
	}

	tests := []struct {
		name      string
		cells     []model.CellSignal
		cooldowns map[model.CellID]model.Instant
		now       model.Instant
		want      []Command
	}{
		{
			name:  "p99 over split threshold splits",
			cells: []model.CellSignal{cell("a", 50, 25_000_000)}, // 25ms > 20ms
			want:  []Command{{Op: Split, Cell: "a"}},
		},
		{
			name: "p99 well under merge threshold merges",
			cells: []model.CellSignal{
				cell("a", 20, 5_000_000),
				cell("b", 30, 5_000_000),
			},
			want: []Command{{Op: Merge, Cell: "a", Other: "b"}},
		},
		{
			name:  "p99 in hysteresis dead zone does neither",
			cells: []model.CellSignal{cell("a", 20, 15_000_000)}, // between 10ms and 20ms
			want:  nil,
		},
		{
			name: "merge-eligible individually but combined size >= target does not merge",
			cells: []model.CellSignal{
				cell("a", 60, 5_000_000),
				cell("b", 60, 5_000_000),
			},
			want: nil,
		},
		{
			name:      "split suppressed during cooldown even with a splitting p99",
			cells:     []model.CellSignal{cell("a", 50, 25_000_000)},
			cooldowns: map[model.CellID]model.Instant{"a": 500},
			now:       900, // 900-500 = 400 < 1000 window
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecideSignal(tt.cells, cfg, tt.cooldowns, tt.now)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("DecideSignal() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestDecideSignalSubsumesCount is the headline P6 property: when every
// cell's P99 is absent (the zero value, unmeasured), DecideSignal reproduces
// exactly what Decide decides for the equivalent []CellView — signal-based
// subsumes count-based, so P0's existing mitosis semantics keep holding
// under the new core, for any coupling.
func TestDecideSignalSubsumesCount(t *testing.T) {
	cfg := Thresholds{Target: 100, CooldownNS: 1000}
	sigCfg := model.SignalThresholds{
		Target: cfg.Target, CooldownNS: cfg.CooldownNS,
		SLO: model.SLO{Objective: 0.999, AtRisk: 0.5},
	}
	cell := func(id string, size int) model.CellView {
		return model.CellView{ID: model.CellID(id), Size: size}
	}
	couplings := []model.Coupling{model.Independent, model.Barrier, model.Leader, model.MessagePassing}

	tests := []struct {
		name      string
		cells     []model.CellView
		cooldowns map[model.CellID]model.Instant
		now       model.Instant
	}{
		{name: "in band is a no-op", cells: []model.CellView{cell("a", 100), cell("b", 150)}},
		{name: "over 2T splits", cells: []model.CellView{cell("a", 201)}},
		{
			name:      "split suppressed during cooldown",
			cells:     []model.CellView{cell("a", 201)},
			cooldowns: map[model.CellID]model.Instant{"a": 500},
			now:       900,
		},
		{name: "two under-full cells merge", cells: []model.CellView{cell("a", 20), cell("b", 30)}},
		{name: "under-full but combined >= T does not merge", cells: []model.CellView{cell("a", 60), cell("b", 60)}},
		{
			name: "mixed splits and merges",
			cells: []model.CellView{
				cell("a", 250), cell("b", 10), cell("c", 15), cell("d", 400),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := Decide(tt.cells, cfg, tt.cooldowns, tt.now)

			for _, coupling := range couplings {
				sigs := make([]model.CellSignal, len(tt.cells))
				for i, v := range tt.cells {
					sigs[i] = model.CellSignal{Cell: v.ID, Coupling: coupling, Size: v.Size}
				}

				got := DecideSignal(sigs, sigCfg, tt.cooldowns, tt.now)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("coupling=%v: DecideSignal(P99=0) = %+v, want Decide() = %+v", coupling, got, want)
				}
			}
		})
	}
}

// TestDecideSignalIsDeterministic guards the same defining property as
// TestDecideIsDeterministic: identical inputs always produce identical
// output.
func TestDecideSignalIsDeterministic(t *testing.T) {
	cfg := model.SignalThresholds{Target: 50, CooldownNS: 0, SLO: model.SLO{AtRisk: 1}}
	cells := []model.CellSignal{
		{Cell: "x", Coupling: model.Barrier, P99: 25_000_000},
		{Cell: "y", Coupling: model.Independent, Size: 10},
		{Cell: "z", Coupling: model.Independent, Size: 10},
	}
	first := DecideSignal(cells, cfg, nil, 0)
	for i := 0; i < 100; i++ {
		if got := DecideSignal(cells, cfg, nil, 0); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// TestDecideSignalSplitSetIsOrderIndependent guards against map-iteration or
// positional nondeterminism: reordering the input cells must not change
// which cells split (merge pairing is intentionally order-dependent, exactly
// as Decide's own consecutive-pairing merge is).
func TestDecideSignalSplitSetIsOrderIndependent(t *testing.T) {
	cfg := model.SignalThresholds{Target: 100, CooldownNS: 0, SLO: model.SLO{AtRisk: 1}}
	a := model.CellSignal{Cell: "a", Coupling: model.Barrier, P99: 25_000_000}
	b := model.CellSignal{Cell: "b", Coupling: model.Leader, P99: 35_000_000}
	c := model.CellSignal{Cell: "c", Coupling: model.Independent, Size: 500}

	orders := [][]model.CellSignal{
		{a, b, c},
		{c, b, a},
		{b, a, c},
	}

	splitSet := func(cmds []Command) map[model.CellID]bool {
		set := make(map[model.CellID]bool, len(cmds))
		for _, cmd := range cmds {
			set[cmd.Cell] = true
		}
		return set
	}

	want := splitSet(DecideSignal(orders[0], cfg, nil, 0))
	for _, order := range orders[1:] {
		got := splitSet(DecideSignal(order, cfg, nil, 0))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("split set changed with cell order %+v: got %+v, want %+v", order, got, want)
		}
	}
}
