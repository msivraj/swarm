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
