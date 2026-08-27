package registry

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestApply(t *testing.T) {
	populated := func() Registry {
		reg := Registry{}
		reg, _ = Apply(reg, RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 5})
		reg, _ = Apply(reg, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"})
		reg, _ = Apply(reg, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "y"})
		return reg
	}

	tests := []struct {
		name        string
		reg         Registry
		ev          RegistryEvent
		wantChanges []Change
		wantView    []model.CellView
	}{
		{
			name:        "CellUp on empty registry adds the cell",
			reg:         Registry{},
			ev:          RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 5},
			wantChanges: []Change{{Kind: CellAdded, Cell: "a", Capacity: 5}},
			wantView:    []model.CellView{{ID: "a", Size: 0, Free: 5}},
		},
		{
			name:        "CellUp on a cell that already exists is a no-op",
			reg:         populated(),
			ev:          RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 99},
			wantChanges: nil,
			wantView:    []model.CellView{{ID: "a", Size: 2, Free: 3}},
		},
		{
			name:        "CellDown on empty registry is a no-op",
			reg:         Registry{},
			ev:          RegistryEvent{Kind: CellDown, Cell: "a"},
			wantChanges: nil,
			wantView:    nil,
		},
		{
			name:        "CellDown removes an existing cell and its members",
			reg:         populated(),
			ev:          RegistryEvent{Kind: CellDown, Cell: "a"},
			wantChanges: []Change{{Kind: CellRemoved, Cell: "a"}},
			wantView:    nil,
		},
		{
			name:        "CapacityChanged on empty registry is a no-op",
			reg:         Registry{},
			ev:          RegistryEvent{Kind: CapacityChanged, Cell: "a", Capacity: 10},
			wantChanges: nil,
			wantView:    nil,
		},
		{
			name:        "CapacityChanged updates a populated cell's free capacity",
			reg:         populated(),
			ev:          RegistryEvent{Kind: CapacityChanged, Cell: "a", Capacity: 10},
			wantChanges: []Change{{Kind: CapacityUpdated, Cell: "a", Capacity: 10}},
			wantView:    []model.CellView{{ID: "a", Size: 2, Free: 8}},
		},
		{
			name:        "CapacityChanged to the same capacity is a no-op",
			reg:         populated(),
			ev:          RegistryEvent{Kind: CapacityChanged, Cell: "a", Capacity: 5},
			wantChanges: nil,
			wantView:    []model.CellView{{ID: "a", Size: 2, Free: 3}},
		},
		{
			name:        "AgentJoined on empty registry is a no-op",
			reg:         Registry{},
			ev:          RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "z"},
			wantChanges: nil,
			wantView:    nil,
		},
		{
			name:        "AgentJoined adds a new member and shrinks free capacity",
			reg:         populated(),
			ev:          RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "z"},
			wantChanges: []Change{{Kind: AgentAdded, Cell: "a", Agent: "z"}},
			wantView:    []model.CellView{{ID: "a", Size: 3, Free: 2}},
		},
		{
			name:        "AgentJoined for an existing member is a no-op",
			reg:         populated(),
			ev:          RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"},
			wantChanges: nil,
			wantView:    []model.CellView{{ID: "a", Size: 2, Free: 3}},
		},
		{
			name:        "AgentLeft on empty registry is a no-op",
			reg:         Registry{},
			ev:          RegistryEvent{Kind: AgentLeft, Cell: "a", Agent: "x"},
			wantChanges: nil,
			wantView:    nil,
		},
		{
			name:        "AgentLeft removes a member and grows free capacity",
			reg:         populated(),
			ev:          RegistryEvent{Kind: AgentLeft, Cell: "a", Agent: "x"},
			wantChanges: []Change{{Kind: AgentRemoved, Cell: "a", Agent: "x"}},
			wantView:    []model.CellView{{ID: "a", Size: 1, Free: 4}},
		},
		{
			name:        "AgentLeft for a non-member is a no-op",
			reg:         populated(),
			ev:          RegistryEvent{Kind: AgentLeft, Cell: "a", Agent: "not-here"},
			wantChanges: nil,
			wantView:    []model.CellView{{ID: "a", Size: 2, Free: 3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, changes := Apply(tt.reg, tt.ev)
			if !reflect.DeepEqual(changes, tt.wantChanges) {
				t.Fatalf("Apply() changes = %+v, want %+v", changes, tt.wantChanges)
			}
			if got := Snapshot(next); !reflect.DeepEqual(got, tt.wantView) {
				t.Fatalf("Snapshot(next) = %+v, want %+v", got, tt.wantView)
			}
		})
	}
}

func TestSnapshot(t *testing.T) {
	tests := []struct {
		name string
		reg  Registry
		want []model.CellView
	}{
		{
			name: "empty registry snapshots to nil",
			reg:  Registry{},
			want: nil,
		},
		{
			name: "multiple cells are sorted by CellID",
			reg: buildRegistry(
				RegistryEvent{Kind: CellUp, Cell: "c", Capacity: 3},
				RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 1},
				RegistryEvent{Kind: CellUp, Cell: "b", Capacity: 2},
			),
			want: []model.CellView{
				{ID: "a", Size: 0, Free: 1},
				{ID: "b", Size: 0, Free: 2},
				{ID: "c", Size: 0, Free: 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Snapshot(tt.reg); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Snapshot() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// buildRegistry folds a sequence of events into a Registry starting from
// empty, for tests that only care about the resulting state.
func buildRegistry(evs ...RegistryEvent) Registry {
	var reg Registry
	for _, ev := range evs {
		reg, _ = Apply(reg, ev)
	}
	return reg
}

// TestApplyIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestApplyIsDeterministic(t *testing.T) {
	reg := buildRegistry(
		RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 5},
		RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"},
	)
	ev := RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "y"}

	firstReg, firstChanges := Apply(reg, ev)
	for i := 0; i < 100; i++ {
		gotReg, gotChanges := Apply(reg, ev)
		if !reflect.DeepEqual(gotChanges, firstChanges) {
			t.Fatalf("non-deterministic changes on run %d: %+v vs %+v", i, gotChanges, firstChanges)
		}
		if !reflect.DeepEqual(Snapshot(gotReg), Snapshot(firstReg)) {
			t.Fatalf("non-deterministic registry on run %d: %+v vs %+v", i, Snapshot(gotReg), Snapshot(firstReg))
		}
	}
}

// TestSnapshotIsOrderDeterministic guards the acceptance criterion that
// Snapshot is stable and order-deterministic despite Go's randomized map
// iteration order.
func TestSnapshotIsOrderDeterministic(t *testing.T) {
	reg := buildRegistry(
		RegistryEvent{Kind: CellUp, Cell: "z", Capacity: 1},
		RegistryEvent{Kind: CellUp, Cell: "m", Capacity: 1},
		RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 1},
		RegistryEvent{Kind: CellUp, Cell: "q", Capacity: 1},
	)
	first := Snapshot(reg)
	for i := 0; i < 100; i++ {
		if got := Snapshot(reg); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic snapshot order on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// TestApplyDoesNotMutateInput is the immutability property test the
// acceptance criteria name: Apply must never mutate its input Registry — the
// caller's existing snapshot has to stay valid after Apply runs, since other
// readers may still hold it.
func TestApplyDoesNotMutateInput(t *testing.T) {
	original := buildRegistry(
		RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 5},
		RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"},
	)
	before := Snapshot(original)

	events := []RegistryEvent{
		{Kind: CellUp, Cell: "b", Capacity: 3},
		{Kind: AgentJoined, Cell: "a", Agent: "y"},
		{Kind: AgentJoined, Cell: "a", Agent: "z"},
		{Kind: AgentLeft, Cell: "a", Agent: "x"},
		{Kind: CapacityChanged, Cell: "a", Capacity: 20},
		{Kind: CellDown, Cell: "a"},
	}
	for _, ev := range events {
		Apply(original, ev)
		if got := Snapshot(original); !reflect.DeepEqual(got, before) {
			t.Fatalf("Apply mutated its input for %+v: original snapshot = %+v, want %+v", ev, got, before)
		}
	}
}
