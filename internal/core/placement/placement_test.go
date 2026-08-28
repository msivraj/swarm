package placement

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestPlace(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	cell := func(id string, free int) model.CellView {
		return model.CellView{ID: model.CellID(id), Free: free}
	}

	tests := []struct {
		name  string
		cells []model.CellView
		want  Placement
	}{
		{
			name:  "empty slice returns NoCapacity",
			cells: nil,
			want:  Placement{Kind: NoCapacity},
		},
		{
			name:  "all cells full returns NoCapacity",
			cells: []model.CellView{cell("a", 0), cell("b", 0)},
			want:  Placement{Kind: NoCapacity},
		},
		{
			name:  "exactly one cell with capacity is assigned",
			cells: []model.CellView{cell("a", 0), cell("b", 3)},
			want:  Placement{Kind: Assign, Cell: "b"},
		},
		{
			name:  "multiple eligible cells picks first in slice order",
			cells: []model.CellView{cell("a", 0), cell("b", 5), cell("c", 7)},
			want:  Placement{Kind: Assign, Cell: "b"},
		},
		{
			name:  "first cell with free capacity is assigned",
			cells: []model.CellView{cell("a", 1), cell("b", 1)},
			want:  Placement{Kind: Assign, Cell: "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Place(task, tt.cells)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Place() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestPlaceIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestPlaceIsDeterministic(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	cells := []model.CellView{
		{ID: "x", Free: 0},
		{ID: "y", Free: 2},
		{ID: "z", Free: 4},
	}
	first := Place(task, cells)
	for i := 0; i < 100; i++ {
		if got := Place(task, cells); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

func TestPlaceAcross(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	cell := func(id string, free int) model.CellView {
		return model.CellView{ID: model.CellID(id), Free: free}
	}
	region := func(id string, free int, h model.Health) model.RegionView {
		return model.RegionView{ID: model.RegionID(id), Free: free, Health: h}
	}

	tests := []struct {
		name  string
		local []model.CellView
		peers []model.RegionView
		want  Placement
	}{
		{
			name:  "local has capacity: assigned locally, never spills",
			local: []model.CellView{cell("a", 0), cell("b", 3)},
			peers: []model.RegionView{region("r1", 10, model.Healthy)},
			want:  Placement{Kind: Assign, Cell: "b"},
		},
		{
			name:  "local full, one peer with capacity: spills to it",
			local: []model.CellView{cell("a", 0), cell("b", 0)},
			peers: []model.RegionView{region("r1", 5, model.Healthy)},
			want:  Placement{Kind: Spill, Region: "r1"},
		},
		{
			name:  "local full, several peers with capacity: deterministic first-fit pick",
			local: []model.CellView{cell("a", 0)},
			peers: []model.RegionView{
				region("r1", 0, model.Healthy),
				region("r2", 4, model.Healthy),
				region("r3", 9, model.Healthy),
			},
			want: Placement{Kind: Spill, Region: "r2"},
		},
		{
			name:  "local full, no peer with capacity: NoCapacity",
			local: []model.CellView{cell("a", 0)},
			peers: []model.RegionView{region("r1", 0, model.Healthy), region("r2", 0, model.Healthy)},
			want:  Placement{Kind: NoCapacity},
		},
		{
			name:  "empty local and empty peers: NoCapacity",
			local: nil,
			peers: nil,
			want:  Placement{Kind: NoCapacity},
		},
		{
			name:  "local full, peer has capacity but is Degraded: not chosen",
			local: []model.CellView{cell("a", 0)},
			peers: []model.RegionView{region("r1", 5, model.Degraded)},
			want:  Placement{Kind: NoCapacity},
		},
		{
			name:  "local full, peer has capacity but is Unreachable: not chosen",
			local: []model.CellView{cell("a", 0)},
			peers: []model.RegionView{region("r1", 5, model.Unreachable)},
			want:  Placement{Kind: NoCapacity},
		},
		{
			name:  "local full, unhealthy peer skipped in favor of a healthy one later in slice order",
			local: []model.CellView{cell("a", 0)},
			peers: []model.RegionView{
				region("r1", 5, model.Degraded),
				region("r2", 5, model.Healthy),
			},
			want: Placement{Kind: Spill, Region: "r2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PlaceAcross(task, tt.local, tt.peers)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("PlaceAcross() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestPlaceAcrossLocalityFirst guards the priority law the phase doc names:
// whenever any local cell has free capacity, PlaceAcross must assign locally
// and never spill, regardless of what the peer snapshots look like.
func TestPlaceAcrossLocalityFirst(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	localWithRoom := []model.CellView{
		{ID: "a", Free: 0},
		{ID: "b", Free: 1},
		{ID: "c", Free: 0},
	}

	peerCases := [][]model.RegionView{
		nil,
		{{ID: "r1", Free: 0, Health: model.Healthy}},
		{{ID: "r1", Free: 100, Health: model.Healthy}},
		{{ID: "r1", Free: 100, Health: model.Unreachable}, {ID: "r2", Free: 50, Health: model.Healthy}},
	}

	for i, peers := range peerCases {
		got := PlaceAcross(task, localWithRoom, peers)
		if got.Kind == Spill {
			t.Fatalf("peer case %d: PlaceAcross spilled while local had room: %+v", i, got)
		}
		if got.Kind != Assign || got.Cell != "b" {
			t.Fatalf("peer case %d: PlaceAcross() = %+v, want Assign{b}", i, got)
		}
	}
}

// TestPlaceAcrossIsDeterministic guards the core's defining property for the
// spill path too: identical (task, local, peers) inputs always return the
// identical Placement.
func TestPlaceAcrossIsDeterministic(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	local := []model.CellView{{ID: "a", Free: 0}, {ID: "b", Free: 0}}
	peers := []model.RegionView{
		{ID: "r1", Free: 0, Health: model.Healthy},
		{ID: "r2", Free: 3, Health: model.Healthy},
		{ID: "r3", Free: 8, Health: model.Healthy},
	}

	first := PlaceAcross(task, local, peers)
	for i := 0; i < 100; i++ {
		if got := PlaceAcross(task, local, peers); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

func TestSatisfies(t *testing.T) {
	tests := []struct {
		name     string
		offered  model.CapSet
		required model.CapSet
		want     bool
	}{
		{
			name:     "required subset of offered => true",
			offered:  model.CapSet{"gpu", "nvlink"},
			required: model.CapSet{"gpu"},
			want:     true,
		},
		{
			name:     "required equals offered => true",
			offered:  model.CapSet{"gpu", "nvlink"},
			required: model.CapSet{"gpu", "nvlink"},
			want:     true,
		},
		{
			name:     "missing a required cap => false",
			offered:  model.CapSet{"gpu"},
			required: model.CapSet{"gpu", "nvlink"},
			want:     false,
		},
		{
			name:     "offered has none of the required caps => false",
			offered:  model.CapSet{"cpu"},
			required: model.CapSet{"gpu"},
			want:     false,
		},
		{
			name:     "nil required is satisfied by nil offered",
			offered:  nil,
			required: nil,
			want:     true,
		},
		{
			name:     "nil required is satisfied by any offered",
			offered:  model.CapSet{"gpu"},
			required: nil,
			want:     true,
		},
		{
			name:     "empty required is satisfied by nil offered",
			offered:  nil,
			required: model.CapSet{},
			want:     true,
		},
		{
			name:     "required non-empty but offered nil => false",
			offered:  nil,
			required: model.CapSet{"gpu"},
			want:     false,
		},
		{
			name:     "unsorted offered still matches",
			offered:  model.CapSet{"nvlink", "gpu", "cpu"},
			required: model.CapSet{"gpu", "cpu"},
			want:     true,
		},
		{
			name:     "duplicated offered tags still match",
			offered:  model.CapSet{"gpu", "gpu", "nvlink"},
			required: model.CapSet{"gpu", "nvlink"},
			want:     true,
		},
		{
			name:     "duplicated and unsorted required tags still match",
			offered:  model.CapSet{"gpu", "nvlink"},
			required: model.CapSet{"nvlink", "gpu", "nvlink"},
			want:     true,
		},
		{
			name:     "duplicated required tag missing from offered => false",
			offered:  model.CapSet{"gpu"},
			required: model.CapSet{"nvlink", "nvlink"},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Satisfies(tt.offered, tt.required); got != tt.want {
				t.Fatalf("Satisfies(%v, %v) = %v, want %v", tt.offered, tt.required, got, tt.want)
			}
		})
	}
}

// TestSatisfiesIsDeterministic guards the core's defining property for the
// capability predicate: identical inputs always produce identical output.
func TestSatisfiesIsDeterministic(t *testing.T) {
	offered := model.CapSet{"nvlink", "gpu", "gpu", "cpu"}
	required := model.CapSet{"gpu", "nvlink"}
	first := Satisfies(offered, required)
	for i := 0; i < 100; i++ {
		if got := Satisfies(offered, required); got != first {
			t.Fatalf("non-deterministic output on run %d: %v vs %v", i, got, first)
		}
	}
}

func TestPlaceCapable(t *testing.T) {
	cell := func(id string, free int, caps ...string) model.CellView {
		var cs model.CapSet
		if len(caps) > 0 {
			cs = model.CapSet(caps)
		}
		return model.CellView{ID: model.CellID(id), Free: free, Caps: cs}
	}

	tests := []struct {
		name  string
		task  model.Task
		cells []model.CellView
		want  Placement
	}{
		{
			name:  "capless task behaves like Place: assigns first cell with capacity",
			task:  model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{cell("a", 0), cell("b", 3, "gpu")},
			want:  Placement{Kind: Assign, Cell: "b"},
		},
		{
			name:  "capless task on capless cells: assigns first with capacity",
			task:  model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{cell("a", 0), cell("b", 2)},
			want:  Placement{Kind: Assign, Cell: "b"},
		},
		{
			name: "GPU-required task skips CPU-only cell with free capacity, lands on GPU cell",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				cell("cpu-only", 5),
				cell("gpu-cell", 3, "gpu"),
			},
			want: Placement{Kind: Assign, Cell: "gpu-cell"},
		},
		{
			name: "capable cell with no free capacity is skipped for a later capable cell",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				cell("gpu-full", 0, "gpu"),
				cell("gpu-free", 4, "gpu"),
			},
			want: Placement{Kind: Assign, Cell: "gpu-free"},
		},
		{
			name: "capable cell picked over an earlier incapable cell with free capacity",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				cell("no-gpu", 10),
				cell("has-gpu", 1, "gpu"),
			},
			want: Placement{Kind: Assign, Cell: "has-gpu"},
		},
		{
			name: "no capable cell has free capacity => NoCapacity even though an incapable cell does",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				cell("no-gpu", 10),
				cell("gpu-full", 0, "gpu"),
			},
			want: Placement{Kind: NoCapacity},
		},
		{
			name: "no cell offers the required capability at all => NoCapacity",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				cell("a", 5),
				cell("b", 5),
			},
			want: Placement{Kind: NoCapacity},
		},
		{
			name:  "empty cells => NoCapacity",
			task:  model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: nil,
			want:  Placement{Kind: NoCapacity},
		},
		{
			name: "multi-capability requirement: only cell with both caps is capable",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu", "nvlink"}},
			cells: []model.CellView{
				cell("gpu-only", 5, "gpu"),
				cell("gpu-nvlink", 5, "gpu", "nvlink"),
			},
			want: Placement{Kind: Assign, Cell: "gpu-nvlink"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PlaceCapable(tt.task, tt.cells)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("PlaceCapable() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestPlaceCapableRegressionMatchesPlace is the ticket-required regression
// guard: for a task with nil Requires, PlaceCapable must return exactly what
// Place returns, across a range of fleets — the capability path must never
// change non-GPU placement.
func TestPlaceCapableRegressionMatchesPlace(t *testing.T) {
	cell := func(id string, free int, caps ...string) model.CellView {
		var cs model.CapSet
		if len(caps) > 0 {
			cs = model.CapSet(caps)
		}
		return model.CellView{ID: model.CellID(id), Free: free, Caps: cs}
	}
	task := model.Task{ID: "t1", JobID: "j1"}

	fleets := [][]model.CellView{
		nil,
		{},
		{cell("a", 0)},
		{cell("a", 0), cell("b", 0)},
		{cell("a", 0), cell("b", 3)},
		{cell("a", 1), cell("b", 1)},
		{cell("a", 0), cell("b", 5, "gpu"), cell("c", 7)},
		{cell("a", 5, "gpu", "nvlink"), cell("b", 5)},
	}

	for i, cells := range fleets {
		want := Place(task, cells)
		got := PlaceCapable(task, cells)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fleet %d: PlaceCapable(nil-Requires task) = %+v, want Place() = %+v", i, got, want)
		}
	}
}

// TestPlaceCapableIsDeterministic guards the core's defining property:
// identical inputs always produce identical output.
func TestPlaceCapableIsDeterministic(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}}
	cells := []model.CellView{
		{ID: "x", Free: 0, Caps: model.CapSet{"gpu"}},
		{ID: "y", Free: 2},
		{ID: "z", Free: 4, Caps: model.CapSet{"gpu"}},
	}
	first := PlaceCapable(task, cells)
	for i := 0; i < 100; i++ {
		if got := PlaceCapable(task, cells); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
