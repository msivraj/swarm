package placement

import (
	"fmt"
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

// rankCell is a small helper for building model.CellView fixtures for the
// Rank/BestFit tests below.
func rankCell(id string, free int, caps ...string) model.CellView {
	var cs model.CapSet
	if len(caps) > 0 {
		cs = model.CapSet(caps)
	}
	return model.CellView{ID: model.CellID(id), Free: free, Caps: cs}
}

func topo(region, az, rack string) model.Topology {
	return model.Topology{Region: model.RegionID(region), AZ: az, Rack: rack}
}

func TestRank(t *testing.T) {
	gpuTask := model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}}
	origin := topo("r1", "az1", "rack1")

	tests := []struct {
		name  string
		task  model.Task
		cells []model.CellView
		loc   model.LocalityGraph
		want  []model.Ranked
	}{
		{
			name:  "empty cands returns empty slice",
			task:  gpuTask,
			cells: nil,
			loc:   model.LocalityGraph{Origin: origin},
			want:  []model.Ranked{},
		},
		{
			name: "capable cell outranks incapable cell regardless of distance/free",
			task: gpuTask,
			cells: []model.CellView{
				rankCell("no-gpu", 100),
				rankCell("has-gpu", 1, "gpu"),
			},
			loc: model.LocalityGraph{
				Origin: origin,
				Zone: map[model.CellID]model.Topology{
					"no-gpu":  origin,               // same rack: distance 0
					"has-gpu": topo("r9", "z", "r"), // cross-region: distance 3
				},
			},
			want: []model.Ranked{
				{Cell: "has-gpu", CapMatch: true, Distance: 3, Free: 1},
				{Cell: "no-gpu", CapMatch: false, Distance: 0, Free: 100},
			},
		},
		{
			name: "equal CapMatch: closer distance ranks first",
			task: model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{
				rankCell("far", 5),
				rankCell("near", 5),
			},
			loc: model.LocalityGraph{
				Origin: origin,
				Zone: map[model.CellID]model.Topology{
					"far":  topo("r9", "az9", "rack9"),
					"near": origin,
				},
			},
			want: []model.Ranked{
				{Cell: "near", CapMatch: true, Distance: 0, Free: 5},
				{Cell: "far", CapMatch: true, Distance: 3, Free: 5},
			},
		},
		{
			name: "equal CapMatch and distance: more free capacity ranks first",
			task: model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{
				rankCell("low", 1),
				rankCell("high", 9),
			},
			loc: model.LocalityGraph{
				Origin: origin,
				Zone: map[model.CellID]model.Topology{
					"low":  origin,
					"high": origin,
				},
			},
			want: []model.Ranked{
				{Cell: "high", CapMatch: true, Distance: 0, Free: 9},
				{Cell: "low", CapMatch: true, Distance: 0, Free: 1},
			},
		},
		{
			name: "equal CapMatch, distance, and free: ordered by CellID ascending",
			task: model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{
				rankCell("charlie", 5),
				rankCell("alpha", 5),
				rankCell("bravo", 5),
			},
			loc: model.LocalityGraph{Origin: origin},
			want: []model.Ranked{
				{Cell: "alpha", CapMatch: true, Distance: 3, Free: 5},
				{Cell: "bravo", CapMatch: true, Distance: 3, Free: 5},
				{Cell: "charlie", CapMatch: true, Distance: 3, Free: 5},
			},
		},
		{
			name: "cell absent from Zone gets max distance",
			task: model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{
				rankCell("known", 5),
				rankCell("unknown", 5),
			},
			loc: model.LocalityGraph{
				Origin: origin,
				Zone:   map[model.CellID]model.Topology{"known": origin},
			},
			want: []model.Ranked{
				{Cell: "known", CapMatch: true, Distance: 0, Free: 5},
				{Cell: "unknown", CapMatch: true, Distance: 3, Free: 5},
			},
		},
		{
			name: "nil Zone treats every cell as max distance, never panics",
			task: model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{
				rankCell("a", 5),
				rankCell("b", 5),
			},
			loc: model.LocalityGraph{Origin: origin, Zone: nil},
			want: []model.Ranked{
				{Cell: "a", CapMatch: true, Distance: 3, Free: 5},
				{Cell: "b", CapMatch: true, Distance: 3, Free: 5},
			},
		},
		{
			name: "same region different AZ: distance 2",
			task: model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{
				rankCell("c", 5),
			},
			loc: model.LocalityGraph{
				Origin: origin,
				Zone:   map[model.CellID]model.Topology{"c": topo("r1", "az2", "rack1")},
			},
			want: []model.Ranked{
				{Cell: "c", CapMatch: true, Distance: 2, Free: 5},
			},
		},
		{
			name: "same region same AZ different rack: distance 1",
			task: model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{
				rankCell("c", 5),
			},
			loc: model.LocalityGraph{
				Origin: origin,
				Zone:   map[model.CellID]model.Topology{"c": topo("r1", "az1", "rack2")},
			},
			want: []model.Ranked{
				{Cell: "c", CapMatch: true, Distance: 1, Free: 5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Rank(tt.task, tt.cells, tt.loc)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Rank() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestRankGreedyTerminates guards the "greedy terminates" property: for
// arbitrary candidate counts (including zero) Rank returns exactly
// len(cands) entries — no unbounded loop, no dropped/duplicated candidates
// — and the result is a genuine total order (every adjacent pair, and by
// transitivity every pair, respects the documented ordering keys).
func TestRankGreedyTerminates(t *testing.T) {
	origin := topo("r1", "az1", "rack1")
	sizes := []int{0, 1, 2, 5, 17, 64}

	for _, n := range sizes {
		cells := make([]model.CellView, n)
		zone := make(map[model.CellID]model.Topology, n)
		for i := 0; i < n; i++ {
			id := model.CellID(fmt.Sprintf("cell-%03d", i))
			cells[i] = model.CellView{ID: id, Free: (i * 7) % 11}
			zone[id] = topo("r1", "az1", "rack1")
		}
		loc := model.LocalityGraph{Origin: origin, Zone: zone}
		task := model.Task{ID: "t1", JobID: "j1"}

		got := Rank(task, cells, loc)
		if len(got) != n {
			t.Fatalf("n=%d: Rank() returned %d entries, want %d", n, len(got), n)
		}

		for i := 1; i < len(got); i++ {
			if !rankLessOrEqual(got[i-1], got[i]) {
				t.Fatalf("n=%d: total order violated at index %d: %+v then %+v", n, i, got[i-1], got[i])
			}
		}
	}
}

// rankLessOrEqual reports whether a legitimately sorts at or before b under
// Rank's documented total order (CapMatch desc, Distance asc, Free desc,
// Cell asc).
func rankLessOrEqual(a, b model.Ranked) bool {
	if a.CapMatch != b.CapMatch {
		return a.CapMatch
	}
	if a.Distance != b.Distance {
		return a.Distance < b.Distance
	}
	if a.Free != b.Free {
		return a.Free > b.Free
	}
	return a.Cell <= b.Cell
}

// TestRankCapabilityMatchRespected guards that CapMatch is the primary sort
// key: an incapable candidate never outranks a capable one, even when the
// incapable candidate has strictly better distance and free capacity.
func TestRankCapabilityMatchRespected(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}}
	origin := topo("r1", "az1", "rack1")
	cells := []model.CellView{
		rankCell("incapable-close-free", 100),   // no gpu, distance 0, free 100
		rankCell("capable-far-tight", 1, "gpu"), // gpu, distance 3, free 1
	}
	loc := model.LocalityGraph{
		Origin: origin,
		Zone: map[model.CellID]model.Topology{
			"incapable-close-free": origin,
			"capable-far-tight":    topo("r9", "az9", "rack9"),
		},
	}

	ranked := Rank(task, cells, loc)
	capableIdx, incapableIdx := -1, -1
	for i, r := range ranked {
		if r.Cell == "capable-far-tight" {
			capableIdx = i
		}
		if r.Cell == "incapable-close-free" {
			incapableIdx = i
		}
	}
	if capableIdx == -1 || incapableIdx == -1 {
		t.Fatalf("Rank() dropped a candidate: %+v", ranked)
	}
	if capableIdx > incapableIdx {
		t.Fatalf("incapable candidate outranked capable one: %+v", ranked)
	}

	if got := BestFit(task, cells, loc); got.Kind != Assign || got.Cell != "capable-far-tight" {
		t.Fatalf("BestFit() = %+v, want Assign{capable-far-tight}", got)
	}
}

// TestRankCloserLocalityPreferred guards the locality-preference property:
// at equal CapMatch and equal Free, the smaller-Distance cell ranks first
// and BestFit picks it.
func TestRankCloserLocalityPreferred(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	origin := topo("r1", "az1", "rack1")
	cells := []model.CellView{
		rankCell("cross-region", 5),
		rankCell("same-rack", 5),
		rankCell("same-az", 5),
	}
	loc := model.LocalityGraph{
		Origin: origin,
		Zone: map[model.CellID]model.Topology{
			"cross-region": topo("r9", "az9", "rack9"),
			"same-rack":    origin,
			"same-az":      topo("r1", "az1", "rack2"),
		},
	}

	ranked := Rank(task, cells, loc)
	if ranked[0].Cell != "same-rack" {
		t.Fatalf("Rank()[0] = %+v, want same-rack first", ranked[0])
	}

	if got := BestFit(task, cells, loc); got.Kind != Assign || got.Cell != "same-rack" {
		t.Fatalf("BestFit() = %+v, want Assign{same-rack}", got)
	}
}

// TestRankDeterministicTieBreak guards that candidates equal on
// CapMatch/Distance/Free are ordered by CellID, and that shuffling the input
// slice — and building LocalityGraph.Zone via different insertion orders —
// never changes the result, since Rank only ever looks Zone up by key and
// never ranges over it.
func TestRankDeterministicTieBreak(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	origin := topo("r1", "az1", "rack1")
	ids := []string{"zulu", "mike", "alpha", "kilo", "bravo"}
	want := []model.CellID{"alpha", "bravo", "kilo", "mike", "zulu"}

	// Two different insertion orders for the same Zone contents.
	zoneA := map[model.CellID]model.Topology{}
	for _, id := range ids {
		zoneA[model.CellID(id)] = origin
	}
	zoneB := map[model.CellID]model.Topology{}
	for i := len(ids) - 1; i >= 0; i-- {
		zoneB[model.CellID(ids[i])] = origin
	}

	shuffles := [][]string{
		{"zulu", "mike", "alpha", "kilo", "bravo"},
		{"alpha", "bravo", "kilo", "mike", "zulu"},
		{"bravo", "zulu", "alpha", "mike", "kilo"},
		{"kilo", "alpha", "zulu", "bravo", "mike"},
	}

	for _, zone := range []map[model.CellID]model.Topology{zoneA, zoneB} {
		loc := model.LocalityGraph{Origin: origin, Zone: zone}
		for _, order := range shuffles {
			cells := make([]model.CellView, len(order))
			for i, id := range order {
				cells[i] = rankCell(id, 5)
			}
			got := Rank(task, cells, loc)
			gotIDs := make([]model.CellID, len(got))
			for i, r := range got {
				gotIDs[i] = r.Cell
			}
			if !reflect.DeepEqual(gotIDs, want) {
				t.Fatalf("Rank() cell order = %v, want %v", gotIDs, want)
			}
		}
	}
}

func TestBestFit(t *testing.T) {
	origin := topo("r1", "az1", "rack1")

	tests := []struct {
		name  string
		task  model.Task
		cells []model.CellView
		loc   model.LocalityGraph
		want  Placement
	}{
		{
			name:  "empty cands: NoCapacity",
			task:  model.Task{ID: "t1", JobID: "j1"},
			cells: nil,
			loc:   model.LocalityGraph{Origin: origin},
			want:  Placement{Kind: NoCapacity},
		},
		{
			name: "single capable cell with free capacity is assigned",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				rankCell("gpu-cell", 3, "gpu"),
			},
			loc:  model.LocalityGraph{Origin: origin},
			want: Placement{Kind: Assign, Cell: "gpu-cell"},
		},
		{
			name: "capable but full cell is skipped for a farther capable cell with room",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				rankCell("gpu-full", 0, "gpu"),
				rankCell("gpu-room", 4, "gpu"),
			},
			loc: model.LocalityGraph{
				Origin: origin,
				Zone: map[model.CellID]model.Topology{
					"gpu-full": origin,
					"gpu-room": topo("r9", "az9", "rack9"),
				},
			},
			want: Placement{Kind: Assign, Cell: "gpu-room"},
		},
		{
			name: "no capable cell at all: NoCapacity",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				rankCell("a", 5),
				rankCell("b", 5),
			},
			loc:  model.LocalityGraph{Origin: origin},
			want: Placement{Kind: NoCapacity},
		},
		{
			name: "capable cells all full: NoCapacity, never falls back to incapable cell with room",
			task: model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}},
			cells: []model.CellView{
				rankCell("gpu-full", 0, "gpu"),
				rankCell("no-gpu-room", 9),
			},
			loc:  model.LocalityGraph{Origin: origin},
			want: Placement{Kind: NoCapacity},
		},
		{
			name: "tie broken deterministically by CellID",
			task: model.Task{ID: "t1", JobID: "j1"},
			cells: []model.CellView{
				rankCell("bravo", 5),
				rankCell("alpha", 5),
			},
			loc:  model.LocalityGraph{Origin: origin},
			want: Placement{Kind: Assign, Cell: "alpha"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BestFit(tt.task, tt.cells, tt.loc)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("BestFit() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestBestFitNoCapacityFallback guards the fallback contract: whenever no
// candidate is both capable and has Free > 0, BestFit returns the exact
// NoCapacity sentinel Place/PlaceAcross use, so the shell can safely fall
// back to them.
func TestBestFitNoCapacityFallback(t *testing.T) {
	origin := topo("r1", "az1", "rack1")
	gpuTask := model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}}

	cases := []struct {
		name  string
		task  model.Task
		cells []model.CellView
	}{
		{name: "nil cands", task: gpuTask, cells: nil},
		{name: "empty cands", task: gpuTask, cells: []model.CellView{}},
		{
			name: "capable cells present but none have free capacity",
			task: gpuTask,
			cells: []model.CellView{
				rankCell("a", 0, "gpu"),
				rankCell("b", 0, "gpu"),
			},
		},
		{
			name: "cells have free capacity but none are capable",
			task: gpuTask,
			cells: []model.CellView{
				rankCell("a", 5),
				rankCell("b", 9),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := BestFit(tc.task, tc.cells, model.LocalityGraph{Origin: origin})
			want := Placement{Kind: NoCapacity}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("BestFit() = %+v, want %+v", got, want)
			}
		})
	}
}

// TestRankIsDeterministic and TestBestFitIsDeterministic guard the core's
// defining property: identical inputs always produce identical output.
func TestRankIsDeterministic(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}}
	origin := topo("r1", "az1", "rack1")
	cells := []model.CellView{
		rankCell("x", 0, "gpu"),
		rankCell("y", 2),
		rankCell("z", 4, "gpu"),
	}
	loc := model.LocalityGraph{
		Origin: origin,
		Zone: map[model.CellID]model.Topology{
			"x": origin,
			"y": topo("r1", "az1", "rack2"),
			"z": topo("r9", "az9", "rack9"),
		},
	}

	first := Rank(task, cells, loc)
	for i := 0; i < 100; i++ {
		if got := Rank(task, cells, loc); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

func TestBestFitIsDeterministic(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1", Requires: model.CapSet{"gpu"}}
	origin := topo("r1", "az1", "rack1")
	cells := []model.CellView{
		rankCell("x", 0, "gpu"),
		rankCell("y", 2),
		rankCell("z", 4, "gpu"),
	}
	loc := model.LocalityGraph{Origin: origin}

	first := BestFit(task, cells, loc)
	for i := 0; i < 100; i++ {
		if got := BestFit(task, cells, loc); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
