package observability

import (
	"fmt"
	"math"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

const eps = 1e-9

// -----------------------------------------------------------------------
// test helpers
// -----------------------------------------------------------------------

// regionApproxEqual compares two RegionMetrics with float epsilon on Gauge
// (weighted-average arithmetic is not bit-exact across groupings) and exact
// equality on the integer fields.
func regionApproxEqual(a, b model.RegionMetrics) bool {
	return a.Region == b.Region &&
		a.Count == b.Count &&
		a.Samples == b.Samples &&
		math.Abs(a.Gauge-b.Gauge) <= eps
}

// globalApproxEqual is regionApproxEqual's GlobalMetrics counterpart.
func globalApproxEqual(a, b model.GlobalMetrics) bool {
	return a.Count == b.Count &&
		a.Samples == b.Samples &&
		math.Abs(a.Gauge-b.Gauge) <= eps
}

// permutations returns every ordering of xs via Heap's algorithm — a
// deterministic enumeration (no randomness), mirroring
// internal/core/aggregate/aggregate_test.go's helper of the same name.
func permutations(xs []model.CellMetrics) [][]model.CellMetrics {
	var out [][]model.CellMetrics
	n := len(xs)
	buf := make([]model.CellMetrics, n)
	copy(buf, xs)
	c := make([]int, n)

	snapshot := func() []model.CellMetrics {
		cp := make([]model.CellMetrics, n)
		copy(cp, buf)
		return cp
	}

	out = append(out, snapshot())
	for i := 0; i < n; {
		if c[i] < i {
			if i%2 == 0 {
				buf[0], buf[i] = buf[i], buf[0]
			} else {
				buf[c[i]], buf[i] = buf[i], buf[c[i]]
			}
			out = append(out, snapshot())
			c[i]++
			i = 0
		} else {
			c[i] = 0
			i++
		}
	}
	return out
}

// makeCells deterministically builds n CellMetrics — enumerated from a loop
// counter, never math/rand — with varied Count/Gauge/Samples so a sum-based
// test can't pass by coincidence (e.g. every element being identical).
func makeCells(n int) []model.CellMetrics {
	cells := make([]model.CellMetrics, n)
	for i := 0; i < n; i++ {
		cells[i] = model.CellMetrics{
			Cell:    model.CellID(fmt.Sprintf("cell-%d", i)),
			Count:   int64(i % 11),
			Gauge:   float64(i%7) + 0.5,
			Samples: int64(i%5) + 1, // always > 0, so no zero-weight terms
		}
	}
	return cells
}

// makeRegions is makeCells' RegionMetrics counterpart.
func makeRegions(n int) []model.RegionMetrics {
	regions := make([]model.RegionMetrics, n)
	for i := 0; i < n; i++ {
		regions[i] = model.RegionMetrics{
			Region:  model.RegionID(fmt.Sprintf("region-%d", i)),
			Count:   int64(i % 13),
			Gauge:   float64(i%9) + 0.25,
			Samples: int64(i%6) + 1,
		}
	}
	return regions
}

// -----------------------------------------------------------------------
// RollupRegion — table-driven
// -----------------------------------------------------------------------

func TestRollupRegion(t *testing.T) {
	tests := []struct {
		name  string
		cells []model.CellMetrics
		want  model.RegionMetrics
	}{
		{
			name:  "empty input is the zero-value identity",
			cells: nil,
			want:  model.RegionMetrics{},
		},
		{
			name:  "single element is that element's reduction",
			cells: []model.CellMetrics{{Cell: "c0", Count: 10, Gauge: 4.0, Samples: 5}},
			want:  model.RegionMetrics{Count: 10, Gauge: 4.0, Samples: 5},
		},
		{
			name: "single element with zero Samples reduces to zero Gauge, no divide-by-zero",
			cells: []model.CellMetrics{
				{Cell: "c0", Count: 3, Gauge: 99.0, Samples: 0},
			},
			want: model.RegionMetrics{Count: 3, Gauge: 0, Samples: 0},
		},
		{
			name: "Count and Samples sum, Gauge is the Samples-weighted average",
			cells: []model.CellMetrics{
				{Cell: "c0", Count: 10, Gauge: 2.0, Samples: 5}, // contributes 10.0
				{Cell: "c1", Count: 20, Gauge: 4.0, Samples: 3}, // contributes 12.0
			},
			// Count: 30, Samples: 8, Gauge: (10.0+12.0)/8 = 2.75
			want: model.RegionMetrics{Count: 30, Gauge: 2.75, Samples: 8},
		},
		{
			name: "a zero-Samples element contributes zero weight, not a divide-by-zero",
			cells: []model.CellMetrics{
				{Cell: "c0", Count: 5, Gauge: 500.0, Samples: 0}, // weight 0, ignored by the average
				{Cell: "c1", Count: 7, Gauge: 6.0, Samples: 2},
			},
			// Count: 12, Samples: 2, Gauge: (0 + 12.0)/2 = 6.0
			want: model.RegionMetrics{Count: 12, Gauge: 6.0, Samples: 2},
		},
		{
			name: "all-zero-Samples input keeps Gauge at zero across the whole fold",
			cells: []model.CellMetrics{
				{Cell: "c0", Count: 1, Gauge: 10.0, Samples: 0},
				{Cell: "c1", Count: 2, Gauge: 20.0, Samples: 0},
			},
			want: model.RegionMetrics{Count: 3, Gauge: 0, Samples: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RollupRegion(tt.cells)
			if !regionApproxEqual(got, tt.want) {
				t.Fatalf("RollupRegion() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// RollupGlobal — table-driven
// -----------------------------------------------------------------------

func TestRollupGlobal(t *testing.T) {
	tests := []struct {
		name string
		rs   []model.RegionMetrics
		want model.GlobalMetrics
	}{
		{
			name: "empty input is the zero-value identity",
			rs:   nil,
			want: model.GlobalMetrics{},
		},
		{
			name: "single element is that element's reduction",
			rs:   []model.RegionMetrics{{Region: "r0", Count: 100, Gauge: 3.5, Samples: 40}},
			want: model.GlobalMetrics{Count: 100, Gauge: 3.5, Samples: 40},
		},
		{
			name: "single element with zero Samples reduces to zero Gauge, no divide-by-zero",
			rs:   []model.RegionMetrics{{Region: "r0", Count: 9, Gauge: 250.0, Samples: 0}},
			want: model.GlobalMetrics{Count: 9, Gauge: 0, Samples: 0},
		},
		{
			name: "Count and Samples sum, Gauge is the Samples-weighted average",
			rs: []model.RegionMetrics{
				{Region: "r0", Count: 30, Gauge: 2.75, Samples: 8}, // contributes 22.0
				{Region: "r1", Count: 10, Gauge: 5.0, Samples: 2},  // contributes 10.0
			},
			// Count: 40, Samples: 10, Gauge: (22.0+10.0)/10 = 3.2
			want: model.GlobalMetrics{Count: 40, Gauge: 3.2, Samples: 10},
		},
		{
			name: "a zero-Samples element contributes zero weight, not a divide-by-zero",
			rs: []model.RegionMetrics{
				{Region: "r0", Count: 4, Gauge: 500.0, Samples: 0},
				{Region: "r1", Count: 6, Gauge: 8.0, Samples: 3},
			},
			// Count: 10, Samples: 3, Gauge: (0 + 24.0)/3 = 8.0
			want: model.GlobalMetrics{Count: 10, Gauge: 8.0, Samples: 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RollupGlobal(tt.rs)
			if !globalApproxEqual(got, tt.want) {
				t.Fatalf("RollupGlobal() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Budget — table-driven, monotonic ordering
// -----------------------------------------------------------------------

func TestBudget(t *testing.T) {
	tests := []struct {
		name  string
		level model.Level
		want  model.Cardinality
	}{
		{"LevelCell", model.LevelCell, cellBudget},
		{"LevelRegion", model.LevelRegion, regionBudget},
		{"LevelGlobal", model.LevelGlobal, globalBudget},
		{"unrecognized level gets the most conservative bound", model.Level(99), globalBudget},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Budget(tt.level); got != tt.want {
				t.Fatalf("Budget(%v) = %d, want %d", tt.level, got, tt.want)
			}
		})
	}
}

func TestBudgetIsMonotonicByLevel(t *testing.T) {
	cell, region, global := Budget(model.LevelCell), Budget(model.LevelRegion), Budget(model.LevelGlobal)
	if !(cell > region && region > global) {
		t.Fatalf("Budget must be strictly decreasing from LevelCell to LevelGlobal: cell=%d region=%d global=%d", cell, region, global)
	}
}

// -----------------------------------------------------------------------
// Algebraic laws — commutative, associative/grouping-independent, identity
// -----------------------------------------------------------------------

func TestRollupRegionCommutative(t *testing.T) {
	cells := []model.CellMetrics{
		{Cell: "c0", Count: 10, Gauge: 2.0, Samples: 5},
		{Cell: "c1", Count: 20, Gauge: 4.0, Samples: 3},
		{Cell: "c2", Count: 7, Gauge: 6.0, Samples: 2},
		{Cell: "c3", Count: 0, Gauge: 500.0, Samples: 0}, // zero-weight, should not skew order
	}

	want := RollupRegion(cells)
	for i, order := range permutations(cells) {
		got := RollupRegion(order)
		if !regionApproxEqual(got, want) {
			t.Fatalf("order %d not order-independent: got %+v, want %+v (order=%+v)", i, got, want, order)
		}
	}
}

func TestRollupGlobalCommutative(t *testing.T) {
	orderedRegions := [][]model.RegionMetrics{
		{
			{Region: "r0", Count: 10, Gauge: 2.0, Samples: 5},
			{Region: "r1", Count: 20, Gauge: 4.0, Samples: 3},
			{Region: "r2", Count: 7, Gauge: 6.0, Samples: 2},
		},
		{
			{Region: "r2", Count: 7, Gauge: 6.0, Samples: 2},
			{Region: "r0", Count: 10, Gauge: 2.0, Samples: 5},
			{Region: "r1", Count: 20, Gauge: 4.0, Samples: 3},
		},
		{
			{Region: "r1", Count: 20, Gauge: 4.0, Samples: 3},
			{Region: "r2", Count: 7, Gauge: 6.0, Samples: 2},
			{Region: "r0", Count: 10, Gauge: 2.0, Samples: 5},
		},
	}

	want := RollupGlobal(orderedRegions[0])
	for i, order := range orderedRegions[1:] {
		got := RollupGlobal(order)
		if !globalApproxEqual(got, want) {
			t.Fatalf("order %d not order-independent: got %+v, want %+v", i, got, want)
		}
	}
}

func TestRollupRegionIdentity(t *testing.T) {
	cells := []model.CellMetrics{
		{Cell: "c0", Count: 10, Gauge: 2.0, Samples: 5},
		{Cell: "c1", Count: 20, Gauge: 4.0, Samples: 3},
	}
	withoutEmpty := RollupRegion(cells)
	withEmptyPrepended := RollupRegion(append([]model.CellMetrics{}, cells...))
	if !regionApproxEqual(withoutEmpty, withEmptyPrepended) {
		t.Fatalf("appending nothing changed the result: %+v vs %+v", withoutEmpty, withEmptyPrepended)
	}
	if got := RollupRegion(nil); !regionApproxEqual(got, model.RegionMetrics{}) {
		t.Fatalf("RollupRegion(nil) = %+v, want the zero RegionMetrics", got)
	}
}

func TestRollupGlobalIdentity(t *testing.T) {
	if got := RollupGlobal(nil); !globalApproxEqual(got, model.GlobalMetrics{}) {
		t.Fatalf("RollupGlobal(nil) = %+v, want the zero GlobalMetrics", got)
	}
}

// -----------------------------------------------------------------------
// Hierarchical == flat (the ticket's named "rollup" property): folding
// cell -> region -> global must equal a flat fold of all cells directly,
// regardless of how cells are partitioned across regions.
// -----------------------------------------------------------------------

func TestHierarchicalEqualsFlat(t *testing.T) {
	cells := []model.CellMetrics{
		{Cell: "c0", Count: 10, Gauge: 2.0, Samples: 5},
		{Cell: "c1", Count: 20, Gauge: 4.0, Samples: 3},
		{Cell: "c2", Count: 5, Gauge: 1.0, Samples: 0}, // zero-Samples: must not break associativity
		{Cell: "c3", Count: 15, Gauge: 3.0, Samples: 7},
		{Cell: "c4", Count: 8, Gauge: 6.0, Samples: 2},
		{Cell: "c5", Count: 12, Gauge: 5.0, Samples: 4},
	}

	// "Flat": every cell folded into one region, then one global — the
	// baseline every grouping below must match.
	flat := RollupGlobal([]model.RegionMetrics{RollupRegion(cells)})

	partitions := [][][]model.CellMetrics{
		{cells},                // one region
		{cells[:3], cells[3:]}, // two regions
		{cells[:1], cells[1:3], cells[3:5], cells[5:]},                           // four regions
		{{cells[0]}, {cells[1]}, {cells[2]}, {cells[3]}, {cells[4]}, {cells[5]}}, // six regions, one cell each
		{cells[:2], cells[2:4], cells[4:5], cells[5:]},                           // uneven grouping
	}

	for i, groups := range partitions {
		var regions []model.RegionMetrics
		for _, g := range groups {
			regions = append(regions, RollupRegion(g))
		}
		got := RollupGlobal(regions)
		if !globalApproxEqual(got, flat) {
			t.Fatalf("partition %d (%d regions): hierarchical = %+v, want (flat) %+v", i, len(groups), got, flat)
		}
	}
}

func TestHierarchicalEqualsFlatAllZeroSamples(t *testing.T) {
	// Every cell has zero Samples: the weighted average is 0 at every tier,
	// no NaN/Inf from a 0/0 division anywhere in the hierarchy.
	cells := []model.CellMetrics{
		{Cell: "c0", Count: 1, Gauge: 10.0, Samples: 0},
		{Cell: "c1", Count: 2, Gauge: 20.0, Samples: 0},
		{Cell: "c2", Count: 3, Gauge: 30.0, Samples: 0},
	}

	flat := RollupGlobal([]model.RegionMetrics{RollupRegion(cells)})

	regions := []model.RegionMetrics{
		RollupRegion(cells[:1]),
		RollupRegion(cells[1:]),
	}
	got := RollupGlobal(regions)

	if !globalApproxEqual(got, flat) {
		t.Fatalf("hierarchical = %+v, want (flat) %+v", got, flat)
	}
	if math.IsNaN(got.Gauge) || math.IsInf(got.Gauge, 0) {
		t.Fatalf("Gauge is not finite: %v", got.Gauge)
	}
}

// -----------------------------------------------------------------------
// Boundedness: no matter how many cells/regions feed the fold, the output
// is exactly one series — this is the mechanism that keeps cardinality
// bounded as the fleet scales, and it stays within Budget at every level.
// -----------------------------------------------------------------------

func TestRollupRegionBounded(t *testing.T) {
	const outputSeries = model.Cardinality(1) // RollupRegion always folds to one RegionMetrics
	sizes := []int{0, 1, 2, 10, 100, 1000, 10000}
	for _, n := range sizes {
		RollupRegion(makeCells(n)) // exercise the fold at this fleet size
		if outputSeries > Budget(model.LevelRegion) {
			t.Fatalf("n=%d cells: output series %d exceeds region budget %d", n, outputSeries, Budget(model.LevelRegion))
		}
	}
}

func TestRollupGlobalBounded(t *testing.T) {
	const outputSeries = model.Cardinality(1) // RollupGlobal always folds to one GlobalMetrics
	sizes := []int{0, 1, 2, 10, 100, 1000, 10000}
	for _, n := range sizes {
		RollupGlobal(makeRegions(n)) // exercise the fold at this fleet size
		if outputSeries > Budget(model.LevelGlobal) {
			t.Fatalf("n=%d regions: output series %d exceeds global budget %d", n, outputSeries, Budget(model.LevelGlobal))
		}
	}
}

// -----------------------------------------------------------------------
// Determinism
// -----------------------------------------------------------------------

func TestRollupRegionIsDeterministic(t *testing.T) {
	cells := makeCells(50)
	first := RollupRegion(cells)
	for i := 0; i < 100; i++ {
		if got := RollupRegion(cells); got != first {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

func TestRollupGlobalIsDeterministic(t *testing.T) {
	regions := makeRegions(50)
	first := RollupGlobal(regions)
	for i := 0; i < 100; i++ {
		if got := RollupGlobal(regions); got != first {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

func TestBudgetIsDeterministic(t *testing.T) {
	for _, level := range []model.Level{model.LevelCell, model.LevelRegion, model.LevelGlobal} {
		first := Budget(level)
		for i := 0; i < 100; i++ {
			if got := Budget(level); got != first {
				t.Fatalf("Budget(%v) non-deterministic on run %d: %d vs %d", level, i, got, first)
			}
		}
	}
}
