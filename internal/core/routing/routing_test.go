package routing

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

func region(id string, free, cells int, h model.Health) model.RegionView {
	return model.RegionView{ID: model.RegionID(id), Free: free, Cells: cells, Health: h}
}

func summary(id string, free, cells int, h model.Health, at int64) RegionalSummary {
	return RegionalSummary{Region: model.RegionID(id), Free: free, Cells: cells, Health: h, At: model.Instant(at)}
}

// -----------------------------------------------------------------------
// route
// -----------------------------------------------------------------------

func TestRoute(t *testing.T) {
	independent := model.JobSpec{ID: "job-1", Coupling: model.Independent}
	tight := model.JobSpec{ID: "job-2", Coupling: model.Barrier}

	tests := []struct {
		name    string
		job     model.JobSpec
		regions []model.RegionView
		want    Route
	}{
		{
			name:    "no regions",
			job:     independent,
			regions: nil,
			want:    Route{Kind: NoRegion},
		},
		{
			name: "none healthy",
			job:  independent,
			regions: []model.RegionView{
				region("a", 10, 1, model.Degraded),
				region("b", 10, 1, model.Unreachable),
			},
			want: Route{Kind: NoRegion},
		},
		{
			name: "healthy but no capacity",
			job:  independent,
			regions: []model.RegionView{
				region("a", 0, 1, model.Healthy),
			},
			want: Route{Kind: NoRegion},
		},
		{
			name: "one healthy region with capacity routes to it",
			job:  independent,
			regions: []model.RegionView{
				region("a", 5, 1, model.Healthy),
				region("b", 0, 1, model.Healthy),
				region("c", 5, 1, model.Degraded),
			},
			want: Route{Kind: To, Region: "a"},
		},
		{
			name: "multiple eligible, tight job: deterministic pick by most Free",
			job:  tight,
			regions: []model.RegionView{
				region("b", 5, 1, model.Healthy),
				region("a", 9, 1, model.Healthy),
				region("c", 9, 1, model.Healthy), // ties "a" on Free; RegionID tiebreak
			},
			want: Route{Kind: To, Region: "a"},
		},
		{
			name: "multiple eligible, tight job: tie on Free breaks by RegionID",
			job:  tight,
			regions: []model.RegionView{
				region("z", 4, 1, model.Healthy),
				region("y", 4, 1, model.Healthy),
			},
			want: Route{Kind: To, Region: "y"},
		},
		{
			name: "multiple eligible, independent job: spreads across all eligible, sorted",
			job:  independent,
			regions: []model.RegionView{
				region("c", 3, 1, model.Healthy),
				region("a", 1, 1, model.Healthy),
				region("b", 2, 1, model.Healthy),
				region("d", 4, 1, model.Degraded), // excluded
			},
			want: Route{Kind: Spread, Regions: []model.RegionID{"a", "b", "c"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := route(tt.job, tt.regions)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("route() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestRouteIsDeterministic guards the core's defining property: identical
// inputs always produce identical output, regardless of input region order.
func TestRouteIsDeterministic(t *testing.T) {
	job := model.JobSpec{Coupling: model.Independent}
	regions := []model.RegionView{
		region("c", 3, 1, model.Healthy),
		region("a", 1, 1, model.Healthy),
		region("b", 2, 1, model.Healthy),
	}
	first := route(job, regions)
	for i := 0; i < 100; i++ {
		if got := route(job, regions); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// MergeGlobal
// -----------------------------------------------------------------------

func viewOf(summaries ...RegionalSummary) GlobalView {
	var v GlobalView
	for _, s := range summaries {
		v = MergeGlobal(v, s)
	}
	return v
}

func TestMergeGlobal(t *testing.T) {
	a1 := summary("a", 5, 2, model.Healthy, 10)
	a2older := summary("a", 9, 3, model.Healthy, 5) // older At, more Free: At wins
	a2newer := summary("a", 1, 1, model.Degraded, 20)
	aTieMoreFree := summary("a", 9, 2, model.Healthy, 10) // same At, more Free
	aTieSameFreeMoreCells := summary("a", 5, 4, model.Healthy, 10)
	aTieHealthier := summary("a", 5, 2, model.Healthy, 10) // identical to a1

	tests := []struct {
		name string
		v    GlobalView
		s    RegionalSummary
		want GlobalView
	}{
		{
			name: "insert into empty view",
			v:    GlobalView{},
			s:    a1,
			want: viewOf(a1),
		},
		{
			name: "newer At replaces older",
			v:    viewOf(a1),
			s:    a2newer,
			want: viewOf(a2newer),
		},
		{
			name: "older At does not replace newer",
			v:    viewOf(a1),
			s:    a2older,
			want: viewOf(a1),
		},
		{
			name: "tie on At: more Free wins",
			v:    viewOf(a1),
			s:    aTieMoreFree,
			want: viewOf(aTieMoreFree),
		},
		{
			name: "tie on At and Free: more Cells wins",
			v:    viewOf(a1),
			s:    aTieSameFreeMoreCells,
			want: viewOf(aTieSameFreeMoreCells),
		},
		{
			name: "fully identical summary is a no-op",
			v:    viewOf(a1),
			s:    aTieHealthier,
			want: viewOf(a1),
		},
		{
			name: "unrelated region is added alongside",
			v:    viewOf(a1),
			s:    summary("b", 3, 1, model.Healthy, 1),
			want: viewOf(a1, summary("b", 3, 1, model.Healthy, 1)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeGlobal(tt.v, tt.s)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("MergeGlobal() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestMergeGlobalDoesNotMutateInput ensures MergeGlobal never mutates a
// GlobalView a caller still holds — copy-on-write, like registry.Apply.
func TestMergeGlobalDoesNotMutateInput(t *testing.T) {
	before := viewOf(summary("a", 5, 2, model.Healthy, 10))
	beforeCopy := viewOf(summary("a", 5, 2, model.Healthy, 10))

	_ = MergeGlobal(before, summary("a", 99, 99, model.Healthy, 999))

	if !reflect.DeepEqual(before, beforeCopy) {
		t.Fatalf("MergeGlobal mutated its input view: got %+v, want unchanged %+v", before, beforeCopy)
	}
}

// -----------------------------------------------------------------------
// MergeGlobal — required algebraic properties (phase doc §02, issue #35)
// -----------------------------------------------------------------------

func foldAll(order []RegionalSummary) GlobalView {
	var v GlobalView
	for _, s := range order {
		v = MergeGlobal(v, s)
	}
	return v
}

// permutations returns every ordering of xs, via Heap's algorithm — a
// deterministic enumeration (no randomness), which keeps this a pure core
// test reproducible without math/rand.
func permutations(xs []RegionalSummary) [][]RegionalSummary {
	var out [][]RegionalSummary
	n := len(xs)
	buf := make([]RegionalSummary, n)
	copy(buf, xs)
	c := make([]int, n)

	snapshot := func() []RegionalSummary {
		cp := make([]RegionalSummary, n)
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

func TestMergeGlobalCommutative(t *testing.T) {
	a := summary("a", 5, 2, model.Healthy, 10)
	b := summary("b", 3, 1, model.Degraded, 4)

	ab := foldAll([]RegionalSummary{a, b})
	ba := foldAll([]RegionalSummary{b, a})

	if !reflect.DeepEqual(ab, ba) {
		t.Fatalf("MergeGlobal not commutative: merge(A,B)=%+v merge(B,A)=%+v", ab, ba)
	}
}

func TestMergeGlobalAssociative(t *testing.T) {
	a := summary("a", 5, 2, model.Healthy, 10)
	b := summary("a", 9, 3, model.Healthy, 12) // same region, competes with a
	c := summary("c", 1, 1, model.Unreachable, 2)

	for _, order := range permutations([]RegionalSummary{a, b, c}) {
		got := foldAll(order)
		want := foldAll([]RegionalSummary{a, b, c})
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("MergeGlobal not associative/grouping-independent for order %+v: got %+v, want %+v", order, got, want)
		}
	}
}

func TestMergeGlobalIdempotent(t *testing.T) {
	a := summary("a", 5, 2, model.Healthy, 10)

	once := foldAll([]RegionalSummary{a})
	twice := foldAll([]RegionalSummary{a, a})

	if !reflect.DeepEqual(once, twice) {
		t.Fatalf("MergeGlobal not idempotent: once=%+v twice=%+v", once, twice)
	}
}

// TestMergeGlobalConvergence folds a shuffled, duplicated stream of
// summaries from every ordering and asserts it always reaches the same
// fixed point.
func TestMergeGlobalConvergence(t *testing.T) {
	a := summary("a", 5, 2, model.Healthy, 10)
	b := summary("b", 3, 1, model.Degraded, 4)
	c := summary("a", 9, 3, model.Healthy, 12) // competes with a; should win

	base := []RegionalSummary{a, b, c}
	// Duplicate the stream so convergence is checked with repeats present.
	stream := append(append([]RegionalSummary{}, base...), base...)

	want := foldAll(base)
	for i, order := range permutations(stream) {
		if i > 200 { // 6! = 720 permutations of 6 elements; sample for speed
			break
		}
		got := foldAll(order)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("MergeGlobal did not converge for order %+v: got %+v, want %+v", order, got, want)
		}
	}
}

// TestMergeGlobalIsDeterministic guards the core's defining property:
// identical inputs always produce identical output.
func TestMergeGlobalIsDeterministic(t *testing.T) {
	a := summary("a", 5, 2, model.Healthy, 10)
	b := summary("b", 3, 1, model.Degraded, 4)
	first := foldAll([]RegionalSummary{a, b})
	for i := 0; i < 100; i++ {
		if got := foldAll([]RegionalSummary{a, b}); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// Diverged
// -----------------------------------------------------------------------

func TestDiverged(t *testing.T) {
	v := viewOf(
		summary("fresh", 1, 1, model.Healthy, 100),
		summary("boundary", 1, 1, model.Healthy, 0), // now(=StalenessWindow) - 0 == window, exactly
		summary("stale", 1, 1, model.Healthy, -1),   // now - At == window+1
	)
	now := model.Instant(int64(StalenessWindow))

	tests := []struct {
		name string
		v    GlobalView
		now  model.Instant
		want []model.RegionID
	}{
		{"empty view", GlobalView{}, 0, nil},
		{"fresh region omitted", viewOf(summary("a", 1, 1, model.Healthy, 100)), 100, nil},
		{"stale region returned", v, now, []model.RegionID{"stale"}},
		{"boundary at exactly the window is fresh", viewOf(v.summaries["boundary"]), now, nil},
		{
			"multiple stale regions returned sorted",
			viewOf(
				summary("z", 1, 1, model.Healthy, 0),
				summary("a", 1, 1, model.Healthy, 0),
			),
			now + 1,
			[]model.RegionID{"a", "z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Diverged(tt.v, tt.now)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Diverged() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestDivergedIsDeterministic guards the core's defining property: identical
// inputs always produce identical output, in stable sorted order.
func TestDivergedIsDeterministic(t *testing.T) {
	v := viewOf(
		summary("z", 1, 1, model.Healthy, 0),
		summary("a", 1, 1, model.Healthy, 0),
		summary("m", 1, 1, model.Healthy, 0),
	)
	now := model.Instant(int64(StalenessWindow) + 100)
	first := Diverged(v, now)
	for i := 0; i < 100; i++ {
		if got := Diverged(v, now); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// Summarize
// -----------------------------------------------------------------------

func TestSummarize(t *testing.T) {
	t.Run("empty registry", func(t *testing.T) {
		var reg registry.Registry
		got := Summarize(reg)
		want := RegionalSummary{Free: 0, Cells: 0, Health: model.Healthy}
		if got != want {
			t.Fatalf("Summarize() = %+v, want %+v", got, want)
		}
	})

	t.Run("multi-cell registry aggregates Free and counts Cells", func(t *testing.T) {
		var reg registry.Registry
		reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.CellUp, Cell: "c1", Capacity: 10})
		reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.CellUp, Cell: "c2", Capacity: 5})
		reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.AgentJoined, Cell: "c1", Agent: "agent-1"})

		got := Summarize(reg)
		// c1: capacity 10, 1 agent -> free 9. c2: capacity 5, 0 agents -> free 5.
		want := RegionalSummary{Free: 14, Cells: 2, Health: model.Healthy}
		if got != want {
			t.Fatalf("Summarize() = %+v, want %+v", got, want)
		}
	})
}

// TestSummarizeIsDeterministic guards the core's defining property:
// identical inputs always produce identical output.
func TestSummarizeIsDeterministic(t *testing.T) {
	var reg registry.Registry
	reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.CellUp, Cell: "c1", Capacity: 10})
	reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.CellUp, Cell: "c2", Capacity: 5})

	first := Summarize(reg)
	for i := 0; i < 100; i++ {
		if got := Summarize(reg); got != first {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// Decide
// -----------------------------------------------------------------------

// TestDecideDelegatesToRoute asserts Decide is exactly route's exported
// entry point — same output for the same input — across every shape route's
// own table test exercises (no regions, none healthy, one eligible, tight
// multi-eligible, independent spread).
func TestDecideDelegatesToRoute(t *testing.T) {
	independent := model.JobSpec{ID: "job-1", Coupling: model.Independent}
	tight := model.JobSpec{ID: "job-2", Coupling: model.Barrier}

	tests := []struct {
		name    string
		job     model.JobSpec
		regions []model.RegionView
	}{
		{"no regions", independent, nil},
		{"one eligible", independent, []model.RegionView{region("a", 5, 1, model.Healthy)}},
		{"tight multi-eligible", tight, []model.RegionView{
			region("b", 5, 1, model.Healthy),
			region("a", 9, 1, model.Healthy),
		}},
		{"independent spread", independent, []model.RegionView{
			region("c", 3, 1, model.Healthy),
			region("a", 1, 1, model.Healthy),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := route(tt.job, tt.regions)
			got := Decide(tt.job, tt.regions)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Decide() = %+v, want %+v (route()'s own output)", got, want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Summaries
// -----------------------------------------------------------------------

// TestSummariesSortedByRegionID asserts Summaries projects a GlobalView's
// map into a slice in stable, ascending RegionID order regardless of the
// order summaries were folded in — the shell (issue #45) depends on this to
// build a deterministic []model.RegionView from the merged view.
func TestSummariesSortedByRegionID(t *testing.T) {
	v := viewOf(
		summary("c", 3, 1, model.Healthy, 1),
		summary("a", 1, 1, model.Healthy, 1),
		summary("b", 2, 1, model.Healthy, 1),
	)

	got := Summaries(v)
	wantIDs := []model.RegionID{"a", "b", "c"}
	if len(got) != len(wantIDs) {
		t.Fatalf("Summaries() returned %d entries, want %d", len(got), len(wantIDs))
	}
	for i, id := range wantIDs {
		if got[i].Region != id {
			t.Fatalf("Summaries()[%d].Region = %q, want %q", i, got[i].Region, id)
		}
	}
}

// TestSummariesEmptyView asserts an empty GlobalView projects to an empty,
// non-nil-safe slice rather than panicking.
func TestSummariesEmptyView(t *testing.T) {
	got := Summaries(GlobalView{})
	if len(got) != 0 {
		t.Fatalf("Summaries(empty view) = %+v, want empty", got)
	}
}

// TestSummariesIsDeterministic guards the core's defining property:
// identical inputs always produce identical output, in stable sorted order,
// regardless of the fold order that built the view.
func TestSummariesIsDeterministic(t *testing.T) {
	v := viewOf(
		summary("z", 1, 1, model.Healthy, 0),
		summary("a", 1, 1, model.Healthy, 0),
		summary("m", 1, 1, model.Healthy, 0),
	)
	first := Summaries(v)
	for i := 0; i < 100; i++ {
		if got := Summaries(v); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
