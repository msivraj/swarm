package upgrade

import (
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

var (
	v10 = model.Version{Major: 1, Minor: 0}
	v11 = model.Version{Major: 1, Minor: 1}
	v12 = model.Version{Major: 1, Minor: 2}
	v20 = model.Version{Major: 2, Minor: 0}
)

func fleet(versions map[model.CellID]model.Version, jobs map[model.CellID][]model.JobID, cordoned map[model.CellID]bool) model.FleetState {
	return model.FleetState{Versions: versions, Jobs: jobs, Cordoned: cordoned}
}

func TestNextDrain(t *testing.T) {
	tests := []struct {
		name  string
		fleet model.FleetState
		plan  model.UpgradePlan
		want  model.DrainStep
	}{
		{
			name:  "empty fleet is Done",
			fleet: model.FleetState{},
			plan:  model.UpgradePlan{Target: v11},
			want:  model.DrainStep{Kind: model.Done},
		},
		{
			name: "already fully at target is Done",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v11, "b": v11},
				nil, nil,
			),
			plan: model.UpgradePlan{Target: v11},
			want: model.DrainStep{Kind: model.Done},
		},
		{
			name: "picks the smallest CellID when no Order is set",
			fleet: fleet(
				map[model.CellID]model.Version{"c": v10, "a": v10, "b": v10},
				nil, nil,
			),
			plan: model.UpgradePlan{Target: v11},
			want: model.DrainStep{Kind: model.Cordon, Cell: "a"},
		},
		{
			name: "explicit Order is honored over CellID order",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v10, "b": v10},
				nil, nil,
			),
			plan: model.UpgradePlan{Target: v11, Order: []model.CellID{"b", "a"}},
			want: model.DrainStep{Kind: model.Cordon, Cell: "b"},
		},
		{
			name: "already-cordoned cell is skipped",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v10, "b": v10},
				nil,
				map[model.CellID]bool{"a": true},
			),
			plan: model.UpgradePlan{Target: v11},
			want: model.DrainStep{Kind: model.Cordon, Cell: "b"},
		},
		{
			name: "cell already at target is skipped",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v11, "b": v10},
				nil, nil,
			),
			plan: model.UpgradePlan{Target: v11},
			want: model.DrainStep{Kind: model.Cordon, Cell: "b"},
		},
		{
			name: "skew-unsafe candidate is skipped, not returned unsafe",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v20, "b": v10},
				nil, nil,
			),
			plan: model.UpgradePlan{Target: v11},
			want: model.DrainStep{Kind: model.Cordon, Cell: "b"},
		},
		{
			name: "only candidate is skew-unsafe: Done",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v20},
				nil, nil,
			),
			plan: model.UpgradePlan{Target: v11},
			want: model.DrainStep{Kind: model.Done},
		},
		{
			name: "candidate sharing a job with a cordoned/draining cell is skipped",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v10, "b": v10},
				map[model.CellID][]model.JobID{"a": {"job-1"}, "b": {"job-1"}},
				map[model.CellID]bool{"a": true},
			),
			plan: model.UpgradePlan{Target: v11},
			want: model.DrainStep{Kind: model.Done},
		},
		{
			name: "candidate with a disjoint job set is not blocked",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v10, "b": v10},
				map[model.CellID][]model.JobID{"a": {"job-1"}, "b": {"job-2"}},
				map[model.CellID]bool{"a": true},
			),
			plan: model.UpgradePlan{Target: v11},
			want: model.DrainStep{Kind: model.Cordon, Cell: "b"},
		},
		{
			name: "Order referencing a cell absent from the fleet is skipped",
			fleet: fleet(
				map[model.CellID]model.Version{"a": v10},
				nil, nil,
			),
			plan: model.UpgradePlan{Target: v11, Order: []model.CellID{"ghost", "a"}},
			want: model.DrainStep{Kind: model.Cordon, Cell: "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NextDrain(tt.fleet, tt.plan)
			if got != tt.want {
				t.Fatalf("NextDrain() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestNextDrainIsDeterministic guards the core's defining property: identical
// inputs always produce identical output — including under an explicit
// Order and under the default CellID-sorted order, never Go's randomized
// map iteration.
func TestNextDrainIsDeterministic(t *testing.T) {
	f := fleet(
		map[model.CellID]model.Version{"z": v10, "m": v10, "a": v10, "q": v11},
		map[model.CellID][]model.JobID{"z": {"j1"}, "m": {"j2"}, "a": {"j1", "j3"}},
		map[model.CellID]bool{"z": true},
	)
	plans := []model.UpgradePlan{
		{Target: v11},
		{Target: v11, Order: []model.CellID{"q", "a", "m", "z"}},
	}
	for _, plan := range plans {
		first := NextDrain(f, plan)
		for i := 0; i < 100; i++ {
			if got := NextDrain(f, plan); got != first {
				t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
			}
		}
	}
}

// TestNextDrainNeverDrainsTwoCellsOfSameJobAtOnce is THE zero-loss property
// (§03): NextDrain must never return Cordon{cell} for a cell whose running
// jobs intersect a cell already cordoned/draining. Swept over every
// combination of job assignment, cordoned bit, and target-version bit for a
// fixed set of cells and jobs — enumerated, not random (fcischeck scans
// _test.go too).
func TestNextDrainNeverDrainsTwoCellsOfSameJobAtOnce(t *testing.T) {
	cells := []model.CellID{"a", "b", "c"}
	jobs := []model.JobID{"j0", "j1"}
	target := v11
	plan := model.UpgradePlan{Target: target}

	states := enumerateCellStates(jobs)
	for _, sa := range states {
		for _, sb := range states {
			for _, sc := range states {
				f := buildFleet(cells, []cellState{sa, sb, sc}, v10, target)
				step := NextDrain(f, plan)
				if step.Kind != model.Cordon {
					continue
				}
				assertNoJobOverlapWithCordoned(t, f, step.Cell)
			}
		}
	}
}

// TestNextDrainTerminates drives a fleet to completion by repeatedly
// cordoning the returned cell and then "fake rolling" it (bump to target,
// uncordon) before feeding the new FleetState back in — the shell's own
// drive loop, faked for the core test. It must terminate at Done within a
// finite, bounded number of steps, with every cell ending at the target
// version.
func TestNextDrainTerminates(t *testing.T) {
	cells := []model.CellID{"a", "b", "c", "d", "e"}
	target := v11
	plan := model.UpgradePlan{Target: target}

	versions := map[model.CellID]model.Version{}
	jobs := map[model.CellID][]model.JobID{
		"a": {"job-1"},
		"b": {"job-1", "job-2"}, // overlaps a
		"c": {"job-3"},
		"d": {"job-2"}, // overlaps b
		"e": nil,
	}
	cordoned := map[model.CellID]bool{}
	for _, c := range cells {
		versions[c] = v10
	}
	f := model.FleetState{Versions: versions, Jobs: jobs, Cordoned: cordoned}

	maxSteps := len(cells) + 1
	steps := 0
	for ; steps < maxSteps; steps++ {
		step := NextDrain(f, plan)
		if step.Kind == model.Done {
			break
		}
		if step.Kind != model.Cordon {
			t.Fatalf("unexpected DrainStepKind %v", step.Kind)
		}
		// Fake shell: cordon, drain, roll to target, uncordon, in one step.
		f.Cordoned[step.Cell] = false
		f.Versions[step.Cell] = target
	}

	if steps >= maxSteps {
		t.Fatalf("did not terminate within %d steps", maxSteps)
	}
	if got := NextDrain(f, plan); got.Kind != model.Done {
		t.Fatalf("expected Done after draining every cell, got %+v", got)
	}
	for _, c := range cells {
		if f.Versions[c] != target {
			t.Fatalf("cell %s ended at %+v, want %+v", c, f.Versions[c], target)
		}
	}
}

// TestNextDrainAlreadyAtTargetIsImmediatelyDone is the termination
// property's base case named in the ticket: a fleet already fully at target
// returns Done immediately, no steps required.
func TestNextDrainAlreadyAtTargetIsImmediatelyDone(t *testing.T) {
	f := fleet(
		map[model.CellID]model.Version{"a": v11, "b": v11, "c": v11},
		nil, nil,
	)
	plan := model.UpgradePlan{Target: v11}
	if got := NextDrain(f, plan); got.Kind != model.Done {
		t.Fatalf("NextDrain() = %+v, want Done", got)
	}
}

func TestSkewSafe(t *testing.T) {
	tests := []struct {
		name string
		a, b model.Version
		want bool
	}{
		{"identical versions", v10, v10, true},
		{"adjacent minor is safe", v10, v11, true},
		{"adjacent minor reversed is safe", v11, v10, true},
		{"two minors apart is unsafe", v10, v12, false},
		{"different major is unsafe", v10, v20, false},
		{"different major, equal minor is unsafe", model.Version{Major: 1, Minor: 5}, model.Version{Major: 2, Minor: 5}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SkewSafe(tt.a, tt.b); got != tt.want {
				t.Fatalf("SkewSafe(%+v, %+v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// TestSkewSafeReflexiveAndSymmetric enumerates a small fixed version grid
// (no math/rand) and asserts SkewSafe's two declared algebraic laws:
// reflexive (a version is always skew-safe with itself) and symmetric
// (SkewSafe(a, b) == SkewSafe(b, a)).
func TestSkewSafeReflexiveAndSymmetric(t *testing.T) {
	var versions []model.Version
	for major := 0; major <= 3; major++ {
		for minor := 0; minor <= 4; minor++ {
			versions = append(versions, model.Version{Major: major, Minor: minor})
		}
	}

	for _, a := range versions {
		if !SkewSafe(a, a) {
			t.Fatalf("SkewSafe(%+v, %+v) = false, want true (reflexive)", a, a)
		}
		for _, b := range versions {
			if got, want := SkewSafe(a, b), SkewSafe(b, a); got != want {
				t.Fatalf("SkewSafe(%+v, %+v) = %v, SkewSafe(%+v, %+v) = %v, want equal (symmetric)", a, b, got, b, a, want)
			}
		}
	}
}

// --- fixtures for the zero-loss sweep ---

type cellState struct {
	jobs     []model.JobID
	cordoned bool
	atTarget bool
}

// enumerateCellStates returns every combination of a job subset (over the
// given jobs), cordoned bit, and at-target bit — 4 * len(jobs)-subsets
// combinations for a fixed, small job list. Enumerated, never random.
func enumerateCellStates(jobs []model.JobID) []cellState {
	var states []cellState
	subsets := 1 << len(jobs)
	for bits := 0; bits < subsets; bits++ {
		var js []model.JobID
		for i, j := range jobs {
			if bits&(1<<i) != 0 {
				js = append(js, j)
			}
		}
		for _, cordoned := range []bool{false, true} {
			for _, atTarget := range []bool{false, true} {
				states = append(states, cellState{jobs: js, cordoned: cordoned, atTarget: atTarget})
			}
		}
	}
	return states
}

func buildFleet(cells []model.CellID, states []cellState, old, target model.Version) model.FleetState {
	f := model.FleetState{
		Versions: make(map[model.CellID]model.Version, len(cells)),
		Jobs:     make(map[model.CellID][]model.JobID, len(cells)),
		Cordoned: make(map[model.CellID]bool, len(cells)),
	}
	for i, id := range cells {
		s := states[i]
		v := old
		if s.atTarget {
			v = target
		}
		f.Versions[id] = v
		f.Jobs[id] = s.jobs
		f.Cordoned[id] = s.cordoned
	}
	return f
}

func assertNoJobOverlapWithCordoned(t *testing.T, f model.FleetState, chosen model.CellID) {
	t.Helper()
	chosenJobs := f.Jobs[chosen]
	for cell, cordoned := range f.Cordoned {
		if !cordoned || cell == chosen {
			continue
		}
		for _, job := range f.Jobs[cell] {
			for _, cj := range chosenJobs {
				if job == cj {
					t.Fatalf("NextDrain chose %s which shares job %s with already-cordoned/draining cell %s (fleet=%+v)", chosen, job, cell, f)
				}
			}
		}
	}
}
