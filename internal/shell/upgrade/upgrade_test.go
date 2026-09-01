package upgrade

import (
	"reflect"
	"sort"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// --- fake fleet ---
//
// event is one fake-fleet effect call, recorded in call order. It is the
// observation surface the zero-loss ("never two cells of the same job
// cordoned at once") assertions replay.
type event struct {
	kind string // "cordon" | "drain" | "roll" | "uncordon"
	cell model.CellID
}

// fakeFleet is the swarmd-shaped FAKE fleet the #165 ticket calls for: no
// real node, no real binary — cordon/drain/roll/uncordon just mutate plain
// maps and log an event. Kept local to this test file per the ticket's
// package-path note ("keep the fake fleet test-local").
type fakeFleet struct {
	versions map[model.CellID]model.Version
	jobs     map[model.CellID][]model.JobID
	cordoned map[model.CellID]bool
	events   []event

	// stuckUncordon, if non-empty, names a cell whose Uncordon call is a
	// no-op — simulating a Fleet effect that leaves a cordon in flight
	// (#165 caveat 1), so a stuck-Done state can be exercised deliberately
	// rather than only ever arising from a skew-unsafe cell.
	stuckUncordon model.CellID
}

func newFakeFleet(versions map[model.CellID]model.Version, jobs map[model.CellID][]model.JobID) *fakeFleet {
	return &fakeFleet{
		versions: versions,
		jobs:     jobs,
		cordoned: make(map[model.CellID]bool),
	}
}

func (f *fakeFleet) State() model.FleetState {
	versions := make(map[model.CellID]model.Version, len(f.versions))
	for k, v := range f.versions {
		versions[k] = v
	}
	jobs := make(map[model.CellID][]model.JobID, len(f.jobs))
	for k, v := range f.jobs {
		cp := make([]model.JobID, len(v))
		copy(cp, v)
		jobs[k] = cp
	}
	cordoned := make(map[model.CellID]bool, len(f.cordoned))
	for k, v := range f.cordoned {
		cordoned[k] = v
	}
	return model.FleetState{Versions: versions, Jobs: jobs, Cordoned: cordoned}
}

func (f *fakeFleet) Cordon(cell model.CellID) error {
	f.cordoned[cell] = true
	f.events = append(f.events, event{"cordon", cell})
	return nil
}

// Drain fakes checkpoint-migrate: jobs stay put, untouched. Zero loss holds
// by construction here, so the tests can focus on OBSERVING the property
// (no job ever vanishes across a whole rollout) rather than the fake having
// to move anything.
func (f *fakeFleet) Drain(cell model.CellID) error {
	f.events = append(f.events, event{"drain", cell})
	return nil
}

func (f *fakeFleet) Roll(cell model.CellID, target model.Version) error {
	f.versions[cell] = target
	f.events = append(f.events, event{"roll", cell})
	return nil
}

func (f *fakeFleet) Uncordon(cell model.CellID) error {
	f.events = append(f.events, event{"uncordon", cell})
	if cell == f.stuckUncordon {
		return nil // leave f.cordoned[cell] == true: the in-flight-cordon fault
	}
	delete(f.cordoned, cell)
	return nil
}

// --- helpers ---

func cloneJobs(jobs map[model.CellID][]model.JobID) map[model.CellID][]model.JobID {
	out := make(map[model.CellID][]model.JobID, len(jobs))
	for k, v := range jobs {
		cp := make([]model.JobID, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

// allJobIDs flattens every JobID present anywhere in jobs into a sorted,
// deduplicated slice — used to compare "which jobs exist in the fleet"
// before and after a rollout, independent of which cell each is on.
func allJobIDs(jobs map[model.CellID][]model.JobID) []model.JobID {
	set := make(map[model.JobID]struct{})
	for _, ids := range jobs {
		for _, id := range ids {
			set[id] = struct{}{}
		}
	}
	out := make([]model.JobID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// assertNeverConcurrentlyCordoned replays events and fails t if any two
// cells sharing a job (per jobsByCell, the fleet's static job assignment)
// are ever cordoned at the same time — the "never drain two cells of the
// same job at once" zero-loss property, observed through the shell's own
// effect calls rather than asserted about the core in isolation. It also
// fails if more than one cell is ever cordoned at once at all, which is the
// stronger invariant Run's one-cell-at-a-time loop actually provides.
func assertNeverConcurrentlyCordoned(t *testing.T, events []event, jobsByCell map[model.CellID][]model.JobID) {
	t.Helper()
	cordonedNow := make(map[model.CellID]bool)
	busyJobs := make(map[model.JobID]model.CellID)

	for _, e := range events {
		switch e.kind {
		case "cordon":
			if len(cordonedNow) > 0 {
				t.Fatalf("cell %q cordoned while another cell is still cordoned: %v", e.cell, cordonedNow)
			}
			cordonedNow[e.cell] = true
			for _, j := range jobsByCell[e.cell] {
				if owner, ok := busyJobs[j]; ok && owner != e.cell {
					t.Fatalf("job %q cordoned on both %q and %q simultaneously", j, owner, e.cell)
				}
				busyJobs[j] = e.cell
			}
		case "uncordon":
			delete(cordonedNow, e.cell)
			for _, j := range jobsByCell[e.cell] {
				delete(busyJobs, j)
			}
		}
	}
}

// --- tests ---

func TestRun_ZeroLossRollsFleetToTarget(t *testing.T) {
	target := model.Version{Major: 1, Minor: 1}
	start := model.Version{Major: 1, Minor: 0} // within skewWindow (1) of target

	versions := map[model.CellID]model.Version{
		"cellA": start,
		"cellB": start,
		"cellC": start,
	}
	// cellA and cellB run the same job (jobShared) — the pair the
	// zero-loss invariant is about. cellC runs an unrelated job.
	jobs := map[model.CellID][]model.JobID{
		"cellA": {"jobShared"},
		"cellB": {"jobShared"},
		"cellC": {"jobSolo"},
	}
	fleet := newFakeFleet(versions, jobs)
	before := cloneJobs(fleet.jobs)

	plan := model.UpgradePlan{Target: target}
	result, err := Run(fleet, plan)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != Complete {
		t.Fatalf("Outcome = %v, want Complete (Unfinished=%v)", result.Outcome, result.Unfinished)
	}
	if len(result.Unfinished) != 0 {
		t.Fatalf("Unfinished = %v, want empty on Complete", result.Unfinished)
	}

	final := fleet.State()
	for cell, v := range final.Versions {
		if v != target {
			t.Errorf("cell %q ended at %+v, want target %+v", cell, v, target)
		}
	}
	for cell, cordoned := range final.Cordoned {
		if cordoned {
			t.Errorf("cell %q left cordoned after Complete", cell)
		}
	}

	// Zero job loss: the set of jobs present anywhere in the fleet is
	// unchanged by the whole rollout.
	if got, want := allJobIDs(final.Jobs), allJobIDs(before); !reflect.DeepEqual(got, want) {
		t.Fatalf("job set changed across rollout: before=%v after=%v", want, got)
	}

	// Zero-loss, observed through the shell: cellA and cellB (same job)
	// are never cordoned/draining at the same time.
	assertNeverConcurrentlyCordoned(t, fleet.events, jobs)

	// Every cell actually got rolled (Run did real work, not a no-op).
	rolled := make(map[model.CellID]bool)
	for _, e := range fleet.events {
		if e.kind == "roll" {
			rolled[e.cell] = true
		}
	}
	for cell := range versions {
		if !rolled[cell] {
			t.Errorf("cell %q never rolled", cell)
		}
	}
}

func TestRun_SkewUnsafeCellRefusedAndReportedBlocked(t *testing.T) {
	target := model.Version{Major: 2, Minor: 0}

	versions := map[model.CellID]model.Version{
		"cellUnsafe": {Major: 1, Minor: 0}, // different Major: never SkewSafe with target
		"cellSafe":   target,               // already at target
	}
	jobs := map[model.CellID][]model.JobID{
		"cellUnsafe": {"jobA"},
		"cellSafe":   {"jobB"},
	}
	fleet := newFakeFleet(versions, jobs)

	plan := model.UpgradePlan{Target: target}
	result, err := Run(fleet, plan)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != Blocked {
		t.Fatalf("Outcome = %v, want Blocked", result.Outcome)
	}
	if want := []model.CellID{"cellUnsafe"}; !reflect.DeepEqual(result.Unfinished, want) {
		t.Fatalf("Unfinished = %v, want %v", result.Unfinished, want)
	}

	// The refused step must never have been rolled: the fake binary's
	// version is untouched, and no roll event for it was ever recorded.
	final := fleet.State()
	if v := final.Versions["cellUnsafe"]; v != (model.Version{Major: 1, Minor: 0}) {
		t.Errorf("cellUnsafe version changed to %+v despite being skew-unsafe", v)
	}
	for _, e := range fleet.events {
		if e.cell == "cellUnsafe" {
			t.Errorf("unexpected effect %q on skew-unsafe cell %q", e.kind, e.cell)
		}
	}
}

func TestRun_StuckCordonDetectedAsBlocked(t *testing.T) {
	// cellA and cellB share a job. cellA's Uncordon effect is broken (it
	// never clears the cordon), so once cellA is rolled and "uncordoned"
	// it stays cordoned forever — leaving cellB permanently job-conflicted
	// even though cellB is itself skew-safe and would otherwise roll fine.
	// NextDrain will eventually return Done (nothing left it can safely
	// cordon), while cellB never reaches target: a stuck-Done state Run
	// must report as Blocked, not Complete.
	target := model.Version{Major: 1, Minor: 1}
	start := model.Version{Major: 1, Minor: 0}

	versions := map[model.CellID]model.Version{
		"cellA": start,
		"cellB": start,
	}
	jobs := map[model.CellID][]model.JobID{
		"cellA": {"jobShared"},
		"cellB": {"jobShared"},
	}
	fleet := newFakeFleet(versions, jobs)
	fleet.stuckUncordon = "cellA"

	plan := model.UpgradePlan{Target: target, Order: []model.CellID{"cellA", "cellB"}}
	result, err := Run(fleet, plan)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != Blocked {
		t.Fatalf("Outcome = %v, want Blocked", result.Outcome)
	}
	if want := []model.CellID{"cellB"}; !reflect.DeepEqual(result.Unfinished, want) {
		t.Fatalf("Unfinished = %v, want %v", result.Unfinished, want)
	}

	final := fleet.State()
	if v := final.Versions["cellA"]; v != target {
		t.Errorf("cellA version = %+v, want it to have rolled to target %+v before getting stuck", v, target)
	}
	if !final.Cordoned["cellA"] {
		t.Errorf("cellA should still be reported cordoned (the fault this test exercises)")
	}
	if v := final.Versions["cellB"]; v == target {
		t.Errorf("cellB reached target %+v despite the stuck cordon on cellA", v)
	}
}

func TestRun_AlreadyAtTargetIsImmediatelyComplete(t *testing.T) {
	target := model.Version{Major: 1, Minor: 0}
	versions := map[model.CellID]model.Version{"cellA": target, "cellB": target}
	jobs := map[model.CellID][]model.JobID{"cellA": {"jobA"}, "cellB": {"jobB"}}
	fleet := newFakeFleet(versions, jobs)

	result, err := Run(fleet, model.UpgradePlan{Target: target})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != Complete {
		t.Fatalf("Outcome = %v, want Complete", result.Outcome)
	}
	if len(fleet.events) != 0 {
		t.Errorf("expected no effects against an already-at-target fleet, got %v", fleet.events)
	}
}

// TestRun_Deterministic mirrors internal/core/mitosis's determinism
// discipline at the shell level: driving Run against two independently
// built fake fleets from identical starting data produces the identical
// effect sequence.
func TestRun_Deterministic(t *testing.T) {
	build := func() *fakeFleet {
		versions := map[model.CellID]model.Version{
			"cellA": {Major: 1, Minor: 0},
			"cellB": {Major: 1, Minor: 0},
			"cellC": {Major: 1, Minor: 0},
		}
		jobs := map[model.CellID][]model.JobID{
			"cellA": {"jobShared"},
			"cellB": {"jobShared"},
			"cellC": {"jobSolo"},
		}
		return newFakeFleet(versions, jobs)
	}
	plan := model.UpgradePlan{Target: model.Version{Major: 1, Minor: 1}}

	f1, f2 := build(), build()
	r1, err1 := Run(f1, plan)
	r2, err2 := Run(f2, plan)
	if err1 != nil || err2 != nil {
		t.Fatalf("Run errors: %v, %v", err1, err2)
	}
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("Run results diverged: %+v vs %+v", r1, r2)
	}
	if !reflect.DeepEqual(f1.events, f2.events) {
		t.Fatalf("Run effect sequences diverged: %v vs %v", f1.events, f2.events)
	}
}
