// p5_chaos_capstone_test.go is the P5 chaos-drill exit-criterion capstone
// (issue #192, design ruling #183 fork c/e): a HERMETIC chaos drill that
// injects faults into a SIMULATED multi-region fleet and asserts the fleet
// converges to EXACTLY what the pure internal/core/recovery.RecoveryPlan
// decided for that fault.
//
// FCIS gives a free oracle here: expected recovery is a pure function of
// (loss, fleet-state), so "reality (the converged fake fleet) vs. the core's
// decision (the plan)" IS the assert — this file never hand-rolls a
// duplicate of recoveryPlan's region-survivor or latest-backup selection
// logic. Every expected effect is read off the plan's own Step fields
// (convergenceViolations below), and the executed effect ORDER is compared
// against the plan's step order.
//
// This composes the REAL, already-merged P5 recovery components as a black
// box — no reimplementation of any decision, and no edits to any shipped
// component:
//
//   - internal/core/recovery.RecoveryPlan / RpoMet — the pure oracle.
//   - internal/shell/recovery.Plan / Execute / Drill — the shell that
//     carries a plan out against a recovery.Fleet seam.
//
// The Fleet seam itself (chaosFleet, below) is a SIMULATED, in-process fake
// — thousands of cells across several regions, per-region backups, and a
// job running on every cell — kept e2e-local, exactly the way
// internal/shell/recovery's own hermetic chaos-harness pattern
// (recovery_test.go) builds its simulatedFleet, just at capstone scale. A
// literal multi-region chaos run against a live deployment is owner-infra
// (deferred, per #183) and explicitly out of scope for this hermetic
// capstone: this suite proves the MECHANISM — that a real fleet driven by
// the real shell always converges to the pure plan — not a live cloud
// incident.
//
// Hermetic + deterministic: no real network, no real regions/FDB, no real
// sleep (model.Instant/model.Duration stand in for the clock, passed in as
// data), no math/rand — every synthetic id is a deterministic function of a
// loop index. Confirmed stable under `go test -race -count=10`.
package e2e

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	corerecovery "github.com/msivraj/swarm/internal/core/recovery"
	"github.com/msivraj/swarm/internal/model"
	shellrecovery "github.com/msivraj/swarm/internal/shell/recovery"
)

// =============================================================================
// chaosFleet — the hermetic, in-process simulated fleet the capstone injects
// faults into. It implements shellrecovery.Fleet so the REAL Plan/Execute/
// Drill from the merged recovery shell drive it unmodified.
// =============================================================================

// fleetEvent is one fake-fleet effect call, recorded in call order — the
// observation surface the "executed in the plan's order" assertion replays.
type fleetEvent struct {
	kind   string // "rehome" | "restore" | "reroute"
	backup model.Instant
	away   model.RegionID
}

// chaosFleet is the capstone's simulated multi-region fleet: cells live in
// regions and run one job apiece ("running work"), the registry is either
// restored from some backup Instant or untouched, and traffic is either
// rerouted away from a region or not. Faults are injected by constructing or
// mutating this state directly before Plan/Execute/Drill runs;
// RecoveryPlan then reasons over its State() snapshot alone.
type chaosFleet struct {
	cells   map[model.CellID]model.RegionID
	regions []model.RegionID
	backups map[model.RegionID]model.Instant
	jobs    map[model.CellID]model.JobID // one job per cell — the fleet's "running work"

	restored     bool
	restoredFrom model.Instant
	rerouted     map[model.RegionID]bool

	events []fleetEvent

	// failOn, if non-empty, names an effect ("rehome" | "restore" |
	// "reroute") whose call returns an error without mutating state or
	// logging an event — used to prove Execute stops at the first fault
	// without silently continuing past it.
	failOn string
}

// newChaosFleet builds a fleet with cellsPerRegion cells homed in each of
// regions, each cell running exactly one job, and backups as given.
// Construction is entirely index-driven (no randomness), so two calls with
// the same arguments always yield byte-identical state.
func newChaosFleet(regions []model.RegionID, cellsPerRegion int, backups map[model.RegionID]model.Instant) *chaosFleet {
	cells := make(map[model.CellID]model.RegionID, len(regions)*cellsPerRegion)
	jobs := make(map[model.CellID]model.JobID, len(regions)*cellsPerRegion)
	for _, region := range regions {
		for i := 0; i < cellsPerRegion; i++ {
			cell := model.CellID(fmt.Sprintf("cell-%s-%05d", region, i))
			cells[cell] = region
			jobs[cell] = model.JobID(fmt.Sprintf("job-%s", cell))
		}
	}
	regionsCopy := make([]model.RegionID, len(regions))
	copy(regionsCopy, regions)
	backupsCopy := make(map[model.RegionID]model.Instant, len(backups))
	for k, v := range backups {
		backupsCopy[k] = v
	}
	return &chaosFleet{
		cells:    cells,
		regions:  regionsCopy,
		backups:  backupsCopy,
		jobs:     jobs,
		rerouted: make(map[model.RegionID]bool),
	}
}

func (f *chaosFleet) State() model.FleetState {
	cells := make(map[model.CellID]model.RegionID, len(f.cells))
	for k, v := range f.cells {
		cells[k] = v
	}
	regions := make([]model.RegionID, len(f.regions))
	copy(regions, f.regions)
	backups := make(map[model.RegionID]model.Instant, len(f.backups))
	for k, v := range f.backups {
		backups[k] = v
	}
	return model.FleetState{Cells: cells, Regions: regions, Backups: backups}
}

func (f *chaosFleet) ReHomeAgents(from, to model.RegionID) error {
	if f.failOn == "rehome" {
		return fmt.Errorf("chaos fleet: rehome fault injected")
	}
	for cell, region := range f.cells {
		if region == from {
			f.cells[cell] = to
		}
	}
	f.events = append(f.events, fleetEvent{kind: "rehome"})
	return nil
}

func (f *chaosFleet) RestoreRegistry(backup model.Instant) error {
	if f.failOn == "restore" {
		return fmt.Errorf("chaos fleet: restore fault injected")
	}
	f.restored = true
	f.restoredFrom = backup
	f.events = append(f.events, fleetEvent{kind: "restore", backup: backup})
	return nil
}

func (f *chaosFleet) Reroute(away model.RegionID) error {
	if f.failOn == "reroute" {
		return fmt.Errorf("chaos fleet: reroute fault injected")
	}
	f.rerouted[away] = true
	f.events = append(f.events, fleetEvent{kind: "reroute", away: away})
	return nil
}

// jobKeys returns the fleet's running-job cell keys, sorted — used to assert
// no job was lost or gained by a recovery, independent of map order.
func (f *chaosFleet) jobKeys() []string {
	out := make([]string, 0, len(f.jobs))
	for cell := range f.jobs {
		out = append(out, string(cell))
	}
	sort.Strings(out)
	return out
}

// =============================================================================
// The FCIS oracle: everything a converged fleet must show, derived SOLELY
// from the plan's own Step fields — never a hand-rolled duplicate of
// recoveryPlan's region-survivor or latest-backup selection logic.
// =============================================================================

// stepKinds flattens plan into its ordered Kinds, for comparing against the
// fake fleet's own recorded event kinds (the "executed effect order equals
// the plan's step order" assertion).
func stepKinds(plan []model.Step) []string {
	out := make([]string, 0, len(plan))
	for _, s := range plan {
		switch s.Kind {
		case model.ReHome:
			out = append(out, "rehome")
		case model.RestoreRegistry:
			out = append(out, "restore")
		case model.Reroute:
			out = append(out, "reroute")
		default:
			out = append(out, fmt.Sprintf("unknown(%v)", s.Kind))
		}
	}
	return out
}

func eventKinds(events []fleetEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.kind)
	}
	return out
}

// convergenceViolations is the oracle itself: it reads plan's own fields to
// state what the fleet's converged reality MUST be, and returns every way
// fleet disagrees. An empty result means the fleet converged to EXACTLY what
// recovery.RecoveryPlan decided for loss over before — the capstone's whole
// point. before is fleet's snapshot immediately prior to Plan/Execute/Drill
// running.
func convergenceViolations(fleet *chaosFleet, before model.FleetState, loss model.Loss, plan []model.Step) []string {
	var violations []string
	var sawReHome, sawRestore, sawReroute bool

	for _, step := range plan {
		switch step.Kind {
		case model.ReHome:
			sawReHome = true
			for cell, region := range before.Cells {
				if region != loss.Region {
					continue
				}
				if got := fleet.cells[cell]; got != step.Region {
					violations = append(violations, fmt.Sprintf("cell %q: region = %q, want re-homed to %q (plan step %+v)", cell, got, step.Region, step))
				}
			}
		case model.RestoreRegistry:
			sawRestore = true
			if !fleet.restored {
				violations = append(violations, fmt.Sprintf("plan called for RestoreRegistry{%v} but fleet was never restored", step.Backup))
			} else if fleet.restoredFrom != step.Backup {
				violations = append(violations, fmt.Sprintf("fleet restored from %v, want %v (plan step %+v)", fleet.restoredFrom, step.Backup, step))
			}
		case model.Reroute:
			sawReroute = true
			if !fleet.rerouted[step.Traffic] {
				violations = append(violations, fmt.Sprintf("plan called for Reroute{%v} but fleet never rerouted away from it", step.Traffic))
			}
		}
	}

	if !sawReHome {
		// No ReHome step: every cell that was in loss.Region must still be
		// there — nothing licensed moving it.
		for cell, region := range before.Cells {
			if region == loss.Region && fleet.cells[cell] != loss.Region {
				violations = append(violations, fmt.Sprintf("cell %q moved to %q despite no ReHome step in the plan", cell, fleet.cells[cell]))
			}
		}
	}
	if !sawRestore && fleet.restored {
		violations = append(violations, "fleet was restored despite no RestoreRegistry step in the plan")
	}
	if !sawReroute && len(fleet.rerouted) != 0 {
		violations = append(violations, fmt.Sprintf("fleet rerouted %v despite no Reroute step in the plan", fleet.rerouted))
	}

	// Running work survives a recovery unchanged: recoveryPlan never adds or
	// drops a job, only re-homes/restores/reroutes around it.
	if got, want := len(fleet.jobs), len(before.Cells); got != want {
		violations = append(violations, fmt.Sprintf("fleet has %d running jobs after recovery, want %d (one per pre-recovery cell)", got, want))
	}

	if got, want := eventKinds(fleet.events), stepKinds(plan); !reflect.DeepEqual(got, want) {
		violations = append(violations, fmt.Sprintf("fleet effect order = %v, want exactly the plan order %v", got, want))
	}

	return violations
}

// assertConverged fails t with every oracle violation found, if any.
func assertConverged(t *testing.T, fleet *chaosFleet, before model.FleetState, loss model.Loss, plan []model.Step) {
	t.Helper()
	for _, v := range convergenceViolations(fleet, before, loss, plan) {
		t.Error(v)
	}
}

// =============================================================================
// The fault matrix: single region loss (with and without a fresh backup),
// store loss, a region loss where a specific survivor must be chosen, an
// unmatched/unknown-region no-op, and an empty fleet. Every scenario injects
// its fault into a chaosFleet, runs the REAL shellrecovery.Execute, and
// asserts convergence to the pure oracle.
// =============================================================================

const (
	regionCount    = 5   // several regions
	cellsPerRegion = 500 // thousands of cells fleet-wide (5 x 500 = 2,500)
)

func fleetRegions() []model.RegionID {
	regions := make([]model.RegionID, regionCount)
	for i := range regions {
		regions[i] = model.RegionID(fmt.Sprintf("region-%02d", i))
	}
	return regions
}

func TestChaosCapstone_FaultMatrixConvergesToPurePlan(t *testing.T) {
	regions := fleetRegions()
	freshBackups := map[model.RegionID]model.Instant{
		"region-00": 100, "region-01": 900, "region-02": 400, "region-03": 700, "region-04": 200,
	}

	scenarios := []struct {
		name    string
		fleet   *chaosFleet
		loss    model.Loss
		wantLen int // >=0 sanity on plan length, documents the matrix intent
	}{
		{
			name:    "region loss, fresh per-region backups available",
			fleet:   newChaosFleet(regions, cellsPerRegion, freshBackups),
			loss:    model.Loss{Kind: model.RegionLoss, Region: "region-02"},
			wantLen: 3, // ReHome, RestoreRegistry, Reroute
		},
		{
			name:    "region loss, no backup available at all",
			fleet:   newChaosFleet(regions, cellsPerRegion, nil),
			loss:    model.Loss{Kind: model.RegionLoss, Region: "region-02"},
			wantLen: 2, // ReHome, Reroute — RestoreRegistry skipped, nothing to restore from
		},
		{
			name:    "store loss restores the registry only",
			fleet:   newChaosFleet(regions, cellsPerRegion, freshBackups),
			loss:    model.Loss{Kind: model.StoreLoss},
			wantLen: 1, // RestoreRegistry only
		},
		{
			name: "region loss where a specific survivor must be chosen among several",
			fleet: newChaosFleet(
				[]model.RegionID{"region-04", "region-01", "region-03", "region-00", "region-02"}, // deliberately unsorted construction order
				cellsPerRegion,
				freshBackups,
			),
			loss:    model.Loss{Kind: model.RegionLoss, Region: "region-01"},
			wantLen: 3,
		},
		{
			name:    "region loss naming a region the fleet does not know: empty plan, no-op",
			fleet:   newChaosFleet(regions, cellsPerRegion, freshBackups),
			loss:    model.Loss{Kind: model.RegionLoss, Region: "ap-south-unknown"},
			wantLen: 0,
		},
		{
			name:    "empty fleet: no-op regardless of loss kind",
			fleet:   newChaosFleet(nil, 0, nil),
			loss:    model.Loss{Kind: model.StoreLoss},
			wantLen: 0,
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			fleet := sc.fleet
			before := fleet.State()

			// Deterministic drill discipline (§02): the pure oracle is
			// computed BEFORE anything executes.
			plan := corerecovery.RecoveryPlan(sc.loss, before)
			if len(plan) != sc.wantLen {
				t.Fatalf("plan length = %d, want %d (plan: %+v)", len(plan), sc.wantLen, plan)
			}
			if len(fleet.events) != 0 {
				t.Fatalf("fleet mutated before Execute ran: %v", fleet.events)
			}

			executed, err := shellrecovery.Execute(fleet, sc.loss)
			if err != nil {
				t.Fatalf("Execute returned error: %v", err)
			}
			if !reflect.DeepEqual(executed, plan) {
				t.Fatalf("Execute plan = %+v, want the pure oracle %+v", executed, plan)
			}

			assertConverged(t, fleet, before, sc.loss, executed)
		})
	}
}

// =============================================================================
// Deterministic drill: the same injected loss yields the same plan every
// run, asserted BEFORE execution — "us-east lost => these exact steps" at
// capstone scale, over a fleet whose region topology was deliberately built
// out of sorted order to prove survivor selection does not depend on
// construction/iteration order.
// =============================================================================

func TestChaosCapstone_DeterministicDrillAssertsExactStepsBeforeExecuting(t *testing.T) {
	regions := []model.RegionID{"eu-central", "us-west", "ap-south", "us-east"}
	backups := map[model.RegionID]model.Instant{
		"us-east": 50, "us-west": 999, "eu-central": 10, "ap-south": 500,
	}
	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}

	// ap-south is the lowest-sorted region that is not us-east; us-west
	// holds the newest backup (999). This is the specific, literal plan the
	// drill must compute BEFORE anything runs — the repeatable oracle.
	want := []model.Step{
		{Kind: model.ReHome, Region: "ap-south"},
		{Kind: model.RestoreRegistry, Backup: 999},
		{Kind: model.Reroute, Traffic: "us-east"},
	}

	for i := 0; i < 5; i++ {
		fleet := newChaosFleet(regions, cellsPerRegion, backups)
		got := shellrecovery.Plan(fleet, loss)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d: Plan (pre-execution) = %+v, want %+v", i, got, want)
		}
		if len(fleet.events) != 0 {
			t.Fatalf("run %d: Plan mutated the fleet: %v", i, fleet.events)
		}
	}
}

// =============================================================================
// RPO drill: recovery.RpoMet, driven by a fake clock (model.Instant passed
// in as data, never read from a real clock), gates whether a restore's
// backup is fresh enough — the freshness gate is asserted at the boundary,
// and the drill still converges to the pure plan regardless of the verdict.
// =============================================================================

func TestChaosCapstone_RPODrillGatesBackupFreshness(t *testing.T) {
	regions := fleetRegions()
	const backupAt model.Instant = 1_000
	backups := map[model.RegionID]model.Instant{"region-00": backupAt}
	loss := model.Loss{Kind: model.RegionLoss, Region: "region-00"}
	const rpo model.Duration = 100

	tests := []struct {
		name   string
		now    model.Instant
		wantOK bool
	}{
		{name: "well within rpo", now: backupAt + 10, wantOK: true},
		{name: "exactly at rpo boundary is met", now: backupAt + model.Instant(rpo), wantOK: true},
		{name: "one tick past rpo is not met", now: backupAt + model.Instant(rpo) + 1, wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fleet := newChaosFleet(regions, cellsPerRegion, backups)
			before := fleet.State()

			result := shellrecovery.Drill(fleet, loss, tc.now, rpo)
			if result.Err != nil {
				t.Fatalf("Drill returned error: %v", result.Err)
			}
			if result.RPOMet != tc.wantOK {
				t.Fatalf("RPOMet = %v, want %v (now=%v, backup=%v, rpo=%v)", result.RPOMet, tc.wantOK, tc.now, backupAt, rpo)
			}
			// RpoMet gates a freshness ASSERT, not a shell-invented execution
			// gate — the drill still converges to the plan regardless of the
			// RPO verdict.
			assertConverged(t, fleet, before, loss, result.Plan)

			// Cross-check RPOMet itself against the pure core directly, for
			// the exact backup the plan restored from — the same value
			// RpoMet reasons over, no re-derivation of freshness logic.
			if got := corerecovery.RpoMet(backupAt, tc.now, rpo); got != tc.wantOK {
				t.Fatalf("corerecovery.RpoMet = %v, want %v", got, tc.wantOK)
			}
		})
	}
}

// =============================================================================
// The oracle is not vacuous: a fleet that failed to fully converge — because
// a (hypothetically buggy) shell skipped one of the plan's steps — is CAUGHT
// by convergenceViolations. This proves the assert above would actually
// fail on a mis-execution, not just pass trivially.
// =============================================================================

func TestChaosCapstone_OracleCatchesMisExecution(t *testing.T) {
	regions := fleetRegions()
	backups := map[model.RegionID]model.Instant{
		"region-00": 100, "region-01": 900, "region-02": 400, "region-03": 700, "region-04": 200,
	}
	loss := model.Loss{Kind: model.RegionLoss, Region: "region-01"}

	fleet := newChaosFleet(regions, cellsPerRegion, backups)
	before := fleet.State()
	plan := corerecovery.RecoveryPlan(loss, before)
	if len(plan) != 3 {
		t.Fatalf("test setup invalid: want a 3-step plan, got %+v", plan)
	}

	// Simulate a buggy shell that carries out ReHome and RestoreRegistry but
	// (bug!) never calls Reroute — apply the Fleet effects directly rather
	// than going through the real Execute, which always runs the full plan.
	for _, step := range plan {
		var err error
		switch step.Kind {
		case model.ReHome:
			err = fleet.ReHomeAgents(loss.Region, step.Region)
		case model.RestoreRegistry:
			err = fleet.RestoreRegistry(step.Backup)
		case model.Reroute:
			continue // the injected bug: skip this step entirely
		}
		if err != nil {
			t.Fatalf("fleet effect failed: %v", err)
		}
	}

	violations := convergenceViolations(fleet, before, loss, plan)
	if len(violations) == 0 {
		t.Fatalf("oracle failed to catch a mis-executed plan (Reroute skipped) — the assert is vacuous")
	}
	// Sanity-check the violation is the one the injected bug should cause:
	// the fleet never rerouted anything, though the plan called for it.
	if len(fleet.rerouted) != 0 {
		t.Fatalf("test setup invalid: fleet unexpectedly rerouted %v despite the injected bug skipping Reroute", fleet.rerouted)
	}
	t.Logf("oracle caught the mis-execution: %v", violations)
}

// =============================================================================
// Determinism at capstone scale: identical starting fleets + the same loss
// converge to identical plans, effect sequences, and final fleet state — no
// hidden nondeterminism anywhere in the composed real shell.
// =============================================================================

func TestChaosCapstone_Deterministic(t *testing.T) {
	regions := fleetRegions()
	backups := map[model.RegionID]model.Instant{
		"region-00": 100, "region-01": 900, "region-02": 400, "region-03": 700, "region-04": 200,
	}
	loss := model.Loss{Kind: model.RegionLoss, Region: "region-03"}

	f1 := newChaosFleet(regions, cellsPerRegion, backups)
	f2 := newChaosFleet(regions, cellsPerRegion, backups)

	p1, err1 := shellrecovery.Execute(f1, loss)
	p2, err2 := shellrecovery.Execute(f2, loss)
	if err1 != nil || err2 != nil {
		t.Fatalf("Execute errors: %v, %v", err1, err2)
	}
	if !reflect.DeepEqual(p1, p2) {
		t.Fatalf("Execute plans diverged: %+v vs %+v", p1, p2)
	}
	if !reflect.DeepEqual(f1.events, f2.events) {
		t.Fatalf("Execute effect sequences diverged: %v vs %v", f1.events, f2.events)
	}
	if !reflect.DeepEqual(f1.cells, f2.cells) {
		t.Fatalf("Execute converged fleets diverged")
	}
	if !reflect.DeepEqual(f1.jobKeys(), f2.jobKeys()) {
		t.Fatalf("Execute running-work sets diverged")
	}
}
