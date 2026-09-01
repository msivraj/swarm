package recovery

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	corerecovery "github.com/msivraj/swarm/internal/core/recovery"
	"github.com/msivraj/swarm/internal/model"
)

// =============================================================================
// simulatedFleet — the hermetic, in-process chaos-harness fleet (#183 fork c:
// no real multi-region infra). Kept test-local; a real swarmd-backed Fleet
// composing the store/FDB registry seam, P1 failover, and the global router
// drops in later. Every mutation is a plain map/field update plus an
// appended event, so a test can both assert on the fleet's converged state
// AND replay the exact order effects fired in.
// =============================================================================

// fleetEvent is one fake-fleet effect call, recorded in call order — the
// observation surface the "executed in order" assertions replay.
type fleetEvent struct {
	kind   string // "rehome" | "restore" | "reroute"
	from   model.RegionID
	to     model.RegionID
	backup model.Instant
	away   model.RegionID
}

// simulatedFleet is the chaos harness's fake fleet: cells live in regions,
// the registry is either "restored from" some backup Instant or untouched,
// and traffic is either rerouted away from a region or not. Faults are
// injected by constructing/mutating this state directly before Execute/Drill
// is called; RecoveryPlan then reasons over its State() snapshot.
type simulatedFleet struct {
	cells   map[model.CellID]model.RegionID
	regions []model.RegionID
	backups map[model.RegionID]model.Instant

	restored     bool
	restoredFrom model.Instant
	rerouted     map[model.RegionID]bool

	events []fleetEvent

	// failOn, if non-empty, names an effect ("rehome" | "restore" |
	// "reroute") whose call returns an error without mutating state or
	// logging an event — the fault this harness uses to exercise Execute's
	// "stop on first error" contract.
	failOn string
}

func newSimulatedFleet(cells map[model.CellID]model.RegionID, regions []model.RegionID, backups map[model.RegionID]model.Instant) *simulatedFleet {
	return &simulatedFleet{
		cells:    cells,
		regions:  regions,
		backups:  backups,
		rerouted: make(map[model.RegionID]bool),
	}
}

func (f *simulatedFleet) State() model.FleetState {
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

func (f *simulatedFleet) ReHomeAgents(from, to model.RegionID) error {
	if f.failOn == "rehome" {
		return errors.New("simulated fleet: rehome fault injected")
	}
	for cell, region := range f.cells {
		if region == from {
			f.cells[cell] = to
		}
	}
	f.events = append(f.events, fleetEvent{kind: "rehome", from: from, to: to})
	return nil
}

func (f *simulatedFleet) RestoreRegistry(backup model.Instant) error {
	if f.failOn == "restore" {
		return errors.New("simulated fleet: restore fault injected")
	}
	f.restored = true
	f.restoredFrom = backup
	f.events = append(f.events, fleetEvent{kind: "restore", backup: backup})
	return nil
}

func (f *simulatedFleet) Reroute(away model.RegionID) error {
	if f.failOn == "reroute" {
		return errors.New("simulated fleet: reroute fault injected")
	}
	f.rerouted[away] = true
	f.events = append(f.events, fleetEvent{kind: "reroute", away: away})
	return nil
}

// --- helpers ---

// stepKinds flattens plan into its ordered Kinds, for comparing against the
// fake fleet's own recorded event kinds.
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

// assertConverged is the FCIS oracle assertion: it derives, from plan alone
// (the PURE core's decision), exactly what the fleet's state must look like
// now, and fails t if reality (fleet's live state) disagrees. before is the
// fleet's snapshot immediately before Execute/Drill ran.
func assertConverged(t *testing.T, fleet *simulatedFleet, before model.FleetState, loss model.Loss, plan []model.Step) {
	t.Helper()

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
					t.Errorf("cell %q: region = %q, want re-homed to %q (plan step %+v)", cell, got, step.Region, step)
				}
			}
		case model.RestoreRegistry:
			sawRestore = true
			if !fleet.restored {
				t.Errorf("plan called for RestoreRegistry{%v} but fleet was never restored", step.Backup)
			} else if fleet.restoredFrom != step.Backup {
				t.Errorf("fleet restored from %v, want %v (plan step %+v)", fleet.restoredFrom, step.Backup, step)
			}
		case model.Reroute:
			sawReroute = true
			if !fleet.rerouted[step.Traffic] {
				t.Errorf("plan called for Reroute{%v} but fleet never rerouted away from it", step.Traffic)
			}
		}
	}

	if !sawReHome {
		// No ReHome step: every cell that was in loss.Region must still be
		// there — nothing licensed moving it.
		for cell, region := range before.Cells {
			if region == loss.Region && fleet.cells[cell] != loss.Region {
				t.Errorf("cell %q moved to %q despite no ReHome step in the plan", cell, fleet.cells[cell])
			}
		}
	}
	if !sawRestore && fleet.restored {
		t.Errorf("fleet was restored despite no RestoreRegistry step in the plan")
	}
	if !sawReroute && len(fleet.rerouted) != 0 {
		t.Errorf("fleet rerouted %v despite no Reroute step in the plan", fleet.rerouted)
	}

	if got, want := eventKinds(fleet.events), stepKinds(plan); !reflect.DeepEqual(got, want) {
		t.Errorf("fleet effect order = %v, want exactly the plan order %v", got, want)
	}
}

func cellSet(cells map[model.CellID]model.RegionID) []model.CellID {
	out := make([]model.CellID, 0, len(cells))
	for c := range cells {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// =============================================================================
// Region-loss recovery (acceptance: "plan executed in order")
// =============================================================================

func TestExecute_RegionLossReHomesRestoresReroutesInOrder(t *testing.T) {
	cells := map[model.CellID]model.RegionID{
		"cell-1": "us-east",
		"cell-2": "us-east",
		"cell-3": "us-west",
	}
	regions := []model.RegionID{"us-east", "us-west"}
	backups := map[model.RegionID]model.Instant{"us-east": 100, "us-west": 300}
	fleet := newSimulatedFleet(cells, regions, backups)

	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}

	// DR-drill discipline (§02): compute + assert the exact plan BEFORE
	// anything executes.
	before := fleet.State()
	wantPlan := []model.Step{
		{Kind: model.ReHome, Region: "us-west"},
		{Kind: model.RestoreRegistry, Backup: 300},
		{Kind: model.Reroute, Traffic: "us-east"},
	}
	gotPlan := corerecovery.RecoveryPlan(loss, before)
	if !reflect.DeepEqual(gotPlan, wantPlan) {
		t.Fatalf("RecoveryPlan (pre-execution) = %+v, want %+v", gotPlan, wantPlan)
	}
	if len(fleet.events) != 0 {
		t.Fatalf("fleet mutated before Execute ran: %v", fleet.events)
	}

	executed, err := Execute(fleet, loss)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !reflect.DeepEqual(executed, wantPlan) {
		t.Fatalf("Execute plan = %+v, want %+v", executed, wantPlan)
	}

	assertConverged(t, fleet, before, loss, executed)

	// Concretely: cell-1 and cell-2 (us-east) re-homed to us-west; cell-3
	// (already us-west) untouched.
	want := map[model.CellID]model.RegionID{"cell-1": "us-west", "cell-2": "us-west", "cell-3": "us-west"}
	if !reflect.DeepEqual(fleet.cells, want) {
		t.Fatalf("fleet.cells = %+v, want %+v", fleet.cells, want)
	}
	if !fleet.restored || fleet.restoredFrom != 300 {
		t.Fatalf("registry restore = (restored=%v, from=%v), want (true, 300)", fleet.restored, fleet.restoredFrom)
	}
	if !fleet.rerouted["us-east"] {
		t.Fatalf("traffic not rerouted away from us-east: %v", fleet.rerouted)
	}
}

// =============================================================================
// Store-loss recovery
// =============================================================================

func TestExecute_StoreLossRestoresRegistryOnly(t *testing.T) {
	cells := map[model.CellID]model.RegionID{"cell-1": "us-east", "cell-2": "us-west"}
	regions := []model.RegionID{"us-east", "us-west"}
	backups := map[model.RegionID]model.Instant{"us-east": 50, "us-west": 900}
	fleet := newSimulatedFleet(cells, regions, backups)

	loss := model.Loss{Kind: model.StoreLoss}
	before := fleet.State()

	wantPlan := []model.Step{{Kind: model.RestoreRegistry, Backup: 900}}
	if got := corerecovery.RecoveryPlan(loss, before); !reflect.DeepEqual(got, wantPlan) {
		t.Fatalf("RecoveryPlan (pre-execution) = %+v, want %+v", got, wantPlan)
	}

	executed, err := Execute(fleet, loss)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !reflect.DeepEqual(executed, wantPlan) {
		t.Fatalf("Execute plan = %+v, want %+v", executed, wantPlan)
	}

	assertConverged(t, fleet, before, loss, executed)

	if !fleet.restored || fleet.restoredFrom != 900 {
		t.Fatalf("registry restore = (restored=%v, from=%v), want (true, 900)", fleet.restored, fleet.restoredFrom)
	}
	// No region topology changed and nothing was rerouted: a StoreLoss never
	// emits ReHome or Reroute.
	if !reflect.DeepEqual(fleet.cells, cells) {
		t.Fatalf("fleet.cells changed on a StoreLoss: %+v, want unchanged %+v", fleet.cells, cells)
	}
	if len(fleet.rerouted) != 0 {
		t.Fatalf("fleet rerouted on a StoreLoss: %v", fleet.rerouted)
	}
}

// =============================================================================
// Chaos convergence property: across several injected fault scenarios, the
// simulated fleet ALWAYS converges to exactly what RecoveryPlan decided —
// reality == the pure oracle. This is the hermetic chaos proof #192 drives.
// =============================================================================

func TestChaos_FleetAlwaysConvergesToPurePlan(t *testing.T) {
	scenarios := []struct {
		name    string
		cells   map[model.CellID]model.RegionID
		regions []model.RegionID
		backups map[model.RegionID]model.Instant
		loss    model.Loss
	}{
		{
			name: "region loss, three regions, multiple cells per region",
			cells: map[model.CellID]model.RegionID{
				"cell-1": "us-east", "cell-2": "us-east",
				"cell-3": "us-west",
				"cell-4": "eu-central",
			},
			regions: []model.RegionID{"us-east", "us-west", "eu-central"},
			backups: map[model.RegionID]model.Instant{"us-east": 10, "us-west": 999, "eu-central": 500},
			loss:    model.Loss{Kind: model.RegionLoss, Region: "us-east"},
		},
		{
			name: "region loss on a different region converges to a different survivor",
			cells: map[model.CellID]model.RegionID{
				"cell-1": "us-east",
				"cell-2": "us-west", "cell-3": "us-west",
			},
			regions: []model.RegionID{"us-east", "us-west"},
			backups: map[model.RegionID]model.Instant{"us-east": 700, "us-west": 200},
			loss:    model.Loss{Kind: model.RegionLoss, Region: "us-west"},
		},
		{
			name:    "store loss, no region topology change",
			cells:   map[model.CellID]model.RegionID{"cell-1": "us-east"},
			regions: []model.RegionID{"us-east"},
			backups: map[model.RegionID]model.Instant{"us-east": 42},
			loss:    model.Loss{Kind: model.StoreLoss},
		},
		{
			name:    "region loss with no backups still re-homes and reroutes",
			cells:   map[model.CellID]model.RegionID{"cell-1": "us-east", "cell-2": "us-west"},
			regions: []model.RegionID{"us-east", "us-west"},
			backups: nil,
			loss:    model.Loss{Kind: model.RegionLoss, Region: "us-east"},
		},
		{
			name:    "region loss naming a region the fleet does not know: empty plan, no-op",
			cells:   map[model.CellID]model.RegionID{"cell-1": "us-east"},
			regions: []model.RegionID{"us-east"},
			backups: map[model.RegionID]model.Instant{"us-east": 1},
			loss:    model.Loss{Kind: model.RegionLoss, Region: "ap-south"},
		},
		{
			name:    "empty fleet: no-op regardless of loss kind",
			cells:   nil,
			regions: nil,
			backups: nil,
			loss:    model.Loss{Kind: model.StoreLoss},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			fleet := newSimulatedFleet(sc.cells, sc.regions, sc.backups)
			before := fleet.State()

			plan := corerecovery.RecoveryPlan(sc.loss, before)

			executed, err := Execute(fleet, sc.loss)
			if err != nil {
				t.Fatalf("Execute returned error: %v", err)
			}
			if !reflect.DeepEqual(executed, plan) {
				t.Fatalf("Execute plan = %+v, want the pure plan %+v", executed, plan)
			}

			assertConverged(t, fleet, before, sc.loss, plan)
		})
	}
}

// =============================================================================
// Execution stops on the first step error (acceptance: "stop + surface on
// the first step error").
// =============================================================================

func TestExecute_StopsAndSurfacesFirstStepError(t *testing.T) {
	tests := []struct {
		name       string
		failOn     string
		wantEvents []string // events expected before the failing step
	}{
		{name: "rehome fails: nothing runs after it", failOn: "rehome", wantEvents: nil},
		{name: "restore fails: rehome already ran, reroute never does", failOn: "restore", wantEvents: []string{"rehome"}},
		{name: "reroute fails: rehome and restore already ran", failOn: "reroute", wantEvents: []string{"rehome", "restore"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cells := map[model.CellID]model.RegionID{"cell-1": "us-east"}
			regions := []model.RegionID{"us-east", "us-west"}
			backups := map[model.RegionID]model.Instant{"us-east": 1, "us-west": 2}
			fleet := newSimulatedFleet(cells, regions, backups)
			fleet.failOn = tc.failOn

			loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}
			executed, err := Execute(fleet, loss)
			if err == nil {
				t.Fatalf("Execute returned nil error, want a fault from %q", tc.failOn)
			}
			// The returned plan is still the full pure plan RecoveryPlan
			// decided — the caller can always see what SHOULD have happened.
			wantPlan := corerecovery.RecoveryPlan(loss, model.FleetState{Cells: cells, Regions: regions, Backups: backups})
			if !reflect.DeepEqual(executed, wantPlan) {
				t.Fatalf("Execute plan = %+v, want the full pure plan %+v", executed, wantPlan)
			}

			got := eventKinds(fleet.events)
			if tc.wantEvents == nil {
				tc.wantEvents = []string{}
			}
			if got == nil {
				got = []string{}
			}
			if !reflect.DeepEqual(got, tc.wantEvents) {
				t.Fatalf("events before the fault = %v, want %v", got, tc.wantEvents)
			}
		})
	}
}

// =============================================================================
// RPO drill: RpoMet with a fake clock gates whether a restore is fresh
// enough — asserted at the shell boundary via Drill.
// =============================================================================

func TestDrill_RPOGateBoundary(t *testing.T) {
	cells := map[model.CellID]model.RegionID{"cell-1": "us-east", "cell-2": "us-west"}
	regions := []model.RegionID{"us-east", "us-west"}
	// Only one backup Instant in the map, so the plan's RestoreRegistry
	// step is pinned to exactly this value — makes the RPO boundary
	// unambiguous.
	const backupAt model.Instant = 1_000
	backups := map[model.RegionID]model.Instant{"us-east": backupAt}
	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}
	const rpo model.Duration = 100

	tests := []struct {
		name   string
		now    model.Instant
		wantOK bool
	}{
		{name: "well within rpo", now: backupAt + 10, wantOK: true},
		{name: "exactly at rpo boundary is met", now: backupAt + model.Instant(rpo), wantOK: true},
		{name: "one tick past rpo is not met", now: backupAt + model.Instant(rpo) + 1, wantOK: false},
		{name: "backup timestamped in the future is always met", now: backupAt - 500, wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fleet := newSimulatedFleet(cloneCells(cells), append([]model.RegionID(nil), regions...), cloneBackups(backups))
			result := Drill(fleet, loss, tc.now, rpo)
			if result.Err != nil {
				t.Fatalf("Drill returned error: %v", result.Err)
			}
			if result.RPOMet != tc.wantOK {
				t.Fatalf("RPOMet = %v, want %v (now=%v, backup=%v, rpo=%v)", result.RPOMet, tc.wantOK, tc.now, backupAt, rpo)
			}
			// The drill still executes and converges regardless of the RPO
			// verdict — RpoMet is an assert-at-the-boundary signal, not a
			// gate the shell adds planning logic around (out of scope here).
			if !fleet.restored || fleet.restoredFrom != backupAt {
				t.Fatalf("drill did not execute the restore step: restored=%v from=%v", fleet.restored, fleet.restoredFrom)
			}
		})
	}
}

func TestDrill_NoRestoreStepIsVacuouslyRPOMet(t *testing.T) {
	// A single-region fleet losing its only region: RecoveryPlan skips
	// ReHome (no survivor) and, with no backups, skips RestoreRegistry too —
	// only Reroute remains. RPOMet must be vacuously true: nothing depends
	// on backup freshness.
	fleet := newSimulatedFleet(
		map[model.CellID]model.RegionID{"cell-1": "us-east"},
		[]model.RegionID{"us-east"},
		nil,
	)
	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}

	result := Drill(fleet, loss, 0, 0)
	if result.Err != nil {
		t.Fatalf("Drill returned error: %v", result.Err)
	}
	if !result.RPOMet {
		t.Fatalf("RPOMet = false, want true (vacuous: no RestoreRegistry step in %+v)", result.Plan)
	}
	for _, s := range result.Plan {
		if s.Kind == model.RestoreRegistry {
			t.Fatalf("test setup invalid: plan unexpectedly has a RestoreRegistry step: %+v", result.Plan)
		}
	}
}

func TestDrill_ComputesPlanBeforeExecuting(t *testing.T) {
	// The plan Drill reports is the same value RecoveryPlan would compute
	// standalone against the pre-drill state — the "checked pure, then
	// carried out" ordering the DR-drill acceptance criterion names.
	cells := map[model.CellID]model.RegionID{"cell-1": "us-east"}
	regions := []model.RegionID{"us-east", "us-west"}
	backups := map[model.RegionID]model.Instant{"us-east": 5, "us-west": 5}
	fleet := newSimulatedFleet(cells, regions, backups)
	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}

	want := corerecovery.RecoveryPlan(loss, fleet.State())
	result := Drill(fleet, loss, 5, 10)
	if !reflect.DeepEqual(result.Plan, want) {
		t.Fatalf("Drill.Plan = %+v, want %+v", result.Plan, want)
	}
}

// =============================================================================
// Determinism (mirrors internal/core/mitosis's discipline at the shell
// level): identical starting fleets + the same loss produce the identical
// plan and effect sequence.
// =============================================================================

func TestExecute_Deterministic(t *testing.T) {
	build := func() *simulatedFleet {
		cells := map[model.CellID]model.RegionID{
			"cell-1": "us-east", "cell-2": "us-east", "cell-3": "us-west",
		}
		regions := []model.RegionID{"us-east", "us-west"}
		backups := map[model.RegionID]model.Instant{"us-east": 100, "us-west": 300}
		return newSimulatedFleet(cells, regions, backups)
	}
	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}

	f1, f2 := build(), build()
	p1, err1 := Execute(f1, loss)
	p2, err2 := Execute(f2, loss)
	if err1 != nil || err2 != nil {
		t.Fatalf("Execute errors: %v, %v", err1, err2)
	}
	if !reflect.DeepEqual(p1, p2) {
		t.Fatalf("Execute plans diverged: %+v vs %+v", p1, p2)
	}
	if !reflect.DeepEqual(f1.events, f2.events) {
		t.Fatalf("Execute effect sequences diverged: %v vs %v", f1.events, f2.events)
	}
	if !reflect.DeepEqual(cellSet(f1.cells), cellSet(f2.cells)) || !reflect.DeepEqual(f1.cells, f2.cells) {
		t.Fatalf("Execute converged fleets diverged: %+v vs %+v", f1.cells, f2.cells)
	}
}

func cloneCells(m map[model.CellID]model.RegionID) map[model.CellID]model.RegionID {
	out := make(map[model.CellID]model.RegionID, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneBackups(m map[model.RegionID]model.Instant) map[model.RegionID]model.Instant {
	out := make(map[model.RegionID]model.Instant, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
