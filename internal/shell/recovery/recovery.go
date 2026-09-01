// Package recovery is the imperative shell that carries out a disaster
// recovery: it calls the pure internal/core/recovery for the ordered plan a
// Loss licenses, then executes each Step against a Fleet seam (re-home
// agents [P1 failover semantics], restore the FDB registry, redirect the
// global router). It also hosts the DR-drill entry point and the
// chaos-convergence harness: because the plan is pure, "reality vs. the
// core's decision" is a free oracle — no real multi-region infra is touched
// here (that is owner-infra), the harness drives an in-process simulated
// Fleet. See docs/phases/swarm-p5-components.txt §02 (DISASTER RECOVERY) and
// §03, and the #183 design ruling (fork c).
//
// # No planning logic in this package
//
// Every Step this package carries out comes from coreecovery.RecoveryPlan;
// Execute only dispatches each Step.Kind to the matching Fleet effect, in
// the order the core returned. Nothing here decides which region survives,
// which backup is latest, or whether a loss matches the fleet — that is all
// the pure core's job.
package recovery

import (
	"fmt"

	corerecovery "github.com/msivraj/swarm/internal/core/recovery"
	"github.com/msivraj/swarm/internal/model"
)

// Fleet is the shell's seam onto the fleet a recovery plan executes against.
// A production implementation composes the existing store/FDB registry seam
// (restore), P1 failover (re-home), and the global router (reroute); the
// hermetic gate drives an in-process simulated fleet (kept test-local — see
// recovery_test.go) that touches no real region, store, or network. This is
// the same seam shape as internal/shell/upgrade.Fleet.
type Fleet interface {
	// State returns the fleet's current snapshot — the exact input
	// RecoveryPlan reasons over. Execute/Drill call this once, so the plan
	// they carry out is always computed from a single consistent snapshot.
	State() model.FleetState
	// ReHomeAgents re-homes every agent from the lost region onto the
	// surviving region to (P1 failover semantics).
	ReHomeAgents(from, to model.RegionID) error
	// RestoreRegistry restores the FDB registry from the backup taken at
	// backup.
	RestoreRegistry(backup model.Instant) error
	// Reroute redirects the global router's traffic away from away.
	Reroute(away model.RegionID) error
}

// Plan returns the pure recovery.RecoveryPlan for loss over fleet's current
// State — exported so a DR drill (or any caller) can compute and assert "us-
// east lost => these exact steps" BEFORE anything executes, without
// executing.
func Plan(fleet Fleet, loss model.Loss) []model.Step {
	return corerecovery.RecoveryPlan(loss, fleet.State())
}

// Execute computes Plan(fleet, loss) and carries out each returned Step, in
// order, against fleet. It stops at — and returns — the first step that
// errors, so a partially executed plan is always surfaced rather than
// silently continued or retried out of order. The returned plan is always
// the full plan RecoveryPlan decided, whether or not execution reached the
// end of it; the error (if any) names which step failed.
func Execute(fleet Fleet, loss model.Loss) ([]model.Step, error) {
	plan := Plan(fleet, loss)
	return plan, run(fleet, loss.Region, plan)
}

// run carries out plan's steps in order against fleet, stopping at the first
// error. lost is the loss's Region — the "from" side of a ReHome step; the
// "to" side is the step's own Region field (the surviving region
// RecoveryPlan chose).
func run(fleet Fleet, lost model.RegionID, plan []model.Step) error {
	for i, step := range plan {
		if err := apply(fleet, lost, step); err != nil {
			return fmt.Errorf("recovery: step %d (%v) failed: %w", i, step.Kind, err)
		}
	}
	return nil
}

// apply dispatches one Step to its matching Fleet effect. It adds no
// decision of its own beyond the Kind switch the sum type requires.
func apply(fleet Fleet, lost model.RegionID, step model.Step) error {
	switch step.Kind {
	case model.ReHome:
		return fleet.ReHomeAgents(lost, step.Region)
	case model.RestoreRegistry:
		return fleet.RestoreRegistry(step.Backup)
	case model.Reroute:
		return fleet.Reroute(step.Traffic)
	default:
		return fmt.Errorf("recovery: step kind %v has no fleet effect", step.Kind)
	}
}

// DrillResult is what a DR drill reports.
type DrillResult struct {
	// Plan is the exact ordered steps recovery.RecoveryPlan decided for the
	// drill's loss — computed, and available to assert against, before
	// Execute carries any of it out.
	Plan []model.Step
	// RPOMet reports whether the backup any RestoreRegistry step in Plan
	// would restore from is fresh enough as of Now (recovery.RpoMet, clock
	// passed in as data). A plan with no RestoreRegistry step has no backup
	// dependency, so RPOMet is vacuously true.
	RPOMet bool
	// Err is the first step-execution error Execute hit, if any.
	Err error
}

// Drill runs a disaster-recovery drill against fleet, on demand: it computes
// the pure plan for loss (Plan — checked BEFORE anything executes), decides
// the RPO gate for that plan's backup as of now (clock supplied by the
// caller, never read by this package), and then executes the plan via
// Execute. No real infra is touched; fleet is the simulated seam a chaos
// harness or a scheduled caller drives.
func Drill(fleet Fleet, loss model.Loss, now model.Instant, rpo model.Duration) DrillResult {
	plan := Plan(fleet, loss)
	result := DrillResult{
		Plan:   plan,
		RPOMet: rpoMetForPlan(plan, now, rpo),
	}
	result.Err = run(fleet, loss.Region, plan)
	return result
}

// rpoMetForPlan reports whether the backup any RestoreRegistry step in plan
// would restore from meets rpo as of now, via the pure core's RpoMet. A plan
// with no RestoreRegistry step (nothing to restore) has no RPO to fail.
func rpoMetForPlan(plan []model.Step, now model.Instant, rpo model.Duration) bool {
	for _, step := range plan {
		if step.Kind == model.RestoreRegistry {
			return corerecovery.RpoMet(step.Backup, now, rpo)
		}
	}
	return true
}
