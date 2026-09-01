// Package upgrade is the imperative shell that executes a rolling upgrade
// against a fleet: it repeatedly calls the pure internal/core/upgrade for
// the next cell to cordon and performs the cordon/drain/roll/uncordon
// effects that decision licenses. See
// docs/phases/swarm-p4-components.txt §02 (ROLLING UPGRADE) and §03
// (zero-loss upgrade SLO), and the #157 design ruling (fork c): the fleet
// this package drives is a swarmd-shaped seam (Fleet below) — a real fleet
// in production, a FAKE in the hermetic gate. There is no real binary swap
// here; that is owner-infra, deferred to the 1M-node run.
//
// # No sequencing logic in this package
//
// Run adds no "which cell next" or "are we done" decision of its own beyond
// what the #165 audit ("Done is overloaded" / "skew-unsafe cells are
// skipped forever") requires the shell to make explicit — see the two
// caveats documented on Run and finish below. Every cell choice comes from
// coreupgrade.NextDrain; every skew judgment comes from coreupgrade.SkewSafe.
package upgrade

import (
	"fmt"
	"sort"

	coreupgrade "github.com/msivraj/swarm/internal/core/upgrade"
	"github.com/msivraj/swarm/internal/model"
)

// Fleet is the shell's seam onto the fleet a rolling upgrade executes
// against. A production implementation drives real cordon/drain/roll RPCs
// against swarmd; the hermetic gate drives a fake (kept test-local — see
// upgrade_test.go) that never touches a real node or binary.
type Fleet interface {
	// State returns the fleet's current snapshot — the exact input
	// NextDrain reasons over. Run calls this once per loop iteration, so a
	// prior Roll always feeds forward into the next decision.
	State() model.FleetState
	// Cordon marks cell cordoned: no new work is scheduled onto it.
	Cordon(cell model.CellID) error
	// Drain lets cell's currently running jobs checkpoint-migrate or drain
	// in place. Zero-loss means no job in State().Jobs[cell] is dropped by
	// this call — a real implementation blocks until that holds.
	Drain(cell model.CellID) error
	// Roll bumps cell's running binary version to target.
	Roll(cell model.CellID, target model.Version) error
	// Uncordon clears cell's cordon, freeing up its jobs' peers (and any
	// cell sharing a job with it) to be chosen next.
	Uncordon(cell model.CellID) error
}

// Outcome is Run's terminal result.
type Outcome int

const (
	// Blocked is the zero value: Run never silently reports success. A
	// Blocked outcome means the loop stopped — NextDrain returned Done —
	// while Unfinished still names cells not at plan.Target. This is the
	// #165 audit's caveat 2 made explicit: a cell whose current version is
	// not SkewSafe with plan.Target is never cordoned by the core, so
	// Done does not by itself mean the upgrade succeeded.
	Blocked Outcome = iota
	// Complete means every cell in the fleet's final State() is at
	// plan.Target.
	Complete
)

// Result is what Run returns: the terminal Outcome plus, for Blocked, which
// cells never reached plan.Target.
type Result struct {
	Outcome Outcome
	// Unfinished lists, in ascending CellID order, every cell not at
	// plan.Target when the loop stopped. Empty when Outcome == Complete.
	Unfinished []model.CellID
}

// Run executes plan against fleet. It loops:
//
//  1. read fleet.State();
//  2. ask the pure core for the next step (coreupgrade.NextDrain);
//  3. if Done, stop and decide the terminal Result (see finish);
//  4. otherwise refuse the step unless coreupgrade.SkewSafe holds for the
//     chosen cell's current version against plan.Target — a defensive
//     check: NextDrain already documents that it never returns an unsafe
//     Cordon, so this should be unreachable, but Run refuses rather than
//     ever rolls an incompatible version if it somehow is reached;
//  5. cordon, drain, roll, and uncordon the chosen cell through Fleet, then
//     loop.
//
// Run performs no sequencing decision of its own: every "which cell next"
// answer comes from NextDrain, and it is invoked with a fresh State() on
// every iteration so a completed Roll is always visible to the next call —
// this is what lets a single-cell-at-a-time loop make progress on cells
// that were job-conflicted with an in-flight cordon a moment before.
//
// Completion (the #165 caveat-1 fix): NextDrain's Done is overloaded — it
// means EITHER "every cell is at target" OR "nothing is drainable right
// now" (e.g. every remaining candidate is skew-unsafe, or job-conflicted
// with a cordon a failed Fleet effect left in flight). Run therefore never
// treats Done alone as success; see finish.
func Run(fleet Fleet, plan model.UpgradePlan) (Result, error) {
	for {
		state := fleet.State()
		step := coreupgrade.NextDrain(state, plan)
		if step.Kind == model.Done {
			return finish(state, plan), nil
		}

		version, ok := state.Versions[step.Cell]
		if !ok {
			return Result{}, fmt.Errorf("upgrade: NextDrain chose cell %q not present in fleet state", step.Cell)
		}
		if !coreupgrade.SkewSafe(version, plan.Target) {
			return Result{}, fmt.Errorf("upgrade: refusing skew-unsafe step for cell %q (%+v -> %+v)", step.Cell, version, plan.Target)
		}

		if err := roll(fleet, step.Cell, plan.Target); err != nil {
			return Result{}, err
		}
	}
}

// roll performs the cordon/drain/roll/uncordon effect sequence for cell,
// stopping at (and reporting) the first effect that fails.
func roll(fleet Fleet, cell model.CellID, target model.Version) error {
	if err := fleet.Cordon(cell); err != nil {
		return fmt.Errorf("upgrade: cordon %q: %w", cell, err)
	}
	if err := fleet.Drain(cell); err != nil {
		return fmt.Errorf("upgrade: drain %q: %w", cell, err)
	}
	if err := fleet.Roll(cell, target); err != nil {
		return fmt.Errorf("upgrade: roll %q: %w", cell, err)
	}
	if err := fleet.Uncordon(cell); err != nil {
		return fmt.Errorf("upgrade: uncordon %q: %w", cell, err)
	}
	return nil
}

// finish decides Run's terminal Result once NextDrain has returned Done
// against state: Complete only when every cell state.Versions names is at
// plan.Target. Otherwise Blocked, naming every cell that is not — whether
// it is permanently skew-unsafe (the core will never cordon it, #165
// caveat 2) or was left cordoned by a Fleet effect that never completed
// (#165 caveat 1) — either way, Done is never silently read as success.
func finish(state model.FleetState, plan model.UpgradePlan) Result {
	var unfinished []model.CellID
	for cell, version := range state.Versions {
		if version != plan.Target {
			unfinished = append(unfinished, cell)
		}
	}
	if len(unfinished) == 0 {
		return Result{Outcome: Complete}
	}
	sort.Slice(unfinished, func(i, j int) bool { return unfinished[i] < unfinished[j] })
	return Result{Outcome: Blocked, Unfinished: unfinished}
}
