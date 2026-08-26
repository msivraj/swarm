// Package placement is a pure core: it decides which cell a task lands on.
// It performs no I/O and reads no clock — it is a pure function of a task and
// a point-in-time snapshot of the fleet's cells. This package follows the
// shape set by internal/core/mitosis: take data, return a decision, never
// execute an effect.
package placement

import "github.com/msivraj/swarm/internal/model"

// Kind is the tag of a Placement's tagged union.
type Kind int

const (
	// NoCapacity means no cell in the snapshot has free capacity for the task.
	NoCapacity Kind = iota
	// Assign means the task is placed on Placement.Cell.
	Assign
)

// Placement is a placement decision the shell will execute. It is a tagged
// union: Assign{Cell} | NoCapacity. Cores return Placements; they never carry
// them out.
type Placement struct {
	Kind Kind
	Cell model.CellID // set when Kind == Assign
}

// Place picks a cell with free capacity for the task, or returns NoCapacity
// if none exists.
//
// P0 placement rule: any cell with free capacity is eligible; the doc leaves
// tie-breaking among multiple eligible cells unspecified. This implementation
// resolves that ambiguity with deterministic first-fit — the first cell in
// slice order with Free > 0 — the simplest stable rule, matching mitosis's
// convention of deciding on slice order rather than on cell identity or load.
// The task itself carries no bearing on P0 eligibility; it is accepted here
// only to fix the signature the shell will call for later phases that do
// consider it (e.g. affinity, task size).
func Place(_ model.Task, cells []model.CellView) Placement {
	for _, c := range cells {
		if c.Free > 0 {
			return Placement{Kind: Assign, Cell: c.ID}
		}
	}
	return Placement{Kind: NoCapacity}
}
