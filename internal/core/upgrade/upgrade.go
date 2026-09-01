// Package upgrade is a pure core: it sequences a rolling upgrade one cell at
// a time with zero job loss, across compatible versions only. It performs no
// I/O and reads no clock — like internal/core/mitosis, it takes plain data
// (a fleet snapshot, an upgrade plan) and returns a description of the next
// effect (a drain step) for the shell to carry out. Cordoning a cell,
// letting its jobs checkpoint-migrate or drain, rolling the binary, and
// uncordoning are all shell concerns (see
// docs/phases/swarm-p4-components.txt §02 ROLLING UPGRADE).
package upgrade

import (
	"sort"

	"github.com/msivraj/swarm/internal/model"
)

// skewWindow is the maximum allowed |Minor| version distance between two
// wire-compatible versions within the same Major, per the P4 zero-loss
// upgrade SLO's "tolerated version skew" (docs/phases/swarm-p4-components.txt
// §03): a rolling upgrade lets adjacent minor releases coexist in the fleet,
// but never crosses a Major line, and never drifts more than one minor
// release apart.
const skewWindow = 1

// SkewSafe reports whether a and b are wire-compatible enough to run side by
// side during a rollout: the same Major version, and at most skewWindow
// minor releases apart. It is reflexive (SkewSafe(v, v) is always true,
// since the distance to itself is 0) and symmetric (SkewSafe(a, b) ==
// SkewSafe(b, a), since Major equality and the absolute Minor distance are
// both symmetric) by construction.
func SkewSafe(a, b model.Version) bool {
	if a.Major != b.Major {
		return false
	}
	diff := a.Minor - b.Minor
	if diff < 0 {
		diff = -diff
	}
	return diff <= skewWindow
}

// NextDrain returns the next cell to cordon toward plan.Target, or Done when
// no cell can be safely drained right now — either because every cell is
// already at Target, or because the remaining candidates are all cordoned,
// skew-unsafe, or job-conflicted (see the selection rule below).
//
// Selection rule (deterministic — mirrors internal/core/mitosis's
// take-data-return-commands shape):
//  1. Candidates are scanned in plan.Order, if it is non-empty; otherwise in
//     ascending CellID order over fleet.Versions' keys. This is a stable,
//     explicit sort — NEVER Go's randomized map-iteration order — so the
//     same (fleet, plan) always yields the same DrainStep.
//  2. The first candidate in that sequence that is:
//     (a) not already in fleet.Cordoned,
//     (b) not already at plan.Target,
//     (c) SkewSafe from its current version to plan.Target, and
//     (d) shares no running JobID with any cell already cordoned/draining
//     ("never drain two cells of the same job at once" — the zero-loss
//     invariant, §03),
//     is returned as Cordon{cell}.
//  3. If no candidate satisfies all four, NextDrain returns Done. A
//     skew-unsafe or job-conflicted candidate is skipped, not returned as an
//     unsafe Cordon — the shell never receives a step it must refuse.
func NextDrain(fleet model.FleetState, plan model.UpgradePlan) model.DrainStep {
	busy := draining(fleet)

	for _, cell := range candidateOrder(fleet, plan) {
		version, ok := fleet.Versions[cell]
		if !ok {
			continue // unknown to the fleet snapshot: nothing to drain
		}
		if fleet.Cordoned[cell] {
			continue
		}
		if version == plan.Target {
			continue
		}
		if !SkewSafe(version, plan.Target) {
			continue
		}
		if jobsOverlap(fleet.Jobs[cell], busy) {
			continue
		}
		return model.DrainStep{Kind: model.Cordon, Cell: cell}
	}
	return model.DrainStep{Kind: model.Done}
}

// candidateOrder returns the cells NextDrain scans, in the deterministic
// order it scans them: plan.Order verbatim when non-empty, else
// fleet.Versions' keys sorted ascending by CellID.
func candidateOrder(fleet model.FleetState, plan model.UpgradePlan) []model.CellID {
	if len(plan.Order) > 0 {
		return plan.Order
	}
	ids := make([]model.CellID, 0, len(fleet.Versions))
	for id := range fleet.Versions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// draining collects every JobID running on a cell already cordoned/draining
// — the set a new candidate's own jobs must not intersect. It is built by
// accumulating into a set, so the order fleet.Cordoned is walked in never
// affects the result: this stays deterministic even though the walk itself
// uses Go's randomized map iteration.
func draining(fleet model.FleetState) map[model.JobID]struct{} {
	busy := make(map[model.JobID]struct{})
	for cell, cordoned := range fleet.Cordoned {
		if !cordoned {
			continue
		}
		for _, job := range fleet.Jobs[cell] {
			busy[job] = struct{}{}
		}
	}
	return busy
}

// jobsOverlap reports whether any of jobs is in busy.
func jobsOverlap(jobs []model.JobID, busy map[model.JobID]struct{}) bool {
	for _, job := range jobs {
		if _, ok := busy[job]; ok {
			return true
		}
	}
	return false
}
