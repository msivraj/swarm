// Package recovery is a pure core: given a loss event and a fleet snapshot,
// it decides the disaster-recovery plan — it performs no I/O and reads no
// clock. Executing the plan (re-homing agents, restoring the FDB registry,
// redirecting the global router) and running DR drills belong to the shell
// (#189). Because the plan is pure, a drill can assert "us-east lost => these
// exact steps" before anything executes. See
// docs/phases/swarm-p5-components.txt §02 (DISASTER RECOVERY).
package recovery

import (
	"sort"

	"github.com/msivraj/swarm/internal/model"
)

// RecoveryPlan returns the deterministic, ordered recovery steps for loss
// over fleet.
//
// RegionLoss{region}: if region is not present in fleet.Regions, the loss
// does not match anything the fleet knows about and RecoveryPlan returns an
// empty plan (never a partial or unsafe step). Otherwise it emits, in this
// fixed order:
//  1. ReHome{Region: surviving} — surviving is the lowest-sorted RegionID in
//     fleet.Regions that is not the lost region (agents re-home there).
//     Skipped if no other region exists to re-home onto.
//  2. RestoreRegistry{Backup: latest} — latest is the newest Instant across
//     fleet.Backups (the FDB registry is one logical store; the freshest
//     backup from any region is the one to restore from — the max is
//     order-independent, so this stays deterministic regardless of map
//     iteration order). Skipped if fleet.Backups is empty.
//  3. Reroute{Traffic: region} — steer the global router away from the lost
//     region. Always emitted once the loss has matched a known region.
//
// StoreLoss: emits [RestoreRegistry{latest}] — latest as above — or an empty
// plan if fleet.Backups is empty (there is nothing to restore from).
//
// Any other/unrecognized LossKind yields an empty plan.
//
// Region and backup selection never range over a Go map for anything but a
// commutative reduction (max), so identical (loss, fleet) always yields an
// identical plan regardless of how the fleet's maps were built.
func RecoveryPlan(loss model.Loss, fleet model.FleetState) []model.Step {
	switch loss.Kind {
	case model.RegionLoss:
		return regionLossPlan(loss.Region, fleet)
	case model.StoreLoss:
		return storeLossPlan(fleet)
	default:
		return nil
	}
}

func regionLossPlan(lost model.RegionID, fleet model.FleetState) []model.Step {
	if !contains(fleet.Regions, lost) {
		return nil
	}

	var steps []model.Step

	if surviving, ok := survivingRegion(fleet.Regions, lost); ok {
		steps = append(steps, model.Step{Kind: model.ReHome, Region: surviving})
	}
	if backup, ok := latestBackup(fleet.Backups); ok {
		steps = append(steps, model.Step{Kind: model.RestoreRegistry, Backup: backup})
	}
	steps = append(steps, model.Step{Kind: model.Reroute, Traffic: lost})

	return steps
}

func storeLossPlan(fleet model.FleetState) []model.Step {
	backup, ok := latestBackup(fleet.Backups)
	if !ok {
		return nil
	}
	return []model.Step{{Kind: model.RestoreRegistry, Backup: backup}}
}

// contains reports whether regions holds id.
func contains(regions []model.RegionID, id model.RegionID) bool {
	for _, r := range regions {
		if r == id {
			return true
		}
	}
	return false
}

// survivingRegion returns the lowest-sorted RegionID in regions that is not
// lost, and whether one exists. regions is copied before sorting so the
// caller's slice is never mutated.
func survivingRegion(regions []model.RegionID, lost model.RegionID) (model.RegionID, bool) {
	sorted := make([]model.RegionID, len(regions))
	copy(sorted, regions)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for _, r := range sorted {
		if r != lost {
			return r, true
		}
	}
	return "", false
}

// latestBackup returns the newest Instant in backups and whether the map has
// any entries. The result is the max of the map's values, a commutative
// reduction — the map's iteration/construction order never affects it.
func latestBackup(backups map[model.RegionID]model.Instant) (model.Instant, bool) {
	var latest model.Instant
	found := false
	for _, at := range backups {
		if !found || at > latest {
			latest = at
			found = true
		}
	}
	return latest, found
}

// RpoMet reports whether the recovery point objective is still met: the
// elapsed time since lastBackup does not exceed rpo. The clock is passed in
// as data (now) — this function never reads it itself.
//
// Boundary: exactly now-lastBackup == rpo is MET (<=); one tick past is not.
// A backup timestamped in the future (lastBackup > now) yields a negative
// elapsed duration, which is always <= a nonnegative rpo, so it is treated as
// met.
func RpoMet(lastBackup, now model.Instant, rpo model.Duration) bool {
	elapsed := model.Duration(now - lastBackup)
	return elapsed <= rpo
}
