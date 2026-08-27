// Package region is a pure core: it decides which region an agent should
// dial next when its home region's Failover command fires. It performs no
// I/O and reads no clock — the shell supplies the known-region list, the
// health map, and the attempt count as data. P0's dial->enroll->join->
// heartbeat state machine (internal/core/agentreg) is unchanged and reused
// verbatim; this package only picks the dial target.
package region

import "github.com/msivraj/swarm/internal/model"

// SelectRegion chooses which region an agent should dial next: prefer home,
// then the nearest healthy region.
//
// The phase doc (§03) gives the signature but leaves "nearest" and the
// degenerate cases undefined; the contract below is the resolution recorded
// on issue #37:
//
//   - known[0] is the agent's home region; known[1:] are peer regions in
//     nearest-first order. The core has no independent distance metric —
//     slice position IS the nearness ranking, supplied by the shell.
//   - A region is a viable candidate when it is Healthy or Degraded (reachable,
//     even if stale) but never Unreachable. A region with no entry in health
//     is treated as unreachable (fail closed) — the shell is expected to
//     carry an entry for every region in known.
//   - homeFirst == true keeps home as the first candidate when home is
//     reachable; homeFirst == false excludes home entirely, even if it is
//     otherwise reachable, so the walk starts at the nearest healthy peer.
//   - attempt walks the ranked candidate list cyclically: attempt 0 is the
//     first candidate, attempt 1 the next, wrapping with %, so repeated
//     failover keeps retrying the ranked list.
//   - An empty known, or no reachable region, returns the zero RegionID
//     (""); the shell reads "" as "no target yet, keep retrying home."
func SelectRegion(known []model.RegionID, health map[model.RegionID]model.Health, homeFirst bool, attempt int) model.RegionID {
	if len(known) == 0 {
		return ""
	}

	var candidates []model.RegionID
	for i, r := range known {
		if i == 0 && !homeFirst {
			continue // homeFirst==false: never offer home as a candidate
		}
		if reachable(health, r) {
			candidates = append(candidates, r)
		}
	}

	if len(candidates) == 0 {
		return ""
	}
	idx := ((attempt % len(candidates)) + len(candidates)) % len(candidates)
	return candidates[idx]
}

// reachable reports whether a region is a viable dial target: Healthy or
// Degraded, but never Unreachable. A region absent from health fails closed.
func reachable(health map[model.RegionID]model.Health, r model.RegionID) bool {
	h, ok := health[r]
	if !ok {
		return false
	}
	return h != model.Unreachable
}
