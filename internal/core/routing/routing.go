// Package routing is a pure core: the global routing layer that routes a job
// to a region and folds regional summaries into one eventually-consistent
// view. It performs no I/O and reads no clock — the shell injects `now` as a
// model.Instant. It follows the shape set by internal/core/mitosis: take
// data, return a value or a description of a decision, never execute an
// effect.
package routing

import (
	"sort"

	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

// RouteKind is the tag of a Route's tagged union.
type RouteKind int

const (
	// NoRegion means no region can take the job.
	NoRegion RouteKind = iota
	// To means the whole job goes to Route.Region.
	To
	// Spread means the job's tasks are spread across Route.Regions.
	Spread
)

// Route is a tagged union: To{RegionID} | Spread{[]RegionID} | NoRegion.
type Route struct {
	Kind    RouteKind
	Region  model.RegionID   // set when Kind == To
	Regions []model.RegionID // set when Kind == Spread (deterministic order)
}

// RegionalSummary is what one region publishes upward: its capacity, health,
// and cell count, stamped with the region id and the Instant it was produced.
type RegionalSummary struct {
	Region model.RegionID
	Free   int
	Cells  int
	Health model.Health
	At     model.Instant // when the region produced this summary
}

// GlobalView is the merged, eventually-consistent view of all regions: a
// last-writer-wins register keyed by RegionID, kept as copy-on-write data —
// a value, never a live handle. This shape (rather than e.g. a slice) is
// what makes mergeGlobal commutative, associative, and idempotent: folding
// summaries into a map keyed by RegionID, keeping the "winning" summary per
// key by a fixed total order, is a max-reduction, and max-reductions have
// exactly those algebraic properties regardless of fold order (phase doc
// §02). GlobalView's shape is left unspecified by the doc; this is the
// builder's resolution (issue #35 notes).
type GlobalView struct {
	summaries map[model.RegionID]RegionalSummary
}

// StalenessWindow is the age past which a region's last summary is
// considered diverged (stale) by Diverged. Not pinned by the phase doc;
// chosen here as 30s of injected Instant nanoseconds — long enough to
// absorb a missed gossip round, short enough to catch a genuinely stuck
// region. Flagged for the auditor (issue #35 notes).
const StalenessWindow model.Instant = 30_000_000_000 // 30s, in Instant's ns unit

// route picks a region (or a spread) for a job from the current region
// views. It is unexported (not Route) because the exported type Route
// already claims that identifier — the issue's parenthetical suggestion to
// export all four functions verbatim collides with the Route type name, so
// this one function keeps its lowercase, doc-verbatim name and is exercised
// by an in-package test file instead.
//
// Eligibility and the To/Spread/NoRegion rule (issue #35 notes — JobSpec
// carries no size, so "has capacity" is resolved as Free > 0):
//   - a region is eligible only when Health == model.Healthy and Free > 0;
//     Degraded/Unreachable regions are never routed to directly.
//   - zero eligible regions -> NoRegion.
//   - exactly one eligible region -> To{that region}.
//   - multiple eligible regions and job.Coupling == model.Independent ->
//     Spread{all eligible regions, deterministic RegionID order}: independent
//     tasks may fan out, so use the healthy set jointly rather than picking
//     just one.
//   - multiple eligible regions and any other Coupling (tight jobs never
//     spread in P1, phase doc §03/§05) -> To{the deterministic pick}: the
//     eligible region with the most Free capacity, ties broken by the
//     smaller RegionID.
func route(job model.JobSpec, regions []model.RegionView) Route {
	eligible := eligibleRegions(regions)

	switch {
	case len(eligible) == 0:
		return Route{Kind: NoRegion}
	case len(eligible) == 1:
		return Route{Kind: To, Region: eligible[0].ID}
	case job.Coupling == model.Independent:
		return Route{Kind: Spread, Regions: sortedRegionIDs(eligible)}
	default:
		return Route{Kind: To, Region: pickBest(eligible)}
	}
}

// eligibleRegions filters regions to those route may send a job to.
func eligibleRegions(regions []model.RegionView) []model.RegionView {
	var out []model.RegionView
	for _, r := range regions {
		if r.Health == model.Healthy && r.Free > 0 {
			out = append(out, r)
		}
	}
	return out
}

// pickBest deterministically selects one region from eligible: the region
// with the most Free capacity, ties broken by the smaller RegionID. This
// comparator is a strict total order over (Free, RegionID) pairs, so the
// result does not depend on the input slice's order.
func pickBest(eligible []model.RegionView) model.RegionID {
	best := eligible[0]
	for _, r := range eligible[1:] {
		if r.Free > best.Free || (r.Free == best.Free && r.ID < best.ID) {
			best = r
		}
	}
	return best.ID
}

// sortedRegionIDs extracts eligible's RegionIDs in ascending order, so
// Route.Regions is stable regardless of the input order.
func sortedRegionIDs(eligible []model.RegionView) []model.RegionID {
	ids := make([]model.RegionID, len(eligible))
	for i, r := range eligible {
		ids[i] = r.ID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// MergeGlobal folds one region's summary into the global view. It is
// commutative, associative, and idempotent (phase doc §02): per RegionID it
// keeps the summary that wins under summaryWins, a fixed total order, so the
// result of folding any multiset of summaries into a GlobalView never
// depends on the order or number of times each summary is applied.
func MergeGlobal(v GlobalView, s RegionalSummary) GlobalView {
	cur, ok := v.summaries[s.Region]
	if ok && !summaryWins(s, cur) {
		return v
	}
	out := cloneSummaries(v.summaries)
	out[s.Region] = s
	return GlobalView{summaries: out}
}

// summaryWins reports whether incoming should replace cur as the winning
// summary for their shared RegionID. The comparator chain (larger At, then
// larger Free, then larger Cells, then better Health, then keep cur) is a
// fixed total order, which is what makes the MergeGlobal fold order- and
// duplicate-independent (issue #35 notes).
func summaryWins(incoming, cur RegionalSummary) bool {
	if incoming.At != cur.At {
		return incoming.At > cur.At
	}
	if incoming.Free != cur.Free {
		return incoming.Free > cur.Free
	}
	if incoming.Cells != cur.Cells {
		return incoming.Cells > cur.Cells
	}
	if incoming.Health != cur.Health {
		return incoming.Health < cur.Health // Healthy(0) beats Degraded(1) beats Unreachable(2)
	}
	return false
}

// cloneSummaries copies v's summary map so MergeGlobal never mutates a
// GlobalView a caller is still holding — copy-on-write, same discipline as
// registry.Registry.
func cloneSummaries(m map[model.RegionID]RegionalSummary) map[model.RegionID]RegionalSummary {
	out := make(map[model.RegionID]RegionalSummary, len(m)+1)
	for k, val := range m {
		out[k] = val
	}
	return out
}

// Diverged reports the regions whose last known summary is stale as of now:
// older than StalenessWindow. The result is sorted by RegionID so it is
// stable regardless of the GlobalView's internal map iteration order. A
// region exactly StalenessWindow old is still considered fresh (the bound
// is exclusive).
func Diverged(v GlobalView, now model.Instant) []model.RegionID {
	var ids []model.RegionID
	for id, s := range v.summaries {
		if now-s.At > StalenessWindow {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Summarize projects a region's registry into the summary it publishes
// upward: aggregate free capacity and cell count. Region and At are left at
// their zero values — the signature (copied verbatim from the phase doc)
// takes no RegionID or Instant, so the shell stamps both when it publishes
// the summary. Health is set to model.Healthy: Registry carries no health
// notion, so the global layer downgrades a region's health itself, via
// Diverged against the summary's shell-stamped At (issue #35 notes).
func Summarize(reg registry.Registry) RegionalSummary {
	views := registry.Snapshot(reg)
	var free int
	for _, v := range views {
		free += v.Free
	}
	return RegionalSummary{
		Free:   free,
		Cells:  len(views),
		Health: model.Healthy,
	}
}
