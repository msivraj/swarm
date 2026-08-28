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
	// Spill means the task is forwarded to peer region Placement.Region,
	// because the local region was full and a peer had capacity. Added in
	// P1 (issue #36); NoCapacity and Assign keep their P0 values.
	Spill
)

// Placement is a placement decision the shell will execute. It is a tagged
// union: Assign{Cell} | Spill{Region} | NoCapacity. Cores return Placements;
// they never carry them out.
type Placement struct {
	Kind   Kind
	Cell   model.CellID   // set when Kind == Assign
	Region model.RegionID // set when Kind == Spill
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

// PlaceAcross extends Place with cross-region spill (P1, issue #36): it
// prefers a local cell — reusing Place's first-fit scan so the local-branch
// rule never diverges from P0's Place — and only when the local region is
// full does it consider spilling to a peer region. Locality is preferred
// over spilling, and only independent tasks reach this function at all: the
// shell is responsible for calling PlaceAcross only for independent jobs
// (Task carries no Coupling — that lives on JobSpec, out of a task's own
// data); this core has no coupling check to make.
//
// Peer selection rule (left unspecified by the phase doc, resolved here):
// among peers, choose the first in slice order with Free > 0 AND
// Health == model.Healthy — deterministic first-fit, mirroring Place's own
// tie-break convention (decide on slice order, not on region identity or
// load). A Degraded or Unreachable peer is never chosen, even if it reports
// free capacity, since its summary may be stale. If local has room, or no
// peer qualifies, PlaceAcross returns Place's own result (Assign or
// NoCapacity) — it never invents a decision Place would not have made for
// the local-only case.
func PlaceAcross(t model.Task, local []model.CellView, peers []model.RegionView) Placement {
	if p := Place(t, local); p.Kind == Assign {
		return p
	}
	for _, r := range peers {
		if r.Free > 0 && r.Health == model.Healthy {
			return Placement{Kind: Spill, Region: r.ID}
		}
	}
	return Placement{Kind: NoCapacity}
}

// Satisfies reports whether an offered CapSet covers every capability in a
// required CapSet — required ⊆ offered. A nil or empty required CapSet is
// always satisfied, regardless of offered (including a nil/empty offered).
//
// CapSet's documented contract (issue #58) is that callers pass it sorted
// and de-duplicated, but Satisfies does not trust that contract on its
// input: order and duplicates in either offered or required must not affect
// the result, so this builds a local set (map) from offered and checks each
// tag in required against it. That keeps the result correct — and
// independent of slice order or duplicates — regardless of whether a caller
// upheld the contract, at the cost of an O(n) allocation per call;
// placement-time cardinalities (a handful of capability tags per cell) make
// that cost irrelevant.
func Satisfies(offered, required model.CapSet) bool {
	if len(required) == 0 {
		return true
	}
	have := make(map[string]struct{}, len(offered))
	for _, tag := range offered {
		have[tag] = struct{}{}
	}
	for _, tag := range required {
		if _, ok := have[tag]; !ok {
			return false
		}
	}
	return true
}

// PlaceCapable is Place restricted to cells whose Caps satisfy t.Requires.
// It resolves the ambiguity the ticket leaves open by pre-filtering cells on
// Satisfies(c.Caps, t.Requires) and then delegating to Place unchanged: this
// keeps the first-fit tie-break identical to Place's own (decide on slice
// order of the filtered cells, not on cell identity or load), and — since
// Satisfies always returns true for an empty/nil t.Requires — makes
// PlaceCapable behave byte-identically to Place for every capless task, the
// regression the ticket requires, without duplicating Place's scan logic.
func PlaceCapable(t model.Task, cells []model.CellView) Placement {
	capable := make([]model.CellView, 0, len(cells))
	for _, c := range cells {
		if Satisfies(c.Caps, t.Requires) {
			capable = append(capable, c)
		}
	}
	return Place(t, capable)
}
