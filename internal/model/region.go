package model

// RegionID uniquely identifies a region within the multi-region fleet. The
// regional analog of CellID.
type RegionID string

// Health classifies a region's reachability, as observed by the global layer
// and carried in a RegionView. The global layer downgrades a region to
// Unreachable when it goes stale (see routing.diverged).
type Health int

const (
	// Healthy means the region is reachable and its summary is fresh.
	Healthy Health = iota
	// Degraded means the region is reachable but its summary is stale or
	// partial.
	Degraded
	// Unreachable means the region is not reachable, or its summary is too
	// old to trust.
	Unreachable
)

// RegionView is a point-in-time snapshot of one region that pure cores
// reason over — the regional analog of CellView. A value, never a live
// handle.
//
// Free and Cells mirror CellView.Free/Size at region granularity, so route
// and placeAcross can reason about "a peer has capacity" without importing
// registry internals. This shape resolves an ambiguity left open by the
// phase doc (see issue #34): the doc specifies the P1 core signatures but
// not RegionView's fields, so this struct is derived from the doc's stated
// regional summary content (capacity, health, cell count).
type RegionView struct {
	ID     RegionID
	Free   int    // aggregate free capacity across the region's cells
	Cells  int    // number of cells in the region
	Health Health // region health as seen by the global layer
}
