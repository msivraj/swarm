// Package model holds Swarm's boundary types — plain, pure data that crosses
// component and core/shell boundaries. Nothing here performs I/O; the core may
// take and return these values freely.
package model

// Instant is a monotonic timestamp in nanoseconds, supplied by the shell.
// The core never reads the clock itself — it receives Instants as data, which
// is what keeps its decisions deterministic and reproducible in tests.
type Instant int64

// CellID uniquely identifies a cell within the fleet.
type CellID string

// CellView is a point-in-time snapshot of a cell that pure cores reason over —
// a value, never a live handle.
type CellView struct {
	ID   CellID
	Size int // current member count
	Free int // free capacity
}
