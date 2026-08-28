package model

// Tier classifies the trust/latency tier a job runs on. Adaptive-by-tier
// detection (O4) sets a fast timeout for the tight core tier and a patient
// one for the open tier.
type Tier int

const (
	// Core is the trusted, low-latency tier (seconds-scale timeouts).
	Core Tier = iota
	// Open is the untrusted/open tier (tens-of-seconds timeouts).
	Open
)

// Duration is a span in nanoseconds, the delta analog of Instant. deadline()
// returns one; the shell adds it to an Instant to get an eviction time.
type Duration int64

// CapSet is an unordered set of capability tags a cell offers or a job
// requires (e.g. "gpu", "nvlink").
//
// CapSet is a bare slice, not a type with a constructor: callers are
// responsible for passing it sorted and de-duplicated, which is what gives
// pure cores deterministic comparison without this package inventing a
// normalization API the phase doc does not call for (see issue #58).
// Capability matching itself belongs to the placement core, not here.
type CapSet []string

// CellCapacity is a point-in-time free-capacity + capability snapshot of one
// cell that gang admission (B4) reasons over. Distinct from CellView (which
// carries Size for mitosis); admitGang only needs free slots + caps.
type CellCapacity struct {
	ID   CellID
	Free int
	Caps CapSet
}
