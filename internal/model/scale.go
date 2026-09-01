package model

// P4 boundary types: going to a million machines is a SHELL concern (the
// store, the transport, the metrics pipeline) — almost none of it touches a
// core. The few new pure functions P4 does add (shardOf, the metric
// rollups, admitUnderLoad/updateLoad, nextDrain/skewSafe) share the plain
// data below. See docs/phases/swarm-p4-components.txt §02.

// --- Sharded registry ---

// Key is a registry key — a cell/agent membership key — the sharded store
// partitions on.
type Key string

// ShardID names the partition that owns a Key range. shardOf is a range
// partition: contiguous Key ranges map to one ShardID, represented as a
// plain index into the shard space (the FoundationDB shell maps a ShardID
// to its transaction range). shardOf (registry) is the only producer.
type ShardID uint32

// --- Observability rollup ---

// Level names a tier of the rollup tree metrics fold up through.
type Level int

const (
	// LevelCell is the per-cell raw tier — the leaf of the rollup tree.
	LevelCell Level = iota
	// LevelRegion is cells rolled up per region.
	LevelRegion
	// LevelGlobal is regions rolled up global.
	LevelGlobal
)

// Cardinality is a bounded series-count budget a Level is allowed to keep,
// so metric detail does not explode with fleet size.
type Cardinality int

// CellMetrics carries one cell's reduced metric series — the leaf a region
// rollup folds over. Count is an additive counter (combine: sum, e.g. total
// requests observed); Gauge is a sampled value (e.g. latency or load) that
// combines by weighted average, which is why Samples travels with it — a
// plain average of averages is not associative, but a Samples-weighted one
// is, which is what lets the cell -> region -> global reduce equal a flat
// reduce over all cells.
type CellMetrics struct {
	Cell    CellID
	Count   int64   // additive counter; combine rule: sum
	Gauge   float64 // sampled value (e.g. mean latency); combine rule: Samples-weighted average
	Samples int64   // number of raw samples Gauge was computed over; combine rule: sum
}

// RegionMetrics carries one region's rolled-up metric series — CellMetrics
// folded per region. Same fields, same combine rules, as CellMetrics: that
// symmetry is what makes the region -> global rollup reuse the identical
// associative fold.
type RegionMetrics struct {
	Region  RegionID
	Count   int64   // combine rule: sum
	Gauge   float64 // combine rule: Samples-weighted average
	Samples int64   // combine rule: sum
}

// GlobalMetrics carries the fleet-wide rolled-up metric series — all
// RegionMetrics folded to one value. No identifier: there is exactly one.
type GlobalMetrics struct {
	Count   int64   // combine rule: sum
	Gauge   float64 // combine rule: Samples-weighted average
	Samples int64   // combine rule: sum
}

// --- Backpressure & rate-limiting ---

// Req is an inbound control-plane request admitUnderLoad decides on.
type Req struct {
	// Priority is the request's importance; higher is more important. 0 is
	// the lowest priority — the zero value never claims special treatment.
	Priority int
}

// LoadState is a point-in-time load snapshot the shell measures (queue
// depth, in-flight requests). Pure data — updated only via updateLoad.
type LoadState struct {
	InFlight   int // requests currently being served
	QueueDepth int // requests queued, not yet admitted
}

// Limits are the configured admission thresholds admitUnderLoad enforces.
type Limits struct {
	// Capacity is the maximum number of in-flight requests the control
	// plane is sized to serve.
	Capacity int
	// ShedThreshold is the fraction of Capacity (0..1) at or above which a
	// low-priority Req is shed rather than admitted or throttled (e.g. 0.95
	// for "at 95% capacity, low-priority -> Shed").
	ShedThreshold float64
}

// LoadEvent folds a measured change into LoadState (arrival, completion, a
// queue-depth sample). updateLoad applies the deltas; it never reads them
// from a clock or a channel itself.
type LoadEvent struct {
	InFlightDelta int // e.g. +1 on arrival, -1 on completion
	QueueDelta    int // e.g. +1 on enqueue, -1 on dequeue
}

// LoadDecisionKind is the admission outcome admitUnderLoad returns.
type LoadDecisionKind int

const (
	// Shed rejects the request now. It is the zero value: an uninitialized
	// LoadDecision must never silently admit under load.
	Shed LoadDecisionKind = iota
	// AdmitLoad accepts the request now. (Named AdmitLoad, not Admit, to
	// avoid colliding with the existing open-tier Admit struct in open.go.)
	AdmitLoad
	// Throttle accepts the request after Delay.
	Throttle
)

// LoadDecision is AdmitLoad | Throttle{Delay} | Shed — the sum type
// admitUnderLoad returns. The zero value is Shed (the conservative default):
// an uninitialized decision refuses service rather than risk overload.
type LoadDecision struct {
	Kind LoadDecisionKind
	// Delay is how long the shell must wait before admitting the request.
	// Set only when Kind == Throttle; a duration (relative span), not a
	// timestamp, so admitUnderLoad stays clock-free.
	Delay Duration
}

// --- Rolling upgrade ---

// Version identifies a fleet binary version for skew-safety checks.
type Version struct {
	Major int
	Minor int
}

// FleetState is the point-in-time fleet the upgrade planner reasons over.
type FleetState struct {
	// Versions is each cell's currently running binary version.
	Versions map[CellID]Version
	// Jobs is the set of jobs currently running on each cell — nextDrain
	// needs this to enforce "never drain two cells of the same job at once."
	Jobs map[CellID][]JobID
	// Cordoned is the set of cells already cordoned/draining; nextDrain
	// skips these when choosing the next cell.
	Cordoned map[CellID]bool
	// Cells maps each cell to the region it lives in — the topology
	// recoveryPlan (P5) needs to re-home agents out of a lost region.
	Cells map[CellID]RegionID
	// Regions lists the fleet's regions in deterministic order, so
	// recoveryPlan can pick the surviving region deterministically (the
	// lowest-sorted healthy RegionID).
	Regions []RegionID
	// Backups is the last registry backup Instant per region, so
	// recoveryPlan can pick the latest backup to restore from.
	Backups map[RegionID]Instant
}

// UpgradePlan is the target of the rolling upgrade.
type UpgradePlan struct {
	// Target is the version every cell is being rolled to.
	Target Version
	// Order is an optional explicit cell drain order; nil lets nextDrain
	// choose (e.g. any not-yet-cordoned, skew-safe cell).
	Order []CellID
}

// DrainStepKind is the next action nextDrain returns.
type DrainStepKind int

const (
	// Done means nothing is left to drain. It is the zero value: a nil or
	// zero-value plan drains nothing rather than cordoning something by
	// accident.
	Done DrainStepKind = iota
	// Cordon means the shell must cordon Cell next.
	Cordon
)

// DrainStep is Cordon{CellID} | Done — the sum type nextDrain returns. The
// zero value is Done (the safe default): an uninitialized step drains
// nothing.
type DrainStep struct {
	Kind DrainStepKind
	// Cell is the cell to cordon next. Set only when Kind == Cordon.
	Cell CellID
}
