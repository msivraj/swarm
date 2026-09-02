package model

// P6 boundary types: the last three cores — signal-based adaptive mitosis,
// capability/locality placement refinement, and reputation maturity — reason
// over the plain data below. See docs/phases/swarm-p6-components.txt §02.

// --- Signal-based adaptive mitosis (mitosis §03 upgrade) ---

// CellSignal is a point-in-time MEASURED signal for one cell that
// signal-based mitosis reasons over. Size is the P0 count proxy; P99 and
// Tput are the P6 measured signals fed from P4's observability rollups.
// Cell and Coupling are the identity/classification the decision keys on
// (mirroring CellView carrying ID) — they are not part of the doc's 3-field
// signal payload {size,p99,tput} but are required so DecideSignal can emit a
// Command{Cell:...}, look up cooldowns, and pick a per-coupling threshold.
//
// A zero CellSignal (P99 == 0) means "no measured signal" — DecideSignal
// falls back to the Size/count path, so signal-based subsumes count-based
// rather than requiring every cell to have measured latency before mitosis
// can decide anything.
type CellSignal struct {
	Cell     CellID
	Coupling Coupling
	Size     int      // P0 count proxy; drives the count fallback when P99 == 0
	P99      Duration // measured barrier-completion / queue-latency p99; 0 == absent
	Tput     float64  // measured throughput (informational; reserved for merge band)
}

// SignalThresholds configures signal-based mitosis. It SUBSUMES P0's
// Thresholds: Target/CooldownNS reproduce the count-based band when P99 is
// absent, and SLO is the objective the measured P99 is judged against.
type SignalThresholds struct {
	Target     int   // count fallback: split above 2*Target, merge two neighbors under Target
	CooldownNS int64 // suppress a cell's resize for this long after its last one
	SLO        SLO   // objective the measured P99 is judged against (from P5)
}

// Threshold is the per-coupling latency band signalThreshold derives from a
// SignalThresholds.SLO. The zero value (SplitP99 == MergeP99 == 0) is not a
// meaningful band on its own — signalThreshold always derives a nonzero one
// from a per-coupling base latency, so callers should treat an unset
// Threshold as "not yet derived" rather than "never split, always merge."
type Threshold struct {
	SplitP99 Duration // split when measured P99 strictly exceeds this
	MergeP99 Duration // merge-eligible when measured P99 is at/below this ("well under")
}

// --- Capability/locality placement refinement (placement delta) ---

// Topology is a cell's network-location coordinates for locality ranking.
type Topology struct {
	Region RegionID
	AZ     string
	Rack   string
}

// LocalityGraph gives the network distance from a placement Origin to each
// candidate cell. Origin is where the task should be near (its data/
// submitter), supplied by the shell; Zone maps each candidate cell to its
// coordinates. A nil Zone means "no locality info" — rank/bestFit fall back
// to capability match and free capacity alone, so a caller that never builds
// a LocalityGraph gets P0/P1 placement behavior unchanged.
type LocalityGraph struct {
	Origin Topology
	Zone   map[CellID]Topology
}

// Ranked is one candidate cell's placement ranking. It carries the exact
// keys the ranking sorted on — CapMatch (desc), Distance (asc), Free (desc),
// Cell (asc, final tie-break) — so a test can assert the order
// deterministically.
type Ranked struct {
	Cell     CellID
	CapMatch bool // candidate satisfies the task's required capabilities
	Distance int  // locality distance from Origin (0 rack .. 3 cross-region)
	Free     int  // free capacity (secondary preference)
}

// --- Reputation maturity (verification delta) ---

// RepTier is a coarse verification-maturity bucket derived from a
// Reputation. It is monotonic in reputation and consistent with the P3
// freeze, but does NOT change Weight/NeedsK/Eligible — it is an additional
// coarse read. RepTier is distinct from Tier (Core/Open): it measures trust
// maturity WITHIN a tier, not which tier a job runs on.
type RepTier int

const (
	// RepUntrusted is a fresh or frozen identity — the zero value, so an
	// identity that has never been classified reads as the lowest-trust
	// bucket, not the highest.
	RepUntrusted RepTier = iota
	// RepProvisional is an identity with some honest history and a mid score.
	RepProvisional
	// RepTrusted is a mature identity: high Observations AND high Score.
	RepTrusted
)
