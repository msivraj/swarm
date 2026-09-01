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
	Size int    // current member count
	Free int    // free capacity
	Caps CapSet // capabilities this cell offers; nil == none (P0/P1 behavior unchanged)
}

// JobID uniquely identifies a submitted job.
type JobID string

// TaskID uniquely identifies one task within a job.
type TaskID string

// Coupling classifies how a job's tasks relate to one another. P0 only runs
// Independent; the other values exist so later phases have nothing to
// retrofit.
type Coupling int

const (
	// Independent tasks share no state and can complete in any order.
	Independent Coupling = iota
	// Barrier tasks must all reach a checkpoint before any proceeds past it.
	Barrier
	// Leader tasks are coordinated by one elected task among them.
	Leader
	// MessagePassing tasks exchange messages with one another during execution.
	MessagePassing
)

// JobSpec is a submitted job before decomposition into Tasks.
type JobSpec struct {
	ID       JobID
	Template string
	Coupling Coupling
	// Params holds string CLI/job-file parameters. Widen to typed values in a
	// follow-up if the planner needs them — not guessed here.
	Params map[string]string
	// MinMembers is the gang admission floor (B4): the job needs at least
	// this many members placed together before it starts. 0 means the job
	// is not a gang and has no floor (P0/P1 behavior unchanged).
	MinMembers int
	// Tier selects the trust/execution path: Core (default, zero value) is
	// the existing trusted native path; Open routes the job through P3's
	// WASM-sandboxed, quorum-verified path (P0-P2 behavior unchanged).
	Tier Tier
	// Tenant is the owning tenant for P5 quota/fairness. "" (the zero
	// value) is the default/untagged tenant, so P0-P4 jobs — which never
	// set this — are unaffected.
	Tenant TenantID
	// Demand is this job's per-resource request, a normalized share of
	// cluster capacity (see ResourceVec). Nil (the zero value) means no
	// declared demand, matching pre-P5 behavior.
	Demand ResourceVec
}

// Task is one unit of independent work decomposed from a JobSpec.
type Task struct {
	ID      TaskID
	JobID   JobID
	Input   []byte
	Attempt int
	// Requires lists the capabilities this task needs; nil means none
	// required (P0/P1 behavior unchanged).
	Requires CapSet
	// Declared is the WASI capability set this task declares it needs when
	// run in P3's open-tier sandbox (see internal/core/sandbox.Grants). The
	// zero value declares nothing — no ambient authority — so P0-P2 tasks,
	// which never set it, are unaffected.
	Declared WasiCaps
}

// TaskResult is the outcome an agent reports for a Task.
type TaskResult struct {
	TaskID TaskID
	Output []byte
	OK     bool
}

// Aggregate is the merged result of a job's TaskResults.
type Aggregate struct {
	JobID JobID
	Value []byte
	Done  bool
}
