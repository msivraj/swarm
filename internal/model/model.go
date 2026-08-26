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
}

// Task is one unit of independent work decomposed from a JobSpec.
type Task struct {
	ID      TaskID
	JobID   JobID
	Input   []byte
	Attempt int
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
