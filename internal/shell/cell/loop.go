package cell

import (
	"context"
	"sync"

	"github.com/msivraj/swarm/internal/model"
)

// Executor carries out the I/O a Command describes — AllReduce, AssignWork,
// Checkpoint, Send, ... Loop and the pure Driver it hosts never perform an
// effect themselves; only an Executor does (see transportexec.go for the
// production implementation wired to the CellLeader transport, issue #68).
type Executor interface {
	Exec(ctx context.Context, cmds []Command) error
}

// ExecutorFunc adapts a plain function to Executor.
type ExecutorFunc func(ctx context.Context, cmds []Command) error

// Exec calls f.
func (f ExecutorFunc) Exec(ctx context.Context, cmds []Command) error { return f(ctx, cmds) }

// RecordingExecutor wraps another Executor (Next may be nil) and additionally
// appends every Command Loop asks it to execute to History, in the exact
// order Loop.Handle produced them. Tests use it to assert the sequence of
// executed commands matches the hosted core's own output for the events fed
// in — issue #69's first acceptance criterion.
type RecordingExecutor struct {
	Next Executor

	mu      sync.Mutex
	History []Command
}

// Exec records cmds, then (if Next is set) delegates to it.
func (r *RecordingExecutor) Exec(ctx context.Context, cmds []Command) error {
	r.mu.Lock()
	r.History = append(r.History, cmds...)
	r.mu.Unlock()
	if r.Next == nil {
		return nil
	}
	return r.Next.Exec(ctx, cmds)
}

// Snapshot returns a copy of the commands recorded so far, in order.
func (r *RecordingExecutor) Snapshot() []Command {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Command, len(r.History))
	copy(out, r.History)
	return out
}

// Loop is the driver-agnostic run loop the phase doc names: gather an event,
// fold it through the hosted Driver's pure Step, replicate the resulting
// command log (Apply — typically a *raft.Raft's Apply, see raft.go), execute
// the commands (Exec). Swapping Driver — BarrierDriver, LeaderDriver,
// MessagePassingDriver — never changes Loop's code; see loop_test.go's
// TestLoop_DriverAgnostic for the same loop hosting all three.
type Loop struct {
	Driver Driver
	Exec   Executor
	// Apply replicates a step's command log — typically a *raft.Raft node's
	// Apply, wired through raft.go's ApplyFunc. nil disables replication,
	// which a unit test that only exercises Step+Exec (no raft cluster) can
	// leave unset.
	Apply func(cmds []Command) error

	mu    sync.Mutex
	state State
}

// NewLoop returns a Loop hosting driver, starting from state, executing
// commands through exec, and replicating each step's command log through
// apply (nil disables replication).
func NewLoop(driver Driver, state State, exec Executor, apply func([]Command) error) *Loop {
	return &Loop{Driver: driver, Exec: exec, Apply: apply, state: state}
}

// State returns the loop's current driver state.
func (l *Loop) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// SetState overwrites the loop's driver state — the run loop's landing spot
// for a newly-elected leader's rebuilt state, once Driver.Resume has folded
// the replicated log forward from the last checkpoint (issue #69's failover
// path: rebuild, then SetState, then keep calling Handle as events arrive).
func (l *Loop) SetState(s State) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state = s
}

// Handle is the run loop's one step — the caller (an RPC handler, a timer)
// already gathered ev; Handle folds it through the driver, replicates the
// resulting commands, and executes them:
//
//	state, cmds = driver.Step(state, ev, now)   // pure
//	raft.Apply(cmds)                             // replicate the command log
//	shell.exec(cmds)                             // execute
//
// If Step produces no commands, neither Apply nor Exec is called — an event
// that folds into new state without an externally-visible effect (e.g. a
// Done that has not yet completed a step) does not append an empty entry to
// the replicated log.
func (l *Loop) Handle(ctx context.Context, ev Event, now model.Instant) ([]Command, error) {
	l.mu.Lock()
	next, cmds := l.Driver.Step(l.state, ev, now)
	l.state = next
	l.mu.Unlock()

	if len(cmds) == 0 {
		return cmds, nil
	}
	if l.Apply != nil {
		if err := l.Apply(cmds); err != nil {
			return cmds, err
		}
	}
	if l.Exec != nil {
		if err := l.Exec.Exec(ctx, cmds); err != nil {
			return cmds, err
		}
	}
	return cmds, nil
}
