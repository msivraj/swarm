// Package cell is the P2 per-cell leader shell (issue #69): it elects one
// leader per cell (hashicorp/raft), replicates the command log the hosted
// driver emits, runs the driver-agnostic loop the phase doc names —
//
//	ev := shell.next()                              // I/O: gRPC reports, timers, gossip
//	state, cmds = driver.Step(state, ev, clock())   // pure
//	raft.Apply(cmds)                                 // replicate the command log
//	shell.exec(cmds)                                 // I/O: AllReduce, Assign, Checkpoint, Send, ...
//
// — and, on leader loss, has the newly-elected leader rebuild state by
// calling the driver's Resume(replicatedLog, lastCheckpoint) before
// continuing the loop.
//
// The three P2 drivers (internal/core/barrier, internal/core/leader,
// internal/core/messagepassing) are pure cores this package hosts but never
// edits: each gets a small Driver adapter (adapter_*.go) that translates the
// shell-level Event/Command envelope in this file to and from the core's own
// sum types. Loop (loop.go) is driver-agnostic — it only ever calls through
// the Driver interface, so swapping the injected Driver is the only thing
// that changes between a barrier job, a leader job, and a message-passing
// job; see loop_test.go's TestLoop_DriverAgnostic.
package cell

import (
	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/leader"
	"github.com/msivraj/swarm/internal/core/messagepassing"
	"github.com/msivraj/swarm/internal/model"
)

// EventKind tags Event's sum type: the union of every hosted driver's event
// vocabulary. Only the Driver adapter that owns a Kind interprets the fields
// that go with it — Loop itself never inspects them (see loop.go).
type EventKind int

const (
	// EventDone — barrier: a member reported its partial for the current step.
	EventDone EventKind = iota
	// EventDeadline — barrier: the per-step deadline fired.
	EventDeadline
	// EventLost — barrier: a member vanished mid-step.
	EventLost
	// EventRestored — barrier: the shell reloaded a checkpoint.
	EventRestored
	// EventGiveUp — barrier: a give-up timeout fired on a stalled barrier.
	EventGiveUp
	// EventRefill — barrier: a member rejoins a same-cell barrier (issue
	// #117's core Refill event; issue #122's shell wiring — LeaderHost's
	// H1-C refill-poll, internal/shell/agent/leader.go), growing membership
	// back toward MinMembers after a Stall.
	EventRefill
	// EventReport — leader: a follower reported its result for the superstep.
	EventReport
	// EventRoundTimeout — leader: the per-superstep deadline fired.
	EventRoundTimeout
	// EventMessage — message-passing: a message arrived for an actor.
	EventMessage
	// EventCrash — message-passing: an actor crashed.
	EventCrash
	// EventAggregate — message-passing: the shell decided a step boundary
	// has arrived (issue #73's msgpass/agent-sim combine wiring) and asks
	// the driver to gather every currently-tracked actor's state for
	// combining. messagepassing has no global step of its own (see that
	// package's doc) — unlike barrier's Deadline or leader's RoundTimeout,
	// which core itself decides — so this "step" boundary is entirely the
	// shell's call (a timer, a job-level cadence, ...); the driver only
	// gathers what it already has when asked.
	EventAggregate
)

// Event is the shell-level event Loop.Handle feeds to whichever Driver is
// hosted. Reusing the pure cores' own types for its fields (rather than
// re-declaring shell-private mirrors of them) keeps every adapter a thin,
// obviously-correct translation instead of a second copy of each core's
// vocabulary to keep in sync.
type Event struct {
	Kind EventKind

	// barrier: EventDone, EventLost, EventRestored, EventDeadline,
	// EventGiveUp, EventRefill
	Worker  barrier.WorkerID
	Partial []byte             // EventDone
	Ckpt    barrier.Checkpoint // EventRestored

	// leader: EventReport, EventRoundTimeout
	Follower leader.FollowerID
	Result   []byte // EventReport

	// message-passing: EventMessage, EventCrash
	Message messagepassing.Message // EventMessage
	Crashed messagepassing.ActorID // EventCrash
	// EventAggregate carries no fields: the driver gathers whatever actor
	// states it is already tracking (see EventAggregate's doc above).
}

// CmdOp tags Command's sum type: the union of every hosted driver's command
// vocabulary. Field reuse across variants follows the same convention the
// cores themselves use (e.g. barrier.Command's Step field serves both
// Release and CheckpointOp) — see each field's doc for which Ops read it.
type CmdOp int

const (
	// OpAllReduce — barrier: combine every member's reported partial.
	OpAllReduce CmdOp = iota
	// OpRelease — barrier: advance followers to Step.
	OpRelease
	// OpCheckpoint — barrier: snapshot the just-completed Step.
	OpCheckpoint
	// OpEvict — barrier: drop Worker (straggler or lost member).
	OpEvict
	// OpRollback — barrier: reload BarrierCkpt.
	OpRollback
	// OpStall — barrier: parked under the MinMembers floor (Have/Need).
	OpStall
	// OpFail — barrier: a give-up timeout fired; terminal (BarrierCkpt preserved).
	OpFail
	// OpAddMember — barrier: Worker was added back to membership by an
	// EventRefill (issue #117's core, issue #122's shell wiring).
	// Replicated through the log like every other command so a
	// BarrierDriver.Resume replay after failover reconstructs the grown
	// membership deterministically instead of a shell-side out-of-band
	// mutation of State.Members.
	OpAddMember

	// OpAssign — leader: hand Follower its Work for a superstep.
	OpAssign
	// OpFold — leader: combine every follower's reported Results.
	OpFold
	// OpAdvance — leader: move followers on to Superstep.
	OpAdvance
	// OpReassign — leader: re-hand Follower its (unchanged) Work after a timeout.
	OpReassign

	// OpSend — message-passing: deliver Send to a mailbox.
	OpSend
	// OpRestart — message-passing: restart Restart (optionally FromSnapshot).
	OpRestart
	// OpAggregate — message-passing: combine every currently-tracked actor's
	// state (AggregateStates), issue #73's msgpass/agent-sim combine wiring
	// — see EventAggregate.
	OpAggregate
)

// Command is a description of an effect Loop's Executor will carry out.
// Adapters translate their hosted core's own Command/Send/Supervise sum type
// into this shell-level envelope; Loop and the raft FSM that replicates the
// command log only ever see this type, never a driver-specific one.
type Command struct {
	Op CmdOp

	// barrier
	Partials    map[barrier.WorkerID][]byte // OpAllReduce
	Step        int                         // OpRelease, OpCheckpoint
	Worker      barrier.WorkerID            // OpEvict, OpAddMember
	BarrierCkpt barrier.Checkpoint          // OpRollback, OpFail
	Have, Need  int                         // OpStall

	// leader
	Follower  leader.FollowerID            // OpAssign, OpReassign
	Work      []byte                       // OpAssign, OpReassign
	Results   map[leader.FollowerID][]byte // OpFold
	Superstep int                          // OpAdvance

	// message-passing
	Send    messagepassing.Send    // OpSend
	Restart messagepassing.ActorID // OpRestart
	// FoldedActor is the actor's post-fold state after the React call that
	// produced this OpSend command — a shell-only addition to the
	// replicated log entry (React itself never returns it) so Resume can
	// rebuild MessagePassingState.Actors from the command log alone; see
	// adapter_messagepassing.go.
	FoldedActor     messagepassing.Actor
	FromSnapshot    bool                              // OpRestart
	AggregateStates map[messagepassing.ActorID][]byte // OpAggregate

	// Combined is issue #73's driver->template combine wiring's output:
	// the pure template combine (internal/core/templates) applied to this
	// command's gathered per-worker payloads (Partials for OpAllReduce,
	// Results for OpFold, AggregateStates for OpAggregate). nil until a
	// CombiningDriver (combine.go) fills it in — a plain BarrierDriver/
	// LeaderDriver/MessagePassingDriver never sets this field, matching
	// every other Command field these adapters populate only when they have
	// something to say.
	Combined []byte // OpAllReduce, OpFold, OpAggregate
}

// State is the opaque driver state Loop threads through Handle calls. Loop
// never inspects it; only the Driver that owns it does (barrier.State,
// leader.Super, or MessagePassingState).
type State = any

// Driver is the pure core each job selects, hosted behind one shell-side
// adapter interface (issue #69's Design section) so Loop's run loop never
// changes when the injected Driver does. Step and Resume are backed by the
// hosted core's own pure functions; Snapshot is this package's own addition
// (the doc's Driver sketch is pseudocode, not a literal final signature) —
// Loop's Checkpoint execution needs a driver-agnostic way to serialize State
// into checkpoint.State.DriverBlob, and only the adapter that owns State's
// concrete type can do that.
type Driver interface {
	// Step folds one Event into next State and the Commands the shell must
	// execute. Pure core underneath; the adapter itself does no I/O either.
	Step(s State, ev Event, now model.Instant) (State, []Command)

	// Resume rebuilds State from a replicated command log plus the last
	// checkpoint, for a newly-elected leader recovering after a leader loss
	// (the phase doc's "leader failover is tested by feeding a log +
	// checkpoint to resume").
	Resume(log []Command, ckpt checkpoint.State) State

	// Snapshot serializes s's driver-specific bytes for
	// checkpoint.State.DriverBlob.
	Snapshot(s State) []byte
}
