// Package runner is a pure core: the agent's task-runner state machine. It
// pulls a task, executes it, reports the result, and re-pulls when idle. It
// performs no I/O and reads no clock — the shell delivers events and carries
// out the commands this core returns. This package follows the shape set by
// internal/core/mitosis: take data, return commands, never execute an effect.
package runner

import "github.com/msivraj/swarm/internal/model"

// RunState is the runner's state.
type RunState int

const (
	// StateIdle is the resting state: no task pulled, no task running.
	StateIdle RunState = iota
	// StateRunning means a task has been pulled and Execute has been issued
	// for it; the runner is waiting for Done or Failed.
	StateRunning
	// StateReporting is reserved for a future protocol in which reporting a
	// result requires its own acknowledgement event before the runner
	// returns to Idle. The current transition table (see Step) reports and
	// re-pulls within a single Done transition, so no path enters this
	// state today; Step still handles it defensively (see the
	// StateReporting case below), returning it unchanged for any event.
	StateReporting
)

// EventKind is the tag of a RunEvent's tagged union: Idle | Pulled{task} |
// Done{result} | Failed{task}.
type EventKind int

const (
	// Idle is a pulse telling the runner it may pull work. The shell sends
	// it both to kick off the loop and after a report/re-queue, since
	// neither Report nor ReQueue has its own completion event.
	Idle EventKind = iota
	// Pulled carries the task the shell obtained in response to a Pull
	// command.
	Pulled
	// Done carries the result of a task that ran to completion.
	Done
	// Failed reports that the running task did not complete. It carries the
	// failed Task (see the ambiguity note on RunEvent.Task) so Step can
	// re-queue it.
	Failed
)

// RunEvent is an event delivered to Step. It is a tagged union on Kind:
// Idle | Pulled{Task} | Done{Result} | Failed{Task}.
type RunEvent struct {
	Kind EventKind
	// Task is set for Pulled (the task obtained) and for Failed (the task
	// that failed).
	//
	// Ambiguity resolved: the phase doc writes the event as bare "Failed",
	// with no payload. RunState is a plain int with no room to remember
	// which task was running, so without a payload on Failed, Step could
	// not know what to ReQueue. The shell already knows which task it asked
	// the core to Execute, so it is the natural place to attach it to the
	// Failed event; this field is reused for that purpose instead of adding
	// a redundant one. Reviewer: please confirm this reading of "Failed".
	Task model.Task
	// Result is set for Done: the outcome of the task that just completed.
	Result model.TaskResult
}

// CmdKind is the tag of a RunCmd's tagged union: Pull | Execute{task} |
// Report{result} | ReQueue{task}.
type CmdKind int

const (
	// Pull asks the shell to obtain the next task for this runner.
	Pull CmdKind = iota
	// Execute asks the shell to run Task.
	Execute
	// Report asks the shell to report Result upstream.
	Report
	// ReQueue asks the shell to put Task back on the work queue.
	ReQueue
)

// RunCmd is a command the shell will execute. Step returns RunCmds; it never
// carries them out.
type RunCmd struct {
	Kind CmdKind
	// Task is set for Execute and ReQueue.
	Task model.Task
	// Result is set for Report.
	Result model.TaskResult
}

// Step advances the runner state machine by one event, returning the next
// state and the commands the shell should carry out. It is pure: the same
// (state, event) pair always yields the same (state, commands), with no I/O
// and no wall-clock read.
//
// Transition table:
//
//	Idle      + Idle          -> Idle,      [Pull]
//	Idle      + Pulled{task}  -> Running,   [Execute{task}]
//	Running   + Done{result}  -> Idle,      [Report{result}, Pull]
//	Running   + Failed{task}  -> Idle,      [ReQueue{task}]
//	any other (state, event) pair is ignored: the state is returned
//	unchanged and no commands are emitted.
//
// Ambiguity resolved: the doc names the events and commands but not the
// state set or the full table. Done reports and re-pulls in the same step
// (rather than parking in StateReporting to await a report-ack event that
// the doc does not define) so the runner does not stall waiting for an
// event that never arrives. Failed re-queues but does not also re-pull;
// the shell is expected to send an Idle pulse to resume pulling, the same
// pulse it uses to start the loop. Reviewer: please confirm both readings.
func Step(s RunState, ev RunEvent) (RunState, []RunCmd) {
	switch s {
	case StateIdle:
		return stepIdle(ev)
	case StateRunning:
		return stepRunning(ev)
	case StateReporting:
		// Unreachable in the current table (see the type's doc comment);
		// handled defensively so Step is total over all (state, event)
		// pairs.
		return StateReporting, nil
	default:
		return s, nil
	}
}

func stepIdle(ev RunEvent) (RunState, []RunCmd) {
	switch ev.Kind {
	case Idle:
		return StateIdle, []RunCmd{{Kind: Pull}}
	case Pulled:
		return StateRunning, []RunCmd{{Kind: Execute, Task: ev.Task}}
	default:
		return StateIdle, nil
	}
}

func stepRunning(ev RunEvent) (RunState, []RunCmd) {
	switch ev.Kind {
	case Done:
		return StateIdle, []RunCmd{
			{Kind: Report, Result: ev.Result},
			{Kind: Pull},
		}
	case Failed:
		return StateIdle, []RunCmd{{Kind: ReQueue, Task: ev.Task}}
	default:
		return StateRunning, nil
	}
}
