// Package barrier is a pure core: it marches coupled workers in lockstep —
// compute -> wait for all -> all-reduce -> advance -> checkpoint every K ->
// evict stragglers. No I/O, no clock; `now` is supplied by the shell as data
// and is currently unused by step's logic (the Deadline event itself, not a
// clock read, is what tells the core a step's deadline fired) but is kept in
// the signature to match the doc and leave room for future timing-aware
// rules without breaking callers.
//
// This is the P2 "barrier spike" (issue #56): only the pure step. Checkpoint
// (de)serialization and the leader/Raft shell that hosts this driver are
// separate, later P2 tickets.
package barrier

import "github.com/msivraj/swarm/internal/model"

// WorkerID identifies one worker in the barrier's membership.
type WorkerID string

// Checkpoint is an opaque handle to the last snapshotted step. The bytes/store
// are the shell's concern; the core only tracks which step it pins.
type Checkpoint struct {
	Step int
}

// State (the "Barrier") is the driver's whole state — plain data.
type State struct {
	Step           int                 // current step N
	K              int                 // checkpoint cadence: checkpoint when step%K==0 (K>0)
	Members        []WorkerID          // the barrier's membership for this step
	Partials       map[WorkerID][]byte // per-worker Done payloads collected for Step
	LastCheckpoint Checkpoint          // most recent checkpoint to roll back to
}

// EventKind tags Event's sum type.
type EventKind int

const (
	// Done{worker, partial} — a member reported its result for the current step.
	Done EventKind = iota
	// Deadline — the per-step deadline fired.
	Deadline
	// Lost{worker} — a member vanished mid-step. Not named in the phase doc's
	// one-line rule table (§03 lists Done/Deadline/Restored) but required by
	// its own prose ("member lost -> Rollback") and by the Command shape,
	// which already includes Evict/Rollback; added here as the fourth event
	// variant per issue #56.
	Lost
	// Restored{checkpoint} — the shell reloaded a checkpoint.
	Restored
)

// Event is the sum type folded by step: Done | Deadline | Lost | Restored.
type Event struct {
	Kind    EventKind
	Worker  WorkerID   // Done, Lost
	Partial []byte     // Done
	Ckpt    Checkpoint // Restored
}

// CmdOp tags Command's sum type.
type CmdOp int

const (
	// AllReduce{partials} — combine every member's reported partial for the
	// step that just completed.
	AllReduce CmdOp = iota
	// Release{step} — advance the barrier's followers to Step.
	Release
	// CheckpointOp{step} — snapshot the just-completed step. Named CheckpointOp
	// rather than the doc's literal "Checkpoint" because that identifier is
	// already claimed by the Checkpoint type in this package (the same
	// collision internal/core/routing hit with Route/route — see that
	// package's route doc for the precedent); the doc allows the builder to
	// refine names as long as the variant and its payload match.
	CheckpointOp
	// Evict{worker} — drop a straggler or a lost member from membership.
	Evict
	// Rollback{ckpt} — reload the last checkpoint after a member is lost.
	Rollback
)

// Command is a description of an effect the shell will execute. Cores return
// Commands; they never carry them out.
type Command struct {
	Op       CmdOp
	Partials map[WorkerID][]byte // AllReduce
	Step     int                 // Release (step to advance to), CheckpointOp (step snapshotted)
	Worker   WorkerID            // Evict
	Ckpt     Checkpoint          // Rollback
}

// step folds one event into new state and the commands the shell must run.
// Pure: no I/O, no clock read — now is supplied by the caller as data.
//
// Resolved ordering decisions (issue #56 notes, flagged for auditor + human):
//
//   - A. Checkpoint-step ordering: when the completing step is also a
//     cadence step, the order is AllReduce -> Checkpoint{N} -> Release{N+1}
//     — reduce, snapshot the reduced step, then release, so a crash right
//     after Release still has step N checkpointed.
//   - B. Checkpoint carries the just-completed step N, not N+1.
//   - C. Deadline evicts stragglers; if the survivors are then fully Done,
//     the same Deadline event ALSO completes the step (AllReduce ->
//     Checkpoint on cadence -> Release), so the last stragglers being
//     evicted can't deadlock a barrier waiting for a release that would
//     otherwise never come.
//   - D. Empty/fully-evicted membership emits no Release (and, for an empty
//     Members to start with, no command at all) — a dead cell has no
//     partials to reduce and nothing to release to.
//   - E. Lost with no checkpoint yet rolls back to the zero Checkpoint
//     (step 0 / genesis).
//   - F. Step 0 checkpoints under the literal Step%K==0 rule (spike
//     default).
func step(s State, ev Event, now model.Instant) (State, []Command) {
	switch ev.Kind {
	case Done:
		return stepDone(s, ev.Worker, ev.Partial)
	case Deadline:
		return stepDeadline(s)
	case Lost:
		return stepLost(s, ev.Worker)
	case Restored:
		return stepRestored(s, ev.Ckpt)
	default:
		// Unknown EventKind: no-op rather than panic — a pure core must
		// never crash on unexpected input.
		return s, nil
	}
}

// Step is step's exported entry point, so a shell (later P2 ticket) has a
// symbol to call.
func Step(s State, ev Event, now model.Instant) (State, []Command) {
	return step(s, ev, now)
}

// stepDone folds a Done{w, partial} event. A Done from a non-member is
// ignored; a duplicate Done for a member already recorded this step
// overwrites its partial and, since the member already counted toward
// completion, emits nothing new. When every current member has reported,
// the step completes (rule B).
func stepDone(s State, w WorkerID, partial []byte) (State, []Command) {
	if !isMember(s.Members, w) {
		return s, nil
	}

	partials := clonePartials(s.Partials)
	partials[w] = partial
	next := s
	next.Partials = partials

	if !allDone(s.Members, partials) {
		return next, nil
	}
	return completeStep(next, partials)
}

// stepDeadline folds a Deadline event: it evicts every member that has not
// reported Done for Step (decision C). If evicting the stragglers leaves at
// least one survivor, those survivors are — by construction — all Done, so
// the same event also completes the step. If no survivors remain, only the
// Evicts are emitted (decision D): a fully evicted cell has no partials to
// reduce and nothing to release.
func stepDeadline(s State) (State, []Command) {
	var stragglers, survivors []WorkerID
	for _, w := range s.Members {
		if _, ok := s.Partials[w]; ok {
			survivors = append(survivors, w)
		} else {
			stragglers = append(stragglers, w)
		}
	}

	var cmds []Command
	for _, w := range stragglers {
		cmds = append(cmds, Command{Op: Evict, Worker: w})
	}

	next := s
	next.Members = survivors

	if len(survivors) == 0 {
		next.Partials = nil
		return next, cmds
	}

	survivorPartials := make(map[WorkerID][]byte, len(survivors))
	for _, w := range survivors {
		survivorPartials[w] = s.Partials[w]
	}
	completed, completeCmds := completeStep(next, survivorPartials)
	return completed, append(cmds, completeCmds...)
}

// stepLost folds a Lost{w} event: a coupled member vanished mid-step, so the
// barrier rolls back to its LastCheckpoint (the zero Checkpoint — step 0 /
// genesis — if none has been taken yet, decision E) and drops w from
// membership on the restored state.
func stepLost(s State, w WorkerID) (State, []Command) {
	ckpt := s.LastCheckpoint
	next := State{
		Step:           ckpt.Step,
		K:              s.K,
		Members:        removeWorker(s.Members, w),
		Partials:       nil,
		LastCheckpoint: ckpt,
	}
	return next, []Command{{Op: Rollback, Ckpt: ckpt}}
}

// stepRestored folds a Restored{ckpt} event: the shell has already reloaded
// the checkpoint's bytes, so the core only resets its bookkeeping — Step and
// LastCheckpoint move to ckpt (so a later Lost rolls back to this restored
// point, not a stale earlier one) and Partials is cleared. No command: the
// recovery I/O already happened in the shell before this event was raised.
func stepRestored(s State, ckpt Checkpoint) (State, []Command) {
	next := s
	next.Step = ckpt.Step
	next.LastCheckpoint = ckpt
	next.Partials = nil
	return next, nil
}

// completeStep emits the step-completion commands for s's current Step given
// the (fully Done) partials collected for it, and returns the advanced
// state: AllReduce{partials}, then — iff Step%K==0 (K>0) — Checkpoint{Step},
// then Release{Step+1} (decisions A, B, F).
func completeStep(s State, partials map[WorkerID][]byte) (State, []Command) {
	cmds := []Command{{Op: AllReduce, Partials: partials}}

	next := s
	next.Partials = nil
	next.Step = s.Step + 1

	if isCheckpointStep(s.Step, s.K) {
		cmds = append(cmds, Command{Op: CheckpointOp, Step: s.Step})
		next.LastCheckpoint = Checkpoint{Step: s.Step}
	}

	cmds = append(cmds, Command{Op: Release, Step: next.Step})
	return next, cmds
}

// isCheckpointStep reports whether step is a checkpoint-cadence step under
// K. K<=0 is treated as "never checkpoint" (decision D) rather than
// rejected at construction, so a misconfigured K degrades to "no
// checkpoints" instead of a divide-by-zero panic.
func isCheckpointStep(step, k int) bool {
	if k <= 0 {
		return false
	}
	return step%k == 0
}

// isMember reports whether w is a current member of members.
func isMember(members []WorkerID, w WorkerID) bool {
	for _, m := range members {
		if m == w {
			return true
		}
	}
	return false
}

// allDone reports whether every member has a recorded partial.
func allDone(members []WorkerID, partials map[WorkerID][]byte) bool {
	for _, w := range members {
		if _, ok := partials[w]; !ok {
			return false
		}
	}
	return true
}

// removeWorker returns members without w, copy-on-write so it never mutates
// the caller's slice.
func removeWorker(members []WorkerID, w WorkerID) []WorkerID {
	if !isMember(members, w) {
		return members
	}
	out := make([]WorkerID, 0, len(members))
	for _, m := range members {
		if m != w {
			out = append(out, m)
		}
	}
	return out
}

// clonePartials copies m so folding a Done event never mutates a Partials
// map a caller is still holding — copy-on-write, the same discipline as
// internal/core/routing's GlobalView.
func clonePartials(m map[WorkerID][]byte) map[WorkerID][]byte {
	out := make(map[WorkerID][]byte, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}
