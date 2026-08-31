package cell

import (
	"encoding/json"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/model"
)

// BarrierDriver adapts internal/core/barrier's pure Step behind the Driver
// interface Loop hosts. It never edits the core: it only translates the
// shell-level Event/Command envelope (cell.go) to and from barrier's own sum
// types, and folds the replicated command log forward from a checkpoint for
// Resume.
type BarrierDriver struct{}

var _ Driver = BarrierDriver{}

// Step translates ev into a barrier.Event, folds it through barrier.Step,
// and translates the resulting barrier.Command slice back into this
// package's Command envelope. s must be a barrier.State (or nil/zero-value
// State, which decodes to the zero barrier.State — a fresh barrier with no
// members, matching barrier.Step's own zero-value behavior).
//
// One shell-only augmentation: barrier.Command's Rollback variant (emitted
// by stepLost) carries only the checkpoint to reload, not which member was
// lost — Members' change is only visible in barrier's returned State, not in
// the Command a Lost event produces. So that Resume can still rebuild
// Members from the log alone, an EventLost's translated OpRollback command
// is tagged with the lost Worker here (Worker is otherwise unused by
// OpRollback — see applyBarrierCommand, which only removes a worker on
// replay when one is tagged, leaving the MinMembers-floor stall's own
// Rollback, whose Members are already correct, untouched).
func (BarrierDriver) Step(s State, ev Event, now model.Instant) (State, []Command) {
	bs, _ := s.(barrier.State)
	next, cmds := barrier.Step(bs, toBarrierEvent(ev), now)
	out := fromBarrierCommands(cmds)
	if ev.Kind == EventLost {
		for i := range out {
			if out[i].Op == OpRollback {
				out[i].Worker = ev.Worker
			}
		}
	}
	return next, out
}

// Snapshot serializes s (a barrier.State) to JSON for
// checkpoint.State.DriverBlob.
func (BarrierDriver) Snapshot(s State) []byte {
	bs, _ := s.(barrier.State)
	b, err := json.Marshal(bs)
	if err != nil {
		return nil
	}
	return b
}

// Resume rebuilds a barrier.State by decoding ckpt.DriverBlob (the driver
// state as of the last checkpoint, written by Snapshot) and then folding log
// forward over it in order, one applyBarrierCommand per Command — the same
// state transitions the shell already applied the first time each Command
// was executed, replayed from the replicated log instead of live events.
func (BarrierDriver) Resume(log []Command, ckpt checkpoint.State) State {
	var bs barrier.State
	if len(ckpt.DriverBlob) > 0 {
		_ = json.Unmarshal(ckpt.DriverBlob, &bs)
	}
	for _, c := range log {
		bs = applyBarrierCommand(bs, c)
	}
	return bs
}

// toBarrierEvent translates ev (an EventDone/Deadline/Lost/Restored/GiveUp
// shell Event) into barrier's own Event sum type.
func toBarrierEvent(ev Event) barrier.Event {
	switch ev.Kind {
	case EventDone:
		return barrier.Event{Kind: barrier.Done, Worker: ev.Worker, Partial: ev.Partial}
	case EventDeadline:
		return barrier.Event{Kind: barrier.Deadline}
	case EventLost:
		return barrier.Event{Kind: barrier.Lost, Worker: ev.Worker}
	case EventRestored:
		return barrier.Event{Kind: barrier.Restored, Ckpt: ev.Ckpt}
	case EventGiveUp:
		return barrier.Event{Kind: barrier.GiveUp}
	case EventRefill:
		return barrier.Event{Kind: barrier.Refill, Worker: ev.Worker}
	default:
		// Not a barrier event kind (e.g. a leader/message-passing event fed
		// to the wrong driver by mistake): barrier.Step's own default case
		// already no-ops on an unrecognized Kind, so translate to one.
		return barrier.Event{Kind: -1}
	}
}

// fromBarrierCommands translates cmds (barrier's own Command sum type) into
// this package's shell-level Command envelope, one-for-one and in order.
func fromBarrierCommands(cmds []barrier.Command) []Command {
	if cmds == nil {
		return nil
	}
	out := make([]Command, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, fromBarrierCommand(c))
	}
	return out
}

func fromBarrierCommand(c barrier.Command) Command {
	switch c.Op {
	case barrier.AllReduce:
		return Command{Op: OpAllReduce, Partials: c.Partials}
	case barrier.Release:
		return Command{Op: OpRelease, Step: c.Step}
	case barrier.CheckpointOp:
		return Command{Op: OpCheckpoint, Step: c.Step}
	case barrier.Evict:
		return Command{Op: OpEvict, Worker: c.Worker}
	case barrier.Rollback:
		return Command{Op: OpRollback, BarrierCkpt: c.Ckpt}
	case barrier.Stall:
		return Command{Op: OpStall, Have: c.Have, Need: c.Need}
	case barrier.Fail:
		return Command{Op: OpFail, BarrierCkpt: c.Ckpt}
	case barrier.AddMember:
		return Command{Op: OpAddMember, Worker: c.Worker}
	default:
		return Command{Op: -1}
	}
}

// applyBarrierCommand folds one already-executed Command into bs, the same
// state transition barrier.Step's own commit path already applied when the
// command was first produced — see barrier.go's completeStep/stall/stepLost/
// stepRefill for the transitions this mirrors. Only OpRelease, OpCheckpoint,
// OpEvict, OpRollback, OpFail, and OpAddMember carry a state transition;
// OpAllReduce, OpStall (whose Rollback always precedes or accompanies it in
// the same batch), and unrecognized ops are no-ops here.
func applyBarrierCommand(bs barrier.State, c Command) barrier.State {
	switch c.Op {
	case OpRelease:
		bs.Step = c.Step
		bs.Partials = nil
	case OpCheckpoint:
		bs.LastCheckpoint = barrier.Checkpoint{Step: c.Step}
	case OpEvict:
		bs.Members = removeBarrierWorker(bs.Members, c.Worker)
	case OpRollback:
		bs.Step = c.BarrierCkpt.Step
		bs.LastCheckpoint = c.BarrierCkpt
		bs.Partials = nil
		if c.Worker != "" {
			bs.Members = removeBarrierWorker(bs.Members, c.Worker)
		}
	case OpFail:
		bs.Failed = true
	case OpAddMember:
		bs.Members = addBarrierWorker(bs.Members, c.Worker)
	}
	return bs
}

// removeBarrierWorker returns members without w, copy-on-write — the same
// discipline barrier.go's own (unexported) removeWorker uses; duplicated
// here rather than exported from the core, since a core exports only its
// pure Step entry point, not its private helpers.
func removeBarrierWorker(members []barrier.WorkerID, w barrier.WorkerID) []barrier.WorkerID {
	out := make([]barrier.WorkerID, 0, len(members))
	for _, m := range members {
		if m != w {
			out = append(out, m)
		}
	}
	return out
}

// addBarrierWorker returns members with w appended, copy-on-write, unless w
// is already present — the same idempotency barrier.go's own (unexported)
// appendWorker/stepRefill apply to a live Refill fold, mirrored here so
// replaying an OpAddMember from the log (possibly more than once, e.g. a
// re-elected leader replaying the same post-checkpoint suffix twice before
// its own next checkpoint) never duplicates the worker.
func addBarrierWorker(members []barrier.WorkerID, w barrier.WorkerID) []barrier.WorkerID {
	for _, m := range members {
		if m == w {
			return members
		}
	}
	out := make([]barrier.WorkerID, len(members), len(members)+1)
	copy(out, members)
	return append(out, w)
}
