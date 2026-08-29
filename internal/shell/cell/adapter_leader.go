package cell

import (
	"encoding/json"

	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/leader"
	"github.com/msivraj/swarm/internal/model"
)

// LeaderDriver adapts internal/core/leader's pure Step behind the Driver
// interface Loop hosts, the same shape BarrierDriver follows. leader.Step
// takes no `now` (the phase doc omits it for this driver — see leader.go's
// package doc), so Step's now parameter is accepted for interface
// conformance and ignored, matching leader.Step's own signature.
type LeaderDriver struct{}

var _ Driver = LeaderDriver{}

// Step translates ev into a leader.Event, folds it through leader.Step, and
// translates the resulting leader.Command slice back into this package's
// Command envelope. s must be a leader.Super (or nil/zero-value State,
// which decodes to the zero leader.Super).
func (LeaderDriver) Step(s State, ev Event, _ model.Instant) (State, []Command) {
	ls, _ := s.(leader.Super)
	next, cmds := leader.Step(ls, toLeaderEvent(ev))
	return next, fromLeaderCommands(cmds)
}

// Snapshot serializes s (a leader.Super) to JSON for
// checkpoint.State.DriverBlob.
func (LeaderDriver) Snapshot(s State) []byte {
	ls, _ := s.(leader.Super)
	b, err := json.Marshal(ls)
	if err != nil {
		return nil
	}
	return b
}

// Resume rebuilds a leader.Super by decoding ckpt.DriverBlob (as of the last
// checkpoint) and folding log forward over it, one applyLeaderCommand per
// Command — mirroring BarrierDriver.Resume's replay strategy.
func (LeaderDriver) Resume(log []Command, ckpt checkpoint.State) State {
	var ls leader.Super
	if len(ckpt.DriverBlob) > 0 {
		_ = json.Unmarshal(ckpt.DriverBlob, &ls)
	}
	for _, c := range log {
		ls = applyLeaderCommand(ls, c)
	}
	return ls
}

// toLeaderEvent translates ev (an EventReport/EventRoundTimeout shell Event)
// into leader's own Event sum type.
func toLeaderEvent(ev Event) leader.Event {
	switch ev.Kind {
	case EventReport:
		return leader.Event{Kind: leader.Report, Follower: ev.Follower, Result: ev.Result}
	case EventRoundTimeout:
		return leader.Event{Kind: leader.RoundTimeout}
	default:
		return leader.Event{Kind: -1}
	}
}

// fromLeaderCommands translates cmds (leader's own Command sum type) into
// this package's shell-level Command envelope, one-for-one and in order.
func fromLeaderCommands(cmds []leader.Command) []Command {
	if cmds == nil {
		return nil
	}
	out := make([]Command, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, fromLeaderCommand(c))
	}
	return out
}

func fromLeaderCommand(c leader.Command) Command {
	switch c.Op {
	case leader.Assign:
		return Command{Op: OpAssign, Follower: c.Follower, Work: c.Work}
	case leader.Fold:
		return Command{Op: OpFold, Results: c.Results}
	case leader.Advance:
		return Command{Op: OpAdvance, Superstep: c.Superstep}
	case leader.Reassign:
		return Command{Op: OpReassign, Follower: c.Follower, Work: c.Work}
	default:
		return Command{Op: -1}
	}
}

// applyLeaderCommand folds one already-executed Command into ls, mirroring
// leader.go's completeRound: only OpAdvance carries a persistent state
// transition (Superstep). OpAssign/OpReassign hand out per-round work that
// the shell (not the replicated log) tracks in Assigns — see leader.go's
// decision C — and OpFold carries no state beyond what OpAdvance already
// reflects, so both are no-ops here.
func applyLeaderCommand(ls leader.Super, c Command) leader.Super {
	if c.Op == OpAdvance {
		ls.Superstep = c.Superstep
		ls.Results = nil
	}
	return ls
}
