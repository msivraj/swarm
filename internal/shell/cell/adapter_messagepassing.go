package cell

import (
	"encoding/json"

	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/messagepassing"
	"github.com/msivraj/swarm/internal/model"
)

// MessagePassingState is the message-passing driver's whole State as Loop
// sees it: the per-actor states messagepassing.React folds (there is no
// global step for this driver — see messagepassing's package doc), keyed by
// actor so Step can look up the addressed actor for one message at a time.
type MessagePassingState struct {
	Actors map[messagepassing.ActorID]messagepassing.Actor
}

// MessagePassingDriver adapts internal/core/messagepassing's pure React (and
// OnCrash for EventCrash) behind the Driver interface Loop hosts, the same
// shape BarrierDriver and LeaderDriver follow. Route is the shell's own
// concern for choosing a delivery target across cells (see
// transportexec.go's use of messagepassing.Route) rather than part of the
// fold Step performs, since Route resolves a message's cell, not an actor's
// state.
type MessagePassingDriver struct{}

var _ Driver = MessagePassingDriver{}

// Step folds ev through messagepassing.React (EventMessage) or
// messagepassing.OnCrash (EventCrash). s must be a MessagePassingState (or
// nil/zero-value State, which is treated as an empty actor set).
func (MessagePassingDriver) Step(s State, ev Event, _ model.Instant) (State, []Command) {
	mps, _ := s.(MessagePassingState)
	if mps.Actors == nil {
		mps.Actors = make(map[messagepassing.ActorID]messagepassing.Actor)
	}

	switch ev.Kind {
	case EventMessage:
		return stepMessage(mps, ev.Message)
	case EventCrash:
		return stepCrash(mps, ev.Crashed)
	case EventAggregate:
		return mps, stepAggregate(mps)
	default:
		return mps, nil
	}
}

// stepAggregate gathers every currently-tracked actor's state into one
// OpAggregate Command (issue #73's msgpass/agent-sim combine wiring — see
// EventAggregate's doc in cell.go). State itself is unchanged: aggregating
// does not fold anything new into any actor, it only reads what React has
// already folded so far. An empty actor set emits no command, mirroring
// barrier's own "nothing to reduce, nothing to emit" convention (see
// barrier.go's decision D).
func stepAggregate(mps MessagePassingState) []Command {
	if len(mps.Actors) == 0 {
		return nil
	}
	states := make(map[messagepassing.ActorID][]byte, len(mps.Actors))
	for id, actor := range mps.Actors {
		states[id] = actor.State
	}
	return []Command{{Op: OpAggregate, AggregateStates: states}}
}

// stepMessage folds m into its addressed actor via messagepassing.React,
// records the actor's new state, and translates the returned Sends into
// OpSend Commands. Every OpSend Command also carries the actor's post-fold
// state (Command.FoldedActor) — a shell-only addition beyond what React
// itself returns, so Resume can rebuild Actors from the command log alone
// (React's Send does not carry the folded actor state, only the outbound
// acknowledgement).
func stepMessage(mps MessagePassingState, m messagepassing.Message) (MessagePassingState, []Command) {
	actor := mps.Actors[m.To]
	if actor.ID == "" {
		actor = messagepassing.Actor{ID: m.To}
	}
	nextActor, sends := messagepassing.React(actor, m)

	nextActors := make(map[messagepassing.ActorID]messagepassing.Actor, len(mps.Actors)+1)
	for k, v := range mps.Actors {
		nextActors[k] = v
	}
	nextActors[m.To] = nextActor
	next := MessagePassingState{Actors: nextActors}

	if len(sends) == 0 {
		return next, nil
	}
	cmds := make([]Command, 0, len(sends))
	for _, snd := range sends {
		cmds = append(cmds, Command{Op: OpSend, Send: snd, FoldedActor: nextActor})
	}
	return next, cmds
}

// stepCrash folds an EventCrash via messagepassing.OnCrash into a single
// OpRestart Command. State is unchanged: OnCrash only decides supervision,
// and the crashed actor's own state is restored by the shell from a
// snapshot (out of this ticket's scope — see messagepassing.OnCrash's doc).
func stepCrash(mps MessagePassingState, id messagepassing.ActorID) (MessagePassingState, []Command) {
	sup := messagepassing.OnCrash(id)
	return mps, []Command{{Op: OpRestart, Restart: id, FromSnapshot: sup.FromSnapshot}}
}

// Snapshot serializes s (a MessagePassingState) to JSON for
// checkpoint.State.DriverBlob.
func (MessagePassingDriver) Snapshot(s State) []byte {
	mps, _ := s.(MessagePassingState)
	b, err := json.Marshal(mps)
	if err != nil {
		return nil
	}
	return b
}

// Resume rebuilds a MessagePassingState by decoding ckpt.DriverBlob (as of
// the last checkpoint) and folding log forward: every OpSend Command's
// FoldedActor is the definitive post-fold state for that actor as of that
// point in the log, so replaying them in order reconstructs Actors exactly
// as Step would have left it. OpRestart carries no persistent state change
// here (see stepCrash).
func (MessagePassingDriver) Resume(log []Command, ckpt checkpoint.State) State {
	var mps MessagePassingState
	if len(ckpt.DriverBlob) > 0 {
		_ = json.Unmarshal(ckpt.DriverBlob, &mps)
	}
	if mps.Actors == nil {
		mps.Actors = make(map[messagepassing.ActorID]messagepassing.Actor)
	}
	for _, c := range log {
		if c.Op != OpSend || c.FoldedActor.ID == "" {
			continue
		}
		next := make(map[messagepassing.ActorID]messagepassing.Actor, len(mps.Actors)+1)
		for k, v := range mps.Actors {
			next[k] = v
		}
		next[c.FoldedActor.ID] = c.FoldedActor
		mps.Actors = next
	}
	return mps
}
