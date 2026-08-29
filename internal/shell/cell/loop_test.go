package cell

import (
	"context"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/leader"
	"github.com/msivraj/swarm/internal/core/messagepassing"
	"github.com/msivraj/swarm/internal/model"
)

// TestLoop_BarrierEndToEnd is issue #69's first acceptance criterion: the
// run loop drives a barrier job end-to-end — Done reports in -> AllReduce +
// (on cadence) Checkpoint + Release executed, in order — and the sequence of
// executed commands matches what barrier.Step itself returns for the same
// events, computed independently in this test.
func TestLoop_BarrierEndToEnd(t *testing.T) {
	start := barrier.State{Step: 0, K: 2, Members: []barrier.WorkerID{"a", "b"}}
	rec := &RecordingExecutor{}
	l := NewLoop(BarrierDriver{}, start, rec, nil)

	events := []Event{
		{Kind: EventDone, Worker: "a", Partial: []byte("pa")},
		{Kind: EventDone, Worker: "b", Partial: []byte("pb")}, // completes step 0, K=2 -> checkpoint
	}

	var wantCmds []Command
	refState := start
	for _, ev := range events {
		var cmds []barrier.Command
		refState, cmds = barrier.Step(refState, toBarrierEvent(ev), 0)
		wantCmds = append(wantCmds, fromBarrierCommands(cmds)...)

		if _, err := l.Handle(context.Background(), ev, model.Instant(0)); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}

	got := rec.Snapshot()
	if !reflect.DeepEqual(got, wantCmds) {
		t.Fatalf("executed commands = %#v, want %#v", got, wantCmds)
	}
	if got[0].Op != OpAllReduce || got[1].Op != OpCheckpoint || got[2].Op != OpRelease {
		t.Fatalf("expected AllReduce -> Checkpoint -> Release order, got %#v", got)
	}

	if !reflect.DeepEqual(l.State(), refState) {
		t.Fatalf("loop state = %#v, want %#v", l.State(), refState)
	}
}

// TestLoop_DriverAgnostic is issue #69's third acceptance criterion: the
// SAME Loop code hosts a leader-driver job and a message-passing-driver job
// — only the injected Driver differs.
func TestLoop_DriverAgnostic(t *testing.T) {
	t.Run("leader", func(t *testing.T) {
		start := leader.Super{Superstep: 0, Assigns: map[leader.FollowerID][]byte{"f1": []byte("w1"), "f2": []byte("w2")}}
		rec := &RecordingExecutor{}
		l := NewLoop(LeaderDriver{}, start, rec, nil)

		if _, err := l.Handle(context.Background(), Event{Kind: EventReport, Follower: "f1", Result: []byte("r1")}, 0); err != nil {
			t.Fatalf("Handle f1: %v", err)
		}
		if len(rec.Snapshot()) != 0 {
			t.Fatalf("expected no commands before the round completes, got %#v", rec.Snapshot())
		}

		if _, err := l.Handle(context.Background(), Event{Kind: EventReport, Follower: "f2", Result: []byte("r2")}, 0); err != nil {
			t.Fatalf("Handle f2: %v", err)
		}

		got := rec.Snapshot()
		want := []Command{
			{Op: OpFold, Results: map[leader.FollowerID][]byte{"f1": []byte("r1"), "f2": []byte("r2")}},
			{Op: OpAdvance, Superstep: 1},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("executed commands = %#v, want %#v", got, want)
		}
	})

	t.Run("messagepassing", func(t *testing.T) {
		rec := &RecordingExecutor{}
		l := NewLoop(MessagePassingDriver{}, MessagePassingState{}, rec, nil)

		msg := messagepassing.Message{ID: "m1", From: "sender", To: "actor1", Body: []byte("hi")}
		if _, err := l.Handle(context.Background(), Event{Kind: EventMessage, Message: msg}, 0); err != nil {
			t.Fatalf("Handle: %v", err)
		}

		got := rec.Snapshot()
		if len(got) != 1 || got[0].Op != OpSend {
			t.Fatalf("executed commands = %#v, want one OpSend", got)
		}
		if got[0].Send.To != "sender" || got[0].Send.ID != "ack:m1" {
			t.Fatalf("Send = %#v, want an ack to sender", got[0].Send)
		}

		state, _ := l.State().(MessagePassingState)
		if _, ok := state.Actors["actor1"]; !ok {
			t.Fatalf("actor1 missing from loop state: %#v", state)
		}
	})
}

// TestLoop_NoCommandsNoApplyNoExec asserts that an event which folds into
// new state without producing a command neither replicates (Apply) nor
// executes (Exec) — Loop.Handle's doc comment names this explicitly.
func TestLoop_NoCommandsNoApplyNoExec(t *testing.T) {
	applyCalls := 0
	rec := &RecordingExecutor{}
	l := NewLoop(BarrierDriver{}, barrier.State{Members: []barrier.WorkerID{"a", "b"}}, rec,
		func(cmds []Command) error { applyCalls++; return nil })

	if _, err := l.Handle(context.Background(), Event{Kind: EventDone, Worker: "a", Partial: []byte("pa")}, 0); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if applyCalls != 0 {
		t.Fatalf("Apply called %d times, want 0 (only one of two members reported)", applyCalls)
	}
	if len(rec.Snapshot()) != 0 {
		t.Fatalf("Exec recorded %#v, want none", rec.Snapshot())
	}
}

// TestLoop_ApplyReplicatesCommandLog asserts Apply is called with exactly
// the commands Step produced, before Exec runs.
func TestLoop_ApplyReplicatesCommandLog(t *testing.T) {
	var applied []Command
	rec := &RecordingExecutor{}
	l := NewLoop(BarrierDriver{}, barrier.State{Members: []barrier.WorkerID{"a"}}, rec,
		func(cmds []Command) error { applied = append(applied, cmds...); return nil })

	if _, err := l.Handle(context.Background(), Event{Kind: EventDone, Worker: "a", Partial: []byte("pa")}, 0); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	want := []Command{{Op: OpAllReduce, Partials: map[barrier.WorkerID][]byte{"a": []byte("pa")}}, {Op: OpRelease, Step: 1}}
	if !reflect.DeepEqual(applied, want) {
		t.Fatalf("applied = %#v, want %#v", applied, want)
	}
	if !reflect.DeepEqual(rec.Snapshot(), want) {
		t.Fatalf("executed = %#v, want %#v", rec.Snapshot(), want)
	}
}
