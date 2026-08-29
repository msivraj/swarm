package cell

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/leader"
	"github.com/msivraj/swarm/internal/core/messagepassing"
)

// TestResume_Barrier_Failover is issue #69's headline acceptance criterion:
// feed a replicated command LOG + a last CHECKPOINT to a fresh leader's
// Resume and assert the rebuilt state equals the pre-loss state, and the
// loop continues from the right step.
//
// The checkpoint here is produced by driving a real Loop through the
// PRODUCTION checkpoint-write path — TransportExecutor's OpCheckpoint
// handling, via a *Loop backing its State() — rather than hand-building a
// checkpoint.State with DriverBlob set directly. A hand-built checkpoint
// masked a real bug (a follow-up audit of #69 found TransportExecutor never
// called Driver.Snapshot, so the checkpoint it actually persisted in
// production had a nil DriverBlob and Resume could never recover Members/K/
// MinMembers from it): only exercising the real write path here proves that
// gap is closed and stays closed.
//
// The scenario: a barrier with K=2, MinMembers=2 runs through step 0's
// completion (which checkpoints via the real write path), then continues
// through step 1's completion and an eviction on step 2 before the leader is
// lost. Resume is fed only the checkpoint the real write path persisted plus
// the command log recorded AFTER it — never the live state — and must land
// on exactly the state the original, uninterrupted run reached, including
// the fields the command log alone cannot supply (Members, K, MinMembers).
func TestResume_Barrier_Failover(t *testing.T) {
	driver := BarrierDriver{}
	initial := barrier.State{Step: 0, K: 2, MinMembers: 2, Members: []barrier.WorkerID{"a", "b", "c"}}

	store := NewMemCheckpointStore()
	te := &TransportExecutor{JobID: "job-1", Checkpoint: store}
	rec := &RecordingExecutor{Next: te}
	loop := NewLoop(driver, initial, rec, nil)
	te.Driver = driver
	te.State = loop.State // the real production wiring: OpCheckpoint snapshots the loop's own current state

	ctx := context.Background()
	handle := func(ev Event) {
		if _, err := loop.Handle(ctx, ev, 0); err != nil {
			t.Fatalf("Handle(%+v): %v", ev, err)
		}
	}

	// Drive step 0 to completion through the real loop: checkpoints (K=2,
	// step 0 % 2 == 0) via TransportExecutor's real OpCheckpoint handling.
	handle(Event{Kind: EventDone, Worker: "a", Partial: []byte("pa0")})
	handle(Event{Kind: EventDone, Worker: "b", Partial: []byte("pb0")})
	handle(Event{Kind: EventDone, Worker: "c", Partial: []byte("pc0")}) // completes step 0 -> Checkpoint{0}, Release{1}

	ckpt, ok := store.Last("job-1")
	if !ok {
		t.Fatalf("no checkpoint was persisted by the real write path")
	}
	if len(ckpt.DriverBlob) == 0 {
		t.Fatalf("persisted checkpoint has an empty DriverBlob — the production checkpoint-write bug is back")
	}
	preCheckpointState := loop.State().(barrier.State)
	preCheckpointLogLen := len(rec.Snapshot())

	// Continue past the checkpoint: step 1 completes (no cadence checkpoint,
	// 1 % 2 != 0), then a Lost event on step 2 evicts "b" and rolls back.
	handle(Event{Kind: EventDone, Worker: "a", Partial: []byte("pa1")})
	handle(Event{Kind: EventDone, Worker: "b", Partial: []byte("pb1")})
	handle(Event{Kind: EventDone, Worker: "c", Partial: []byte("pc1")}) // completes step 1 -> Release{2}
	handle(Event{Kind: EventLost, Worker: "b"})                         // rolls back to LastCheckpoint, drops "b"

	preLossState := loop.State().(barrier.State) // the state the original leader held right before it was lost
	fullLog := rec.Snapshot()
	logAfterCheckpoint := append([]Command(nil), fullLog[preCheckpointLogLen:]...)

	// The new leader never saw preLossState directly — only the checkpoint
	// the real write path persisted, plus the log recorded after it.
	rebuilt := driver.Resume(logAfterCheckpoint, ckpt).(barrier.State)

	if !reflect.DeepEqual(rebuilt, preLossState) {
		t.Fatalf("Resume rebuilt state = %#v, want the pre-loss state %#v", rebuilt, preLossState)
	}
	// The fields the command log alone cannot supply — only the checkpoint's
	// DriverBlob can — explicitly, per the follow-up audit.
	if !reflect.DeepEqual(rebuilt.Members, preLossState.Members) {
		t.Fatalf("rebuilt Members = %v, want %v", rebuilt.Members, preLossState.Members)
	}
	if rebuilt.K != preLossState.K {
		t.Fatalf("rebuilt K = %d, want %d", rebuilt.K, preLossState.K)
	}
	if rebuilt.MinMembers != preLossState.MinMembers {
		t.Fatalf("rebuilt MinMembers = %d, want %d", rebuilt.MinMembers, preLossState.MinMembers)
	}
	// Sanity: the checkpoint alone (with no log replayed past it) is NOT
	// already equal to the pre-loss state, so this test actually exercises
	// the log replay rather than trivially matching on the checkpoint.
	if reflect.DeepEqual(preCheckpointState, preLossState) {
		t.Fatalf("test setup bug: checkpoint state already equals pre-loss state")
	}

	// The loop continues from the right step: feeding the SAME next event to
	// a loop seeded from the resumed state produces the same commands (and
	// next state) as feeding it to the original, uninterrupted state.
	nextEvent := Event{Kind: EventDone, Worker: "a", Partial: []byte("pa2")}

	wantState, wantCmds := driver.Step(preLossState, nextEvent, 0)

	resumedLoop := NewLoop(driver, rebuilt, &RecordingExecutor{}, nil)
	gotCmds, err := resumedLoop.Handle(ctx, nextEvent, 0)
	if err != nil {
		t.Fatalf("Handle after resume: %v", err)
	}
	if !reflect.DeepEqual(gotCmds, wantCmds) {
		t.Fatalf("commands after resume = %#v, want %#v", gotCmds, wantCmds)
	}
	if !reflect.DeepEqual(resumedLoop.State(), wantState) {
		t.Fatalf("state after resume = %#v, want %#v", resumedLoop.State(), wantState)
	}
}

// TestResume_MessagePassing_Failover exercises the same real-write-path
// scenario for MessagePassingDriver: the checkpoint's DriverBlob is written
// by the production TransportExecutor.OpCheckpoint path (not hand-built),
// and Resume rebuilds Actors from that DriverBlob plus the post-checkpoint
// log's FoldedActor-tagged OpSend commands (adapter_messagepassing.go's
// shell-only augmentation — untested against the real write path before this
// follow-up audit).
func TestResume_MessagePassing_Failover(t *testing.T) {
	driver := MessagePassingDriver{}

	store := NewMemCheckpointStore()
	te := &TransportExecutor{JobID: "job-1", Checkpoint: store}
	rec := &RecordingExecutor{Next: te}
	loop := NewLoop(driver, MessagePassingState{}, rec, nil)
	te.Driver = driver
	te.State = loop.State

	ctx := context.Background()
	handle := func(ev Event) {
		if _, err := loop.Handle(ctx, ev, 0); err != nil {
			t.Fatalf("Handle(%+v): %v", ev, err)
		}
	}

	// Fold a message into "actor1" before any checkpoint exists.
	handle(Event{Kind: EventMessage, Message: messagepassing.Message{ID: "m1", From: "s", To: "actor1", Body: []byte("hi")}})

	// The message-passing driver never emits OpCheckpoint itself (no global
	// step — see messagepassing's package doc), so a real deployment
	// checkpoints it on a shell-driven cadence instead of a core-emitted
	// command. Simulate that here by executing an OpCheckpoint through the
	// SAME production Exec chain (Loop.Exec, not a bypass), the way a
	// timer-triggered checkpoint would.
	if err := loop.Exec.Exec(ctx, []Command{{Op: OpCheckpoint, Step: 0}}); err != nil {
		t.Fatalf("checkpoint Exec: %v", err)
	}

	ckpt, ok := store.Last("job-1")
	if !ok {
		t.Fatalf("no checkpoint was persisted by the real write path")
	}
	if len(ckpt.DriverBlob) == 0 {
		t.Fatalf("persisted checkpoint has an empty DriverBlob — the production checkpoint-write bug is back")
	}
	preCheckpointLogLen := len(rec.Snapshot())

	// Fold two more messages into two different actors after the checkpoint.
	handle(Event{Kind: EventMessage, Message: messagepassing.Message{ID: "m2", From: "s", To: "actor1", Body: []byte("again")}})
	handle(Event{Kind: EventMessage, Message: messagepassing.Message{ID: "m3", From: "s", To: "actor2", Body: []byte("new actor")}})

	preLossState := loop.State().(MessagePassingState)
	fullLog := rec.Snapshot()
	logAfterCheckpoint := append([]Command(nil), fullLog[preCheckpointLogLen:]...)

	rebuilt := driver.Resume(logAfterCheckpoint, ckpt).(MessagePassingState)

	if !reflect.DeepEqual(rebuilt, preLossState) {
		t.Fatalf("Resume rebuilt state = %#v, want the pre-loss state %#v", rebuilt, preLossState)
	}
	// actor1's pre-checkpoint fold must survive — it can only come from the
	// checkpoint's DriverBlob, never from logAfterCheckpoint alone.
	if _, ok := rebuilt.Actors["actor1"]; !ok {
		t.Fatalf("rebuilt state lost actor1, which was folded before the checkpoint")
	}
	if _, ok := rebuilt.Actors["actor2"]; !ok {
		t.Fatalf("rebuilt state missing actor2, folded after the checkpoint")
	}
}

// TestResume_Barrier_EmptyLog checks the degenerate case: no commands were
// replicated after the checkpoint, so Resume must reproduce the checkpointed
// state exactly (Driver.Snapshot/Resume round-trip).
func TestResume_Barrier_EmptyLog(t *testing.T) {
	driver := BarrierDriver{}
	state := barrier.State{Step: 4, K: 2, Members: []barrier.WorkerID{"x", "y"}, LastCheckpoint: barrier.Checkpoint{Step: 4}}
	ckpt := checkpoint.State{Step: 4, DriverBlob: driver.Snapshot(state)}

	got := driver.Resume(nil, ckpt)
	if !reflect.DeepEqual(got, state) {
		t.Fatalf("Resume(nil, ckpt) = %#v, want %#v", got, state)
	}
}

// TestResume_Leader_Failover mirrors TestResume_Barrier_Failover for the
// leader driver: Resume from a checkpoint + post-checkpoint log reconstructs
// the same Superstep an uninterrupted run reaches.
func TestResume_Leader_Failover(t *testing.T) {
	driver := LeaderDriver{}
	state := leader.Super{Superstep: 0, Assigns: map[leader.FollowerID][]byte{"f1": []byte("w1"), "f2": []byte("w2")}}

	var s any
	var cmds []Command
	s, cmds = driver.Step(state, Event{Kind: EventReport, Follower: "f1", Result: []byte("r1")}, 0)
	state = s.(leader.Super)
	_ = cmds

	ckpt := checkpoint.State{DriverBlob: driver.Snapshot(state)}

	var logAfter []Command
	s, cmds = driver.Step(state, Event{Kind: EventReport, Follower: "f2", Result: []byte("r2")}, 0) // completes round 0 -> Advance{1}
	state = s.(leader.Super)
	logAfter = append(logAfter, cmds...)

	rebuilt := driver.Resume(logAfter, ckpt)
	if !reflect.DeepEqual(rebuilt, state) {
		t.Fatalf("Resume = %#v, want %#v", rebuilt, state)
	}
}

// TestBarrierDriver_SnapshotRoundTrip is a small law test on the adapter's
// own Snapshot: json.Unmarshal(Snapshot(s)) reproduces s, the property
// Resume's checkpoint decoding depends on.
func TestBarrierDriver_SnapshotRoundTrip(t *testing.T) {
	s := barrier.State{Step: 3, K: 2, Members: []barrier.WorkerID{"a", "b"}, LastCheckpoint: barrier.Checkpoint{Step: 2}}
	b := BarrierDriver{}.Snapshot(s)

	var got barrier.State
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip = %#v, want %#v", got, s)
	}
}
