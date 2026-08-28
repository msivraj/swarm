package cell

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/leader"
)

// TestResume_Barrier_Failover is issue #69's headline acceptance criterion:
// feed a replicated command LOG + a last CHECKPOINT to a fresh leader's
// Resume and assert the rebuilt state equals the pre-loss state, and the
// loop continues from the right step.
//
// The scenario: a barrier with K=2 runs through step 0's completion (which
// checkpoints), takes a checkpoint snapshot there, then continues through
// step 1's completion and an eviction on step 2 before the leader is lost.
// Resume is fed only the checkpoint taken at step 0 plus the command log
// produced AFTER it — never the live state — and must land on exactly the
// state the original, uninterrupted run reached.
func TestResume_Barrier_Failover(t *testing.T) {
	driver := BarrierDriver{}
	state := barrier.State{Step: 0, K: 2, Members: []barrier.WorkerID{"a", "b", "c"}}

	apply := func(cmds []Command, log *[]Command) { *log = append(*log, cmds...) }

	// Drive step 0 to completion: checkpoints (K=2, step 0 % 2 == 0).
	var fullLog []Command
	events := []Event{
		{Kind: EventDone, Worker: "a", Partial: []byte("pa0")},
		{Kind: EventDone, Worker: "b", Partial: []byte("pb0")},
		{Kind: EventDone, Worker: "c", Partial: []byte("pc0")}, // completes step 0 -> Checkpoint{0}, Release{1}
	}
	var cmds []Command
	for _, ev := range events {
		var s any
		s, cmds = driver.Step(state, ev, 0)
		state = s.(barrier.State)
		apply(cmds, &fullLog)
	}

	// Take the checkpoint here — the "last checkpoint" a fresh leader would
	// have available after the loss below.
	ckpt := checkpoint.State{
		Step:       state.Step,
		DriverBlob: driver.Snapshot(state),
	}
	preCheckpointState := state

	// Continue past the checkpoint: step 1 completes (no cadence checkpoint,
	// 1 % 2 != 0), then a Lost event on step 2 evicts "b" and rolls back.
	var logAfterCheckpoint []Command
	postEvents := []Event{
		{Kind: EventDone, Worker: "a", Partial: []byte("pa1")},
		{Kind: EventDone, Worker: "b", Partial: []byte("pb1")},
		{Kind: EventDone, Worker: "c", Partial: []byte("pc1")}, // completes step 1 -> Release{2}
		{Kind: EventLost, Worker: "b"},                         // rolls back to LastCheckpoint (step 0... wait see below)
	}
	for _, ev := range postEvents {
		var s any
		s, cmds = driver.Step(state, ev, 0)
		state = s.(barrier.State)
		apply(cmds, &logAfterCheckpoint)
	}
	preLossState := state // the state the original leader held right before it was lost

	// The new leader never saw preLossState directly — only the checkpoint
	// and the log recorded after it.
	rebuilt := driver.Resume(logAfterCheckpoint, ckpt)

	if !reflect.DeepEqual(rebuilt, preLossState) {
		t.Fatalf("Resume rebuilt state = %#v, want the pre-loss state %#v", rebuilt, preLossState)
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
	gotCmds, err := resumedLoop.Handle(context.Background(), nextEvent, 0)
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
