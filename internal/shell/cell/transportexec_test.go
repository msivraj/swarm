package cell

import (
	"context"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/leader"
	"github.com/msivraj/swarm/internal/core/messagepassing"
	"github.com/msivraj/swarm/internal/shell/transport"
)

func TestMemCheckpointStore_PutLast(t *testing.T) {
	s := NewMemCheckpointStore()
	if _, ok := s.Last("job-1"); ok {
		t.Fatalf("Last on empty store returned ok=true")
	}

	ckpt := checkpoint.State{Step: 3, Members: []string{"a", "b"}}
	if err := s.Put("job-1", ckpt); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok := s.Last("job-1")
	if !ok || !reflect.DeepEqual(got, ckpt) {
		t.Fatalf("Last = (%#v, %v), want (%#v, true)", got, ok, ckpt)
	}
}

func TestTransportExecutor_AllReduceFoldCheckpoint(t *testing.T) {
	var gotPartials map[barrier.WorkerID][]byte
	var gotResults map[string][]byte
	store := NewMemCheckpointStore()

	e := &TransportExecutor{
		JobID: "job-1",
		AllReduce: func(_ context.Context, partials map[barrier.WorkerID][]byte) error {
			gotPartials = partials
			return nil
		},
		Fold: func(_ context.Context, results map[string][]byte) error {
			gotResults = results
			return nil
		},
		Checkpoint: store,
	}

	cmds := []Command{
		{Op: OpAllReduce, Partials: map[barrier.WorkerID][]byte{"a": []byte("pa")}},
		{Op: OpFold, Results: map[leader.FollowerID][]byte{"f1": []byte("r1")}},
		{Op: OpCheckpoint, Step: 2},
	}
	if err := e.Exec(context.Background(), cmds); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	if !reflect.DeepEqual(gotPartials, map[barrier.WorkerID][]byte{"a": []byte("pa")}) {
		t.Fatalf("AllReduce got %#v", gotPartials)
	}
	if !reflect.DeepEqual(gotResults, map[string][]byte{"f1": []byte("r1")}) {
		t.Fatalf("Fold got %#v", gotResults)
	}
	ckpt, ok := store.Last("job-1")
	if !ok || ckpt.Step != 2 {
		t.Fatalf("Checkpoint stored = (%#v, %v), want Step=2", ckpt, ok)
	}
}

func TestTransportExecutor_NoHooksAreNoOps(t *testing.T) {
	e := &TransportExecutor{JobID: "job-1"}
	cmds := []Command{
		{Op: OpAllReduce},
		{Op: OpFold},
		{Op: OpCheckpoint},
		{Op: OpEvict},
		{Op: OpRollback},
		{Op: OpStall},
		{Op: OpFail},
		{Op: OpAssign},
		{Op: OpRestart},
	}
	if err := e.Exec(context.Background(), cmds); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

func TestTransportExecutor_ReassignDialsAssignWork(t *testing.T) {
	follower := &fakeFollower{}
	client := dialBufconn(t, follower)

	e := &TransportExecutor{
		JobID: "job-1",
		Dial:  func(string) (transport.CellLeaderClient, error) { return client, nil },
	}
	cmds := []Command{{Op: OpReassign, Follower: "f1", Work: []byte("w")}}
	if err := e.Exec(context.Background(), cmds); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()
	if len(follower.assigns) != 1 || follower.assigns[0].GetWorker() != "f1" {
		t.Fatalf("assigns = %#v, want one AssignWork to f1", follower.assigns)
	}
}

func TestTransportExecutor_SendDialsDeliverMessage(t *testing.T) {
	follower := &fakeFollower{}
	client := dialBufconn(t, follower)

	e := &TransportExecutor{
		JobID: "job-1",
		Dial:  func(string) (transport.CellLeaderClient, error) { return client, nil },
	}
	cmds := []Command{{Op: OpSend, Send: messagepassing.Send{To: "actor1", Body: []byte("hi"), ID: "m1"}}}
	if err := e.Exec(context.Background(), cmds); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()
	if len(follower.delivers) != 1 || follower.delivers[0].GetMessageId() != "m1" {
		t.Fatalf("delivers = %#v, want one DeliverMessage m1", follower.delivers)
	}
}
