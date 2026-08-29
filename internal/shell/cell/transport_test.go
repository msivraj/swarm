package cell

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// fakeFollower is an in-process CellLeader server standing in for a job
// worker: it records every AssignWork/DeliverMessage call it receives, the
// only two RPCs a leader ever calls out to a follower with (see
// transportexec.go).
type fakeFollower struct {
	transport.UnimplementedCellLeaderServer

	mu       sync.Mutex
	assigns  []*transport.AssignWorkRequest
	delivers []*transport.DeliverMessageRequest
}

func (f *fakeFollower) AssignWork(_ context.Context, req *transport.AssignWorkRequest) (*transport.AssignWorkResponse, error) {
	f.mu.Lock()
	f.assigns = append(f.assigns, req)
	f.mu.Unlock()
	return &transport.AssignWorkResponse{Accepted: true}, nil
}

func (f *fakeFollower) DeliverMessage(_ context.Context, req *transport.DeliverMessageRequest) (*transport.DeliverMessageResponse, error) {
	f.mu.Lock()
	f.delivers = append(f.delivers, req)
	f.mu.Unlock()
	return &transport.DeliverMessageResponse{Ok: true}, nil
}

// dialBufconn starts srv on an in-process bufconn listener and returns a
// client connected to it plus a cleanup func.
func dialBufconn(t *testing.T, srv transport.CellLeaderServer) transport.CellLeaderClient {
	t.Helper()
	const bufSize = 1 << 20
	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	transport.RegisterCellLeaderServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return transport.NewCellLeaderClient(conn)
}

// TestServerAndTransportExecutor_BarrierEndToEnd wires issue #69's
// acceptance criterion "in-process fake follower set" end to end over real
// gRPC (bufconn): a cell.Server (the leader's inbound RPC surface) hosts a
// Loop over BarrierDriver; two followers call StepReport in, and once the
// step completes the loop's TransportExecutor calls AssignWork back out to
// a fake follower server for the next step.
func TestServerAndTransportExecutor_BarrierEndToEnd(t *testing.T) {
	follower := &fakeFollower{}
	followerClient := dialBufconn(t, follower)

	members := []string{"a", "b"}
	exec := &TransportExecutor{
		JobID:   "job-1",
		Dial:    func(string) (transport.CellLeaderClient, error) { return followerClient, nil },
		Members: func() []string { return members },
	}

	loop := NewLoop(BarrierDriver{}, barrier.State{K: 0, Members: []barrier.WorkerID{"a", "b"}}, exec, nil)
	leader := NewServer(loop, DriverBarrier, func() model.Instant { return 0 })
	leaderClient := dialBufconn(t, leader)

	ctx := context.Background()
	if _, err := leaderClient.StepReport(ctx, &transport.StepReportRequest{JobId: "job-1", Worker: "a", Step: 0, Payload: []byte("pa")}); err != nil {
		t.Fatalf("StepReport a: %v", err)
	}
	follower.mu.Lock()
	gotBeforeComplete := len(follower.assigns)
	follower.mu.Unlock()
	if gotBeforeComplete != 0 {
		t.Fatalf("AssignWork called before the step completed: %d calls", gotBeforeComplete)
	}

	if _, err := leaderClient.StepReport(ctx, &transport.StepReportRequest{JobId: "job-1", Worker: "b", Step: 0, Payload: []byte("pb")}); err != nil {
		t.Fatalf("StepReport b: %v", err)
	}

	follower.mu.Lock()
	assigns := append([]*transport.AssignWorkRequest(nil), follower.assigns...)
	follower.mu.Unlock()
	if len(assigns) != len(members) {
		t.Fatalf("AssignWork calls = %d, want %d (one per member of Release{1})", len(assigns), len(members))
	}
	for _, a := range assigns {
		if a.GetStep() != 1 {
			t.Fatalf("AssignWork step = %d, want 1", a.GetStep())
		}
	}

	if bs, ok := loop.State().(barrier.State); !ok || bs.Step != 1 {
		t.Fatalf("loop state = %#v, want Step=1", loop.State())
	}

	if _, ok := leader.LastSeen("b"); !ok {
		t.Fatalf("StepReport did not record liveness for worker b")
	}
}

// TestServer_DeliverMessage feeds an inbound DeliverMessage RPC through the
// leader's Server into a message-passing Loop, asserting the loop folds it
// via messagepassing.React (issue #69: "DeliverMessage for message-passing").
func TestServer_DeliverMessage(t *testing.T) {
	rec := &RecordingExecutor{}
	loop := NewLoop(MessagePassingDriver{}, MessagePassingState{}, rec, nil)
	leader := NewServer(loop, DriverMessagePassing, func() model.Instant { return 0 })
	client := dialBufconn(t, leader)

	ctx := context.Background()
	resp, err := client.DeliverMessage(ctx, &transport.DeliverMessageRequest{
		JobId: "job-1", ToActor: "actor1", MessageId: "m1", Payload: []byte("hi"),
	})
	if err != nil || !resp.GetOk() {
		t.Fatalf("DeliverMessage: resp=%+v err=%v", resp, err)
	}

	got := rec.Snapshot()
	if len(got) != 1 || got[0].Op != OpSend || got[0].Send.ID != "ack:m1" {
		t.Fatalf("executed commands = %#v, want one ack Send", got)
	}
}

// TestServer_MemberHeartbeat records liveness (issue #69: "MemberHeartbeat
// feeding liveness").
func TestServer_MemberHeartbeat(t *testing.T) {
	loop := NewLoop(BarrierDriver{}, barrier.State{}, &RecordingExecutor{}, nil)
	var now model.Instant = 42
	leader := NewServer(loop, DriverBarrier, func() model.Instant { return now })
	client := dialBufconn(t, leader)

	if _, err := client.MemberHeartbeat(context.Background(), &transport.MemberHeartbeatRequest{JobId: "job-1", Worker: "a"}); err != nil {
		t.Fatalf("MemberHeartbeat: %v", err)
	}

	seen, ok := leader.LastSeen("a")
	if !ok || seen != now {
		t.Fatalf("LastSeen(a) = (%v, %v), want (%v, true)", seen, ok, now)
	}
}
