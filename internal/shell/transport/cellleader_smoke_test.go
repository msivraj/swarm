package transport

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// stubCellLeader is a trivial in-process CellLeader server: it does no
// coordination logic (that belongs to the per-cell leader shell), it only
// acknowledges each RPC so this smoke test proves the generated
// client/server stubs for the P2 driver surface round-trip over gRPC.
type stubCellLeader struct {
	UnimplementedCellLeaderServer
	got struct {
		assign, report, deliver, heartbeat bool
	}
}

func (s *stubCellLeader) AssignWork(_ context.Context, _ *AssignWorkRequest) (*AssignWorkResponse, error) {
	s.got.assign = true
	return &AssignWorkResponse{Accepted: true}, nil
}

func (s *stubCellLeader) StepReport(_ context.Context, _ *StepReportRequest) (*StepReportResponse, error) {
	s.got.report = true
	return &StepReportResponse{Ok: true}, nil
}

func (s *stubCellLeader) DeliverMessage(_ context.Context, _ *DeliverMessageRequest) (*DeliverMessageResponse, error) {
	s.got.deliver = true
	return &DeliverMessageResponse{Ok: true}, nil
}

func (s *stubCellLeader) MemberHeartbeat(_ context.Context, _ *MemberHeartbeatRequest) (*MemberHeartbeatResponse, error) {
	s.got.heartbeat = true
	return &MemberHeartbeatResponse{Ok: true}, nil
}

// TestCellLeaderRoundTrips exercises one round-trip per new P2 RPC against an
// in-process bufconn server, per issue #68's acceptance criterion.
func TestCellLeaderRoundTrips(t *testing.T) {
	const bufSize = 1 << 20
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	stub := &stubCellLeader{}
	RegisterCellLeaderServer(srv, stub)

	go func() {
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Serve: %v", err)
		}
	}()
	defer srv.Stop()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	c := NewCellLeaderClient(conn)
	ctx := context.Background()

	if r, err := c.AssignWork(ctx, &AssignWorkRequest{JobId: "j", Worker: "w", Step: 1, Payload: []byte("shard")}); err != nil || !r.Accepted {
		t.Fatalf("AssignWork round-trip: resp=%+v err=%v", r, err)
	}
	if r, err := c.StepReport(ctx, &StepReportRequest{JobId: "j", Worker: "w", Step: 1, Payload: []byte("partial")}); err != nil || !r.Ok {
		t.Fatalf("StepReport round-trip: resp=%+v err=%v", r, err)
	}
	if r, err := c.DeliverMessage(ctx, &DeliverMessageRequest{JobId: "j", ToActor: "a", MessageId: "m1", Payload: []byte("hi")}); err != nil || !r.Ok {
		t.Fatalf("DeliverMessage round-trip: resp=%+v err=%v", r, err)
	}
	if r, err := c.MemberHeartbeat(ctx, &MemberHeartbeatRequest{JobId: "j", Worker: "w"}); err != nil || !r.Ok {
		t.Fatalf("MemberHeartbeat round-trip: resp=%+v err=%v", r, err)
	}

	if !(stub.got.assign && stub.got.report && stub.got.deliver && stub.got.heartbeat) {
		t.Fatalf("server did not observe every RPC: %+v", stub.got)
	}
}
