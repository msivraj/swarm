package transport

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// stubStatusServer is a trivial ControlPlane server: it does no gang logic (that
// belongs to internal/shell/controlplane), it only acknowledges ReportCellStatus
// so this smoke test proves the generated client/server stubs for the new
// upward RPC round-trip over gRPC.
type stubStatusServer struct {
	UnimplementedControlPlaneServer
	got *CellStatusRequest
}

func (s *stubStatusServer) ReportCellStatus(_ context.Context, r *CellStatusRequest) (*CellStatusResponse, error) {
	s.got = r
	return &CellStatusResponse{Accepted: true}, nil
}

// TestReportCellStatusRoundTrip exercises one bufconn round-trip of the new
// leader->control-plane status RPC (issue #121).
func TestReportCellStatusRoundTrip(t *testing.T) {
	const bufSize = 1 << 20
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	stub := &stubStatusServer{}
	RegisterControlPlaneServer(srv, stub)

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

	c := NewControlPlaneClient(conn)
	req := &CellStatusRequest{JobId: "job-9", AgentId: "a-0", Stalled: true, Have: 1, Need: 3}
	resp, err := c.ReportCellStatus(context.Background(), req)
	if err != nil || !resp.Accepted {
		t.Fatalf("ReportCellStatus round-trip: resp=%+v err=%v", resp, err)
	}
	if stub.got == nil || stub.got.JobId != "job-9" || !stub.got.Stalled || stub.got.Have != 1 || stub.got.Need != 3 {
		t.Fatalf("server did not observe the request fields: %+v", stub.got)
	}
}
