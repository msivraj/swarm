package controlplane

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/msivraj/swarm/internal/shell/transport"
)

// GlobalRouterDialer opens a connection to the P1 global routing layer's
// GlobalRouter service, used by the publish loop (PublishSummary,
// GetGlobalView) and by the roll-up path for a global-sink job
// (ReportPartial). Tests supply one backed by an in-process (bufconn) fake
// GlobalRouter; production uses GRPCGlobalRouterDialer. This mirrors
// internal/shell/agent's GlobalViewDialer seam.
type GlobalRouterDialer func(ctx context.Context, target string) (transport.GlobalRouterClient, io.Closer, error)

// GRPCGlobalRouterDialer is the GlobalRouterDialer the shell uses in
// production: a plaintext gRPC dial, matching agent.GRPCDialer's trust
// assumptions (P0/P1 assume trusted machines).
func GRPCGlobalRouterDialer() GlobalRouterDialer {
	return func(_ context.Context, target string) (transport.GlobalRouterClient, io.Closer, error) {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return transport.NewGlobalRouterClient(conn), conn, nil
	}
}

// PeerDialer opens a connection to a peer region's ControlPlane service —
// used both to forward a spilled task (DispatchTasks) and, at the spill's
// origin, to receive that task's raw result back (an inbound
// ControlPlane.ReportResult call the peer makes using this same dialer
// shape). Tests supply one backed by an in-process (bufconn) fake
// ControlPlane; production uses GRPCPeerDialer.
type PeerDialer func(ctx context.Context, target string) (transport.ControlPlaneClient, io.Closer, error)

// GRPCPeerDialer is the PeerDialer the shell uses in production: a plaintext
// gRPC dial, matching agent.GRPCDialer's trust assumptions.
func GRPCPeerDialer() PeerDialer {
	return func(_ context.Context, target string) (transport.ControlPlaneClient, io.Closer, error) {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return transport.NewControlPlaneClient(conn), conn, nil
	}
}
