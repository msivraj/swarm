package global

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/msivraj/swarm/internal/shell/transport"
)

// RegionDialer opens a connection to a region's ControlPlane service — used
// to dispatch a routed job's tasks (DispatchTasks) and, for a To route, to
// proxy JobStatus to the region that owns the job's completion. Tests supply
// one backed by an in-process (bufconn) fake ControlPlane; production uses
// GRPCRegionDialer. Mirrors controlplane.PeerDialer's seam.
type RegionDialer func(ctx context.Context, target string) (transport.ControlPlaneClient, io.Closer, error)

// GRPCRegionDialer is the RegionDialer the shell uses in production: a
// plaintext gRPC dial, matching controlplane.GRPCPeerDialer's trust
// assumptions (P0/P1 assume trusted machines).
func GRPCRegionDialer() RegionDialer {
	return func(_ context.Context, target string) (transport.ControlPlaneClient, io.Closer, error) {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return transport.NewControlPlaneClient(conn), conn, nil
	}
}
