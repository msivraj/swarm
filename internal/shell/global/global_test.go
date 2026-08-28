package global

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/store"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// This file holds test infrastructure shared by acceptance_test.go: a
// controllable clock, a bufconn-backed Server harness (mirrors
// controlplane's newTestServer), and a fake region ControlPlane that mimics
// S2's DispatchTasks/JobStatus surface (issue #45).

const bufSize = 1 << 20

// testClock is a controllable, monotonic fake clock for tests: it never
// reads time.Now, so advancing it deterministically drives the diverge-sweep
// loop and Diverged's staleness check without any wall-clock sleeping in the
// test. Mirrors controlplane's testClock.
type testClock struct {
	ns atomic.Int64
}

func (c *testClock) now() model.Instant { return model.Instant(c.ns.Load()) }
func (c *testClock) advance(d time.Duration) {
	c.ns.Add(int64(d))
}

// newTestServer starts a Server over an in-process bufconn listener and
// returns a connected GlobalRouterClient plus teardown. Mirrors
// controlplane.newTestServer's shape.
func newTestServer(t *testing.T, cfg Config, clock *testClock) (transport.GlobalRouterClient, *Server, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := New(store.NewMemStore(), cfg, clock.now)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Serve: %v", err)
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet-global",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	teardown := func() {
		_ = conn.Close()
		srv.Stop()
		wg.Wait()
	}
	return transport.NewGlobalRouterClient(conn), srv, teardown
}

// testConfig returns a Config wired to targets/dial with the background
// diverge sweep parked far in the future (tests drive GetGlobalView's own
// live recomputation directly; they don't need the sweep loop's wall-clock
// ticker racing their assertions).
func testConfig(targets map[model.RegionID]string, dial RegionDialer) Config {
	cfg := DefaultConfig()
	cfg.RegionTargets = targets
	cfg.RegionDialer = dial
	cfg.SelfAddress = "global-self-addr"
	cfg.DivergeSweep = time.Hour
	return cfg
}

// --- fake region ControlPlane -------------------------------------------

// fakeRegionControlPlane is an in-process stand-in for a region's
// ControlPlane service (S2's surface): it records every DispatchTasks call
// it receives and serves a configurable JobStatus response.
type fakeRegionControlPlane struct {
	transport.UnimplementedControlPlaneServer

	mu             sync.Mutex
	dispatchedReqs []*transport.DispatchTasksRequest
	rejectDispatch bool
	jobStatus      map[string]*transport.JobStatusResponse
}

func (f *fakeRegionControlPlane) DispatchTasks(_ context.Context, req *transport.DispatchTasksRequest) (*transport.DispatchTasksResponse, error) {
	f.mu.Lock()
	f.dispatchedReqs = append(f.dispatchedReqs, req)
	reject := f.rejectDispatch
	f.mu.Unlock()
	if reject {
		return &transport.DispatchTasksResponse{Accepted: false, Reason: "rejected by fake region"}, nil
	}
	return &transport.DispatchTasksResponse{Accepted: true}, nil
}

func (f *fakeRegionControlPlane) JobStatus(_ context.Context, req *transport.JobStatusRequest) (*transport.JobStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if resp, ok := f.jobStatus[req.GetJobId()]; ok {
		return resp, nil
	}
	return &transport.JobStatusResponse{Done: false}, nil
}

func (f *fakeRegionControlPlane) dispatched() []*transport.DispatchTasksRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*transport.DispatchTasksRequest, len(f.dispatchedReqs))
	copy(out, f.dispatchedReqs)
	return out
}

func (f *fakeRegionControlPlane) setJobStatus(jobID string, resp *transport.JobStatusResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jobStatus == nil {
		f.jobStatus = make(map[string]*transport.JobStatusResponse)
	}
	f.jobStatus[jobID] = resp
}

// startFakeRegions runs one real gRPC server per id, each backed by its own
// fakeRegionControlPlane over an in-process bufconn listener, and returns
// the fakes keyed by RegionID, a Config.RegionTargets map naming each one's
// (fake) dial address, and a RegionDialer that routes to the right bufconn
// listener by that address — unlike controlplane's regional tests (one fake
// peer, dialed regardless of target), issue #45's Spread tests need to tell
// several regions' dispatches apart by which one received them.
func startFakeRegions(t *testing.T, ids ...model.RegionID) (map[model.RegionID]*fakeRegionControlPlane, map[model.RegionID]string, RegionDialer) {
	t.Helper()

	fakes := make(map[model.RegionID]*fakeRegionControlPlane, len(ids))
	targets := make(map[model.RegionID]string, len(ids))
	listeners := make(map[string]*bufconn.Listener, len(ids))

	for _, id := range ids {
		lis := bufconn.Listen(bufSize)
		srv := grpc.NewServer()
		fake := &fakeRegionControlPlane{}
		transport.RegisterControlPlaneServer(srv, fake)

		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)

		target := "region-" + string(id) + "-addr"
		fakes[id] = fake
		targets[id] = target
		listeners[target] = lis
	}

	dial := func(ctx context.Context, target string) (transport.ControlPlaneClient, io.Closer, error) {
		lis, ok := listeners[target]
		if !ok {
			return nil, nil, fmt.Errorf("no fake region dialer registered for target %q", target)
		}
		conn, err := grpc.NewClient("passthrough:///"+target,
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return transport.NewControlPlaneClient(conn), conn, nil
	}
	return fakes, targets, dial
}

// publishSummary calls PublishSummary with a wire RegionalSummary built from
// plain values, failing the test on any error or a non-ok response.
func publishSummary(t *testing.T, ctx context.Context, client transport.GlobalRouterClient, region model.RegionID, free, cells int, health model.Health, at model.Instant) {
	t.Helper()
	resp, err := client.PublishSummary(ctx, &transport.PublishSummaryRequest{
		Summary: &transport.RegionalSummary{
			Region: string(region),
			Free:   int32(free),
			Cells:  int32(cells),
			Health: toProtoHealth(health),
			At:     int64(at),
		},
	})
	if err != nil {
		t.Fatalf("PublishSummary(%s): %v", region, err)
	}
	if !resp.GetOk() {
		t.Fatalf("PublishSummary(%s): Ok = false", region)
	}
}

// submitMonteCarlo submits a monte-carlo job with the given trial/block
// count (one task per block, since blockSize divides trials here) and
// returns the assigned JobID. Mirrors controlplane's submitMonteCarlo
// helper.
func submitMonteCarlo(t *testing.T, ctx context.Context, client transport.GlobalRouterClient, blocks int) string {
	t.Helper()
	resp, err := client.Submit(ctx, &transport.SubmitJobRequest{
		Template: "monte-carlo",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{
			"trials":    fmt.Sprintf("%d", blocks),
			"blockSize": "1",
			"seed":      "0",
		},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if resp.GetJobId() == "" {
		t.Fatalf("Submit: empty job id")
	}
	return resp.GetJobId()
}
