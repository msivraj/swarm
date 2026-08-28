package agent

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// Region IDs and dial addresses used across the tests below. The addresses
// are opaque strings, not real host:ports — multiDialer routes on them the
// same way Config.RegionTargets would in production, just against bufconn
// listeners instead of real sockets.
const (
	regionHome  model.RegionID = "region-home"
	regionPeer1 model.RegionID = "region-peer1"
	regionPeer2 model.RegionID = "region-peer2"

	addrHome  = "addr-home"
	addrPeer1 = "addr-peer1"
	addrPeer2 = "addr-peer2"
)

// --- fake GlobalRouter ----------------------------------------------------

// fakeGlobalRouter is an in-process stand-in for the P1 global routing
// layer's GetGlobalView RPC. It exists purely to feed execFailover's health
// map; S1-S3 (the real global layer) are out of scope for #46, which tests
// against a fake view instead.
type fakeGlobalRouter struct {
	transport.UnimplementedGlobalRouterServer

	mu       sync.Mutex
	regions  []*transport.RegionView
	diverged []string
}

func newFakeGlobalRouter() *fakeGlobalRouter {
	return &fakeGlobalRouter{}
}

func (f *fakeGlobalRouter) GetGlobalView(context.Context, *transport.GlobalViewRequest) (*transport.GlobalViewResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &transport.GlobalViewResponse{Regions: f.regions, Diverged: f.diverged}, nil
}

// setView replaces the health view GetGlobalView reports.
func (f *fakeGlobalRouter) setView(regions []*transport.RegionView, diverged []string) {
	f.mu.Lock()
	f.regions = regions
	f.diverged = diverged
	f.mu.Unlock()
}

// startFakeGlobalRouter runs a real gRPC server backed by fakeGlobalRouter
// over an in-process bufconn listener, and returns a GlobalViewDialer that
// connects to it.
func startFakeGlobalRouter(t *testing.T) (*fakeGlobalRouter, GlobalViewDialer) {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	fake := newFakeGlobalRouter()
	transport.RegisterGlobalRouterServer(srv, fake)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dial := func(ctx context.Context, _ string) (transport.GlobalRouterClient, io.Closer, error) {
		conn, err := grpc.NewClient("passthrough:///bufnet-global",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return transport.NewGlobalRouterClient(conn), conn, nil
	}
	return fake, dial
}

func regionView(id model.RegionID, health transport.Health) *transport.RegionView {
	return &transport.RegionView{Id: string(id), Health: health}
}

// --- multi-target control-plane dialer ------------------------------------

// startFakeControlPlaneListener is startFakeControlPlane without a
// single-target Dialer attached: it hands back the bufconn.Listener so
// multiDialer can serve several fakes — one per region — behind one Dialer,
// keyed by target string exactly like Config.RegionTargets' RegionID ->
// address map resolves in production.
func startFakeControlPlaneListener(t *testing.T) (*fakeControlPlane, *bufconn.Listener) {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	fake := newFakeControlPlane()
	transport.RegisterControlPlaneServer(srv, fake)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return fake, lis
}

// multiDialer routes a Dial by its target string to the matching bufconn
// listener in byTarget. Reachability of a given region is controlled at the
// fakeControlPlane.setUnreachable level, not by removing it from this map —
// every configured region always has a listener, matching how a real
// region's control plane keeps existing even while its network path to the
// agent is down.
func multiDialer(byTarget map[string]*bufconn.Listener) Dialer {
	return func(ctx context.Context, target string) (transport.ControlPlaneClient, io.Closer, error) {
		lis, ok := byTarget[target]
		if !ok {
			return nil, nil, status.Errorf(codes.NotFound, "no fake control plane for target %q", target)
		}
		conn, err := grpc.NewClient("passthrough:///"+target,
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return transport.NewControlPlaneClient(conn), conn, nil
	}
}

// waitJoined blocks until ch fires or timeout elapses, failing the test with
// msg in the latter case.
func waitJoined(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

// baseFailoverConfig returns the Config fields every multi-region test in
// this file shares; each test fills in Dialer, RegionTargets and
// GlobalViewDialer for its own set of fakes.
func baseFailoverConfig() Config {
	return Config{
		AgentID:            "agent-1",
		Region:             "r1",
		Caps:               1,
		Jitter:             func() float64 { return 0 },
		HeartbeatInterval:  10 * time.Millisecond,
		PullInterval:       10 * time.Millisecond,
		HomeRegion:         regionHome,
		GlobalViewInterval: 20 * time.Millisecond,
	}
}

// TestAgentMultiRegionFailoverPrefersNearestHealthyPeer covers the ticket's
// first acceptance criterion: home region unreachable, two peers healthy ->
// the agent dials the first (nearest) healthy peer and joins there.
func TestAgentMultiRegionFailoverPrefersNearestHealthyPeer(t *testing.T) {
	homeFake, homeLis := startFakeControlPlaneListener(t)
	peer1Fake, peer1Lis := startFakeControlPlaneListener(t)
	peer2Fake, peer2Lis := startFakeControlPlaneListener(t)
	homeFake.setUnreachable(true)

	gr, grDial := startFakeGlobalRouter(t)
	gr.setView([]*transport.RegionView{
		regionView(regionHome, transport.Health_HEALTH_UNREACHABLE),
		regionView(regionPeer1, transport.Health_HEALTH_HEALTHY),
		regionView(regionPeer2, transport.Health_HEALTH_HEALTHY),
	}, nil)

	cfg := baseFailoverConfig()
	cfg.Dialer = multiDialer(map[string]*bufconn.Listener{
		addrHome:  homeLis,
		addrPeer1: peer1Lis,
		addrPeer2: peer2Lis,
	})
	cfg.KnownRegions = []model.RegionID{regionHome, regionPeer1, regionPeer2}
	cfg.RegionTargets = map[model.RegionID]string{
		regionHome:  addrHome,
		regionPeer1: addrPeer1,
		regionPeer2: addrPeer2,
	}
	cfg.GlobalRouter = "global"
	cfg.GlobalViewDialer = grDial

	a := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	waitJoined(t, peer1Fake.joined, 10*time.Second,
		"timed out waiting for the agent to fail over and join the nearest healthy peer")

	if got := homeFake.joinCount(); got != 0 {
		t.Fatalf("home join calls = %d, want 0 (home was unreachable from the start)", got)
	}
	if got := peer2Fake.joinCount(); got != 0 {
		t.Fatalf("peer2 join calls = %d, want 0 (peer1 is nearer and healthy)", got)
	}
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count = %d, want 1", got)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestAgentMultiRegionHomePreferredWhenReachable covers the ticket's second
// acceptance criterion: with home healthy, homeFirst keeps the agent on
// home — no spurious failover even though a healthy peer exists.
func TestAgentMultiRegionHomePreferredWhenReachable(t *testing.T) {
	homeFake, homeLis := startFakeControlPlaneListener(t)
	peer1Fake, peer1Lis := startFakeControlPlaneListener(t)

	gr, grDial := startFakeGlobalRouter(t)
	gr.setView([]*transport.RegionView{
		regionView(regionHome, transport.Health_HEALTH_HEALTHY),
		regionView(regionPeer1, transport.Health_HEALTH_HEALTHY),
	}, nil)

	cfg := baseFailoverConfig()
	cfg.Dialer = multiDialer(map[string]*bufconn.Listener{
		addrHome:  homeLis,
		addrPeer1: peer1Lis,
	})
	cfg.KnownRegions = []model.RegionID{regionHome, regionPeer1}
	cfg.RegionTargets = map[model.RegionID]string{
		regionHome:  addrHome,
		regionPeer1: addrPeer1,
	}
	cfg.GlobalRouter = "global"
	cfg.GlobalViewDialer = grDial

	a := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	waitJoined(t, homeFake.joined, 2*time.Second, "timed out waiting for the agent to join home")

	// Give the agent time to run several heartbeats; a spurious failover
	// would show up as a peer1 join within this window.
	time.Sleep(200 * time.Millisecond)

	if got := peer1Fake.joinCount(); got != 0 {
		t.Fatalf("peer1 join calls = %d, want 0 — home is healthy, no failover should occur", got)
	}
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count = %d, want 1", got)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestAgentMultiRegionReRegistersAfterRegionLoss covers the ticket's third
// acceptance criterion: an agent joined to home loses its home control
// plane entirely (not just one dropped heartbeat) -> it fails over via
// region.SelectRegion, re-dials the selected peer, and re-joins there, with
// EnrollCount staying 1 (enroll-once across a cross-region reconnect too).
func TestAgentMultiRegionReRegistersAfterRegionLoss(t *testing.T) {
	homeFake, homeLis := startFakeControlPlaneListener(t)
	peer1Fake, peer1Lis := startFakeControlPlaneListener(t)

	gr, grDial := startFakeGlobalRouter(t)
	gr.setView([]*transport.RegionView{
		regionView(regionHome, transport.Health_HEALTH_HEALTHY),
		regionView(regionPeer1, transport.Health_HEALTH_HEALTHY),
	}, nil)

	cfg := baseFailoverConfig()
	cfg.Dialer = multiDialer(map[string]*bufconn.Listener{
		addrHome:  homeLis,
		addrPeer1: peer1Lis,
	})
	cfg.KnownRegions = []model.RegionID{regionHome, regionPeer1}
	cfg.RegionTargets = map[model.RegionID]string{
		regionHome:  addrHome,
		regionPeer1: addrPeer1,
	}
	cfg.GlobalRouter = "global"
	cfg.GlobalViewDialer = grDial

	a := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	waitJoined(t, homeFake.joined, 2*time.Second, "timed out waiting for the initial join to home")
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count after initial join = %d, want 1", got)
	}

	// Lose the whole home region, not just one heartbeat: drop the current
	// connection and make every subsequent dial to home fail too, so the
	// registration core is forced through a real cross-region failover
	// rather than a same-region reconnect.
	homeFake.forceReconnect()
	homeFake.setUnreachable(true)

	waitJoined(t, peer1Fake.joined, 15*time.Second,
		"timed out waiting for the agent to fail over and re-register with peer1 after losing home")

	if got := homeFake.joinCount(); got != 1 {
		t.Fatalf("home join calls = %d, want 1 (only the original join)", got)
	}
	if got := peer1Fake.joinCount(); got != 1 {
		t.Fatalf("peer1 join calls = %d, want 1", got)
	}
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count after region failover = %d, want 1 — identity must survive a cross-region failover", got)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestAgentMultiRegionAllUnreachableRetriesHomeThenRecovers covers the
// ticket's fourth acceptance criterion: region.SelectRegion returning ""
// (no reachable target) must not crash the agent or send it to a bogus
// target — it keeps retrying whatever it was already dialing (home) — and
// it must recover once a region becomes reachable again.
func TestAgentMultiRegionAllUnreachableRetriesHomeThenRecovers(t *testing.T) {
	homeFake, homeLis := startFakeControlPlaneListener(t)
	peer1Fake, peer1Lis := startFakeControlPlaneListener(t)
	homeFake.setUnreachable(true)
	peer1Fake.setUnreachable(true)

	gr, grDial := startFakeGlobalRouter(t)
	gr.setView([]*transport.RegionView{
		regionView(regionHome, transport.Health_HEALTH_UNREACHABLE),
		regionView(regionPeer1, transport.Health_HEALTH_UNREACHABLE),
	}, nil)

	cfg := baseFailoverConfig()
	cfg.Dialer = multiDialer(map[string]*bufconn.Listener{
		addrHome:  homeLis,
		addrPeer1: peer1Lis,
	})
	cfg.KnownRegions = []model.RegionID{regionHome, regionPeer1}
	cfg.RegionTargets = map[model.RegionID]string{
		regionHome:  addrHome,
		regionPeer1: addrPeer1,
	}
	cfg.GlobalRouter = "global"
	cfg.GlobalViewDialer = grDial

	a := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	// Give the agent time to run at least one failover decision (with
	// SelectRegion returning "" every time, since nothing is reachable)
	// without crashing or joining anywhere.
	time.Sleep(3500 * time.Millisecond)
	select {
	case err := <-runErr:
		t.Fatalf("Run() exited early (%v) while every region was unreachable; want it still retrying", err)
	default:
	}
	if got := peer1Fake.joinCount(); got != 0 {
		t.Fatalf("peer1 join calls = %d, want 0 — SelectRegion had nothing reachable to offer", got)
	}
	if got := homeFake.joinCount(); got != 0 {
		t.Fatalf("home join calls = %d, want 0 — home is still unreachable at this point", got)
	}

	// Home recovers: the agent's next scheduled dial (still targeting home,
	// since execFailover never had anywhere else to send it) should
	// succeed.
	homeFake.setUnreachable(false)

	waitJoined(t, homeFake.joined, 30*time.Second,
		"timed out waiting for the agent to recover once home became reachable again")

	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count = %d, want 1", got)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestAgentMultiRegionCyclesThroughPeersOnRepeatedFailover covers the
// ticket's fifth acceptance criterion: repeated failover with multiple
// healthy-in-the-view peers walks them, not stuck retrying the first one
// forever. Home and peer1 are both actually unreachable (their real dial
// fails, even though peer1 is reported healthy), forcing a second failover
// that must move on to peer2 — which is actually reachable, so the agent
// eventually joins there.
func TestAgentMultiRegionCyclesThroughPeersOnRepeatedFailover(t *testing.T) {
	homeFake, homeLis := startFakeControlPlaneListener(t)
	peer1Fake, peer1Lis := startFakeControlPlaneListener(t)
	peer2Fake, peer2Lis := startFakeControlPlaneListener(t)
	homeFake.setUnreachable(true)
	peer1Fake.setUnreachable(true)

	gr, grDial := startFakeGlobalRouter(t)
	gr.setView([]*transport.RegionView{
		regionView(regionHome, transport.Health_HEALTH_UNREACHABLE),
		regionView(regionPeer1, transport.Health_HEALTH_HEALTHY),
		regionView(regionPeer2, transport.Health_HEALTH_HEALTHY),
	}, nil)

	cfg := baseFailoverConfig()
	cfg.Dialer = multiDialer(map[string]*bufconn.Listener{
		addrHome:  homeLis,
		addrPeer1: peer1Lis,
		addrPeer2: peer2Lis,
	})
	cfg.KnownRegions = []model.RegionID{regionHome, regionPeer1, regionPeer2}
	cfg.RegionTargets = map[model.RegionID]string{
		regionHome:  addrHome,
		regionPeer1: addrPeer1,
		regionPeer2: addrPeer2,
	}
	cfg.GlobalRouter = "global"
	cfg.GlobalViewDialer = grDial

	a := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	waitJoined(t, peer2Fake.joined, 30*time.Second,
		"timed out waiting for the agent to cycle past peer1 and join peer2")

	if got := peer1Fake.psCallCount(); got == 0 {
		t.Fatalf("peer1 was never probed; want the failover walk to have visited it before reaching peer2")
	}
	if got := homeFake.joinCount(); got != 0 {
		t.Fatalf("home join calls = %d, want 0", got)
	}
	if got := peer1Fake.joinCount(); got != 0 {
		t.Fatalf("peer1 join calls = %d, want 0 (it never became reachable)", got)
	}
	if got := peer2Fake.joinCount(); got != 1 {
		t.Fatalf("peer2 join calls = %d, want 1", got)
	}
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count = %d, want 1", got)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestAgentSingleRegionFailoverUnaffectedByMultiRegionFields is a regression
// check: with GlobalRouter left empty, execFailover must still fall back to
// wrapping over Targets exactly as P0 did, even if HomeRegion/KnownRegions/
// RegionTargets happen to be set to something (which a caller should not do,
// but the shell must not misbehave if it does).
func TestAgentSingleRegionFailoverUnaffectedByMultiRegionFields(t *testing.T) {
	fake, dial := startFakeControlPlane(t)

	a := New(Config{
		AgentID:           "agent-1",
		Region:            "r1",
		Caps:              1,
		Targets:           []string{"bufnet"},
		Dialer:            dial,
		Jitter:            func() float64 { return 0 },
		HeartbeatInterval: 20 * time.Millisecond,
		PullInterval:      10 * time.Millisecond,
		HomeRegion:        regionHome,
		KnownRegions:      []model.RegionID{regionHome, regionPeer1},
		RegionTargets:     map[model.RegionID]string{regionHome: "unused"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	waitJoined(t, fake.joined, 2*time.Second, "timed out waiting for the agent to join")

	if got := a.currentTarget(); got != "bufnet" {
		t.Fatalf("currentTarget() = %q, want %q (single-region mode must ignore RegionTargets)", got, "bufnet")
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}
