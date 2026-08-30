// This file is the P1 exit-criterion end-to-end test (ticket #47, S5): two
// regional control planes (S2, internal/shell/controlplane) plus one global
// routing layer (S3, internal/shell/global) plus real agents (S4,
// internal/shell/agent), all wired over bufconn with real worker processes.
// It mirrors e2e_test.go's shape (buildWorker, testClock, fastConfig,
// bufconn dial, waitFor) but extends it to a three-server topology: instead
// of e2e_test.go's single bufconn listener (whose dialer ignores the target
// string because there is only ever one server to reach), dialRegistry below
// keys a listener per dial address so every P1 dialer type (agent.Dialer,
// controlplane.PeerDialer/GlobalRouterDialer, global.RegionDialer,
// agent.GlobalViewDialer — all the same func(ctx, target) shape) can reach
// the right one of two regions or the global router.
//
// No new production code: every behavior below is driven through S1-S4's
// existing public shell surfaces. The one exception, documented at its call
// site (TestTightCouplingJobStaysInOneRegion), mirrors an established
// pattern already in this codebase (internal/shell/controlplane/
// regional_test.go's TestSpillNeverCrossesForTightCoupling): P0's
// admission.Admit rejects any non-Independent Coupling outright (a
// documented P0 limitation, unrelated to and unchanged by S1-S4), so a
// tight-coupling job can only ever reach a region's queue via DispatchTasks
// directly — never through SubmitJob/Submit — until a later phase adds a
// driver for it.
package e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/core/routing"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/agent"
	"github.com/msivraj/swarm/internal/shell/controlplane"
	"github.com/msivraj/swarm/internal/shell/global"
	"github.com/msivraj/swarm/internal/shell/store"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// Fixed region identities and bufconn dial addresses for every test in this
// file: two regions and one global router, keyed by these constant strings
// throughout.
const (
	regionUSEast = model.RegionID("us-east")
	regionEUWest = model.RegionID("eu-west")

	addrUSEast = "us-east-cp"
	addrEUWest = "eu-west-cp"
	addrGlobal = "global-router"
)

// advance moves clock forward by d. Unlike e2e_test.go's P0 tests (whose
// testClock never needs to move — the reaper/mitosis decisions they drive
// are timing-insensitive within the test), the staleness check behind
// criterion 4 (routing.StalenessWindow, a fixed 30s of injected Instant
// nanoseconds) needs the clock to actually advance — deterministically, with
// no real-time sleeping — mirroring controlplane_test.go's and
// global_test.go's own testClock.advance.
func (c *testClock) advance(d time.Duration) {
	c.ns.Add(int64(d))
}

// --- multi-server bufconn wiring ------------------------------------------

// dialRegistry lets every P1 dialer reach any other in-process server in
// this file's topology by its dial address — the extension e2e_test.go's
// single-listener newControlPlane does not need (its dialer connects to the
// one bufconn listener regardless of the target string it receives).
type dialRegistry struct {
	mu  sync.Mutex
	lis map[string]*bufconn.Listener
}

func newDialRegistry() *dialRegistry {
	return &dialRegistry{lis: make(map[string]*bufconn.Listener)}
}

// register allocates a fresh bufconn listener and records it under addr.
func (r *dialRegistry) register(addr string) *bufconn.Listener {
	lis := bufconn.Listen(bufSize)
	r.mu.Lock()
	r.lis[addr] = lis
	r.mu.Unlock()
	return lis
}

// dial opens a gRPC connection to the listener registered under addr.
func (r *dialRegistry) dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	r.mu.Lock()
	lis, ok := r.lis[addr]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("dialRegistry: no listener registered for %q", addr)
	}
	_ = ctx
	return grpc.NewClient("passthrough:///"+addr,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// controlPlaneDial is shaped like agent.Dialer, controlplane.PeerDialer, and
// global.RegionDialer alike (all func(ctx, target) (transport.ControlPlaneClient,
// io.Closer, error)) — an unnamed function value with that signature is
// assignable to any of those three named types, so this one method serves
// all three call sites below.
func (r *dialRegistry) controlPlaneDial(ctx context.Context, addr string) (transport.ControlPlaneClient, io.Closer, error) {
	conn, err := r.dial(ctx, addr)
	if err != nil {
		return nil, nil, err
	}
	return transport.NewControlPlaneClient(conn), conn, nil
}

// globalRouterDial is shaped like controlplane.GlobalRouterDialer and
// agent.GlobalViewDialer alike; see controlPlaneDial's doc.
func (r *dialRegistry) globalRouterDial(ctx context.Context, addr string) (transport.GlobalRouterClient, io.Closer, error) {
	conn, err := r.dial(ctx, addr)
	if err != nil {
		return nil, nil, err
	}
	return transport.NewGlobalRouterClient(conn), conn, nil
}

// startRegion starts a controlplane.Server for cfg (which must already carry
// RegionID and any P1 fields the test needs — GlobalRouter/SummaryInterval/
// PeerTargets/SelfAddress) at addr, registers it in reg so its peers and the
// global layer can dial it, and returns a client for the test's own RPCs,
// the *Server (so a test can call the returned stop func to simulate region
// loss mid-test), and that stop func. t.Cleanup handles final teardown
// regardless of whether the test already called stop.
func startRegion(t *testing.T, reg *dialRegistry, addr string, cfg controlplane.Config, clock *testClock) (transport.ControlPlaneClient, func()) {
	t.Helper()

	cfg.PeerDialer = reg.controlPlaneDial
	cfg.GlobalRouterDialer = reg.globalRouterDial

	lis := reg.register(addr)
	srv := controlplane.New(store.NewMemStore(), cfg, clock.now)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	client, closer, err := reg.controlPlaneDial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial region %s: %v", cfg.RegionID, err)
	}

	var stopOnce sync.Once
	stop := func() { stopOnce.Do(srv.Stop) }

	t.Cleanup(func() {
		_ = closer.Close()
		stop()
		if err := <-serveErr; err != nil && err != grpc.ErrServerStopped {
			t.Errorf("region %s Serve: %v", cfg.RegionID, err)
		}
	})
	return client, stop
}

// startGlobalRouter starts a global.Server for cfg at addr, registers it in
// reg, and returns a client for the test's own RPCs.
func startGlobalRouter(t *testing.T, reg *dialRegistry, addr string, cfg global.Config, clock *testClock) transport.GlobalRouterClient {
	t.Helper()

	cfg.RegionDialer = reg.controlPlaneDial

	lis := reg.register(addr)
	srv := global.New(store.NewMemStore(), cfg, clock.now)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	client, closer, err := reg.globalRouterDial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial global router: %v", err)
	}

	t.Cleanup(func() {
		_ = closer.Close()
		srv.Stop()
		if err := <-serveErr; err != nil && err != grpc.ErrServerStopped {
			t.Errorf("global router Serve: %v", err)
		}
	})
	return client
}

// regionConfig returns a controlplane.Config for regionID wired for P1
// regional mode: fastConfig's short reaper/mitosis intervals, an explicit
// DefaultCellCapacity (so a test's predicted Free — capacity minus joined
// agents — is exact rather than riding on DefaultConfig's own default), a
// short summary publish cadence, and this region's one peer target.
func regionConfig(regionID model.RegionID, cellCapacity int, selfAddr string, peer model.RegionID, peerAddr string) controlplane.Config {
	cfg := fastConfig()
	cfg.RegionID = regionID
	cfg.DefaultCellCapacity = cellCapacity
	cfg.GlobalRouter = addrGlobal
	cfg.SummaryInterval = 10 * time.Millisecond
	cfg.SelfAddress = selfAddr
	cfg.PeerTargets = map[model.RegionID]string{peer: peerAddr}
	return cfg
}

// startSingleRegionAgent joins an Agent to the ControlPlane at addr only —
// no cross-region failover configured — running argv (if any) for every
// task it pulls. Mirrors e2e_test.go's startAgent, adjusted for
// dialRegistry's address-keyed dialer (in place of the single-listener
// "bufnet" placeholder startAgent uses, which newControlPlane's dialer
// ignores).
func startSingleRegionAgent(t *testing.T, reg *dialRegistry, id string, region model.RegionID, addr string, argv []string) {
	t.Helper()

	a := agent.New(agent.Config{
		AgentID:           id,
		Region:            string(region),
		Caps:              1,
		Targets:           []string{addr},
		Dialer:            reg.controlPlaneDial,
		Jitter:            func() float64 { return 0 },
		HeartbeatInterval: 20 * time.Millisecond,
		PullInterval:      10 * time.Millisecond,
		Process:           agent.ProcessSpec{Argv: argv},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("agent %s Run: %v", id, err)
		}
	})
}

// startFailoverAgent joins an Agent to home via reg's dialer, configured for
// cross-region failover (RegionTargets/HomeRegion/KnownRegions/GlobalRouter),
// and returns it so a test can read EnrollCount.
func startFailoverAgent(t *testing.T, reg *dialRegistry, id string, home model.RegionID, known []model.RegionID, targets map[model.RegionID]string, argv []string) *agent.Agent {
	t.Helper()

	a := agent.New(agent.Config{
		AgentID:            id,
		Region:             string(home),
		Caps:               1,
		Dialer:             reg.controlPlaneDial,
		Jitter:             func() float64 { return 0 },
		HeartbeatInterval:  20 * time.Millisecond,
		PullInterval:       10 * time.Millisecond,
		RegionTargets:      targets,
		HomeRegion:         home,
		KnownRegions:       known,
		GlobalRouter:       addrGlobal,
		GlobalViewDialer:   reg.globalRouterDial,
		GlobalViewInterval: 10 * time.Millisecond,
		Process:            agent.ProcessSpec{Argv: argv},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("agent %s Run: %v", id, err)
		}
	})
	return a
}

// --- wire conversions (GetGlobalView response -> model.RegionView) --------

// regionViewsFromProto converts a GetGlobalView response's regions to the
// []model.RegionView the pure routing core reasons over — the same
// projection controlplane's publishOnce and global's GetGlobalView handler
// each do on their own side of the wire, done here so a test can call
// routing.Decide directly against a real, live global view.
func regionViewsFromProto(rvs []*transport.RegionView) []model.RegionView {
	out := make([]model.RegionView, 0, len(rvs))
	for _, rv := range rvs {
		out = append(out, model.RegionView{
			ID:     model.RegionID(rv.GetId()),
			Free:   int(rv.GetFree()),
			Cells:  int(rv.GetCells()),
			Health: healthFromProto(rv.GetHealth()),
		})
	}
	return out
}

func healthFromProto(h transport.Health) model.Health {
	switch h {
	case transport.Health_HEALTH_DEGRADED:
		return model.Degraded
	case transport.Health_HEALTH_UNREACHABLE:
		return model.Unreachable
	default:
		return model.Healthy
	}
}

// toProtoJobSpec and toProtoTasks mirror controlplane.toProtoJobSpec/
// toProtoTasks and global.toProtoJobSpec/toProtoTasks (unexported in both
// packages, so this file carries its own copy) — needed only by
// TestTightCouplingJobStaysInOneRegion, which calls DispatchTasks directly.
func toProtoJobSpec(spec model.JobSpec) *transport.JobSpec {
	return &transport.JobSpec{
		Id:       string(spec.ID),
		Template: spec.Template,
		Coupling: toProtoCoupling(spec.Coupling),
		Params:   spec.Params,
	}
}

func toProtoCoupling(c model.Coupling) transport.Coupling {
	switch c {
	case model.Barrier:
		return transport.Coupling_COUPLING_BARRIER
	case model.Leader:
		return transport.Coupling_COUPLING_LEADER
	case model.MessagePassing:
		return transport.Coupling_COUPLING_MESSAGE_PASSING
	default:
		return transport.Coupling_COUPLING_INDEPENDENT
	}
}

func toProtoTasks(tasks []model.Task) []*transport.Task {
	out := make([]*transport.Task, 0, len(tasks))
	for _, tk := range tasks {
		out = append(out, &transport.Task{
			Id:      string(tk.ID),
			JobId:   string(tk.JobID),
			Input:   tk.Input,
			Attempt: int32(tk.Attempt),
		})
	}
	return out
}

// waitForGlobalJobDone polls the global router's JobStatus until Done,
// mirroring e2e_test.go's waitForJobDone (which polls a regional
// ControlPlaneClient) for the GlobalRouterClient surface instead.
func waitForGlobalJobDone(t *testing.T, client transport.GlobalRouterClient, jobID string) *transport.JobStatusResponse {
	t.Helper()

	var status *transport.JobStatusResponse
	waitFor(t, 20*time.Second, func() bool {
		resp, err := client.JobStatus(context.Background(), &transport.JobStatusRequest{JobId: jobID})
		if err != nil {
			t.Fatalf("JobStatus: %v", err)
		}
		status = resp
		return status.GetDone()
	})
	return status
}

// waitForRegionFree polls GetGlobalView until region's reported Free equals
// want, or fails the test after timeout — used to synchronize a test with
// the publish loop's wall-clock cadence before submitting a job whose route
// depends on the exact Free values regions have reported.
func waitForRegionFree(t *testing.T, client transport.GlobalRouterClient, timeout time.Duration, region model.RegionID, want int) {
	t.Helper()

	waitFor(t, timeout, func() bool {
		resp, err := client.GetGlobalView(context.Background(), &transport.GlobalViewRequest{})
		if err != nil {
			t.Fatalf("GetGlobalView: %v", err)
		}
		for _, r := range resp.GetRegions() {
			if model.RegionID(r.GetId()) == region {
				return int(r.GetFree()) == want
			}
		}
		return false
	})
}

// --- criterion 1 (headline): spread + hierarchical cross-region roll-up ---

// TestSpreadAndHierarchicalRollupAcrossRegions stands up two regions (2
// agents at us-east, 1 at eu-west) and a global router, submits an
// Independent monte-carlo job at GlobalRouter.Submit, and asserts:
//   - routing.Route spreads it (multiple healthy regions, Independent
//     coupling) proportional to each region's reported Free — a
//     DefaultCellCapacity chosen per region makes that split exact (Free 8
//     at us-east, 4 at eu-west; 6 tasks split 4/2) rather than riding on
//     any package default, and both regions' Ps().Jobs count confirms both
//     actually received (and, since a region only ever reports its partial
//     once every one of its own dispatched tasks has been pulled, executed
//     by a real worker process, and reported back — see maybeRollup's
//     distinct>=total gate — actually ran) a nonempty share.
//   - each region reports exactly one region PARTIAL upward via
//     ReportPartial — never raw per-task results: the GlobalRouter gRPC
//     service (internal/shell/transport) has no per-task result RPC at all,
//     only ReportPartial, so this is a structural guarantee of the wire
//     surface this test drives for real, not something to reverse-engineer
//     from the black box.
//   - GlobalRouter.JobStatus returns the same Aggregate expectedMCAggregate
//     computes directly (decomposing with the real templates.MonteCarloDecompose
//     and summing NextValue itself, never calling MonteCarloMerge) — proving
//     spreading the work across two real regions changed where it ran, not
//     the answer.
func TestSpreadAndHierarchicalRollupAcrossRegions(t *testing.T) {
	worker := buildWorker(t, "./workers/montecarlo", "montecarlo")
	clock := &testClock{}
	reg := newDialRegistry()

	globalClient := startGlobalRouter(t, reg, addrGlobal, global.Config{
		RegionTargets: map[model.RegionID]string{regionUSEast: addrUSEast, regionEUWest: addrEUWest},
		SelfAddress:   addrGlobal,
		DivergeSweep:  time.Hour,
	}, clock)

	usClient, _ := startRegion(t, reg, addrUSEast, regionConfig(regionUSEast, 10, addrUSEast, regionEUWest, addrEUWest), clock)
	euClient, _ := startRegion(t, reg, addrEUWest, regionConfig(regionEUWest, 5, addrEUWest, regionUSEast, addrUSEast), clock)

	startSingleRegionAgent(t, reg, "us-agent-1", regionUSEast, addrUSEast, []string{worker})
	startSingleRegionAgent(t, reg, "us-agent-2", regionUSEast, addrUSEast, []string{worker})
	startSingleRegionAgent(t, reg, "eu-agent-1", regionEUWest, addrEUWest, []string{worker})

	// DefaultCellCapacity 10 at us-east minus 2 joined agents = Free 8;
	// DefaultCellCapacity 5 at eu-west minus 1 joined agent = Free 4 — wait
	// for both to have actually propagated through the publish loop (a real
	// wall-clock timer) before submitting the job the split depends on.
	waitForRegionFree(t, globalClient, 5*time.Second, regionUSEast, 8)
	waitForRegionFree(t, globalClient, 5*time.Second, regionEUWest, 4)

	const (
		trials    = int64(600)
		blockSize = int64(100)
		baseSeed  = int64(42)
	)

	ctx := context.Background()
	submitResp, err := globalClient.Submit(ctx, &transport.SubmitJobRequest{
		Template: templates.TemplateMonteCarlo,
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{
			"trials":    strconv.FormatInt(trials, 10),
			"blockSize": strconv.FormatInt(blockSize, 10),
			"seed":      strconv.FormatInt(baseSeed, 10),
		},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	jobID := submitResp.GetJobId()
	if jobID == "" {
		t.Fatalf("Submit returned empty job id")
	}

	// Submit's Spread dispatch is synchronous (each participating region's
	// DispatchTasks is called before Submit returns), so both regions'
	// Ps().Jobs already reflect the split by the time Submit responds.
	usPs, err := usClient.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("us-east Ps: %v", err)
	}
	if usPs.GetJobs() != 1 {
		t.Fatalf("us-east Jobs = %d, want 1 (its share of the spread job)", usPs.GetJobs())
	}
	euPs, err := euClient.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("eu-west Ps: %v", err)
	}
	if euPs.GetJobs() != 1 {
		t.Fatalf("eu-west Jobs = %d, want 1 (its share of the spread job)", euPs.GetJobs())
	}

	status := waitForGlobalJobDone(t, globalClient, jobID)

	got, ok := DecodeMCAggregate(status.GetAggregate())
	if !ok {
		t.Fatalf("Aggregate = %x, not a valid 32-byte monte-carlo aggregate", status.GetAggregate())
	}
	want := expectedMCAggregate(trials, blockSize, baseSeed)
	if got.Count != want.Count {
		t.Fatalf("Count = %d, want %d", got.Count, want.Count)
	}
	const epsilon = 1e-6
	if diff := got.Sum - want.Sum; diff > epsilon || diff < -epsilon {
		t.Fatalf("Sum = %v, want %v", got.Sum, want.Sum)
	}
	if diff := got.Mean - want.Mean; diff > epsilon || diff < -epsilon {
		t.Fatalf("Mean = %v, want %v", got.Mean, want.Mean)
	}
	if diff := got.Variance - want.Variance; diff > epsilon || diff < -epsilon {
		t.Fatalf("Variance = %v, want %v", got.Variance, want.Variance)
	}
}

// --- criterion 2 (headline): region loss triggers failover ----------------

// TestRegionLossTriggersAgentFailover joins one agent to us-east, waits for
// both regions to be visible and healthy in the global view, then stops
// us-east's control plane entirely. It asserts the agent fails over via
// region.SelectRegion to eu-west, re-registers there (EnrollCount stays 1 —
// "enroll once, even across reconnects", agentreg's own headline law), and
// then actually pulls and runs a task submitted directly to eu-west — the
// fleet keeps progressing despite the region loss.
func TestRegionLossTriggersAgentFailover(t *testing.T) {
	worker := buildWorker(t, "./workers/keyspace", "keyspace")
	const targetKey = 5
	t.Setenv("SWARM_E2E_TARGET_KEY", strconv.FormatUint(targetKey, 10))

	clock := &testClock{}
	reg := newDialRegistry()

	globalClient := startGlobalRouter(t, reg, addrGlobal, global.Config{
		RegionTargets: map[model.RegionID]string{regionUSEast: addrUSEast, regionEUWest: addrEUWest},
		SelfAddress:   addrGlobal,
		DivergeSweep:  10 * time.Millisecond,
	}, clock)

	usClient, stopUSEast := startRegion(t, reg, addrUSEast, regionConfig(regionUSEast, 4, addrUSEast, regionEUWest, addrEUWest), clock)
	euClient, _ := startRegion(t, reg, addrEUWest, regionConfig(regionEUWest, 4, addrEUWest, regionUSEast, addrUSEast), clock)

	// Both regions must be observably healthy in the global view before the
	// agent's own health-cache poller can ever consider eu-west a reachable
	// failover candidate (region.SelectRegion fails a region closed if it has
	// no entry in the health map at all).
	waitFor(t, 15*time.Second, func() bool {
		resp, err := globalClient.GetGlobalView(context.Background(), &transport.GlobalViewRequest{})
		if err != nil {
			t.Fatalf("GetGlobalView: %v", err)
		}
		healthy := map[model.RegionID]bool{}
		for _, r := range resp.GetRegions() {
			healthy[model.RegionID(r.GetId())] = r.GetHealth() == transport.Health_HEALTH_HEALTHY
		}
		return healthy[regionUSEast] && healthy[regionEUWest]
	})

	regionTargets := map[model.RegionID]string{regionUSEast: addrUSEast, regionEUWest: addrEUWest}
	a := startFailoverAgent(t, reg, "failover-agent", regionUSEast, []model.RegionID{regionUSEast, regionEUWest}, regionTargets, []string{worker})

	ctx := context.Background()
	waitFor(t, 15*time.Second, func() bool {
		resp, err := usClient.Ps(ctx, &transport.PsRequest{})
		if err != nil {
			t.Fatalf("us-east Ps: %v", err)
		}
		return resp.GetMachines() == 1
	})
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("EnrollCount before failover = %d, want 1", got)
	}

	// Simulate the region loss: us-east's control plane goes away entirely.
	stopUSEast()

	// Advance the shared clock past routing.StalenessWindow (30s of injected
	// Instant nanoseconds) so the global layer's next Diverged computation
	// flags us-east's now-frozen last publish as stale — deterministically,
	// with no real-time sleeping for the 30s window itself. eu-west's publish
	// loop, still running on a real wall-clock ticker, re-publishes with a
	// fresh At stamped from the jumped clock shortly after, keeping it out of
	// the diverged set.
	clock.advance(31 * time.Second)

	waitFor(t, 30*time.Second, func() bool {
		resp, err := globalClient.GetGlobalView(context.Background(), &transport.GlobalViewRequest{})
		if err != nil {
			t.Fatalf("GetGlobalView: %v", err)
		}
		var usUnreachable, euHealthy bool
		for _, r := range resp.GetRegions() {
			switch model.RegionID(r.GetId()) {
			case regionUSEast:
				usUnreachable = r.GetHealth() == transport.Health_HEALTH_UNREACHABLE
			case regionEUWest:
				euHealthy = r.GetHealth() == transport.Health_HEALTH_HEALTHY
			}
		}
		return usUnreachable && euHealthy
	})

	// The agent's own registration loop discovers the loss on its next
	// heartbeat/dial attempt, retries with backoff (agentreg.FailoverThreshold
	// consecutive DialFail events before it emits Failover), and only then
	// asks region.SelectRegion for a new target — this is real wall-clock
	// backoff time (not sped up by the injected clock, which agentreg.Step
	// does not use for its own timing), so this waits generously for it.
	waitFor(t, 20*time.Second, func() bool {
		resp, err := euClient.Ps(ctx, &transport.PsRequest{})
		if err != nil {
			t.Fatalf("eu-west Ps: %v", err)
		}
		return resp.GetMachines() == 1
	})
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("EnrollCount after failover = %d, want 1 (enroll once, even across reconnects)", got)
	}

	// The fleet keeps progressing: a job submitted directly to eu-west after
	// failover is pulled and run by the same (now re-registered) agent.
	submitResp, err := euClient.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: templates.TemplateKeyspaceSearch,
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"start": "0", "end": "8", "shards": "4"},
	})
	if err != nil {
		t.Fatalf("eu-west SubmitJob: %v", err)
	}
	jobID := submitResp.GetJobId()
	if jobID == "" {
		t.Fatalf("eu-west SubmitJob returned empty job id")
	}

	status := waitForJobDone(t, euClient, jobID)
	key, ok := DecodeKeyspaceHit(status.GetAggregate())
	if !ok {
		t.Fatalf("Aggregate = %x, not a valid 8-byte keyspace hit", status.GetAggregate())
	}
	if key != targetKey {
		t.Fatalf("Aggregate key = %d, want %d", key, targetKey)
	}
}

// --- additional assertion: tight coupling never crosses a boundary --------

// TestTightCouplingJobStaysInOneRegion stands up two regions with unequal
// Free (so routing.Decide's pickBest tiebreak is unambiguous — us-east wins)
// and asserts a Barrier-coupled job routes To exactly one region and
// aggregates there, never touching the other.
//
// admission.Admit rejects any non-Independent Coupling outright (P0's
// documented "coupling must be Independent" rule — unrelated to and
// unchanged by S1-S4), so Submit itself cannot carry a Barrier job past
// admission; the only way a tight-coupling job reaches a region's queue
// today is DispatchTasks directly, exactly the pattern
// internal/shell/controlplane/regional_test.go's own
// TestSpillNeverCrossesForTightCoupling already establishes for the
// single-region spill case. This test drives the real, pure
// routing.Decide against the real two-region GlobalView (fetched live via
// GetGlobalView) to pick the one region, then dispatches the whole task set
// there via the real DispatchTasks RPC — proving the region-boundary
// guarantee end to end rather than working around a broken surface (nothing
// here is broken: it is P0's admission gate for tight coupling that has not
// been lifted yet, a currently out-of-scope, separately tracked gap, not an
// S1-S4 bug).
func TestTightCouplingJobStaysInOneRegion(t *testing.T) {
	worker := buildWorker(t, "./workers/montecarlo", "montecarlo")
	clock := &testClock{}
	reg := newDialRegistry()

	globalClient := startGlobalRouter(t, reg, addrGlobal, global.Config{
		RegionTargets: map[model.RegionID]string{regionUSEast: addrUSEast, regionEUWest: addrEUWest},
		SelfAddress:   addrGlobal,
		DivergeSweep:  time.Hour,
	}, clock)

	usClient, _ := startRegion(t, reg, addrUSEast, regionConfig(regionUSEast, 10, addrUSEast, regionEUWest, addrEUWest), clock)
	euClient, _ := startRegion(t, reg, addrEUWest, regionConfig(regionEUWest, 5, addrEUWest, regionUSEast, addrUSEast), clock)

	startSingleRegionAgent(t, reg, "us-agent-1", regionUSEast, addrUSEast, []string{worker})
	startSingleRegionAgent(t, reg, "eu-agent-1", regionEUWest, addrEUWest, []string{worker})

	// DefaultCellCapacity 10 - 1 agent = Free 9 at us-east; 5 - 1 = Free 4 at
	// eu-west: unequal, so pickBest's "most Free" tiebreak deterministically
	// picks us-east.
	waitForRegionFree(t, globalClient, 5*time.Second, regionUSEast, 9)
	waitForRegionFree(t, globalClient, 5*time.Second, regionEUWest, 4)

	ctx := context.Background()
	viewResp, err := globalClient.GetGlobalView(ctx, &transport.GlobalViewRequest{})
	if err != nil {
		t.Fatalf("GetGlobalView: %v", err)
	}
	regions := regionViewsFromProto(viewResp.GetRegions())

	const (
		trials    = int64(20)
		blockSize = int64(10)
		baseSeed  = int64(7)
	)
	spec := model.JobSpec{ID: "barrier-job-1", Template: templates.TemplateMonteCarlo, Coupling: model.Barrier}
	tasks := templates.MonteCarloDecompose(templates.MCJob{JobID: spec.ID, Trials: trials, BlockSize: blockSize, BaseSeed: baseSeed})
	if len(tasks) == 0 {
		t.Fatalf("MonteCarloDecompose produced no tasks")
	}

	route := routing.Decide(spec, regions)
	if route.Kind != routing.To {
		t.Fatalf("routing.Decide kind = %v, want To (multiple healthy regions, tight coupling never spreads)", route.Kind)
	}
	if route.Region != regionUSEast {
		t.Fatalf("routing.Decide region = %q, want %q (the region with the most Free capacity)", route.Region, regionUSEast)
	}

	targetClient, otherClient, otherRegion := usClient, euClient, regionEUWest
	if route.Region == regionEUWest {
		targetClient, otherClient, otherRegion = euClient, usClient, regionUSEast
	}

	dispatchResp, err := targetClient.DispatchTasks(ctx, &transport.DispatchTasksRequest{
		Job:        toProtoJobSpec(spec),
		Tasks:      toProtoTasks(tasks),
		ResultSink: "",
	})
	if err != nil {
		t.Fatalf("DispatchTasks: %v", err)
	}
	if !dispatchResp.GetAccepted() {
		t.Fatalf("DispatchTasks not accepted: %s", dispatchResp.GetReason())
	}

	status := waitForJobDone(t, targetClient, string(spec.ID))
	got, ok := DecodeMCAggregate(status.GetAggregate())
	if !ok {
		t.Fatalf("Aggregate = %x, not a valid 32-byte monte-carlo aggregate", status.GetAggregate())
	}
	want := expectedMCAggregate(trials, blockSize, baseSeed)
	if got.Count != want.Count {
		t.Fatalf("Count = %d, want %d", got.Count, want.Count)
	}
	const epsilon = 1e-6
	if diff := got.Sum - want.Sum; diff > epsilon || diff < -epsilon {
		t.Fatalf("Sum = %v, want %v", got.Sum, want.Sum)
	}

	otherPs, err := otherClient.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("%s Ps: %v", otherRegion, err)
	}
	if otherPs.GetJobs() != 0 {
		t.Fatalf("%s Jobs = %d, want 0 — the Barrier job must never cross the region boundary", otherRegion, otherPs.GetJobs())
	}
}

// --- additional assertion: global view convergence -------------------------

// TestGlobalViewConvergesAndFlagsDivergedRegion asserts GetGlobalView
// reflects both regions once each has published, and that once us-east
// stops publishing (its control plane goes away) and the shared clock
// crosses routing.StalenessWindow relative to its last publish, GetGlobalView
// flags it in Diverged and downgrades its reported Health to Unreachable —
// while eu-west, which keeps publishing with a fresh At, stays out of both.
func TestGlobalViewConvergesAndFlagsDivergedRegion(t *testing.T) {
	clock := &testClock{}
	reg := newDialRegistry()

	globalClient := startGlobalRouter(t, reg, addrGlobal, global.Config{
		RegionTargets: map[model.RegionID]string{regionUSEast: addrUSEast, regionEUWest: addrEUWest},
		SelfAddress:   addrGlobal,
		DivergeSweep:  10 * time.Millisecond,
	}, clock)

	_, stopUSEast := startRegion(t, reg, addrUSEast, regionConfig(regionUSEast, 8, addrUSEast, regionEUWest, addrEUWest), clock)
	_, _ = startRegion(t, reg, addrEUWest, regionConfig(regionEUWest, 8, addrEUWest, regionUSEast, addrUSEast), clock)

	ctx := context.Background()
	waitFor(t, 15*time.Second, func() bool {
		resp, err := globalClient.GetGlobalView(ctx, &transport.GlobalViewRequest{})
		if err != nil {
			t.Fatalf("GetGlobalView: %v", err)
		}
		if len(resp.GetDiverged()) != 0 {
			return false
		}
		healthy := map[model.RegionID]bool{}
		for _, r := range resp.GetRegions() {
			healthy[model.RegionID(r.GetId())] = r.GetHealth() == transport.Health_HEALTH_HEALTHY
		}
		return healthy[regionUSEast] && healthy[regionEUWest]
	})

	stopUSEast()
	clock.advance(31 * time.Second)

	waitFor(t, 30*time.Second, func() bool {
		resp, err := globalClient.GetGlobalView(ctx, &transport.GlobalViewRequest{})
		if err != nil {
			t.Fatalf("GetGlobalView: %v", err)
		}
		diverged := resp.GetDiverged()
		if len(diverged) != 1 || model.RegionID(diverged[0]) != regionUSEast {
			return false
		}
		var usUnreachable, euHealthy bool
		for _, r := range resp.GetRegions() {
			switch model.RegionID(r.GetId()) {
			case regionUSEast:
				usUnreachable = r.GetHealth() == transport.Health_HEALTH_UNREACHABLE
			case regionEUWest:
				euHealthy = r.GetHealth() == transport.Health_HEALTH_HEALTHY
			}
		}
		return usUnreachable && euHealthy
	})
}
