package agent

import (
	"bytes"
	"context"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/e2e"
	"github.com/msivraj/swarm/internal/shell/transport"
)

const followerBufSize = 1 << 20

// bufconnListen returns a fresh in-process bufconn listener, mirroring
// agent_test.go's startFakeControlPlane (each test gets its own listener
// rather than sharing one, so tests can run in parallel without cross-talk).
func bufconnListen() *bufconn.Listener {
	return bufconn.Listen(followerBufSize)
}

// buildDisttrainingWorker compiles internal/e2e/workers/disttraining into a
// temp binary, mirroring internal/e2e's own buildWorker helper — building
// and exec'ing the real worker (rather than faking exec.Cmd) is what proves
// the follower's stdin/stdout/exit-code path end to end.
func buildDisttrainingWorker(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "disttraining")
	cmd := exec.Command("go", "build", "-o", out, "github.com/msivraj/swarm/internal/e2e/workers/disttraining")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build disttraining worker: %v\n%s", err, output)
	}
	return out
}

// fakeLeader is an in-process stand-in for the P2 per-cell leader (issue
// #102, not yet built): it dials a follower's CellLeader server to
// AssignWork steps, and hosts its own CellLeader server (StepReport only) so
// the follower can dial back — exactly the D4 no-proto dial-back path this
// ticket implements. It records every StepReport it receives.
type fakeLeader struct {
	transport.UnimplementedCellLeaderServer

	mu       sync.Mutex
	reports  []stepReport
	reportCh chan stepReport
}

type stepReport struct {
	JobID, Worker string
	Step          int32
	Payload       []byte
}

func newFakeLeader() *fakeLeader {
	return &fakeLeader{reportCh: make(chan stepReport, 64)}
}

func (f *fakeLeader) StepReport(_ context.Context, req *transport.StepReportRequest) (*transport.StepReportResponse, error) {
	r := stepReport{JobID: req.GetJobId(), Worker: req.GetWorker(), Step: req.GetStep(), Payload: append([]byte(nil), req.GetPayload()...)}
	f.mu.Lock()
	f.reports = append(f.reports, r)
	f.mu.Unlock()
	f.reportCh <- r
	return &transport.StepReportResponse{Ok: true}, nil
}

// startFakeLeader starts fakeLeader's CellLeader server on a real loopback
// TCP listener (an ephemeral port) and returns its dial address.
func startFakeLeader(t *testing.T, f *fakeLeader) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	transport.RegisterCellLeaderServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// dialCellLeader is a CellLeaderDialer that always dials target over real
// loopback TCP — used by both the follower under test (to reach fakeLeader)
// and, symmetrically, by any test that needs a raw CellLeaderClient.
func dialCellLeader(ctx context.Context, target string) (transport.CellLeaderClient, io.Closer, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return transport.NewCellLeaderClient(conn), conn, nil
}

// fakeCoupledControlPlane is a ControlPlane fake that reports a fixed
// CellAssignment (has_assignment=true, a shard, job/worker ids) and always
// accepts JoinAgent, recording the CellLeaderAddr each JoinAgent call
// advertised.
type fakeCoupledControlPlane struct {
	transport.UnimplementedControlPlaneServer

	assignment *transport.CellAssignmentResponse

	mu                sync.Mutex
	joinAddrs         []string
	cellAssignedCalls int
}

func (f *fakeCoupledControlPlane) Ps(context.Context, *transport.PsRequest) (*transport.PsResponse, error) {
	return &transport.PsResponse{}, nil
}

func (f *fakeCoupledControlPlane) JoinAgent(_ context.Context, req *transport.JoinAgentRequest) (*transport.JoinAgentResponse, error) {
	f.mu.Lock()
	f.joinAddrs = append(f.joinAddrs, req.GetCellLeaderAddr())
	f.mu.Unlock()
	return &transport.JoinAgentResponse{CellId: "cell-0", Accepted: true}, nil
}

func (f *fakeCoupledControlPlane) Heartbeat(context.Context, *transport.HeartbeatRequest) (*transport.HeartbeatResponse, error) {
	return &transport.HeartbeatResponse{Ok: true}, nil
}

func (f *fakeCoupledControlPlane) CellAssignment(context.Context, *transport.CellAssignmentRequest) (*transport.CellAssignmentResponse, error) {
	f.mu.Lock()
	f.cellAssignedCalls++
	f.mu.Unlock()
	return f.assignment, nil
}

func (f *fakeCoupledControlPlane) joinAddrCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.joinAddrs)
}

func (f *fakeCoupledControlPlane) lastJoinAddr() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.joinAddrs) == 0 {
		return ""
	}
	return f.joinAddrs[len(f.joinAddrs)-1]
}

// startFakeControlPlaneServer starts srv (any transport.ControlPlaneServer)
// over an in-process bufconn listener and returns a Dialer that connects to
// it — the same shape as startFakeControlPlane in agent_test.go, generalized
// to take any server so fakeCoupledControlPlane can reuse it.
func startFakeControlPlaneServer(t *testing.T, srv transport.ControlPlaneServer) Dialer {
	t.Helper()

	lis := bufconnListen()
	s := grpc.NewServer()
	transport.RegisterControlPlaneServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	return func(ctx context.Context, _ string) (transport.ControlPlaneClient, io.Closer, error) {
		conn, err := grpc.NewClient("passthrough:///bufnet",
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

// awaitFollowerAddr polls a.FollowerAddr() until it is bound or the deadline
// passes.
func awaitFollowerAddr(t *testing.T, a *Agent, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr, ok := a.FollowerAddr(); ok {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the follower's CellLeader server to bind")
	return ""
}

// TestFollower_StepRoundTrip drives a fake in-process leader through steps
// 0..3 against a real follower: AssignWork carries a known envelope
// (fakeLeader's dial-back address + the previous step's output as this
// step's incoming gradient), and the test asserts the follower execs the
// (fake) worker each step, StepReports the exact partial e2e.DTPartial
// computes for that shard/step/incoming, and feeds each step's output into
// the next step's stdin.
func TestFollower_StepRoundTrip(t *testing.T) {
	const start, end uint64 = 100, 137
	shard := shardInputFor(start, end)

	leader := newFakeLeader()
	leaderAddr := startFakeLeader(t, leader)

	assignment := &transport.CellAssignmentResponse{
		HasAssignment: true,
		JobId:         "job-1",
		WorkerId:      "worker-1",
		ShardInput:    shard,
	}
	cp := &fakeCoupledControlPlane{assignment: assignment}
	dial := startFakeControlPlaneServer(t, cp)

	a := New(Config{
		AgentID:      "agent-1",
		Region:       "r1",
		Caps:         1,
		Targets:      []string{"bufnet"},
		Dialer:       dial,
		Jitter:       func() float64 { return 0 },
		PullInterval: 5 * time.Millisecond,
		Follower: FollowerConfig{
			Listen: "127.0.0.1:0",
			Dialer: dialCellLeader,
			Worker: func(_ context.Context, in []byte) ([]byte, bool) {
				s, e, step, incoming, ok := e2e.DecodeDTStdin(in)
				if !ok {
					return nil, false
				}
				return e2e.EncodeGradient(e2e.DTPartial(s, e, step, incoming)), true
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	followerAddr := awaitFollowerAddr(t, a, 2*time.Second)

	leaderClient, closer, err := dialCellLeader(ctx, followerAddr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer func() { _ = closer.Close() }()

	const steps = 4
	var incoming []byte // step 0's incoming gradient is empty
	for step := int32(0); step < steps; step++ {
		payload := encodeAssignWorkPayload(leaderAddr, incoming)
		resp, err := leaderClient.AssignWork(ctx, &transport.AssignWorkRequest{
			JobId: "job-1", Worker: "worker-1", Step: step, Payload: payload,
		})
		if err != nil {
			t.Fatalf("AssignWork(step=%d): %v", step, err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("AssignWork(step=%d) Accepted = false, want true", step)
		}

		var got stepReport
		select {
		case got = <-leader.reportCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for StepReport at step %d", step)
		}

		wantIncoming, ok := e2e.DecodeGradient(incoming)
		if !ok {
			t.Fatalf("DecodeGradient(incoming) ok = false at step %d", step)
		}
		want := e2e.EncodeGradient(e2e.DTPartial(start, end, uint64(step), wantIncoming))

		if got.JobID != "job-1" || got.Worker != "worker-1" || got.Step != step {
			t.Fatalf("StepReport(step=%d) = %+v, want JobID=job-1 Worker=worker-1 Step=%d", step, got, step)
		}
		if !bytes.Equal(got.Payload, want) {
			t.Fatalf("StepReport(step=%d) payload = %x, want %x", step, got.Payload, want)
		}

		// Feed this step's output into the next step's incoming, exactly as
		// the (not-yet-built) leader is specified to.
		incoming = got.Payload
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return promptly after ctx cancellation")
	}
}

// TestFollower_AdvertisesCellLeaderAddr confirms serveFollower re-advertises
// via JoinAgent with cell_leader_addr set to the address it actually bound
// (important for a ":0" ephemeral Listen, where the configured address and
// the bound address differ).
func TestFollower_AdvertisesCellLeaderAddr(t *testing.T) {
	assignment := &transport.CellAssignmentResponse{
		HasAssignment: true,
		JobId:         "job-1",
		WorkerId:      "worker-1",
		ShardInput:    shardInputFor(0, 10),
	}
	cp := &fakeCoupledControlPlane{assignment: assignment}
	dial := startFakeControlPlaneServer(t, cp)

	a := New(Config{
		AgentID:      "agent-1",
		Targets:      []string{"bufnet"},
		Dialer:       dial,
		Jitter:       func() float64 { return 0 },
		PullInterval: 5 * time.Millisecond,
		Follower: FollowerConfig{
			Listen: "127.0.0.1:0",
			Worker: func(context.Context, []byte) ([]byte, bool) { return nil, false },
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	followerAddr := awaitFollowerAddr(t, a, 2*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cp.lastJoinAddr() == followerAddr {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := cp.lastJoinAddr(); got != followerAddr {
		t.Fatalf("last JoinAgent CellLeaderAddr = %q, want the bound follower address %q", got, followerAddr)
	}
	if got := cp.joinAddrCount(); got < 2 {
		t.Fatalf("JoinAgent calls with a CellLeaderAddr = %d, want at least 2 (initial P0 join + follower advertise)", got)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestFollower_InertWhenUnassigned confirms an agent configured to opt into
// follower mode (Follower.Listen set) but whose control plane never reports
// a CellAssignment stays fully inert: no CellLeader server is ever bound
// (dialing the configured address fails), while runRegistration and
// runRunner keep working exactly as they do for a plain P0/P1 agent.
func TestFollower_InertWhenUnassigned(t *testing.T) {
	catPath := requireCatOnPath(t)
	fake, dial := startFakeControlPlane(t) // agent_test.go's plain fake: CellAssignment is Unimplemented

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve an address: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}

	a := New(Config{
		AgentID:           "agent-1",
		Region:            "r1",
		Caps:              1,
		Targets:           []string{"bufnet"},
		Dialer:            dial,
		Jitter:            func() float64 { return 0 },
		HeartbeatInterval: 20 * time.Millisecond,
		PullInterval:      10 * time.Millisecond,
		Process:           ProcessSpec{Argv: []string{catPath}},
		Follower:          FollowerConfig{Listen: addr},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	// The P0/P1 loops still work unchanged: join, pull, execute, report.
	select {
	case <-fake.joined:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the agent to join")
	}
	var result reportedResult
	select {
	case result = <-fake.reported:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the agent to report a result")
	}
	if !result.OK {
		t.Fatalf("reported OK = false, want true")
	}

	// No CellLeader server was ever bound on the configured address: nothing
	// is listening.
	if _, ok := a.FollowerAddr(); ok {
		t.Fatalf("FollowerAddr() ok = true, want false — no assignment was ever reported")
	}
	if conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatalf("dial %s succeeded; want connection refused (no CellLeader server bound while unassigned)", addr)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestFollower_RealProcessStepRoundTrip proves the stdin/stdout/exit-code
// path with a real built binary rather than an injected fake Worker,
// mirroring internal/e2e's buildWorker pattern: the actual
// internal/e2e/workers/disttraining binary computes the partial gradient
// exactly like the fake Worker in TestFollower_StepRoundTrip does, and a
// non-zero exit (malformed shard_input) leaves AssignWork unaccepted and
// sends no StepReport.
func TestFollower_RealProcessStepRoundTrip(t *testing.T) {
	worker := buildDisttrainingWorker(t)

	const start, end uint64 = 1000, 1137
	shard := shardInputFor(start, end)

	leader := newFakeLeader()
	leaderAddr := startFakeLeader(t, leader)

	assignment := &transport.CellAssignmentResponse{
		HasAssignment: true,
		JobId:         "job-1",
		WorkerId:      "worker-1",
		ShardInput:    shard,
	}
	cp := &fakeCoupledControlPlane{assignment: assignment}
	dial := startFakeControlPlaneServer(t, cp)

	a := New(Config{
		AgentID:      "agent-1",
		Targets:      []string{"bufnet"},
		Dialer:       dial,
		Jitter:       func() float64 { return 0 },
		PullInterval: 5 * time.Millisecond,
		Process:      ProcessSpec{Argv: []string{worker}},
		Follower: FollowerConfig{
			Listen: "127.0.0.1:0",
			Dialer: dialCellLeader,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	followerAddr := awaitFollowerAddr(t, a, 2*time.Second)
	leaderClient, closer, err := dialCellLeader(ctx, followerAddr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer func() { _ = closer.Close() }()

	payload := encodeAssignWorkPayload(leaderAddr, nil)
	resp, err := leaderClient.AssignWork(ctx, &transport.AssignWorkRequest{
		JobId: "job-1", Worker: "worker-1", Step: 0, Payload: payload,
	})
	if err != nil {
		t.Fatalf("AssignWork: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("AssignWork Accepted = false, want true")
	}

	var got stepReport
	select {
	case got = <-leader.reportCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StepReport")
	}

	want := e2e.EncodeGradient(e2e.DTPartial(start, end, 0, nil))
	if !bytes.Equal(got.Payload, want) {
		t.Fatalf("StepReport payload = %x, want EncodeGradient(DTPartial(...)) = %x", got.Payload, want)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestFollower_ExecFailureNotAccepted checks a failing worker (non-zero
// exit) leaves AssignWork unaccepted and sends no StepReport — the same
// Execute -> Failed boundary runProcess enforces for the P0/P1 task runner,
// exercised here through the follower's exec-once-per-step path.
func TestFollower_ExecFailureNotAccepted(t *testing.T) {
	leader := newFakeLeader()
	leaderAddr := startFakeLeader(t, leader)

	assignment := &transport.CellAssignmentResponse{
		HasAssignment: true,
		JobId:         "job-1",
		WorkerId:      "worker-1",
		ShardInput:    shardInputFor(0, 10),
	}
	cp := &fakeCoupledControlPlane{assignment: assignment}
	dial := startFakeControlPlaneServer(t, cp)

	a := New(Config{
		AgentID:      "agent-1",
		Targets:      []string{"bufnet"},
		Dialer:       dial,
		Jitter:       func() float64 { return 0 },
		PullInterval: 5 * time.Millisecond,
		Follower: FollowerConfig{
			Listen: "127.0.0.1:0",
			Dialer: dialCellLeader,
			Worker: func(context.Context, []byte) ([]byte, bool) { return nil, false },
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	followerAddr := awaitFollowerAddr(t, a, 2*time.Second)
	leaderClient, closer, err := dialCellLeader(ctx, followerAddr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer func() { _ = closer.Close() }()

	payload := encodeAssignWorkPayload(leaderAddr, nil)
	resp, err := leaderClient.AssignWork(ctx, &transport.AssignWorkRequest{
		JobId: "job-1", Worker: "worker-1", Step: 0, Payload: payload,
	})
	if err != nil {
		t.Fatalf("AssignWork: %v", err)
	}
	if resp.GetAccepted() {
		t.Fatalf("AssignWork Accepted = true, want false after a failing exec")
	}

	select {
	case got := <-leader.reportCh:
		t.Fatalf("unexpected StepReport after a failing exec: %+v", got)
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// shardInputFor encodes a dist-training shard [start, end) exactly like
// internal/e2e.EncodeKeyspaceRange / the first 16 bytes of
// internal/e2e.EncodeDTStdin — CellAssignmentResponse.shard_input's layout.
func shardInputFor(start, end uint64) []byte {
	return e2e.EncodeDTStdin(start, end, 0, nil)[:16]
}
