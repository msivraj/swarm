package agent

import (
	"bytes"
	"context"
	"io"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/shell/transport"
)

// fakeControlPlane is an in-process stand-in for the real control plane,
// used to drive the agent's registration and task-runner loops end to end
// without any real network. It records what the agent did (joins,
// heartbeats, reported results) and can be told to fail the next Heartbeat
// call, simulating a dropped connection.
type fakeControlPlane struct {
	transport.UnimplementedControlPlaneServer

	mu                sync.Mutex
	joinCalls         int
	heartbeats        int
	failNextHeartbeat bool
	taskPulled        bool

	joined   chan struct{}
	reported chan reportedResult
}

// reportedResult is a plain-data copy of the ReportResultRequest fields the
// tests care about. Generated proto message structs embed a sync.Mutex
// (protoimpl.MessageState), so they must never be copied by value — this
// type is what goes over the reported channel instead.
type reportedResult struct {
	TaskID string
	Output []byte
	OK     bool
}

func newFakeControlPlane() *fakeControlPlane {
	return &fakeControlPlane{
		joined:   make(chan struct{}, 8),
		reported: make(chan reportedResult, 8),
	}
}

func (f *fakeControlPlane) Ps(context.Context, *transport.PsRequest) (*transport.PsResponse, error) {
	return &transport.PsResponse{}, nil
}

func (f *fakeControlPlane) JoinAgent(_ context.Context, req *transport.JoinAgentRequest) (*transport.JoinAgentResponse, error) {
	f.mu.Lock()
	f.joinCalls++
	f.mu.Unlock()
	f.joined <- struct{}{}
	return &transport.JoinAgentResponse{CellId: "cell-0", Accepted: true}, nil
}

func (f *fakeControlPlane) Heartbeat(context.Context, *transport.HeartbeatRequest) (*transport.HeartbeatResponse, error) {
	f.mu.Lock()
	fail := f.failNextHeartbeat
	f.failNextHeartbeat = false
	f.heartbeats++
	f.mu.Unlock()
	if fail {
		return nil, status.Error(codes.Unavailable, "simulated connection loss")
	}
	return &transport.HeartbeatResponse{Ok: true}, nil
}

func (f *fakeControlPlane) PullTask(context.Context, *transport.PullTaskRequest) (*transport.PullTaskResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.taskPulled {
		return &transport.PullTaskResponse{HasTask: false}, nil
	}
	f.taskPulled = true
	return &transport.PullTaskResponse{
		HasTask: true,
		Task:    &transport.Task{Id: "t1", JobId: "j1", Input: []byte("hello swarm")},
	}, nil
}

func (f *fakeControlPlane) ReportResult(_ context.Context, req *transport.ReportResultRequest) (*transport.ReportResultResponse, error) {
	f.reported <- reportedResult{TaskID: req.TaskId, Output: req.Output, OK: req.Ok}
	return &transport.ReportResultResponse{Accepted: true}, nil
}

// forceReconnect tells the fake to fail the next Heartbeat call, simulating
// a dropped connection.
func (f *fakeControlPlane) forceReconnect() {
	f.mu.Lock()
	f.failNextHeartbeat = true
	f.mu.Unlock()
}

func (f *fakeControlPlane) joinCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joinCalls
}

// startFakeControlPlane runs a real gRPC server backed by fakeControlPlane
// over an in-process bufconn listener, and returns a Dialer that connects to
// it. The server is stopped when the test's context is done.
func startFakeControlPlane(t *testing.T) (*fakeControlPlane, Dialer) {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	fake := newFakeControlPlane()
	transport.RegisterControlPlaneServer(srv, fake)

	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	dial := func(ctx context.Context, _ string) (transport.ControlPlaneClient, io.Closer, error) {
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
	return fake, dial
}

func requireCatOnPath(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("cat")
	if err != nil {
		t.Skipf("skipping: no `cat` on PATH: %v", err)
	}
	return path
}

// TestAgentJoinsPullsExecutesReports drives the full happy path against the
// fake control plane: the agent dials, joins, heartbeats, pulls the one
// task the fake offers, executes it as a real native process (`cat`, which
// echoes its stdin), and reports a result whose Output equals the task's
// Input and whose Ok is true.
func TestAgentJoinsPullsExecutesReports(t *testing.T) {
	catPath := requireCatOnPath(t)
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
		Process:           ProcessSpec{Argv: []string{catPath}},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

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

	if result.TaskID != "t1" {
		t.Fatalf("reported TaskID = %q, want %q", result.TaskID, "t1")
	}
	if !result.OK {
		t.Fatalf("reported OK = false, want true")
	}
	if !bytes.Equal(result.Output, []byte("hello swarm")) {
		t.Fatalf("reported Output = %q, want %q (cat should echo the task Input)", result.Output, "hello swarm")
	}

	if got := fake.joinCount(); got != 1 {
		t.Fatalf("join calls = %d, want 1 before any reconnect", got)
	}
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count = %d, want 1", got)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}

// TestAgentReconnectsWithoutReEnrolling simulates a dropped connection (a
// failed Heartbeat) and confirms the registration core drives the agent back
// through Dialing and rejoins — issuing a second JoinAgent call — without
// ever re-running Enroll. That is agentreg's "enroll once, even across
// reconnects" contract, exercised end to end.
func TestAgentReconnectsWithoutReEnrolling(t *testing.T) {
	fake, dial := startFakeControlPlane(t)

	a := New(Config{
		AgentID:           "agent-1",
		Region:            "r1",
		Caps:              1,
		Targets:           []string{"bufnet"},
		Dialer:            dial,
		Jitter:            func() float64 { return 0 },
		HeartbeatInterval: 10 * time.Millisecond,
		PullInterval:      10 * time.Millisecond,
		// No process configured: this test only exercises the registration
		// loop's reconnect path, not the task runner.
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	// Wait for the first join.
	select {
	case <-fake.joined:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the initial join")
	}
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count after initial join = %d, want 1", got)
	}

	// Drop the connection and wait for the agent to rejoin.
	fake.forceReconnect()
	select {
	case <-fake.joined:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the agent to rejoin after a simulated ConnLost")
	}

	if got := fake.joinCount(); got != 2 {
		t.Fatalf("join calls after reconnect = %d, want 2 (one per connect, including the reconnect)", got)
	}
	if got := a.EnrollCount(); got != 1 {
		t.Fatalf("enroll count after reconnect = %d, want 1 — identity must survive a reconnect", got)
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}
