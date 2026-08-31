package agent

import (
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/e2e"
	"github.com/msivraj/swarm/internal/shell/cell"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// requireCleanShutdown asserts err is either nil or some flavor of "ctx was
// cancelled" — plain context.Canceled, or a gRPC status wrapping it (an
// RPC that was still in flight, sharing the leader's dial cache, when the
// test cancelled ctx can surface either shape depending on exactly which
// call was in flight at the time). Anything else is a real failure.
func requireCleanShutdown(t *testing.T, who string, err error) {
	t.Helper()
	if err == nil || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return
	}
	t.Fatalf("%s = %v, want nil or a context-cancellation error", who, err)
}

// fakeRaftNode is a leaderRaftNode test double: it replicates Apply's
// command batches into an in-memory log (mirroring cell.FSM.Log()) and lets
// the test drive leadership transitions directly over leaderCh, standing in
// for a real raft cluster's election — the ticket's own acceptance-criteria
// phrasing, "a fake raft node reporting this agent leader."
type fakeRaftNode struct {
	mu       sync.Mutex
	log      []cell.Command
	leaderCh chan bool
}

func newFakeRaftNode() *fakeRaftNode {
	return &fakeRaftNode{leaderCh: make(chan bool, 4)}
}

func (n *fakeRaftNode) Apply(cmds []cell.Command) error {
	n.mu.Lock()
	n.log = append(n.log, cmds...)
	n.mu.Unlock()
	return nil
}

func (n *fakeRaftNode) Log() []cell.Command {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]cell.Command, len(n.log))
	copy(out, n.log)
	return out
}

func (n *fakeRaftNode) LeaderCh() <-chan bool { return n.leaderCh }

var _ leaderRaftNode = (*fakeRaftNode)(nil)

// assignedStep records one AssignWork call a fakeCellFollower received, once
// decoded through the D4 payload envelope.
type assignedStep struct {
	Step     int32
	Incoming []byte
}

// fakeCellFollower is an in-process stand-in for a coupled cell's OTHER
// members (issue #96's follower half is exercised separately, in
// follower_test.go): it implements AssignWork by decoding the D4 leader
// dial-back envelope (decodeAssignWorkPayload, follower.go — the same
// decoder the real follower uses), recording the call, and dialing back
// StepReport with a fixed per-step partial — exactly what a real follower's
// AssignWork handler does, minus the process exec.
type fakeCellFollower struct {
	transport.UnimplementedCellLeaderServer

	id      string
	dialer  CellLeaderDialer
	partial func(step int32) []byte

	mu      sync.Mutex
	assigns []assignedStep
}

func (f *fakeCellFollower) AssignWork(ctx context.Context, req *transport.AssignWorkRequest) (*transport.AssignWorkResponse, error) {
	leaderAddr, incoming, ok := decodeAssignWorkPayload(req.GetPayload())
	if !ok {
		return &transport.AssignWorkResponse{Accepted: false}, nil
	}

	f.mu.Lock()
	f.assigns = append(f.assigns, assignedStep{Step: req.GetStep(), Incoming: append([]byte(nil), incoming...)})
	f.mu.Unlock()

	client, closer, err := f.dialer(ctx, leaderAddr)
	if err != nil {
		return &transport.AssignWorkResponse{Accepted: false}, nil
	}
	defer func() { _ = closer.Close() }()

	resp, err := client.StepReport(ctx, &transport.StepReportRequest{
		JobId: req.GetJobId(), Worker: f.id, Step: req.GetStep(), Payload: f.partial(req.GetStep()),
	})
	if err != nil || !resp.GetOk() {
		return &transport.AssignWorkResponse{Accepted: false}, nil
	}
	return &transport.AssignWorkResponse{Accepted: true}, nil
}

func (f *fakeCellFollower) assignCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.assigns)
}

func (f *fakeCellFollower) assignAt(i int) assignedStep {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.assigns[i]
}

var _ transport.CellLeaderServer = (*fakeCellFollower)(nil)

// startCellLeaderServer starts srv's CellLeader service on a real loopback
// TCP listener (an ephemeral port) and returns its dial address —
// generalizes follower_test.go's startFakeLeader to any
// transport.CellLeaderServer, since this file's fakeCellFollower has a
// different concrete type.
func startCellLeaderServer(t *testing.T, srv transport.CellLeaderServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	transport.RegisterCellLeaderServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// coupledCompletion is a plain-data copy of a LeaderHost.Report call's
// arguments, delivered over a channel for tests to assert on.
type coupledCompletion struct {
	JobID    string
	Combined []byte
}

// buildFakeCell starts n fakeCellFollowers (ids "w0".."w(n-1)", each
// reporting a fixed gradient of [step+1] regardless of worker id) and
// returns them alongside the CellPeer list a CellAssignmentResponse.Peers
// would carry for them.
func buildFakeCell(t *testing.T, n int) (map[string]*fakeCellFollower, []*transport.CellPeer) {
	t.Helper()
	followers := make(map[string]*fakeCellFollower, n)
	peers := make([]*transport.CellPeer, 0, n)
	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		f := &fakeCellFollower{
			id:     id,
			dialer: dialCellLeader,
			partial: func(step int32) []byte {
				return e2e.EncodeGradient([]float64{float64(step) + 1})
			},
		}
		addr := startCellLeaderServer(t, f)
		followers[id] = f
		peers = append(peers, &transport.CellPeer{AgentId: id, CellLeaderAddr: addr})
	}
	return followers, peers
}

// combineStep returns templates.DistTrainingCombine's result for n
// followers' fixed partial at step, matching buildFakeCell's partial.
func combineStep(n int, step int32) []byte {
	gradients := make([][]byte, n)
	for i := range gradients {
		gradients[i] = e2e.EncodeGradient([]float64{float64(step) + 1})
	}
	return templates.DistTrainingCombine(gradients)
}

// TestLeaderHost_RunsBarrier is issue #102's headline acceptance criterion:
// with a fake raft node reporting this agent leader and in-process
// followers, the host builds barrier.State with the right
// Members/K/MinMembers, AssignWorks step 0, and on all-StepReports emits
// AllReduce -> Combined (== DistTrainingCombine) -> Release{next} —
// asserted on the RPCs the followers actually observed and the final
// completion report.
func TestLeaderHost_RunsBarrier(t *testing.T) {
	const steps = 2
	followers, peers := buildFakeCell(t, 3)

	assignment := &transport.CellAssignmentResponse{
		HasAssignment: true,
		JobId:         "job-1",
		K:             0,
		MinMembers:    0,
		Steps:         steps,
		Peers:         peers,
	}

	node := newFakeRaftNode()
	reportCh := make(chan coupledCompletion, 1)

	host := &LeaderHost{
		Node:       node,
		Assignment: assignment,
		Listen:     "127.0.0.1:0",
		Dialer:     dialCellLeader,
		Report: func(_ context.Context, jobID string, combined []byte) error {
			reportCh <- coupledCompletion{JobID: jobID, Combined: combined}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- host.run(ctx) }()

	node.leaderCh <- true

	var got coupledCompletion
	select {
	case got = <-reportCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the completion report")
	}

	if got.JobID != "job-1" {
		t.Fatalf("reported JobID = %q, want job-1", got.JobID)
	}
	want := combineStep(len(followers), steps-1)
	if !bytes.Equal(got.Combined, want) {
		t.Fatalf("reported combined = %x, want DistTrainingCombine(...) = %x", got.Combined, want)
	}

	// Every follower saw exactly `steps` AssignWork calls: step 0 with no
	// incoming gradient, then step 1 carrying step 0's combined gradient —
	// never a phantom (steps)th round for the terminal Release.
	step0Combined := combineStep(len(followers), 0)
	for id, f := range followers {
		if n := f.assignCount(); n != steps {
			t.Fatalf("follower %s got %d AssignWork calls, want %d", id, n, steps)
		}
		if a0 := f.assignAt(0); a0.Step != 0 || len(a0.Incoming) != 0 {
			t.Fatalf("follower %s step 0 assign = %+v, want step=0 with no incoming gradient", id, a0)
		}
		if a1 := f.assignAt(1); a1.Step != 1 || !bytes.Equal(a1.Incoming, step0Combined) {
			t.Fatalf("follower %s step 1 assign incoming = %x, want step 0's combined %x", id, a1.Incoming, step0Combined)
		}
	}

	// The completion report fires from deep inside a synchronous, nested
	// AssignWork/StepReport call chain (each step's last report cascades
	// straight into the next step's AssignWork, all still unwinding back up
	// through the original kick call at this point) — give it a moment to
	// finish unwinding before cancelling ctx, so an RPC that is still
	// in-flight (not yet a failure, just not yet returned) does not surface
	// a spurious context-canceled error out of run().
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-runErr:
		requireCleanShutdown(t, "run()", err)
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not return promptly after ctx cancellation")
	}
}

// TestLeaderHost_CheckpointHasDriverBlob is the #90 regression guard at its
// production construction site (issue #102's ticket text): a mid-run
// checkpoint must have a non-nil DriverBlob, and
// BarrierDriver.Resume(log, ckpt) must rebuild Members/K/MinMembers from it.
func TestLeaderHost_CheckpointHasDriverBlob(t *testing.T) {
	_, peers := buildFakeCell(t, 2)

	assignment := &transport.CellAssignmentResponse{
		HasAssignment: true,
		JobId:         "job-ckpt",
		K:             1, // checkpoint on every step, including step 0
		MinMembers:    0,
		Steps:         3,
		Peers:         peers,
	}

	node := newFakeRaftNode()
	store := cell.NewMemCheckpointStore()

	host := &LeaderHost{
		Node: node, Assignment: assignment, Listen: "127.0.0.1:0",
		Dialer: dialCellLeader, Checkpoint: store,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- host.run(ctx) }()
	node.leaderCh <- true

	var ok bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !ok {
		_, ok = store.Last("job-ckpt")
		if !ok {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !ok {
		t.Fatal("no checkpoint was persisted by the real write path")
	}
	ckpt, _ := store.Last("job-ckpt")
	if len(ckpt.DriverBlob) == 0 {
		t.Fatal("persisted checkpoint has an empty DriverBlob — the #90 regression is back")
	}

	rebuilt, ok := cell.BarrierDriver{}.Resume(nil, ckpt).(barrier.State)
	if !ok {
		t.Fatalf("Resume(nil, ckpt) did not decode to a barrier.State")
	}
	wantMembers := []barrier.WorkerID{"a", "b"}
	if !reflect.DeepEqual(rebuilt.Members, wantMembers) {
		t.Fatalf("rebuilt Members = %v, want %v", rebuilt.Members, wantMembers)
	}
	if rebuilt.K != 1 {
		t.Fatalf("rebuilt K = %d, want 1", rebuilt.K)
	}
	if rebuilt.MinMembers != 0 {
		t.Fatalf("rebuilt MinMembers = %d, want 0", rebuilt.MinMembers)
	}

	// Give the (near-instant, since the fake followers respond immediately)
	// cascade through the remaining steps a moment to settle before
	// cancelling, for the same reason TestLeaderHost_RunsBarrier does.
	time.Sleep(100 * time.Millisecond)
	cancel()
	requireCleanShutdown(t, "run()", <-runErr)
}

// TestLeaderHost_FailoverResumes simulates a LeaderCh transition: a second
// LeaderHost, sharing the same (fake) raft node's replicated log and the
// same checkpoint store as the first, is elected after the first loses
// leadership mid-job. The new host must Resume from Log()+checkpoint and
// continue from the same step — never repeat step 0, never skip step 1.
func TestLeaderHost_FailoverResumes(t *testing.T) {
	followers, peers := buildFakeCell(t, 2)

	assignment := &transport.CellAssignmentResponse{
		HasAssignment: true,
		JobId:         "job-failover",
		K:             1, // checkpoint step 0 before term 1 is cut off
		Steps:         2,
		Peers:         peers,
	}

	node := newFakeRaftNode()
	store := cell.NewMemCheckpointStore()

	// Term 1: becomes leader, completes+checkpoints step 0, then loses
	// leadership before step 1 completes.
	ctx1, cancel1 := context.WithCancel(context.Background())
	host1 := &LeaderHost{
		Node: node, Assignment: assignment, Listen: "127.0.0.1:0",
		Dialer: dialCellLeader, Checkpoint: store,
	}
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- host1.run(ctx1) }()
	node.leaderCh <- true

	var ok bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !ok {
		_, ok = store.Last("job-failover")
		if !ok {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !ok {
		t.Fatal("term 1 never checkpointed step 0")
	}

	node.leaderCh <- false
	cancel1()
	requireCleanShutdown(t, "host1.run()", <-runErr1)

	// Term 2: a fresh LeaderHost, the SAME node (so Log() carries term 1's
	// replicated history) and the SAME checkpoint store, is elected leader.
	reportCh := make(chan coupledCompletion, 1)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	host2 := &LeaderHost{
		Node: node, Assignment: assignment, Listen: "127.0.0.1:0",
		Dialer: dialCellLeader, Checkpoint: store,
		Report: func(_ context.Context, jobID string, combined []byte) error {
			reportCh <- coupledCompletion{JobID: jobID, Combined: combined}
			return nil
		},
	}
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- host2.run(ctx2) }()
	node.leaderCh <- true

	select {
	case got := <-reportCh:
		if got.JobID != "job-failover" {
			t.Fatalf("reported JobID = %q, want job-failover", got.JobID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for term 2 to complete the job")
	}

	// A leadership transition can interrupt term 1's synchronous
	// AssignWork->StepReport cascade at different points under -race, so the
	// exact call COUNT varies: term 2 idempotently re-kicks the step it resumes
	// on, which may repeat a step term 1 had already sent. Asserting an exact
	// count (==2) is therefore flaky. The invariants that actually matter and
	// hold every time: steps are delivered in non-decreasing order starting at
	// 0, every step 0..1 is delivered at least once (none skipped), and no
	// phantom step >= 2 ever appears.
	for id, f := range followers {
		steps := make([]int32, f.assignCount())
		for i := range steps {
			steps[i] = f.assignAt(i).Step
		}
		if len(steps) < 2 {
			t.Fatalf("follower %s got %d AssignWork calls, want >= 2 (steps 0 and 1): %v", id, len(steps), steps)
		}
		if steps[0] != 0 {
			t.Fatalf("follower %s first assign step = %d, want 0 (term 1): %v", id, steps[0], steps)
		}
		seen := map[int32]bool{}
		for i, st := range steps {
			if st < 0 || st >= 2 {
				t.Fatalf("follower %s assign %d = step %d, want in [0,2) (no phantom step): %v", id, i, st, steps)
			}
			if i > 0 && st < steps[i-1] {
				t.Fatalf("follower %s assign steps out of order: %v", id, steps)
			}
			seen[st] = true
		}
		if !seen[0] || !seen[1] {
			t.Fatalf("follower %s missing a delivered step: saw %v, want both 0 and 1", id, steps)
		}
	}

	cancel2()
	requireCleanShutdown(t, "host2.run()", <-runErr2)
}

// TestCellLeader_InertWhenUnconfigured confirms an agent with Follower mode
// configured (issue #96) but no CellLeader.RaftListen never joins a raft
// cluster or hosts a Loop: runCellLeader's 5th loop stays inert, blocking
// on ctx, and every P0/P1/#96 behavior keeps working unaffected — the
// ticket's own promise, "every existing agent test unaffected."
func TestCellLeader_InertWhenUnconfigured(t *testing.T) {
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
		// CellLeader left zero-valued: RaftListen == "".
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	// The follower half still works unaffected: it binds and advertises.
	_ = awaitFollowerAddr(t, a, 2*time.Second)

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v, want nil after ctx cancellation", err)
	}
}
