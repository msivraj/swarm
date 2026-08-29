package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/msivraj/swarm/internal/shell/transport"
)

// joinAddrControlPlane is a ControlPlane fake that records every
// JoinAgentRequest verbatim (as plain data — the generated proto struct
// embeds a mutex and must never be copied by value, see agent_test.go's
// reportedResult) and never reports a CellAssignment, so runFollower/
// runCellLeader stay parked in their poll loops rather than actually
// binding a raft node or a follower server — this test only cares about
// what execJoinCell advertises at initial registration, before any
// assignment could possibly exist.
type joinAddrControlPlane struct {
	transport.UnimplementedControlPlaneServer

	mu    sync.Mutex
	reqs  []joinAddrRequest
	first chan struct{}
	once  sync.Once
}

type joinAddrRequest struct {
	Agent          string
	RaftAddr       string
	CellLeaderAddr string
}

func newJoinAddrControlPlane() *joinAddrControlPlane {
	return &joinAddrControlPlane{first: make(chan struct{})}
}

func (f *joinAddrControlPlane) Ps(context.Context, *transport.PsRequest) (*transport.PsResponse, error) {
	return &transport.PsResponse{}, nil
}

func (f *joinAddrControlPlane) JoinAgent(_ context.Context, req *transport.JoinAgentRequest) (*transport.JoinAgentResponse, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, joinAddrRequest{
		Agent:          req.GetAgent(),
		RaftAddr:       req.GetRaftAddr(),
		CellLeaderAddr: req.GetCellLeaderAddr(),
	})
	f.mu.Unlock()
	f.once.Do(func() { close(f.first) })
	return &transport.JoinAgentResponse{CellId: "cell-0", Accepted: true}, nil
}

func (f *joinAddrControlPlane) Heartbeat(context.Context, *transport.HeartbeatRequest) (*transport.HeartbeatResponse, error) {
	return &transport.HeartbeatResponse{Ok: true}, nil
}

// CellAssignment never reports an assignment: an UnimplementedControlPlane
// embed would already do this (returning Unimplemented, which
// awaitCellAssignment treats as "not assigned yet"), but implementing it
// explicitly documents that this fake deliberately withholds one, keeping
// runFollower/runCellLeader parked in awaitCellAssignment's poll loop for
// the whole test.
func (f *joinAddrControlPlane) CellAssignment(context.Context, *transport.CellAssignmentRequest) (*transport.CellAssignmentResponse, error) {
	return &transport.CellAssignmentResponse{HasAssignment: false}, nil
}

// firstRequest blocks until JoinAgent has been called at least once (or the
// timeout passes) and returns a copy of the first request it saw.
func (f *joinAddrControlPlane) firstRequest(t *testing.T, timeout time.Duration) joinAddrRequest {
	t.Helper()
	select {
	case <-f.first:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for the initial JoinAgent call")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs[0]
}

// TestExecJoinCell_AdvertisesConfiguredAddrsAtInitialJoin is issue #109's
// regression test: a coupled-capable agent's very FIRST JoinAgent — sent
// before any CellAssignment has ever arrived — already carries its
// configured raft_addr/cell_leader_addr. Before this fix,
// execJoinCell sent only Agent/Region/Caps and
// advertiseFollower/advertiseRaftAddr populated the addresses only AFTER a
// CellAssignment, which the control plane's activateCoupledCellLocked
// (#98) cannot build without them in the first place — the circular
// dependency #109 exists to break.
func TestExecJoinCell_AdvertisesConfiguredAddrsAtInitialJoin(t *testing.T) {
	cp := newJoinAddrControlPlane()
	dial := startFakeControlPlaneServer(t, cp)

	const raftAddr = "127.0.0.1:19091"
	const followerAddr = "127.0.0.1:19092"

	a := New(Config{
		AgentID: "agent-1",
		Targets: []string{"bufnet"},
		Dialer:  dial,
		Jitter:  func() float64 { return 0 },
		Follower: FollowerConfig{
			Listen: followerAddr,
		},
		CellLeader: CellLeaderConfig{
			RaftListen: raftAddr,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	got := cp.firstRequest(t, 2*time.Second)
	if got.RaftAddr != raftAddr {
		t.Fatalf("initial JoinAgent RaftAddr = %q, want configured %q", got.RaftAddr, raftAddr)
	}
	if got.CellLeaderAddr != followerAddr {
		t.Fatalf("initial JoinAgent CellLeaderAddr = %q, want configured %q", got.CellLeaderAddr, followerAddr)
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

// TestExecJoinCell_PlainAgentAdvertisesNeitherAddr confirms the "no
// regression" half of #109: a plain P0/P1 agent — Follower.Listen and
// CellLeader.RaftListen both left at their zero value — advertises empty
// raft_addr/cell_leader_addr at JoinAgent, exactly as it did before this
// fix (execJoinCell previously never set either field at all).
func TestExecJoinCell_PlainAgentAdvertisesNeitherAddr(t *testing.T) {
	cp := newJoinAddrControlPlane()
	dial := startFakeControlPlaneServer(t, cp)

	a := New(Config{
		AgentID: "agent-1",
		Targets: []string{"bufnet"},
		Dialer:  dial,
		Jitter:  func() float64 { return 0 },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	got := cp.firstRequest(t, 2*time.Second)
	if got.RaftAddr != "" {
		t.Fatalf("initial JoinAgent RaftAddr = %q, want empty for a plain P0/P1 agent", got.RaftAddr)
	}
	if got.CellLeaderAddr != "" {
		t.Fatalf("initial JoinAgent CellLeaderAddr = %q, want empty for a plain P0/P1 agent", got.CellLeaderAddr)
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
