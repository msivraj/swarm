package cell

import (
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// fastRaftConfig returns a raft.Config tuned for a fast, in-process test:
// short election/heartbeat timeouts so leader election and failover
// converge in well under a second, and a discarded log sink so the test
// output isn't drowned in raft's own chatter.
func fastRaftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.LogOutput = io.Discard
	return cfg
}

// buildInmemCluster builds n InmemNodes, fully connects their in-memory
// transports, and bootstraps them as a single cluster with every node a
// voter — issue #69's acceptance criterion: "Use raft's in-memory
// transport/store for the test."
func buildInmemCluster(t *testing.T, n int) []*InmemNode {
	t.Helper()
	nodes := make([]*InmemNode, 0, n)
	for i := 0; i < n; i++ {
		node, err := NewInmemNode(string(rune('A'+i)), fastRaftConfig())
		if err != nil {
			t.Fatalf("NewInmemNode: %v", err)
		}
		nodes = append(nodes, node)
	}
	ConnectInmemCluster(nodes)
	if err := BootstrapInmemCluster(nodes); err != nil {
		t.Fatalf("BootstrapInmemCluster: %v", err)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.Raft.Shutdown().Error()
		}
	})
	return nodes
}

// waitForLeader polls nodes until exactly one reports raft.Leader, or fails
// the test after timeout.
func waitForLeader(t *testing.T, nodes []*InmemNode, timeout time.Duration) *InmemNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*InmemNode
		for _, n := range nodes {
			if n.Raft.State() == raft.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within %s", timeout)
	return nil
}

// TestRaftElection_ThreeNodes is issue #69's fourth acceptance criterion: a
// 3-node in-process cluster elects exactly one leader; killing the leader
// elects a new one that resumes.
func TestRaftElection_ThreeNodes(t *testing.T) {
	nodes := buildInmemCluster(t, 3)

	first := waitForLeader(t, nodes, 5*time.Second)

	// Replicate a command log entry through the elected leader, so the
	// surviving followers' FSMs have something to have caught up on before
	// the leader is killed.
	apply := ApplyFunc(first.Raft, time.Second)
	if err := apply([]Command{{Op: OpRelease, Step: 1}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if err := first.Raft.Shutdown().Error(); err != nil {
		t.Fatalf("Shutdown leader: %v", err)
	}

	var remaining []*InmemNode
	for _, n := range nodes {
		if n.ID != first.ID {
			remaining = append(remaining, n)
		}
	}

	second := waitForLeader(t, remaining, 5*time.Second)
	if second.ID == first.ID {
		t.Fatalf("new leader %s is the same as the killed leader", second.ID)
	}

	// The new leader "resumes": its FSM replicated the command log applied
	// before the failover. Raft applies committed entries to the FSM
	// asynchronously, so the entry may not have propagated at the instant the
	// node becomes leader — poll until it catches up rather than reading once
	// (which races under -race on a loaded CI runner).
	deadline := time.Now().Add(5 * time.Second)
	var log []Command
	for time.Now().Before(deadline) {
		if log = second.FSM.Log(); len(log) == 1 && log[0].Op == OpRelease && log[0].Step == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("new leader's replicated log = %#v, want the pre-failover entry", log)
}
