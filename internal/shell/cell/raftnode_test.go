package cell

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// tcpRaftConfig tightens the election/heartbeat timers so a test cluster elects
// quickly, while staying within raft's validation bounds (ElectionTimeout >=
// HeartbeatTimeout, LeaderLeaseTimeout <= HeartbeatTimeout).
func tcpRaftConfig() *raft.Config {
	c := raft.DefaultConfig()
	c.HeartbeatTimeout = 300 * time.Millisecond
	c.ElectionTimeout = 300 * time.Millisecond
	c.LeaderLeaseTimeout = 150 * time.Millisecond
	c.CommitTimeout = 20 * time.Millisecond
	return c
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// buildTCPCluster starts an n-node raft cluster over real TCP loopback and
// registers teardown. Node 0 is the designated bootstrapper carrying the full
// voter set.
func buildTCPCluster(t *testing.T, n int) []*Node {
	t.Helper()
	peers := make([]Peer, n)
	for i := range peers {
		peers[i] = Peer{ID: nodeID(i), RaftAddr: freeTCPAddr(t)}
	}
	nodes := make([]*Node, n)
	for i := range nodes {
		cfg := tcpRaftConfig()
		node, err := NewNode(NodeConfig{
			ID:         peers[i].ID,
			BindAddr:   peers[i].RaftAddr,
			Peers:      peers,
			Bootstrap:  i == 0,
			DataDir:    t.TempDir(),
			RaftConfig: cfg,
		})
		if err != nil {
			t.Fatalf("NewNode %s: %v", peers[i].ID, err)
		}
		nodes[i] = node
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Shutdown()
			}
		}
	})
	return nodes
}

func nodeID(i int) string { return string(rune('a' + i)) }

func waitForNodeLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n != nil && n.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func waitForLog(t *testing.T, n *Node, timeout time.Duration, want []Command) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []Command
	for time.Now().Before(deadline) {
		if got = n.Log(); reflect.DeepEqual(got, want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %s log = %#v, want %#v within %s", n.ID, got, want, timeout)
}

// TestProductionRaftCluster_ElectReplicateConverge: a 3-node cluster forms over
// real TCP, elects a leader, Apply replicates a batch, and every node's FSM log
// converges.
func TestProductionRaftCluster_ElectReplicateConverge(t *testing.T) {
	nodes := buildTCPCluster(t, 3)
	leader := waitForNodeLeader(t, nodes, 15*time.Second)

	want := []Command{{Op: OpRelease, Step: 1}, {Op: OpRelease, Step: 2}}
	if err := leader.Apply(want); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, n := range nodes {
		waitForLog(t, n, 15*time.Second, want)
	}

	// LeaderID agrees across the cluster.
	id, ok := nodes[1].LeaderID()
	if !ok || id != leader.ID {
		t.Fatalf("LeaderID = %q,%v; want %q", id, ok, leader.ID)
	}
}

// TestProductionRaftCluster_Failover: kill the leader; a different node is
// elected and its log still holds every pre-kill entry — the production
// analogue of the in-mem failover test.
func TestProductionRaftCluster_Failover(t *testing.T) {
	nodes := buildTCPCluster(t, 3)
	leader := waitForNodeLeader(t, nodes, 15*time.Second)

	want := []Command{{Op: OpRelease, Step: 1}}
	if err := leader.Apply(want); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, n := range nodes {
		waitForLog(t, n, 15*time.Second, want)
	}

	if err := leader.Shutdown(); err != nil {
		t.Fatalf("shutdown leader: %v", err)
	}
	remaining := make([]*Node, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.ID != leader.ID {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForNodeLeader(t, remaining, 20*time.Second)
	if newLeader.ID == leader.ID {
		t.Fatalf("new leader %s is the killed leader", newLeader.ID)
	}
	waitForLog(t, newLeader, 15*time.Second, want)
}

// TestProductionRaftNode_DurabilityAcrossRestart: a node restarted against the
// same DataDir recovers its replicated log from the durable store.
func TestProductionRaftNode_DurabilityAcrossRestart(t *testing.T) {
	addr := freeTCPAddr(t)
	dir := t.TempDir()
	peers := []Peer{{ID: "solo", RaftAddr: addr}}
	want := []Command{{Op: OpRelease, Step: 1}, {Op: OpRelease, Step: 2}}

	n1, err := NewNode(NodeConfig{ID: "solo", BindAddr: addr, Peers: peers, Bootstrap: true, DataDir: dir, RaftConfig: tcpRaftConfig()})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	waitForNodeLeader(t, []*Node{n1}, 15*time.Second)
	if err := n1.Apply(want); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	waitForLog(t, n1, 15*time.Second, want)
	if err := n1.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Restart against the same DataDir; Bootstrap=false since state is durable.
	n2, err := NewNode(NodeConfig{ID: "solo", BindAddr: addr, Peers: peers, Bootstrap: false, DataDir: dir, RaftConfig: tcpRaftConfig()})
	if err != nil {
		t.Fatalf("NewNode restart: %v", err)
	}
	t.Cleanup(func() { _ = n2.Shutdown() })
	waitForNodeLeader(t, []*Node{n2}, 15*time.Second)
	waitForLog(t, n2, 15*time.Second, want)
}
