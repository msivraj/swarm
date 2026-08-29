package cell

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Peer is one cell agent as a raft voter. The control plane (#98) designates the
// set from the registry's cellAgents plus each agent's advertised raft_addr
// (#101); the ID doubles as the raft.ServerID and the barrier WorkerID.
type Peer struct {
	ID       string
	RaftAddr string
}

// NodeConfig configures one production raft node hosted on a cell's agent.
type NodeConfig struct {
	ID         string       // this agent's id (raft.ServerID / barrier WorkerID)
	BindAddr   string       // TCP address this node binds + advertises for raft
	Peers      []Peer       // the full cell voter set, including self
	Bootstrap  bool         // exactly one agent per cell bootstraps the cluster
	DataDir    string       // durable log/stable/snapshot store dir (raft-boltdb)
	RaftConfig *raft.Config // optional election/heartbeat overrides; nil = default
}

// Node is a production raft node: hashicorp/raft over a real TCP
// NetworkTransport with a durable raft-boltdb log+stable store and a file
// snapshot store. It wraps the SAME FSM/ApplyFunc/Command the InmemNode uses, so
// the driver Loop/Snapshot/Resume are unchanged (#94 invariant). InmemNode
// stays for the package's unit tests; this is the node the cell's agents form a
// real cluster with.
type Node struct {
	ID        string
	Raft      *raft.Raft
	FSM       *FSM
	transport *raft.NetworkTransport
	store     *raftboltdb.BoltStore
	apply     func([]Command) error
}

// NewNode builds the TCP transport + durable stores and starts raft,
// bootstrapping the cluster from cfg.Peers iff cfg.Bootstrap.
func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.ID == "" || cfg.BindAddr == "" {
		return nil, fmt.Errorf("cell: NodeConfig requires ID and BindAddr")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("cell: data dir %q: %w", cfg.DataDir, err)
	}

	rc := cfg.RaftConfig
	if rc == nil {
		rc = raft.DefaultConfig()
	}
	rc.LocalID = raft.ServerID(cfg.ID)

	advertise, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("cell: resolve %q: %w", cfg.BindAddr, err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, advertise, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("cell: tcp transport: %w", err)
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("cell: bolt store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("cell: snapshot store: %w", err)
	}

	fsm := NewFSM()
	// BoltStore satisfies both LogStore and StableStore.
	r, err := raft.NewRaft(rc, fsm, store, store, snaps, transport)
	if err != nil {
		return nil, fmt.Errorf("cell: raft: %w", err)
	}

	if cfg.Bootstrap {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(p.RaftAddr),
			})
		}
		// ErrCantBootstrap => a durable store already holds cluster state (a
		// restart, see the durability path); that is expected, not an error.
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("cell: bootstrap: %w", err)
		}
	}

	return &Node{
		ID:        cfg.ID,
		Raft:      r,
		FSM:       fsm,
		transport: transport,
		store:     store,
		apply:     ApplyFunc(r, 10*time.Second),
	}, nil
}

// Apply replicates a driver command batch through raft (== ApplyFunc); it is
// wired into Loop.Apply on the elected leader.
func (n *Node) Apply(cmds []Command) error { return n.apply(cmds) }

// Barrier blocks until every log entry committed as of this call has been
// applied to this node's own FSM (hashicorp/raft's standard "read your own
// writes" idiom). A newly-elected leader's raft LOG is guaranteed at least
// as up to date as any voter that elected it (raft's election safety
// property), but FSM.Apply — the in-memory command log Log()/Resume read —
// runs on a separate internal apply loop that can still be a step behind at
// the exact instant LeaderCh fires true. A caller that reads Log() (issue
// #100's failover: rebuilding State via Driver.Resume) immediately on
// becoming leader, without this Barrier first, can observe a log missing
// its own most recently committed entries.
func (n *Node) Barrier(timeout time.Duration) error { return n.Raft.Barrier(timeout).Error() }

// Log returns the FSM's replicated command log, for Driver.Resume.
func (n *Node) Log() []Command { return n.FSM.Log() }

// IsLeader reports whether this node currently holds raft leadership — only the
// leader runs the driver Loop.
func (n *Node) IsLeader() bool { return n.Raft.State() == raft.Leader }

// LeaderCh surfaces raft leadership transitions (acquire/lose) so #102 can start
// hosting the Loop or step down.
func (n *Node) LeaderCh() <-chan bool { return n.Raft.LeaderCh() }

// LeaderID returns the current raft leader's agent id, for follower->leader
// discovery. ok is false when there is no leader yet.
func (n *Node) LeaderID() (string, bool) {
	_, id := n.Raft.LeaderWithID()
	if id == "" {
		return "", false
	}
	return string(id), true
}

// Shutdown stops raft and releases the transport + durable store.
func (n *Node) Shutdown() error {
	if err := n.Raft.Shutdown().Error(); err != nil {
		return err
	}
	if err := n.transport.Close(); err != nil {
		return err
	}
	return n.store.Close()
}
