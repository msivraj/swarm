// Package cell's raft.go wires hashicorp/raft: leader election among the
// cell's candidate nodes and replication of the driver's command log. The
// FSM below is deliberately minimal — Apply only appends the already-decided
// Commands to a replicated log (the real I/O for those Commands already ran
// once, on the leader that produced them, via Loop.Exec); Apply's job is
// durability and follower catch-up, not re-execution.
package cell

import (
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// FSM implements raft.FSM: it replicates the command log a Loop's Handle
// produces (via ApplyFunc below), so a newly-elected leader can call
// Driver.Resume(fsm.Log(), lastCheckpoint) to rebuild State after a leader
// loss.
type FSM struct {
	mu  sync.Mutex
	log []Command
}

var _ raft.FSM = (*FSM)(nil)

// NewFSM returns an empty FSM.
func NewFSM() *FSM { return &FSM{} }

// Apply decodes l.Data (a JSON-encoded []Command, see ApplyFunc) and appends
// it to the replicated log. It returns the number of commands appended, or
// an error if l.Data does not decode.
func (f *FSM) Apply(l *raft.Log) any {
	var cmds []Command
	if err := json.Unmarshal(l.Data, &cmds); err != nil {
		return err
	}
	f.mu.Lock()
	f.log = append(f.log, cmds...)
	f.mu.Unlock()
	return len(cmds)
}

// Log returns a copy of the replicated command log applied so far, in
// commit order — the input Driver.Resume rebuilds State from.
func (f *FSM) Log() []Command {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Command, len(f.log))
	copy(out, f.log)
	return out
}

// Snapshot returns an FSMSnapshot capturing the current log, for raft's own
// log-compaction/follower-catch-up mechanism.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{log: f.Log()}, nil
}

// Restore replaces the log with the one decoded from rc, discarding any
// prior state — required by raft.FSM's contract.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var cmds []Command
	if err := json.NewDecoder(rc).Decode(&cmds); err != nil {
		return err
	}
	f.mu.Lock()
	f.log = cmds
	f.mu.Unlock()
	return nil
}

// fsmSnapshot implements raft.FSMSnapshot over a captured command log.
type fsmSnapshot struct {
	log []Command
}

// Persist writes the snapshot's log to sink as JSON.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(s.log)
	if err != nil {
		sink.Cancel() //nolint:errcheck // best-effort cleanup after the real error
		return err
	}
	if _, err := sink.Write(b); err != nil {
		sink.Cancel() //nolint:errcheck // best-effort cleanup after the real error
		return err
	}
	return sink.Close()
}

// Release is a no-op: fsmSnapshot holds no resources beyond the captured
// slice.
func (s *fsmSnapshot) Release() {}

// ApplyFunc adapts a *raft.Raft to Loop's Apply field: it JSON-encodes cmds
// and submits them as one raft log entry via r.Apply, waiting up to timeout
// for the entry to commit.
func ApplyFunc(r *raft.Raft, timeout time.Duration) func([]Command) error {
	return func(cmds []Command) error {
		b, err := json.Marshal(cmds)
		if err != nil {
			return err
		}
		return r.Apply(b, timeout).Error()
	}
}

// InmemNode is one raft node built entirely on hashicorp/raft's in-memory
// transport and stores — for tests only (issue #69's acceptance criterion:
// "Use raft's in-memory transport/store for the test"), never for
// production, where a real transport/store belongs to a later ticket.
type InmemNode struct {
	ID        string
	Raft      *raft.Raft
	FSM       *FSM
	Transport *raft.InmemTransport
}

// NewInmemNode builds one InmemNode with id as both its raft.ServerID and
// in-memory transport address. cfg, if non-nil, overrides raft's default
// Config (tests shrink the election/heartbeat timeouts to keep the suite
// fast); a nil cfg uses raft.DefaultConfig() with LocalID set to id.
func NewInmemNode(id string, cfg *raft.Config) (*InmemNode, error) {
	_, trans := raft.NewInmemTransport(raft.ServerAddress(id))

	if cfg == nil {
		cfg = raft.DefaultConfig()
	}
	cfg.LocalID = raft.ServerID(id)

	fsm := NewFSM()
	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(cfg, fsm, logs, stable, snaps, trans)
	if err != nil {
		return nil, err
	}
	return &InmemNode{ID: id, Raft: r, FSM: fsm, Transport: trans}, nil
}

// ConnectInmemCluster fully connects every pair of nodes' in-memory
// transports (Connect is directional, so both directions of every pair are
// wired) — required before BootstrapCluster or any AppendEntries/RequestVote
// RPC can reach its peer.
func ConnectInmemCluster(nodes []*InmemNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a.ID == b.ID {
				continue
			}
			a.Transport.Connect(raft.ServerAddress(b.ID), b.Transport)
		}
	}
}

// BootstrapInmemCluster bootstraps nodes as a single cluster where every
// node is a voter, via the first node's BootstrapCluster call (any node can
// initiate; raft's bootstrap is a one-time, whole-cluster operation, not a
// per-node one).
func BootstrapInmemCluster(nodes []*InmemNode) error {
	servers := make([]raft.Server, 0, len(nodes))
	for _, n := range nodes {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(n.ID),
			Address:  raft.ServerAddress(n.ID),
		})
	}
	cfg := raft.Configuration{Servers: servers}
	return nodes[0].Raft.BootstrapCluster(cfg).Error()
}
