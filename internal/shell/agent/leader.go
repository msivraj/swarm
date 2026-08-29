// leader.go is issue #102's load-bearing integration: the agent-hosted
// per-cell leader. A coupled cell agent (Follower.Listen configured, #96)
// that also configures CellLeader.RaftListen joins the cell's raft cluster
// as a voter (#95's cell.Node) and, whenever raft elects it, hosts the
// production driver Loop (cell.Loop, #69) that orchestrates the barrier —
// building barrier.State from the assignment's peers, kicking off
// AssignWork, folding followers' StepReports back through the loop, and
// reporting the final combined result to the control plane once the job's
// last step completes.
//
// One deliberate deviation from a literal reading of the ticket: rather
// than serving cell.Server on the SAME address this agent advertises as its
// own cell_leader_addr (Follower.Listen — the address OTHER leaders dial to
// hand this agent AssignWork), the leader half binds its own address
// (CellLeaderConfig.Listen) and embeds THAT address, fresh, in every
// AssignWork payload it sends while leader (the existing D4 dial-back
// envelope, encodeAssignWorkPayload in follower.go). JoinAgentRequest and
// CellPeer each carry exactly one cell_leader_addr per agent — the address
// at which THAT agent receives AssignWork as a follower — and D4 already
// solves "which address does a follower StepReport back to" dynamically,
// per call, without any peer needing to look up "the current leader"
// out of band. Splitting the two roles onto two addresses avoids merging
// two different RPC surfaces (follower's AssignWork, leader's
// StepReport/DeliverMessage/MemberHeartbeat) onto one grpc.Server, and
// needs no change to cell (Loop/driver/Snapshot/Resume stay untouched) or
// to follower.go.
package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/detection"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/cell"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// defaultStragglerInterval is how often watchStragglers re-checks the
// current step's not-yet-Done members against detection.Deadline when
// CellLeaderConfig/LeaderHost leaves StragglerInterval unset — frequent
// enough that a straggler is caught within a small fraction of even
// detection's fastest table entry (Core+Barrier, 2s), without adding
// meaningful overhead (the check itself is a handful of map reads).
const defaultStragglerInterval = 200 * time.Millisecond

// leaderRaftNode is the subset of *cell.Node's surface (issue #95) the
// leader host depends on: replicate a command batch, read back the
// replicated log for Resume, and watch leadership transitions. Depending on
// this narrow interface rather than *cell.Node directly lets tests
// substitute a fake raft node — the ticket's own acceptance-criteria
// phrasing — instead of standing up a real TCP raft cluster; production
// wires *cell.Node behind it unchanged (see CellLeaderConfig.NewRaftNode's
// default, below).
type leaderRaftNode interface {
	Apply(cmds []cell.Command) error
	Log() []cell.Command
	LeaderCh() <-chan bool
}

var _ leaderRaftNode = (*cell.Node)(nil)

// CellLeaderConfig configures P2 agent-hosted per-cell leadership (issue
// #102). Leaving RaftListen empty — the zero value, and the default —
// disables it entirely: runCellLeader never builds a raft.Node, never joins
// a cluster, and blocks on ctx exactly like runFollower's disabled branch —
// no behavior change for a P0/P1 agent, or for a #96 coupled-cell agent
// that has not also opted into hosting the cell's raft cluster.
type CellLeaderConfig struct {
	// RaftListen is the TCP address this agent's raft transport binds and
	// advertises (cell.NodeConfig.BindAddr, JoinAgentRequest.raft_addr).
	// Empty disables cell-leader hosting entirely. Must be a concrete
	// host:port, not ":0" — cell.NewNode advertises BindAddr literally and
	// exposes no way to discover an OS-assigned port afterward.
	RaftListen string
	// RaftDataDir is the durable raft-boltdb log/stable/snapshot directory
	// for this agent's cell.Node. Required whenever RaftListen is set.
	RaftDataDir string
	// RaftConfig optionally overrides raft's election/heartbeat timers —
	// production leaves it nil (raft.DefaultConfig()); tests shrink it for
	// a fast election.
	RaftConfig *raft.Config

	// Listen is the address the leader-hosted cell.Server binds while this
	// agent holds raft leadership, to receive followers'
	// StepReport/DeliverMessage/MemberHeartbeat calls — see the package
	// doc's note on why this is a separate address from Follower.Listen.
	// ":0" (an ephemeral port, the default) is fine: this address is never
	// looked up out of band, only embedded fresh in each AssignWork payload
	// this agent sends while leader.
	Listen string
	// Dialer opens a connection to a follower's own cell_leader_addr (from
	// the assignment's Peers) to hand out AssignWork. Defaults to
	// GRPCCellLeaderDialer — the same production dialer the follower half
	// (#96) uses for its own StepReport calls.
	Dialer CellLeaderDialer
	// Checkpoint persists the driver's OpCheckpoint snapshots and supplies
	// the last checkpoint a newly-elected leader Resumes from. Defaults to
	// an in-memory store; a real deployment wires in the object-store
	// CheckpointStore (issue #62) instead.
	Checkpoint cell.CheckpointStore
	// Template selects the (driver, template) pair a CombiningDriver looks
	// up in Registry (cell.TemplateKey.Template). Defaults to
	// "dist-training", the template this ticket's acceptance criteria name.
	Template string
	// Registry is the driver->template combine lookup a CombiningDriver
	// reads. Defaults to cell.DefaultCombineRegistry().
	Registry cell.CombineRegistry

	// NewRaftNode constructs the raft node runCellLeader joins on a
	// CellAssignment. Defaults to a thin wrapper over cell.NewNode (#95's
	// production node); tests substitute a fake implementing the narrow
	// leaderRaftNode surface instead of a real TCP raft cluster.
	NewRaftNode func(cfg cell.NodeConfig) (leaderRaftNode, error)

	// StragglerInterval is how often the elected leader's straggler-eviction
	// timer (issue #100, LeaderHost.watchStragglers) re-checks the current
	// step's not-yet-Done members against internal/core/detection's
	// tier/coupling deadline. Defaults to defaultStragglerInterval.
	StragglerInterval time.Duration
}

func (c CellLeaderConfig) withDefaults() CellLeaderConfig {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:0"
	}
	if c.Dialer == nil {
		c.Dialer = GRPCCellLeaderDialer()
	}
	if c.Checkpoint == nil {
		c.Checkpoint = cell.NewMemCheckpointStore()
	}
	if c.Template == "" {
		c.Template = "dist-training"
	}
	if c.Registry == nil {
		c.Registry = cell.DefaultCombineRegistry()
	}
	if c.NewRaftNode == nil {
		c.NewRaftNode = func(cfg cell.NodeConfig) (leaderRaftNode, error) { return cell.NewNode(cfg) }
	}
	if c.StragglerInterval <= 0 {
		c.StragglerInterval = defaultStragglerInterval
	}
	return c
}

// runCellLeader is the agent's 5th run loop (issue #102). While unconfigured
// (CellLeader.RaftListen == "", the default) it stays inert — no raft node,
// no CellAssignment polling of its own, matching every existing agent test
// byte-for-byte. Once configured, it waits for a CellAssignment (#101),
// joins the cell's raft cluster (#95), and hands off to a LeaderHost that
// hosts the driver Loop for as long as raft elects this agent leader,
// across as many leadership transitions as occur before ctx is done.
func (a *Agent) runCellLeader(ctx context.Context) error {
	if a.cfg.CellLeader.RaftListen == "" {
		<-ctx.Done()
		return ctx.Err()
	}

	assignment, err := a.awaitCellAssignment(ctx)
	if err != nil {
		return err
	}

	node, cleanup, err := a.joinCellRaft(ctx, assignment)
	if err != nil {
		return err
	}
	defer cleanup()

	host := &LeaderHost{
		Node:              node,
		Assignment:        assignment,
		Listen:            a.cfg.CellLeader.Listen,
		Dialer:            a.cfg.CellLeader.Dialer,
		Checkpoint:        a.cfg.CellLeader.Checkpoint,
		Template:          a.cfg.CellLeader.Template,
		Registry:          a.cfg.CellLeader.Registry,
		StragglerInterval: a.cfg.CellLeader.StragglerInterval,
		Now:               a.now,
		Report:            a.reportCoupledCompletion,
	}
	a.setCellLeaderHost(host)
	return host.run(ctx)
}

// joinCellRaft constructs this agent's raft node from assignment's peers
// (cell.Peer{ID: AgentId, RaftAddr}) and CellLeaderConfig, bootstrapping iff
// assignment says this agent is the bootstrapper. It also fires a best-effort,
// non-blocking re-advertise of this agent's raft_addr (JoinAgentRequest —
// the field follower.go's advertiseFollower deliberately leaves empty,
// noting it is this ticket's scope to fill in) so a control plane computing
// a LATER CellAssignment for this cell has it on file; a failure to advertise
// is not fatal to hosting the CURRENT assignment, whose peer addresses
// already arrived in assignment.Peers.
func (a *Agent) joinCellRaft(ctx context.Context, assignment *transport.CellAssignmentResponse) (leaderRaftNode, func(), error) {
	cfg := a.cfg.CellLeader

	peers := make([]cell.Peer, 0, len(assignment.GetPeers()))
	for _, p := range assignment.GetPeers() {
		peers = append(peers, cell.Peer{ID: p.GetAgentId(), RaftAddr: p.GetRaftAddr()})
	}

	node, err := cfg.NewRaftNode(cell.NodeConfig{
		ID:         a.cfg.AgentID,
		BindAddr:   cfg.RaftListen,
		Peers:      peers,
		Bootstrap:  assignment.GetBootstrap(),
		DataDir:    cfg.RaftDataDir,
		RaftConfig: cfg.RaftConfig,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("agent: join cell raft: %w", err)
	}

	go func() { _ = a.advertiseRaftAddr(ctx, cfg.RaftListen) }()

	cleanup := func() {
		if shutdown, ok := node.(interface{ Shutdown() error }); ok {
			_ = shutdown.Shutdown()
		}
	}
	return node, cleanup, nil
}

// advertiseRaftAddr re-advertises this agent's identity with raft_addr
// populated, alongside whatever cell_leader_addr the follower half has
// already bound (recordJoinAddrLocked on the control plane side overwrites
// both fields together, so omitting a known cell_leader_addr here would
// blank it out).
func (a *Agent) advertiseRaftAddr(ctx context.Context, raftAddr string) error {
	followerAddr, _ := a.FollowerAddr()
	return a.rpcRetry(ctx, func(client transport.ControlPlaneClient) error {
		resp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{
			Agent: a.cfg.AgentID, Region: a.cfg.Region, Caps: a.cfg.Caps,
			RaftAddr: raftAddr, CellLeaderAddr: followerAddr,
		})
		if err != nil {
			return err
		}
		if !resp.GetAccepted() {
			return fmt.Errorf("agent: JoinAgent (raft advertise) rejected: %s", resp.GetReason())
		}
		return nil
	})
}

// reportCoupledCompletion reports jobID's final combined gradient to the
// control plane via ControlPlane.ReportResult, keyed by the job id itself —
// the completion contract #98's ReportResult handler consumes (TaskId ==
// a gang job id -> PutAggregate + Done), reusing the same RPC rather than a
// dedicated one (D6, no new proto).
func (a *Agent) reportCoupledCompletion(ctx context.Context, jobID string, combined []byte) error {
	return a.rpcRetry(ctx, func(client transport.ControlPlaneClient) error {
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: jobID, Output: combined, Ok: true,
		})
		if err != nil {
			return err
		}
		if !resp.GetAccepted() {
			return fmt.Errorf("agent: ReportResult (coupled completion) rejected for job %s", jobID)
		}
		return nil
	})
}

// dialedConn is one cached follower connection a LeaderHost's TransportExecutor
// dials AssignWork through.
type dialedConn struct {
	client transport.CellLeaderClient
	closer io.Closer
}

// LeaderHost hosts the production driver Loop (cell.Loop) for as long as its
// Node reports this agent as raft leader (issue #102). It is deliberately
// decoupled from *Agent (only a Report callback, not the whole Agent, is
// threaded in) so it is directly constructible and testable without a full
// gRPC-backed Agent — see leader_test.go's "fake raft node + in-process
// followers" acceptance tests.
type LeaderHost struct {
	Node       leaderRaftNode
	Assignment *transport.CellAssignmentResponse

	Listen     string
	Dialer     CellLeaderDialer
	Checkpoint cell.CheckpointStore
	Template   string
	Registry   cell.CombineRegistry
	Now        func() model.Instant

	// StragglerInterval is how often the straggler-eviction timer
	// (watchStragglers, issue #100) re-checks the current step's not-yet-Done
	// members. Defaults to defaultStragglerInterval.
	StragglerInterval time.Duration

	// Report delivers jobID's final combined gradient once the assignment's
	// last step completes (D6). Required for completion to be observable —
	// a nil Report just skips the notification, still stopping the loop.
	Report func(ctx context.Context, jobID string, combined []byte) error

	initOnce sync.Once
	peerAddr map[string]string

	mu         sync.Mutex
	loop       *cell.Loop
	srv        *cell.Server
	grpcSrv    *grpc.Server
	lis        net.Listener
	dialCtx    context.Context
	conns      map[string]*dialedConn
	doneOnce   sync.Once
	termCancel context.CancelFunc // stops this term's watchStragglers goroutine

	// evicted, stalled, stallHave, stallNeed, stallRollback are this term's
	// straggler-eviction/stall bookkeeping (issue #100), written only by
	// recordDeadlineOutcome and read by Evicted/StallInfo/Status. reset by
	// resetTermStatus at the start of every term so a later term never
	// inherits an earlier one's status.
	evicted       []string
	stalled       bool
	stallHave     int
	stallNeed     int
	stallRollback bool
}

// init applies defaults and precomputes the peer -> cell_leader_addr lookup
// from Assignment.Peers, once, however LeaderHost is constructed (via
// runCellLeader's production wiring, or a test building one directly).
func (h *LeaderHost) init() {
	h.initOnce.Do(func() {
		if h.Listen == "" {
			h.Listen = "127.0.0.1:0"
		}
		if h.Dialer == nil {
			h.Dialer = GRPCCellLeaderDialer()
		}
		if h.Checkpoint == nil {
			h.Checkpoint = cell.NewMemCheckpointStore()
		}
		if h.Template == "" {
			h.Template = "dist-training"
		}
		if h.Registry == nil {
			h.Registry = cell.DefaultCombineRegistry()
		}
		if h.Now == nil {
			h.Now = func() model.Instant { return 0 }
		}
		if h.StragglerInterval <= 0 {
			h.StragglerInterval = defaultStragglerInterval
		}
		peers := h.Assignment.GetPeers()
		h.peerAddr = make(map[string]string, len(peers))
		for _, p := range peers {
			h.peerAddr[p.GetAgentId()] = p.GetCellLeaderAddr()
		}
	})
}

// jobID is Assignment's job id, the key this host's checkpoints and
// completion report are filed under.
func (h *LeaderHost) jobID() string { return h.Assignment.GetJobId() }

// run watches Node.LeaderCh(), hosting (onBecomeLeader) or stepping down
// (onLoseLeadership) from the Loop as raft leadership transitions, until ctx
// is done. It is the body of runCellLeader's 5th loop, factored out onto
// LeaderHost so it is directly testable without a full Agent (issue #102's
// acceptance criteria: "a fake raft node reporting this agent leader").
func (h *LeaderHost) run(ctx context.Context) error {
	h.init()
	defer h.onLoseLeadership()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case isLeader, ok := <-h.Node.LeaderCh():
			if !ok {
				<-ctx.Done()
				return ctx.Err()
			}
			if isLeader {
				if err := h.onBecomeLeader(ctx); err != nil {
					return err
				}
			} else {
				h.onLoseLeadership()
			}
		}
	}
}

// raftFSMBarrierTimeout bounds awaitFSMCaughtUp's wait for this term's raft
// FSM to catch up to its own already-committed log before Resume reads it —
// generous because it is a real raft round trip (issue #100), never hit in
// the ordinary case where the FSM is already caught up by the time
// LeaderCh fires.
const raftFSMBarrierTimeout = 10 * time.Second

// awaitFSMCaughtUp blocks (via node.Barrier, if node implements it) until
// every log entry committed as of this call has been applied to node's own
// FSM — see cell.Node.Barrier's doc for why this matters: a newly-elected
// leader's raft LOG is guaranteed up to date, but FSM.Apply can still lag it
// by one internal apply-loop cycle at the exact instant LeaderCh fires, and
// buildState's Resume(Node.Log(), ...) call right after this would otherwise
// risk rebuilding State from a log missing its own most recent entries
// (issue #100's failover scenario surfaced this against a REAL raft
// cluster — the fake raft node #102's own unit tests use has no such lag,
// since its Apply is synchronous, so it does not implement Barrier and this
// is a no-op for it).
func awaitFSMCaughtUp(node leaderRaftNode) error {
	barrier, ok := node.(interface {
		Barrier(timeout time.Duration) error
	})
	if !ok {
		return nil
	}
	return barrier.Barrier(raftFSMBarrierTimeout)
}

// onBecomeLeader constructs (or, on a later leadership transition,
// re-Resumes) the barrier.State, builds the production Loop wired exactly
// as the #90 regression guard requires (Driver AND State both set on the
// TransportExecutor — see the package doc), binds this term's cell.Server
// listener, and kicks off AssignWork for the current step. A duplicate
// "became leader" signal (Node.LeaderCh sending true twice without a false
// between) is a no-op: it is idempotent on an already-hosted term.
func (h *LeaderHost) onBecomeLeader(ctx context.Context) error {
	h.mu.Lock()
	alreadyHosting := h.loop != nil
	h.mu.Unlock()
	if alreadyHosting {
		return nil
	}

	if err := awaitFSMCaughtUp(h.Node); err != nil {
		return fmt.Errorf("cell leader: await FSM catch-up: %w", err)
	}

	lis, err := net.Listen("tcp", h.Listen)
	if err != nil {
		return fmt.Errorf("cell leader: listen on %s: %w", h.Listen, err)
	}
	addr := lis.Addr().String()

	state := h.buildState()
	bs, _ := state.(barrier.State)

	driver := cell.CombiningDriver{
		Inner:    cell.BarrierDriver{},
		Registry: h.Registry,
		Key:      cell.TemplateKey{Driver: cell.DriverNameBarrier, Template: h.Template},
	}

	// TransportExecutor.Driver AND .State are wired here, at the production
	// construction site — the #90 regression guard TestResume_Barrier_Failover
	// documents: without them, OpCheckpoint persists a nil DriverBlob and
	// Resume can never recover Members/K/MinMembers. See
	// TestLeaderHost_CheckpointHasDriverBlob.
	te := &cell.TransportExecutor{
		JobID:      h.jobID(),
		Dial:       h.dialWorker,
		Members:    h.membersFunc(),
		Checkpoint: h.Checkpoint,
		Driver:     driver,
	}

	loop := cell.NewLoop(driver, state, nil, h.Node.Apply)
	te.State = loop.State
	loop.Exec = h.wrapExec(te, addr)

	srv := cell.NewServer(loop, cell.DriverBarrier, h.Now)
	grpcSrv := grpc.NewServer()
	transport.RegisterCellLeaderServer(grpcSrv, srv)

	// termCtx bounds this term's straggler-eviction timer (issue #100): it
	// is cancelled — independent of the outer ctx — the moment this term
	// ends, from onLoseLeadership, so a stale term's timer never fires
	// against a NEW term's (freshly rebuilt) loop/srv.
	termCtx, termCancel := context.WithCancel(ctx)

	h.resetTermStatus()
	h.mu.Lock()
	h.loop, h.srv, h.grpcSrv, h.lis, h.dialCtx, h.termCancel = loop, srv, grpcSrv, lis, ctx, termCancel
	h.mu.Unlock()

	go func() { _ = grpcSrv.Serve(lis) }()
	go h.watchStragglers(termCtx, loop, srv)

	payload := combinedForStep(h.Node.Log(), bs.Step)
	return loop.Exec.Exec(ctx, []cell.Command{
		{Op: cell.OpAllReduce, Combined: payload},
		{Op: cell.OpRelease, Step: bs.Step},
	})
}

// onLoseLeadership tears down this term's Loop/server/dial cache/straggler
// timer. It is idempotent — safe to call from run's "no longer leader"
// branch, from a completed term's own async cleanup (finish), and from run's
// deferred shutdown, in any order or combination.
func (h *LeaderHost) onLoseLeadership() {
	h.mu.Lock()
	grpcSrv := h.grpcSrv
	conns := h.conns
	termCancel := h.termCancel
	h.loop, h.srv, h.grpcSrv, h.lis, h.dialCtx, h.conns, h.termCancel = nil, nil, nil, nil, nil, nil, nil
	h.mu.Unlock()

	if termCancel != nil {
		termCancel()
	}
	if grpcSrv != nil {
		grpcSrv.GracefulStop()
	}
	for _, c := range conns {
		_ = c.closer.Close()
	}
}

// resetTermStatus clears this term's straggler-eviction/stall bookkeeping —
// called once per onBecomeLeader so a NEW term never inherits a PRIOR term's
// stale Evicted/StallInfo.
func (h *LeaderHost) resetTermStatus() {
	h.mu.Lock()
	h.evicted = nil
	h.stalled = false
	h.stallHave, h.stallNeed = 0, 0
	h.stallRollback = false
	h.mu.Unlock()
}

// watchStragglers is issue #100's straggler-eviction timer: on a real
// wall-clock tick (StragglerInterval), it checks every current-step member
// that has not yet reported Done against internal/core/detection's
// tier/coupling deadline table, evaluated against cell.Server's LastSeen (or,
// for a member this term has never heard from, this term's own start
// instant — the only lastSeen a brand-new term has for it). The first tick to
// find any such member overdue synthesizes exactly one EventDeadline into
// loop — barrier's own stepDeadline (internal/core/barrier.go) evicts every
// member that has not reported Done for the current step and, if survivors
// remain and are all Done, completes the step in the same fold (decision C);
// under the MinMembers floor, falling short instead rolls back and stalls
// (decision G). This timer does not need to choose between "straggler
// eviction" and "min_members floor" — it always fires the same EventDeadline
// and lets barrier's own step decide which applies.
//
// Resolved ambiguity: CellAssignmentResponse carries no Tier field (the
// coupled-cell wire protocol predates O4's adaptive-by-tier detection), so
// this ticket pins every cell-leader-hosted barrier to model.Core (the
// trusted, low-latency tier real cells run on) — Core+Barrier is
// detection's fastest table entry (2s), matching the phase doc's "core +
// barrier in seconds" pin.
//
// Once a Deadline fold ever produces OpStall or OpFail, this goroutine
// stops: barrier has parked the run under the floor (or given up), and
// re-checking a since-reset Step against members that have not been
// re-assigned work yet (no refill/resume wiring exists for a stalled
// barrier — issue #100 escalates that piece as a follow-up) would misread
// "not yet re-assigned" as "still overdue" and evict survivors that were
// never actually stragglers, cascading the whole membership toward zero.
// Stopping here keeps this timer's blast radius the one behavior issue #100
// actually asks for: evict, or stall-and-park — never a runaway collapse.
func (h *LeaderHost) watchStragglers(ctx context.Context, loop *cell.Loop, srv *cell.Server) {
	interval := h.StragglerInterval
	if interval <= 0 {
		interval = defaultStragglerInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	termStart := h.Now()
	firedStep := -1

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		bs, ok := loop.State().(barrier.State)
		if !ok || bs.Failed || bs.Step == firedStep {
			continue
		}

		now := h.Now()
		if !anyOverdue(bs, srv, termStart, now) {
			continue
		}
		firedStep = bs.Step

		cmds, err := loop.Handle(ctx, cell.Event{Kind: cell.EventDeadline}, now)
		if err != nil {
			return
		}
		if h.recordDeadlineOutcome(cmds) {
			return // OpStall/OpFail observed — see the doc above.
		}
	}
}

// anyOverdue reports whether any of bs's current-step members that have not
// yet reported Done is past its detection.Deadline, evaluated against srv's
// LastSeen (or termStart, for a member this term has never heard from).
func anyOverdue(bs barrier.State, srv *cell.Server, termStart, now model.Instant) bool {
	for _, w := range bs.Members {
		if _, done := bs.Partials[w]; done {
			continue
		}
		lastSeen, seen := srv.LastSeen(string(w))
		if !seen {
			lastSeen = termStart
		}
		dl := lastSeen + model.Instant(detection.Deadline(model.Core, model.Barrier))
		if detection.IsDead(lastSeen, dl, now) {
			return true
		}
	}
	return false
}

// recordDeadlineOutcome folds cmds (the commands an EventDeadline fold just
// produced) into this term's observable status — Evicted for every OpEvict,
// StallInfo for an OpStall (always paired with an OpRollback per barrier's
// own stall(), recorded here from the actual commands rather than assumed).
// It returns true iff cmds contained OpStall or OpFail, telling
// watchStragglers to stop (see its doc).
func (h *LeaderHost) recordDeadlineOutcome(cmds []cell.Command) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	terminal := false
	for _, c := range cmds {
		switch c.Op {
		case cell.OpEvict:
			h.evicted = append(h.evicted, string(c.Worker))
		case cell.OpRollback:
			h.stallRollback = true
		case cell.OpStall:
			h.stalled = true
			h.stallHave, h.stallNeed = c.Have, c.Need
			terminal = true
		case cell.OpFail:
			terminal = true
		}
	}
	return terminal
}

// Evicted returns the barrier worker ids this term's straggler-eviction
// timer has evicted so far, in eviction order.
func (h *LeaderHost) Evicted() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.evicted))
	copy(out, h.evicted)
	return out
}

// StallInfo is this term's MinMembers-floor stall status (issue #100's
// "stalled: have/need" status-surfacing acceptance criterion).
type StallInfo struct {
	Stalled bool
	Have    int
	Need    int
	// Rollback is true iff an OpRollback command accompanied the Stall —
	// barrier's own stall() always pairs them (internal/core/barrier.go); this
	// records that from the actual commands watchStragglers observed rather
	// than assuming it.
	Rollback bool
}

// StallInfo returns this term's current stall status.
func (h *LeaderHost) StallInfo() StallInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	return StallInfo{Stalled: h.stalled, Have: h.stallHave, Need: h.stallNeed, Rollback: h.stallRollback}
}

// Status returns a short human-readable summary of this term's barrier
// progress: "stalled: have=H need=N" once a MinMembers-floor stall has
// parked the run (issue #100), or "running" otherwise. Wiring this onto a
// job's wire-visible JobStatusResponse (a new proto field) is a follow-up —
// this is the in-process surface production code and tests can poll today
// without one.
func (h *LeaderHost) Status() string {
	info := h.StallInfo()
	if info.Stalled {
		return fmt.Sprintf("stalled: have=%d need=%d", info.Have, info.Need)
	}
	return "running"
}

// ListenAddr returns the address this term's cell.Server actually bound, and
// whether one is currently hosted — analogous to Agent.FollowerAddr, for
// tests to discover an ephemeral Listen's real port.
func (h *LeaderHost) ListenAddr() (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lis == nil {
		return "", false
	}
	return h.lis.Addr().String(), true
}

// buildState seeds this term's starting barrier.State: Resume(Node.Log(),
// checkpoint) if a checkpoint has ever been persisted for this job (the
// production analogue of TestResume_Barrier_Failover, now driven by raft
// LeaderCh — issue #102's failover requirement), or else a fresh State from
// the assignment's own Members/K/MinMembers, Step 0 — the correct fallback
// even on a later leadership transition, since barrier's own Lost handling
// rolls back to the zero Checkpoint (step 0) when nothing has been
// checkpointed yet (see barrier.go's decision E): there is no well-formed
// Members/K/MinMembers a bare command log alone can rebuild without a
// checkpoint's DriverBlob to seed from (see applyBarrierCommand's doc).
func (h *LeaderHost) buildState() cell.State {
	if ckpt, ok := h.Checkpoint.Last(h.jobID()); ok {
		log := logAfterCheckpoint(h.Node.Log(), ckpt.Step)
		return cell.BarrierDriver{}.Resume(log, ckpt)
	}
	return barrier.State{
		Step:       0,
		K:          int(h.Assignment.GetK()),
		MinMembers: int(h.Assignment.GetMinMembers()),
		Members:    h.peerWorkerIDs(),
	}
}

// peerWorkerIDs returns Assignment.Peers' agent ids as barrier.WorkerIDs, in
// the order the control plane's activateCoupledCellLocked (deterministic,
// lexicographic) provided them — the barrier's initial Members.
func (h *LeaderHost) peerWorkerIDs() []barrier.WorkerID {
	peers := h.Assignment.GetPeers()
	out := make([]barrier.WorkerID, len(peers))
	for i, p := range peers {
		out[i] = barrier.WorkerID(p.GetAgentId())
	}
	return out
}

// membersFunc returns TransportExecutor.Members's live implementation: the
// CURRENT loop's barrier.State.Members, not the assignment's original peer
// list, so an eviction (OpEvict) is reflected in who gets AssignWork'd on
// the very next Release rather than broadcasting to a member the barrier
// itself has already dropped.
func (h *LeaderHost) membersFunc() func() []string {
	return func() []string {
		h.mu.Lock()
		loop := h.loop
		h.mu.Unlock()
		if loop == nil {
			return nil
		}
		bs, _ := loop.State().(barrier.State)
		out := make([]string, len(bs.Members))
		for i, m := range bs.Members {
			out[i] = string(m)
		}
		return out
	}
}

// dialWorker resolves worker (an agent id) to its assignment-advertised
// cell_leader_addr and dials it, caching the connection for reuse across
// steps — TransportExecutor.Dial's production implementation.
func (h *LeaderHost) dialWorker(worker string) (transport.CellLeaderClient, error) {
	h.mu.Lock()
	if c, ok := h.conns[worker]; ok {
		h.mu.Unlock()
		return c.client, nil
	}
	dialCtx := h.dialCtx
	h.mu.Unlock()

	addr, ok := h.peerAddr[worker]
	if !ok || addr == "" {
		return nil, fmt.Errorf("cell leader: no cell_leader_addr advertised for worker %s", worker)
	}
	if dialCtx == nil {
		dialCtx = context.Background()
	}

	client, closer, err := h.Dialer(dialCtx, addr)
	if err != nil {
		return nil, fmt.Errorf("cell leader: dial %s (%s): %w", worker, addr, err)
	}

	h.mu.Lock()
	if h.conns == nil {
		h.conns = make(map[string]*dialedConn)
	}
	h.conns[worker] = &dialedConn{client: client, closer: closer}
	h.mu.Unlock()
	return client, nil
}

// wrapExec returns the Executor Loop.Exec drives: it delegates every command
// to te, with two additions neither cell nor TransportExecutor know about
// (both are this ticket's D4/D6 wiring, kept out of cell — see the package
// doc):
//
//   - D4: every OpAllReduce/OpFold/OpAggregate's Combined bytes — about to
//     become the payload of the immediately following OpRelease/OpAdvance's
//     AssignWork calls, per TransportExecutor's own documented ordering —
//     are wrapped with encodeAssignWorkPayload(leaderAddr, ...) so followers
//     can decode the dial-back address (decodeAssignWorkPayload,
//     followerServer.AssignWork). This wrapping happens here, AFTER Loop has
//     already replicated the command log (Apply always runs before Exec —
//     see loop.go's Handle), so the replicated log itself keeps the raw
//     combine result, never a network address.
//   - D6: an OpRelease advancing to Assignment.Steps (the step following the
//     last real step) is the completion signal — it is dropped rather than
//     turned into one more (nonexistent) round of AssignWork, and instead
//     triggers finish, reporting the terminal step's combined result to the
//     control plane exactly once.
func (h *LeaderHost) wrapExec(te *cell.TransportExecutor, leaderAddr string) cell.Executor {
	steps := int(h.Assignment.GetSteps())
	return cell.ExecutorFunc(func(ctx context.Context, cmds []cell.Command) error {
		out := make([]cell.Command, 0, len(cmds))
		var lastCombined []byte
		var terminal bool
		var terminalCombined []byte

		for _, c := range cmds {
			switch c.Op {
			case cell.OpAllReduce, cell.OpFold, cell.OpAggregate:
				lastCombined = c.Combined
				c.Combined = encodeAssignWorkPayload(leaderAddr, c.Combined)
			}
			if c.Op == cell.OpRelease && steps > 0 && c.Step == steps {
				terminal = true
				terminalCombined = lastCombined
				continue
			}
			out = append(out, c)
		}

		if err := te.Exec(ctx, out); err != nil {
			return err
		}
		if terminal {
			h.finish(ctx, terminalCombined)
		}
		return nil
	})
}

// finish reports combined to the control plane (once — sync.Once guards
// against a StepReport race re-triggering the same terminal Release). It
// does not itself tear down this term's Loop/server/dial cache: finish runs
// from deep inside the StepReport call chain that just completed the last
// step (see wrapExec), possibly nested under other still-unwinding calls
// this SAME term's own kick/cascade started (an in-flight AssignWork RPC
// sharing a cached dial-cache connection, for instance) — closing shared
// state out from under them here would race. The barrier's own State
// already stops making progress once the terminal step completes (its Step
// has advanced past Assignment.Steps with no further AssignWork ever sent
// for it, so no more legitimate StepReports arrive); the actual
// server/connection teardown happens the ordinary way, when run's own
// select loop next observes a real leadership loss or ctx cancellation
// (onLoseLeadership).
func (h *LeaderHost) finish(ctx context.Context, combined []byte) {
	h.doneOnce.Do(func() {
		if h.Report != nil {
			_ = h.Report(ctx, h.jobID(), combined)
		}
	})
}

// combinedForStep scans a replicated command log for the Combined bytes
// that accompanied the Release advancing to step — i.e. the AssignWork
// payload followers received for step when it was first assigned — by
// tracking the most recent OpAllReduce/OpFold/OpAggregate's Combined and
// returning it the moment a matching OpRelease is seen. It returns nil for
// step 0 fed an empty (or step-0-only) log, matching barrier's own "no
// incoming gradient at step 0" convention. Used both for a fresh term's
// step-0 kick (trivially nil) and a resumed term's re-kick of whatever step
// the recovered State says is current — the same payload the step's
// original AssignWork carried, so a follower that missed it during the
// leadership gap gets it again.
func combinedForStep(log []cell.Command, step int) []byte {
	var lastCombined, result []byte
	for _, c := range log {
		switch c.Op {
		case cell.OpAllReduce, cell.OpFold, cell.OpAggregate:
			lastCombined = c.Combined
		case cell.OpRelease:
			if c.Step == step {
				// Scan to the end rather than returning on the first match:
				// a Rollback can reset Step backward, so the same step
				// value may be the target of an earlier, now-superseded
				// Release too — the LAST one in log order is current.
				result = lastCombined
			}
		}
	}
	return result
}

// logAfterCheckpoint returns the suffix of log recorded after the most
// recent OpCheckpoint entry matching ckptStep — the "log after the
// checkpoint" input Driver.Resume expects (see
// TestResume_Barrier_Failover's doc in internal/shell/cell/resume_test.go).
// log itself, from a raft node's Log(), is the FULL replicated history, not
// pre-sliced; a checkpoint's own Step field is what identifies which
// OpCheckpoint command in that history it corresponds to. No match (should
// not happen for a checkpoint this same log's history produced) falls back
// to the full log rather than silently dropping it.
func logAfterCheckpoint(log []cell.Command, ckptStep int) []cell.Command {
	idx := -1
	for i, c := range log {
		if c.Op == cell.OpCheckpoint && c.Step == ckptStep {
			idx = i
		}
	}
	if idx < 0 {
		return log
	}
	return log[idx+1:]
}
