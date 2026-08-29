// Package agent is the imperative shell for a Swarm agent daemon. It drives
// two pure cores — internal/core/agentreg (registration & reconnect) and
// internal/core/runner (the task runner) — over real gRPC, executing the
// Commands each core returns. The cores stay pure: this package reads the
// clock, draws jitter, dials connections, spawns native processes, and feeds
// the results back in as events. See internal/core/mitosis for the reference
// shape and CLAUDE.md for the FCIS boundary this package sits on the I/O side
// of.
//
// Membership in P0 is CENTRAL: the agent joins by dialing out to the control
// plane (JoinAgent) and stays joined via Heartbeat. It does not participate
// in gossip/SWIM — that is a P1 concern and is intentionally out of scope
// here.
package agent

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/msivraj/swarm/internal/core/region"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

const (
	defaultHeartbeatInterval  = 5 * time.Second
	defaultPullInterval       = 500 * time.Millisecond
	defaultGlobalViewInterval = 2 * time.Second
	clientPollInterval        = 20 * time.Millisecond
)

// Clock supplies the wall-clock instant the shell passes into agentreg.Step
// as data. The core never reads the clock itself.
type Clock interface {
	Now() model.Instant
}

// RealClock is the Clock the shell uses in production: the process's wall
// clock, in nanoseconds.
type RealClock struct{}

// Now returns the current instant. This is I/O (a clock read) and therefore
// lives in the shell, never in core.
func (RealClock) Now() model.Instant {
	return model.Instant(time.Now().UnixNano())
}

// JitterSource draws the jitter fraction (expected in [0,1]) the shell
// passes into agentreg.Step and Backoff as data. Randomness lives here, in
// the shell — never in core.
type JitterSource func() float64

// mathRandJitter is the JitterSource the shell uses in production, drawing
// from math/rand's default source.
func mathRandJitter() float64 {
	return rand.Float64() //nolint:gosec // jitter does not need a CSPRNG
}

// Dialer opens a connection to a control-plane target and returns a client
// and the underlying connection to close on redial or shutdown. Tests supply
// a Dialer backed by an in-process (bufconn) server; production uses
// GRPCDialer.
type Dialer func(ctx context.Context, target string) (transport.ControlPlaneClient, io.Closer, error)

// GRPCDialer is the Dialer the shell uses in production: a plaintext gRPC
// dial. P0 assumes trusted machines (see the ticket's ambiguity note); a
// single-cert/mTLS Dialer can replace this one without touching the rest of
// the shell.
func GRPCDialer() Dialer {
	return func(_ context.Context, target string) (transport.ControlPlaneClient, io.Closer, error) {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return transport.NewControlPlaneClient(conn), conn, nil
	}
}

// probeDial dials target and confirms it is actually reachable with a cheap,
// identity-free RPC (Ps) before reporting success. grpc.NewClient itself
// connects lazily, so without this probe a bad target would not surface as
// DialFail until some later RPC happened to need the connection.
func probeDial(ctx context.Context, dial Dialer, target string) (transport.ControlPlaneClient, io.Closer, error) {
	client, closer, err := dial(ctx, target)
	if err != nil {
		return nil, nil, err
	}
	if _, err := client.Ps(ctx, &transport.PsRequest{}); err != nil {
		_ = closer.Close()
		return nil, nil, err
	}
	return client, closer, nil
}

// ProcessSpec configures how a Task maps onto a native process — the P0
// "independent" driver. The convention (not pinned by the doc, resolved
// here): Argv is the executable and its arguments; the Task's Input is piped
// to the process's stdin, and its stdout is captured verbatim as the
// TaskResult's Output.
type ProcessSpec struct {
	Argv []string
}

// Config configures an Agent.
type Config struct {
	// AgentID identifies this agent to the control plane. Required.
	AgentID string
	// Region is the region this agent asks to join.
	Region string
	// Caps is the capacity this agent offers.
	Caps int32

	// Targets is the ordered list of control-plane dial addresses. Dial
	// always tries the current target; Failover advances to the next one
	// (wrapping), giving the registration core's Failover command somewhere
	// to point. Required, at least one entry.
	Targets []string
	// Dialer opens a connection to one of Targets. Required.
	Dialer Dialer

	// Clock supplies now for agentreg.Step. Defaults to RealClock.
	Clock Clock
	// Jitter supplies the jitter fraction for agentreg.Step and Backoff.
	// Defaults to a math/rand-backed source.
	Jitter JitterSource

	// HeartbeatInterval is how often the shell feeds a Tick event while
	// Member, driving agentreg's Heartbeat command. Defaults to 5s.
	HeartbeatInterval time.Duration
	// PullInterval is how long the runner loop waits between PullTask calls
	// when the control plane has no task ready. Defaults to 500ms.
	PullInterval time.Duration

	// Process configures native task execution.
	Process ProcessSpec

	// RegionTargets maps a RegionID to its control-plane dial address, for
	// multi-region failover. Required (together with HomeRegion,
	// KnownRegions and GlobalRouter) to enable cross-region failover; leave
	// it nil for the single-region P0 behavior, which dials Targets instead.
	RegionTargets map[model.RegionID]string
	// HomeRegion is this agent's home region: the address execFailover
	// starts from and the region.SelectRegion candidate list's first entry.
	// Required for multi-region failover.
	HomeRegion model.RegionID
	// KnownRegions is the ranked candidate list region.SelectRegion walks:
	// KnownRegions[0] must be HomeRegion, KnownRegions[1:] are peer regions
	// in nearest-first order — slice position IS the nearness ranking, the
	// core has no distance metric of its own. Required for multi-region
	// failover.
	KnownRegions []model.RegionID
	// GlobalRouter is the dial address of the global routing layer's
	// GetGlobalView RPC, polled on a timer to build the health map
	// region.SelectRegion needs. Empty disables multi-region failover
	// entirely: execFailover falls back to wrapping over Targets, matching
	// single-region P0 behavior.
	GlobalRouter string
	// GlobalViewDialer opens a connection to GlobalRouter. Defaults to a
	// plaintext gRPC dial (GRPCGlobalViewDialer). Tests supply a
	// GlobalViewDialer backed by an in-process (bufconn) fake GlobalRouter.
	GlobalViewDialer GlobalViewDialer
	// GlobalViewInterval is how often the shell polls GetGlobalView to
	// refresh the cached health map. Defaults to 2s. Ignored when
	// GlobalRouter is empty.
	GlobalViewInterval time.Duration

	// Follower configures P2 coupled-cell follower mode (issue #96). Leaving
	// it zero-valued (Follower.Listen == "") disables it entirely — the
	// agent then behaves exactly like a P0/P1 agent, matching every existing
	// Config in this package.
	Follower FollowerConfig

	// CellLeader configures P2 agent-hosted per-cell leadership (issue
	// #102). Leaving it zero-valued (CellLeader.RaftListen == "") disables
	// it entirely — the 5th run loop (runCellLeader) stays inert, matching
	// every existing Config in this package.
	CellLeader CellLeaderConfig
}

func (c Config) withDefaults() Config {
	if c.Clock == nil {
		c.Clock = RealClock{}
	}
	if c.Jitter == nil {
		c.Jitter = mathRandJitter
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.PullInterval <= 0 {
		c.PullInterval = defaultPullInterval
	}
	if c.GlobalViewDialer == nil {
		c.GlobalViewDialer = GRPCGlobalViewDialer()
	}
	if c.GlobalViewInterval <= 0 {
		c.GlobalViewInterval = defaultGlobalViewInterval
	}
	c.Follower = c.Follower.withDefaults()
	c.CellLeader = c.CellLeader.withDefaults()
	return c
}

// Agent is a Swarm agent daemon: the shell that drives agentreg's
// registration/reconnect core and runner's task-runner core over gRPC.
type Agent struct {
	cfg     Config
	clients *clientHolder
	health  *healthCache

	mu        sync.Mutex
	targetIdx int
	// dialTarget is the current resolved dial address in multi-region mode
	// (GlobalRouter set): execFailover writes it, currentTarget reads it.
	// Unused in single-region mode, where targetIdx/Targets drive dialing
	// instead.
	dialTarget string
	// regionAttempt is the failover attempt counter region.SelectRegion
	// walks its ranked candidate list with. It increments on every
	// execFailover call in multi-region mode and never resets, which is
	// what makes repeated failover cycle through the candidate list rather
	// than get stuck retrying the same one.
	regionAttempt int
	// enrolls counts how many times the shell has executed agentreg's Enroll
	// command. It exists to make "enroll once, even across reconnects" — a
	// property named directly in the ticket — observable from tests in this
	// package without adding any RPC the proto does not define.
	enrolls int
	// followerAddr is the address the follower's CellLeader server actually
	// bound, once serveFollower has bound it. See FollowerAddr.
	followerAddr string
	// cellLeaderHost is the LeaderHost runCellLeader constructs, once this
	// agent has joined the cell's raft cluster (issue #100's resilience
	// wiring). Exposing it (CellLeaderHost) lets a caller — production or a
	// test — observe this term's straggler-eviction/stall status
	// (LeaderHost.Status/StallInfo/Evicted) or the raft node it hosts
	// (LeaderHost.Node) without a dedicated RPC for either. Set once per
	// runCellLeader call; nil for an agent that never configures CellLeader,
	// or before it has joined the cluster.
	cellLeaderHost *LeaderHost
}

// New constructs an Agent from cfg, applying defaults for any field the
// caller left zero.
func New(cfg Config) *Agent {
	cfg = cfg.withDefaults()
	a := &Agent{
		cfg:     cfg,
		clients: newClientHolder(),
		health:  newHealthCache(),
	}
	if cfg.GlobalRouter != "" {
		a.dialTarget = cfg.RegionTargets[cfg.HomeRegion]
	}
	return a
}

// Run drives the registration loop, the task-runner loop, the (in
// multi-region mode) global-view poller, the P2 follower loop, and the P2
// cell-leader loop until ctx is done or one of them returns a
// non-cancellation error, in which case Run cancels the others and returns
// that error. The follower and cell-leader loops are inert unless
// Config.Follower.Listen / Config.CellLeader.RaftListen are set (see
// runFollower, runCellLeader) — a plain P0/P1 Config runs exactly the first
// two loops in substance.
func (a *Agent) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	const loops = 5
	errCh := make(chan error, loops)
	go func() { errCh <- a.runRegistration(ctx) }()
	go func() { errCh <- a.runRunner(ctx) }()
	go func() { errCh <- a.runGlobalView(ctx) }()
	go func() { errCh <- a.runFollower(ctx) }()
	go func() { errCh <- a.runCellLeader(ctx) }()

	var firstErr error
	for i := 0; i < loops; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	return firstErr
}

func (a *Agent) now() model.Instant { return a.cfg.Clock.Now() }
func (a *Agent) jitter() float64    { return a.cfg.Jitter() }

// currentTarget returns the dial target Failover last selected (or the
// first one, initially). In multi-region mode (GlobalRouter set) that is the
// resolved address of whichever region execFailover last picked, starting
// at HomeRegion's address; otherwise it is the current entry of Targets.
func (a *Agent) currentTarget() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cfg.GlobalRouter != "" {
		return a.dialTarget
	}
	return a.cfg.Targets[a.targetIdx%len(a.cfg.Targets)]
}

// execFailover resolves the next dial target for agentreg's Failover
// command. Single-region mode (GlobalRouter unset) keeps P0's behavior:
// advance to the next configured Targets entry, wrapping (a no-op with only
// one target). Multi-region mode asks region.SelectRegion which RegionID to
// try next — home first while it is reachable, then the ranked healthy
// peers, cycling as regionAttempt advances — and resolves the chosen
// RegionID to a dial address via RegionTargets. SelectRegion returning ""
// (no reachable candidate) leaves dialTarget unchanged, so the shell keeps
// retrying whatever it was already dialing (usually home).
func (a *Agent) execFailover() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cfg.GlobalRouter == "" {
		if n := len(a.cfg.Targets); n > 1 {
			a.targetIdx = (a.targetIdx + 1) % n
		}
		return
	}

	selected := region.SelectRegion(a.cfg.KnownRegions, a.health.get(), true, a.regionAttempt)
	a.regionAttempt++
	if selected == "" {
		return
	}
	if addr, ok := a.cfg.RegionTargets[selected]; ok && addr != "" {
		a.dialTarget = addr
	}
}

func (a *Agent) recordEnroll() {
	a.mu.Lock()
	a.enrolls++
	a.mu.Unlock()
}

// EnrollCount reports how many times this Agent has executed agentreg's
// Enroll command. It exists for tests to confirm "enroll once, even across
// reconnects."
func (a *Agent) EnrollCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.enrolls
}

// setCellLeaderHost / CellLeaderHost record and expose the LeaderHost
// runCellLeader constructs once this agent has joined its cell's raft
// cluster. See cellLeaderHost's doc for why this exists.
func (a *Agent) setCellLeaderHost(h *LeaderHost) {
	a.mu.Lock()
	a.cellLeaderHost = h
	a.mu.Unlock()
}

// CellLeaderHost returns the LeaderHost this agent constructed for its cell's
// raft cluster, or nil if it has never joined one (CellLeader.RaftListen
// unset, or runCellLeader has not reached joinCellRaft yet).
func (a *Agent) CellLeaderHost() *LeaderHost {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cellLeaderHost
}

// clientHolder holds the currently connected ControlPlaneClient, if any.
// Both loops read it; the registration loop writes it as Dial succeeds or
// ConnLost is detected.
type clientHolder struct {
	mu     sync.Mutex
	client transport.ControlPlaneClient
	closer io.Closer
}

func newClientHolder() *clientHolder {
	return &clientHolder{}
}

// set installs a new client, closing whatever connection was previously
// held.
func (h *clientHolder) set(c transport.ControlPlaneClient, closer io.Closer) {
	h.mu.Lock()
	old := h.closer
	h.client = c
	h.closer = closer
	h.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// clear drops the current client (e.g. on ConnLost), closing its connection.
func (h *clientHolder) clear() {
	h.set(nil, nil)
}

// get returns the current client, blocking (subject to ctx) until one is
// available.
func (h *clientHolder) get(ctx context.Context) (transport.ControlPlaneClient, error) {
	for {
		h.mu.Lock()
		c := h.client
		h.mu.Unlock()
		if c != nil {
			return c, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(clientPollInterval):
		}
	}
}
