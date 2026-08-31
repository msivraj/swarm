package agent

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/msivraj/swarm/internal/shell/transport"
)

// CellLeaderDialer opens a connection to a CellLeader dial-back address — the
// follower uses it only to call StepReport on whichever leader address
// arrived in the most recent AssignWork's payload envelope (see
// decodeAssignWorkPayload). Tests supply one backed by an in-process
// (bufconn or real loopback) fake leader; production uses
// GRPCCellLeaderDialer.
type CellLeaderDialer func(ctx context.Context, target string) (transport.CellLeaderClient, io.Closer, error)

// GRPCCellLeaderDialer is the CellLeaderDialer the shell uses in production:
// a plaintext gRPC dial, matching GRPCDialer/GRPCGlobalViewDialer's trust
// assumptions (P0 assumes trusted machines).
func GRPCCellLeaderDialer() CellLeaderDialer {
	return func(_ context.Context, target string) (transport.CellLeaderClient, io.Closer, error) {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return transport.NewCellLeaderClient(conn), conn, nil
	}
}

// FollowerConfig configures P2 coupled-cell follower mode (issue #96): an
// agent that, once CellAssignment (issue #101) tells it it belongs to a
// coupled cell, hosts a CellLeader AssignWork server and execs a worker once
// per barrier step (D5: exec-once-per-step). Leaving Listen empty — the
// zero value, and the default — disables follower mode entirely: runFollower
// then never even polls CellAssignment, so an agent that does not opt in
// behaves exactly like a P0/P1 agent.
type FollowerConfig struct {
	// Listen is the address this agent's CellLeader AssignWork server binds.
	// May be "host:0" to pick an ephemeral port — see FollowerAddr for how
	// to discover the address actually bound, which is what gets advertised
	// as JoinAgentRequest.cell_leader_addr. Empty disables follower mode.
	Listen string

	// Worker executes one barrier step given its fully-built stdin (shard
	// range + step + incoming gradient, see encodeStepInput) and returns the
	// step's raw stdout as the partial result, ok=false on any failure.
	// Defaults to a.execProcess, which execs cfg.Process.Argv exactly like
	// runProcess does for the P0/P1 task runner (D5's "reuses runProcess's
	// stdin/stdout/exit-code shape unchanged").
	Worker func(ctx context.Context, in []byte) ([]byte, bool)

	// Dialer opens a connection to the leader's CellLeader dial-back address
	// to call StepReport. Defaults to GRPCCellLeaderDialer.
	Dialer CellLeaderDialer
}

func (f FollowerConfig) withDefaults() FollowerConfig {
	if f.Dialer == nil {
		f.Dialer = GRPCCellLeaderDialer()
	}
	return f
}

// runFollower is the agent's 4th run loop (issue #96): while unconfigured
// (Follower.Listen == "") it is completely inert, blocking on ctx exactly
// like runGlobalView's disabled branch — no CellAssignment traffic, no
// CellLeader server, no behavior change for a P0/P1 agent. Once configured
// it polls CellAssignment until has_assignment is true, then hosts a
// CellLeader server on Follower.Listen implementing AssignWork (the leader
// -> follower call cell.Server leaves Unimplemented) until ctx is done.
func (a *Agent) runFollower(ctx context.Context) error {
	if a.cfg.Follower.Listen == "" {
		<-ctx.Done()
		return ctx.Err()
	}

	assignment, err := a.awaitCellAssignment(ctx)
	if err != nil {
		return err
	}
	return a.serveFollower(ctx, assignment)
}

// awaitCellAssignment polls ControlPlane.CellAssignment (on the same
// clientHolder connection the registration loop maintains) until it reports
// has_assignment, sleeping PullInterval between attempts — the same poll
// cadence execPull uses. A transient RPC failure (including an
// UnimplementedControlPlaneServer, which is what every pre-#96 fake in this
// package's other tests embeds) is treated exactly like "not assigned yet":
// it is not fatal, and the loop just keeps polling.
func (a *Agent) awaitCellAssignment(ctx context.Context) (*transport.CellAssignmentResponse, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		resp, err := a.fetchCellAssignment(ctx)
		if err == nil && resp.GetHasAssignment() {
			return resp, nil
		}

		if err := a.sleep(ctx, a.cfg.PullInterval); err != nil {
			return nil, err
		}
	}
}

// fetchCellAssignment makes ONE ControlPlane.CellAssignment RPC attempt for
// this agent, using the current client connection (a.clients.get) — the
// single-call primitive both awaitCellAssignment's blocking "wait for the
// FIRST assignment" loop and pollCellAssignment's refill-poll (issue #122,
// LeaderHost.PollAssignment) build on.
func (a *Agent) fetchCellAssignment(ctx context.Context) (*transport.CellAssignmentResponse, error) {
	client, err := a.clients.get(ctx)
	if err != nil {
		return nil, err
	}
	return client.CellAssignment(ctx, &transport.CellAssignmentRequest{Agent: a.cfg.AgentID})
}

// pollCellAssignment is LeaderHost.PollAssignment's production wiring (issue
// #122, H1-C): a single, non-retrying CellAssignment poll — a parked
// LeaderHost's own refill-poll loop (awaitRefill, leader.go) already
// supplies the retry cadence, so this only needs to hand back whatever this
// one attempt got, including a transient error, which awaitRefill treats
// exactly like "not refilled yet".
func (a *Agent) pollCellAssignment(ctx context.Context) (*transport.CellAssignmentResponse, error) {
	return a.fetchCellAssignment(ctx)
}

// serveFollower binds Follower.Listen, records the resolved address (see
// FollowerAddr), advertises it to the control plane via JoinAgent, then
// serves the CellLeader AssignWork RPC until ctx is done.
func (a *Agent) serveFollower(ctx context.Context, assignment *transport.CellAssignmentResponse) error {
	lis, err := net.Listen("tcp", a.cfg.Follower.Listen)
	if err != nil {
		return fmt.Errorf("agent: follower listen on %s: %w", a.cfg.Follower.Listen, err)
	}
	addr := lis.Addr().String()
	a.setFollowerAddr(addr)

	if err := a.advertiseFollower(ctx, addr); err != nil {
		_ = lis.Close()
		return err
	}

	srv := grpc.NewServer()
	transport.RegisterCellLeaderServer(srv, &followerServer{agent: a, assignment: assignment})

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	select {
	case <-ctx.Done():
		srv.GracefulStop()
		<-serveErr
		return ctx.Err()
	case err := <-serveErr:
		return err
	}
}

// advertiseFollower re-advertises this agent's identity to the control
// plane, now with cell_leader_addr populated (raft_addr stays empty: hosting
// the cell's raft cluster is issue #102's scope, not this ticket's). It
// retries transient failures via rpcRetry rather than giving up, matching
// execReport/execReQueue's "must not lose the outcome" contract — a follower
// that fails to advertise can never be reached by the leader.
func (a *Agent) advertiseFollower(ctx context.Context, cellLeaderAddr string) error {
	return a.rpcRetry(ctx, func(client transport.ControlPlaneClient) error {
		resp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{
			Agent:          a.cfg.AgentID,
			Region:         a.cfg.Region,
			Caps:           a.cfg.Caps,
			CellLeaderAddr: cellLeaderAddr,
		})
		if err != nil {
			return err
		}
		if !resp.GetAccepted() {
			return fmt.Errorf("agent: JoinAgent (follower advertise) rejected: %s", resp.GetReason())
		}
		return nil
	})
}

// setFollowerAddr / FollowerAddr record and expose the address the
// follower's CellLeader server actually bound to (resolved from
// Follower.Listen, e.g. after a ":0" ephemeral port) — exists so production
// callers and tests can discover the real address without racing a fixed
// port.
func (a *Agent) setFollowerAddr(addr string) {
	a.mu.Lock()
	a.followerAddr = addr
	a.mu.Unlock()
}

// FollowerAddr returns the address the follower's CellLeader server actually
// bound, and whether it has bound yet.
func (a *Agent) FollowerAddr() (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.followerAddr, a.followerAddr != ""
}

// followerServer implements transport.CellLeaderServer's AssignWork on the
// follower's own CellLeader listener — the call cell.Server (the leader
// side) leaves Unimplemented. StepReport/DeliverMessage/MemberHeartbeat stay
// Unimplemented here too: a follower never receives them (see #96's ticket).
type followerServer struct {
	transport.UnimplementedCellLeaderServer

	agent      *Agent
	assignment *transport.CellAssignmentResponse
}

var _ transport.CellLeaderServer = (*followerServer)(nil)

// AssignWork execs the worker for one barrier step (issue #96's D5) and
// reports the result back to the leader:
//  1. decode the payload envelope into the leader's current StepReport
//     dial-back address and the incoming all-reduced gradient (D4's
//     no-proto path — see decodeAssignWorkPayload);
//  2. build the worker's stdin from this follower's shard, the step, and the
//     incoming gradient, and run it (Follower.Worker, default execProcess);
//  3. dial the leader and call StepReport with the worker's raw stdout as
//     the partial result.
//
// It returns Accepted=false (never a Go error — AssignWork has no dedicated
// failure event, exactly like execRegCommand's RPC failures) for a malformed
// payload, a failed exec, or a failed StepReport delivery.
func (s *followerServer) AssignWork(ctx context.Context, req *transport.AssignWorkRequest) (*transport.AssignWorkResponse, error) {
	leaderAddr, incoming, ok := decodeAssignWorkPayload(req.GetPayload())
	if !ok {
		return &transport.AssignWorkResponse{Accepted: false}, nil
	}

	stdin := encodeStepInput(s.assignment.GetShardInput(), req.GetStep(), incoming)

	worker := s.agent.cfg.Follower.Worker
	if worker == nil {
		worker = s.agent.execProcess
	}
	partial, ok := worker(ctx, stdin)
	if !ok {
		return &transport.AssignWorkResponse{Accepted: false}, nil
	}

	if err := s.agent.reportStep(ctx, leaderAddr, req.GetJobId(), req.GetWorker(), req.GetStep(), partial); err != nil {
		return &transport.AssignWorkResponse{Accepted: false}, nil
	}
	return &transport.AssignWorkResponse{Accepted: true}, nil
}

// reportStep dials leaderAddr and calls StepReport with payload as the
// step's partial result.
func (a *Agent) reportStep(ctx context.Context, leaderAddr, jobID, worker string, step int32, payload []byte) error {
	client, closer, err := a.cfg.Follower.Dialer(ctx, leaderAddr)
	if err != nil {
		return fmt.Errorf("agent: dial leader %s: %w", leaderAddr, err)
	}
	defer func() { _ = closer.Close() }()

	resp, err := client.StepReport(ctx, &transport.StepReportRequest{
		JobId: jobID, Worker: worker, Step: step, Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("agent: StepReport to %s: %w", leaderAddr, err)
	}
	if !resp.GetOk() {
		return fmt.Errorf("agent: StepReport to %s rejected", leaderAddr)
	}
	return nil
}

// encodeStepInput builds the stdin the exec-once-per-step worker (see
// internal/e2e/workers/disttraining) expects: shard — this follower's
// DistTrainingDecompose Task.Input, [start,end) as two big-endian uint64s,
// carried verbatim from CellAssignmentResponse.shard_input — followed by the
// barrier step as a third big-endian uint64, followed by the incoming
// all-reduced gradient exactly as it arrived (already wire-encoded
// consecutive big-endian float64s; empty at step 0). This mirrors
// internal/e2e.EncodeDTStdin's byte layout by hand rather than importing
// internal/e2e, which itself imports this package to build agents for its
// own end-to-end tests — importing it back here would be a cycle (the same
// reason internal/e2e/wire.go mirrors internal/core/templates' unexported
// wire format by hand instead of sharing code).
func encodeStepInput(shard []byte, step int32, incoming []byte) []byte {
	out := make([]byte, len(shard), len(shard)+8+len(incoming))
	copy(out, shard)
	var stepBuf [8]byte
	binary.BigEndian.PutUint64(stepBuf[:], uint64(step))
	out = append(out, stepBuf[:]...)
	out = append(out, incoming...)
	return out
}

// AssignWorkRequest.Payload envelope (issue #94's D4 ruling, the no-proto
// leader dial-back path): a 4-byte big-endian length-prefixed leader
// CellLeader dial address, followed by the incoming all-reduced gradient
// verbatim. The leader (issue #102) embeds its own current dial-back address
// on every AssignWork call because raft leadership can change between steps
// — a follower has no raft membership view of its own in this ticket's scope
// — so a stale cached "the leader" address would go wrong on failover, and
// encoding it in the (already opaque []byte) Payload needs no proto change.
//
// encodeAssignWorkPayload is the leader-side encoder; #102 (and this
// ticket's own tests, standing in for #102's not-yet-built leader) use it to
// build AssignWorkRequest.Payload. decodeAssignWorkPayload is its inverse,
// used by followerServer.AssignWork above.
func encodeAssignWorkPayload(leaderAddr string, incoming []byte) []byte {
	out := make([]byte, 4, 4+len(leaderAddr)+len(incoming))
	binary.BigEndian.PutUint32(out, uint32(len(leaderAddr)))
	out = append(out, leaderAddr...)
	out = append(out, incoming...)
	return out
}

func decodeAssignWorkPayload(payload []byte) (leaderAddr string, incoming []byte, ok bool) {
	if len(payload) < 4 {
		return "", nil, false
	}
	n := binary.BigEndian.Uint32(payload[:4])
	if uint32(len(payload)-4) < n {
		return "", nil, false
	}
	addr := string(payload[4 : 4+n])
	rest := payload[4+n:]
	return addr, rest, true
}
