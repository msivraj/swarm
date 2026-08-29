package cell

import (
	"context"
	"fmt"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/messagepassing"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// TransportExecutor is the production Executor: it carries out barrier,
// leader, and message-passing Commands against followers over the
// CellLeader transport (issue #68) — AssignWork to hand out a step/superstep
// (barrier's Release, leader's Advance/Reassign), and DeliverMessage to
// deliver a Send. The remaining Ops (AllReduce, Checkpoint, Fold, Evict,
// Rollback, Stall, Fail, Restart) are local shell decisions this ticket
// surfaces as injectable hooks rather than RPCs — issue #68's CellLeader
// surface only names AssignWork/StepReport/DeliverMessage/MemberHeartbeat,
// and StepReport/MemberHeartbeat are inbound to the leader (see server.go),
// not something this Executor calls out.
type TransportExecutor struct {
	JobID string

	// Dial returns the CellLeaderClient for worker/follower id. Required for
	// OpRelease, OpAdvance, OpReassign, and OpSend.
	Dial func(worker string) (transport.CellLeaderClient, error)

	// Members returns the current membership to broadcast an OpRelease or
	// OpAdvance to (every follower is told the new step/superstep).
	Members func() []string

	// AllReduce carries out OpAllReduce (the real collective — NCCL/custom —
	// is out of this ticket's scope; nil is a no-op).
	AllReduce func(ctx context.Context, partials map[barrier.WorkerID][]byte) error

	// Fold carries out OpFold (nil is a no-op).
	Fold func(ctx context.Context, results map[string][]byte) error

	// Checkpoint persists OpCheckpoint via a CheckpointStore (nil is a
	// no-op).
	Checkpoint CheckpointStore

	// Driver and State together supply OpCheckpoint's payload: Driver.Snapshot
	// of State() is written into checkpoint.State.DriverBlob (alongside
	// c.Step) so a later Resume(log, ckpt) has a driver state to decode
	// before folding the post-checkpoint log forward — Resume seeds its base
	// state FROM DriverBlob, and the log alone cannot re-supply it (it never
	// carries the full state, only deltas — see e.g. applyBarrierCommand).
	// State is typically a *Loop's State method, called at OpCheckpoint
	// execution time, which is after Loop.Handle has already folded the
	// triggering event into the driver's next state. Either being nil
	// degrades to Step-only checkpoint (DriverBlob nil) rather than a panic,
	// matching this Executor's other hooks' nil-is-no-op convention — but a
	// production wiring that leaves them unset cannot recover on failover.
	Driver Driver
	State  func() State
}

var _ Executor = (*TransportExecutor)(nil)

// Exec executes cmds in order, stopping at the first error.
func (e *TransportExecutor) Exec(ctx context.Context, cmds []Command) error {
	for _, c := range cmds {
		if err := e.execOne(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func (e *TransportExecutor) execOne(ctx context.Context, c Command) error {
	switch c.Op {
	case OpRelease:
		return e.assignAll(ctx, c.Step, nil)
	case OpAdvance:
		return e.assignAll(ctx, c.Superstep, nil)
	case OpReassign:
		return e.assignOne(ctx, string(c.Follower), 0, c.Work)
	case OpAllReduce:
		if e.AllReduce == nil {
			return nil
		}
		return e.AllReduce(ctx, c.Partials)
	case OpFold:
		if e.Fold == nil {
			return nil
		}
		results := make(map[string][]byte, len(c.Results))
		for f, r := range c.Results {
			results[string(f)] = r
		}
		return e.Fold(ctx, results)
	case OpCheckpoint:
		if e.Checkpoint == nil {
			return nil
		}
		var blob []byte
		if e.Driver != nil && e.State != nil {
			blob = e.Driver.Snapshot(e.State())
		}
		return e.Checkpoint.Put(e.JobID, checkpoint.State{Step: c.Step, DriverBlob: blob})
	case OpSend:
		return e.deliver(ctx, c.Send)
	default:
		// OpEvict, OpRollback, OpStall, OpFail, OpAssign, OpRestart: local
		// shell bookkeeping this ticket does not turn into an RPC — see the
		// package doc above.
		return nil
	}
}

// assignAll calls AssignWork on every current member for step, in the order
// e.Members returns.
func (e *TransportExecutor) assignAll(ctx context.Context, step int, payload []byte) error {
	if e.Members == nil {
		return nil
	}
	for _, w := range e.Members() {
		if err := e.assignOne(ctx, w, step, payload); err != nil {
			return err
		}
	}
	return nil
}

// assignOne calls AssignWork on worker for step/payload.
func (e *TransportExecutor) assignOne(ctx context.Context, worker string, step int, payload []byte) error {
	if e.Dial == nil {
		return nil
	}
	c, err := e.Dial(worker)
	if err != nil {
		return fmt.Errorf("cell: dial %s: %w", worker, err)
	}
	_, err = c.AssignWork(ctx, &transport.AssignWorkRequest{
		JobId: e.JobID, Worker: worker, Step: int32(step), Payload: payload,
	})
	return err
}

// deliver calls DeliverMessage on send.To, dialed directly as a worker id —
// this ticket's scope is a single cell (see the package doc), so
// messagepassing.Route's cross-cell resolution is exercised at the server
// side (server.go's DeliverMessage handler) rather than here.
func (e *TransportExecutor) deliver(ctx context.Context, send messagepassing.Send) error {
	if e.Dial == nil {
		return nil
	}
	c, err := e.Dial(string(send.To))
	if err != nil {
		return fmt.Errorf("cell: dial %s: %w", send.To, err)
	}
	_, err = c.DeliverMessage(ctx, &transport.DeliverMessageRequest{
		JobId: e.JobID, ToActor: string(send.To), MessageId: send.ID, Payload: send.Body,
	})
	return err
}
