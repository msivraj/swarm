package cell

import (
	"context"
	"sync"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/leader"
	"github.com/msivraj/swarm/internal/core/messagepassing"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// DriverKind selects which hosted driver's events StepReport folds a report
// into — a barrier Done or a leader Report carry the same wire shape
// (StepReportRequest) but a different Event.Kind.
type DriverKind int

const (
	// DriverBarrier folds StepReport into EventDone.
	DriverBarrier DriverKind = iota
	// DriverLeader folds StepReport into EventReport.
	DriverLeader
	// DriverMessagePassing hosts no StepReport traffic (message-passing has
	// no per-step report; see messagepassing's package doc) — StepReport is
	// rejected for this kind.
	DriverMessagePassing
)

// Server implements transport.CellLeaderServer: the leader-side RPC surface
// followers call in to (issue #68's CellLeader service). It wraps a Loop and
// translates each inbound RPC into the Event that RPC feeds the loop:
// StepReport -> EventDone (barrier) or EventReport (leader); DeliverMessage
// -> EventMessage (message-passing); MemberHeartbeat records liveness.
// AssignWork is the leader's OUTBOUND call to followers (see
// transportexec.go) — followers, not this Server, implement AssignWork on
// their own end, which is a worker's job, out of this ticket's scope — so
// Server's AssignWork is unimplemented via the embedded
// UnimplementedCellLeaderServer.
type Server struct {
	transport.UnimplementedCellLeaderServer

	Loop *Loop
	Kind DriverKind
	Now  func() model.Instant

	mu       sync.Mutex
	lastSeen map[string]model.Instant
}

var _ transport.CellLeaderServer = (*Server)(nil)

// NewServer returns a Server hosting loop, folding StepReport events for
// the driver kind, with now supplying the clock for every event this Server
// feeds the loop.
func NewServer(loop *Loop, kind DriverKind, now func() model.Instant) *Server {
	return &Server{Loop: loop, Kind: kind, Now: now, lastSeen: make(map[string]model.Instant)}
}

// StepReport folds a follower's step/superstep report into the loop as
// EventDone (barrier) or EventReport (leader).
func (s *Server) StepReport(ctx context.Context, req *transport.StepReportRequest) (*transport.StepReportResponse, error) {
	var ev Event
	switch s.Kind {
	case DriverBarrier:
		ev = Event{Kind: EventDone, Worker: barrier.WorkerID(req.GetWorker()), Partial: req.GetPayload()}
	case DriverLeader:
		ev = Event{Kind: EventReport, Follower: leader.FollowerID(req.GetWorker()), Result: req.GetPayload()}
	default:
		return &transport.StepReportResponse{Ok: false}, nil
	}

	now := s.now()
	s.mu.Lock()
	s.lastSeen[req.GetWorker()] = now
	s.mu.Unlock()

	if _, err := s.Loop.Handle(ctx, ev, now); err != nil {
		return nil, err
	}
	return &transport.StepReportResponse{Ok: true}, nil
}

// DeliverMessage folds an inbound message into the loop as EventMessage,
// feeding the message-passing driver's React fold.
func (s *Server) DeliverMessage(ctx context.Context, req *transport.DeliverMessageRequest) (*transport.DeliverMessageResponse, error) {
	msg := messagepassing.Message{
		ID:   req.GetMessageId(),
		To:   messagepassing.ActorID(req.GetToActor()),
		Body: req.GetPayload(),
	}
	if _, err := s.Loop.Handle(ctx, Event{Kind: EventMessage, Message: msg}, s.now()); err != nil {
		return nil, err
	}
	return &transport.DeliverMessageResponse{Ok: true}, nil
}

// MemberHeartbeat records worker's liveness. It does not itself synthesize
// an EventLost/EventCrash — deciding a member is dead from a stale lastSeen
// is the adaptive-by-tier detection core's job (internal/core/detection),
// which a caller wires against LastSeen on its own timer; this ticket's
// scope is recording the heartbeat, not the eviction policy on top of it.
func (s *Server) MemberHeartbeat(_ context.Context, req *transport.MemberHeartbeatRequest) (*transport.MemberHeartbeatResponse, error) {
	s.mu.Lock()
	s.lastSeen[req.GetWorker()] = s.now()
	s.mu.Unlock()
	return &transport.MemberHeartbeatResponse{Ok: true}, nil
}

// LastSeen returns worker's last recorded heartbeat/report instant, or
// ok=false if none has been recorded.
func (s *Server) LastSeen(worker string) (model.Instant, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.lastSeen[worker]
	return t, ok
}

func (s *Server) now() model.Instant {
	if s.Now == nil {
		return 0
	}
	return s.Now()
}
