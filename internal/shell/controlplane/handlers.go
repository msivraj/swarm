package controlplane

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/core/rendezvous"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// SubmitJob decomposes req into a JobSpec via admission.Admit and, on
// acceptance, persists the job and places each of its tasks via
// placement.Place: a task that lands on a cell is enqueued on that cell's
// store queue (see internal/shell/store), and a task placement.Place cannot
// currently place (the region is full) is held in the server's ingress
// pending buffer rather than dropped or failing the submit — SubmitJob ends
// by draining that buffer once, so a task another goroutine's JoinAgent had
// already made room for gets picked up immediately too.
func (s *Server) SubmitJob(_ context.Context, req *transport.SubmitJobRequest) (*transport.SubmitJobResponse, error) {
	done, err := s.admitIngress(requestPriority(req.GetParams()))
	if err != nil {
		return nil, err
	}
	defer done()

	s.mu.Lock()
	jobID := s.newJobIDLocked()
	s.mu.Unlock()

	spec := model.JobSpec{
		ID:         jobID,
		Template:   req.GetTemplate(),
		Coupling:   fromProtoCoupling(req.GetCoupling()),
		Params:     req.GetParams(),
		MinMembers: parseMinMembers(req.GetParams()),
	}

	// A gang job (MinMembers > 0, B4) takes a different admission path
	// entirely — admission.AdmitGang's all-or-nothing Place/Wait decision,
	// atomically reserved or queued by submitGang (see gang.go, #71) —
	// rather than admission.Admit's single-job template decomposition,
	// which P0/P1 gate to Independent coupling only.
	if spec.MinMembers > 0 {
		return s.submitGang(spec)
	}

	tasks, rej := admission.Admit(spec)
	if rej.Rejected {
		return nil, status.Error(codes.InvalidArgument, rej.Reason)
	}

	if err := s.store.PutJob(spec); err != nil {
		return nil, status.Errorf(codes.Internal, "put job: %v", err)
	}

	s.mu.Lock()
	for _, t := range tasks {
		s.taskJob[t.ID] = t.JobID
	}
	s.taskTotal[jobID] = len(tasks)
	s.pending = append(s.pending, tasks...)
	err = s.drainPendingLocked()
	s.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "place tasks: %v", err)
	}

	return &transport.SubmitJobResponse{JobId: string(jobID)}, nil
}

// JoinAgent decides where a dialing agent lands via rendezvous.AdmitAgent
// and applies the resulting registry.AgentJoined event (forming a new cell
// first if the decision is NewCell). This is P0's central membership: no
// gossip, no SWIM — the control plane is the sole authority on who is in
// which cell.
func (s *Server) JoinAgent(_ context.Context, req *transport.JoinAgentRequest) (*transport.JoinAgentResponse, error) {
	done, err := s.admitIngress(s.cfg.JoinPriority)
	if err != nil {
		return nil, err
	}
	defer done()

	agent := req.GetAgent()

	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := registry.Snapshot(s.store.Registry())
	decision := rendezvous.AdmitAgent(rendezvous.JoinReq{
		Agent:  rendezvous.AgentID(agent),
		Region: req.GetRegion(),
		Caps:   int(req.GetCaps()),
	}, snapshot)

	switch decision.Kind {
	case rendezvous.Reject:
		return &transport.JoinAgentResponse{Accepted: false, Reason: decision.Reason}, nil

	case rendezvous.Accept:
		s.applyRegistryEventLocked(registry.RegistryEvent{
			Kind: registry.AgentJoined, Cell: decision.Cell, Agent: registry.AgentID(agent),
		})
		s.recordJoinLocked(agent, decision.Cell)
		s.recordJoinAddrLocked(agent, req)
		// A joining agent added capacity, so re-run placement over any tasks
		// the ingress pending buffer is holding — one of them may now fit.
		if err := s.drainPendingLocked(); err != nil {
			return nil, status.Errorf(codes.Internal, "place pending tasks: %v", err)
		}
		// Same capacity-change retry for the gang pending queue (#71): a cell
		// gaining capacity may now be enough for the gang waiting at its head.
		s.retryPendingGangsLocked()
		return &transport.JoinAgentResponse{CellId: string(decision.Cell), Accepted: true}, nil

	case rendezvous.NewCell:
		capacity := s.cfg.DefaultCellCapacity
		if reqCaps := int(req.GetCaps()); reqCaps > capacity {
			capacity = reqCaps
		}
		cellID := s.newCellIDLocked()
		s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellUp, Cell: cellID, Capacity: capacity})
		s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.AgentJoined, Cell: cellID, Agent: registry.AgentID(agent)})
		s.recordJoinLocked(agent, cellID)
		s.recordJoinAddrLocked(agent, req)
		// A new cell appeared, so re-run placement over the pending buffer
		// the same way the Accept branch does.
		if err := s.drainPendingLocked(); err != nil {
			return nil, status.Errorf(codes.Internal, "place pending tasks: %v", err)
		}
		// Same capacity-change retry for the gang pending queue (#71): a
		// brand-new cell may itself be, or complete, what a queued gang needs.
		s.retryPendingGangsLocked()
		return &transport.JoinAgentResponse{CellId: string(cellID), Accepted: true}, nil

	default:
		return nil, status.Error(codes.Internal, "rendezvous: unknown decision kind")
	}
}

// recordJoinLocked updates the shell-held agent<->cell bookkeeping (the
// registry core folds membership but does not expose per-cell membership
// back out, so the shell keeps its own index) and refreshes agent's
// last-seen time. Callers must hold s.mu.
func (s *Server) recordJoinLocked(agent string, cell model.CellID) {
	s.agentCell[agent] = cell
	if s.cellAgents[cell] == nil {
		s.cellAgents[cell] = make(map[string]struct{})
	}
	s.cellAgents[cell][agent] = struct{}{}
	s.lastSeen[agent] = s.now()
}

// recordJoinAddrLocked records agent's advertised raft/cell-leader
// listeners from req (#101's JoinAgentRequest.raft_addr/cell_leader_addr),
// the addresses a coupled cell's raft peer set (activateCoupledCellLocked)
// reads back out. A P0/P1 agent that never joins a coupled cell leaves both
// empty, which is exactly what it sends today — recording them here does
// not change JoinAgent's existing behavior for it. Callers must hold s.mu.
func (s *Server) recordJoinAddrLocked(agent string, req *transport.JoinAgentRequest) {
	s.agentAddrs[agent] = agentAddr{raftAddr: req.GetRaftAddr(), cellLeaderAddr: req.GetCellLeaderAddr()}
}

// Heartbeat refreshes agent's last-contact time, which the reaper loop reads
// to decide whether to evict it.
func (s *Server) Heartbeat(_ context.Context, req *transport.HeartbeatRequest) (*transport.HeartbeatResponse, error) {
	s.mu.Lock()
	s.lastSeen[req.GetAgent()] = s.now()
	s.mu.Unlock()
	return &transport.HeartbeatResponse{Ok: true}, nil
}

// CellAssignment serves req.Agent's pending coupled-cell assignment (#101):
// has_assignment=false if activateCoupledCellLocked has never built one for
// this agent (a plain P0/P1 agent, or one this control plane has not yet
// activated), otherwise the full assignment exactly as
// activateCoupledCellLocked built it — this is the only channel that tells
// an agent it is now in a coupled cell (activating #96/#102). A repeated
// poll for the same agent gets the same response back: nothing here ever
// clears an assignment once made.
func (s *Server) CellAssignment(_ context.Context, req *transport.CellAssignmentRequest) (*transport.CellAssignmentResponse, error) {
	s.mu.Lock()
	assignment, ok := s.cellAssignments[req.GetAgent()]
	s.mu.Unlock()
	if !ok {
		return &transport.CellAssignmentResponse{HasAssignment: false}, nil
	}
	return assignment, nil
}

// ReportCellStatus is the elected cell leader's upward status notice (#116,
// ticket (c) of the member-churn design; the proto is #121). Its
// load-bearing signal is req.Stalled: once a coupled barrier parks under its
// min_members floor (H1-A, the leader's own detection, out of this ticket's
// scope), the control plane releases this gang's reservation and re-enqueues
// it on the pending gang queue — releaseGangReservationLocked(jobID, true) —
// then immediately retries that queue, so the capacity a stalled gang was
// holding is never left stranded while the job is parked (closing the other
// half of the #71 remainder alongside onCoupledComplete's completion-path
// release). A later retryPendingGangsLocked (driven from JoinAgent or the
// mitosis tick, same as any other pending gang) is what actually re-admits
// the job once the cell refills — see activateCoupledCellLocked's doc for
// how that re-decomposes the dataset over the cell's current members.
//
// req.Stalled == false carries no CP-side action today: H1-A's own "runnable
// again" signal is the leader re-polling the existing CellAssignment RPC
// (#101), not a downward push from this RPC — see the CellStatusRequest
// proto doc. This handler always accepts the report; a jobID this control
// plane no longer holds a reservation for (already released by an earlier
// stall report, or already completed) is exactly releaseGangReservationLocked's
// documented no-op, not an error.
func (s *Server) ReportCellStatus(_ context.Context, req *transport.CellStatusRequest) (*transport.CellStatusResponse, error) {
	if req.GetStalled() {
		jobID := model.JobID(req.GetJobId())
		s.mu.Lock()
		s.releaseGangReservationLocked(jobID, true)
		s.retryPendingGangsLocked()
		s.mu.Unlock()
	}
	return &transport.CellStatusResponse{Accepted: true}, nil
}

// PullTask serves the agent's runner loop from its own cell's queue: it
// looks up which cell req's agent belongs to (learned at JoinAgent) and
// dequeues from that cell's queue only — an agent never receives a task
// placed on another cell. An agent the server does not (or no longer) know
// the cell of (never joined, or reaped) gets HasTask:false, the same
// response as an empty queue.
func (s *Server) PullTask(_ context.Context, req *transport.PullTaskRequest) (*transport.PullTaskResponse, error) {
	// PullTask only ever throttles under load, never sheds (fork (b) of
	// #157): a stalled runner loop can only make load worse, never better,
	// so it is slowed instead of hard-rejected. See admitThrottleOnly's doc.
	done := s.admitThrottleOnly(0)
	defer done()

	s.mu.Lock()
	cell, ok := s.agentCell[req.GetAgent()]
	s.mu.Unlock()
	if !ok {
		return &transport.PullTaskResponse{HasTask: false}, nil
	}

	t, ok, err := s.store.DequeueTask(cell)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "dequeue task: %v", err)
	}
	if !ok {
		return &transport.PullTaskResponse{HasTask: false}, nil
	}
	return &transport.PullTaskResponse{
		HasTask: true,
		Task: &transport.Task{
			Id:      string(t.ID),
			JobId:   string(t.JobID),
			Input:   t.Input,
			Attempt: int32(t.Attempt),
		},
	}, nil
}

// ReportResult records the reported TaskResult and, once every DISTINCT task
// SubmitJob admitted for the owning job has reported, merges the collected
// results into the job's Aggregate via its template's merge function and
// marks it done in the store.
//
// A task may legitimately report more than once (e.g. a retry after
// failure); store.PutResult does not dedup (see the store package doc), so
// this handler tracks the set of distinct TaskIDs that have reported per
// job itself, and gates aggregation on that distinct count rather than on
// the raw, possibly-inflated result count — otherwise a duplicate report
// could push the count to total before every distinct task has reported,
// aggregating on incomplete data.
//
// ReportResult is deliberately NEVER gated by backpressure (fork (b) of
// #157, the same design decision as PullTask's throttle-only carve-out,
// taken further here): completed work is precious, and dropping a result
// loses finished compute a worker already spent real time producing, so
// this handler carries no admitIngress/admitThrottleOnly call at all —
// unlike SubmitJob/JoinAgent/PullTask, high control-plane load never makes
// this RPC wait or fail.
func (s *Server) ReportResult(_ context.Context, req *transport.ReportResultRequest) (*transport.ReportResultResponse, error) {
	taskID := model.TaskID(req.GetTaskId())

	// A coupled gang's elected leader reports its final, all-reduced
	// gradient (D6, #98) keyed by the job id itself, reusing this same RPC
	// rather than a dedicated one (owner-decided, no new proto) — gang jobs
	// never go through admission.Admit's per-task decomposition (see
	// gang.go), so there is no TaskID here for store.PutResult to
	// recognize. s.gangJobs only ever holds entries for MinMembers>0 jobs
	// (see admitGangLocked), so this check can never match a P0/P1
	// Independent job's real TaskID and leaves that path byte-for-byte
	// unchanged.
	s.mu.Lock()
	_, isGangJob := s.gangJobs[model.JobID(taskID)]
	s.mu.Unlock()
	if isGangJob {
		if err := s.onCoupledComplete(model.JobID(taskID), req.GetOutput()); err != nil {
			return nil, status.Errorf(codes.Internal, "coupled completion for job %s: %v", taskID, err)
		}
		return &transport.ReportResultResponse{Accepted: true}, nil
	}

	result := model.TaskResult{TaskID: taskID, Output: req.GetOutput(), OK: req.GetOk()}

	if err := s.store.PutResult(result); err != nil {
		return nil, status.Errorf(codes.NotFound, "report result: %v", err)
	}

	s.mu.Lock()
	jobID, known := s.taskJob[taskID]
	total := s.taskTotal[jobID]
	sink := s.resultSink[jobID]
	if known {
		if s.reportedTasks[jobID] == nil {
			s.reportedTasks[jobID] = make(map[model.TaskID]struct{})
		}
		s.reportedTasks[jobID][taskID] = struct{}{}
	}
	s.mu.Unlock()
	if !known {
		// PutResult already validated the task, so this should not happen;
		// there is nothing more this handler can do about it, and the
		// result the caller reported has already been durably recorded.
		return &transport.ReportResultResponse{Accepted: true}, nil
	}

	// A spilled task's result forwards to its origin immediately, per task,
	// rather than waiting on this region's own distinct-task gate — this
	// region never owns that job's completion (ticket #44's "spilled task"
	// semantics; see isSpillForward). A forward failure is not surfaced to
	// the reporting agent: the result is already durably recorded here, and
	// there is no retry queue for this ticket's scope to hang the RPC on.
	if isSpillForward(sink, s.cfg.GlobalRouter) {
		_ = s.forwardResultToOrigin(sink, result)
		return &transport.ReportResultResponse{Accepted: true}, nil
	}

	if err := s.maybeRollup(jobID, total); err != nil {
		return nil, status.Errorf(codes.Internal, "aggregate job %s: %v", jobID, err)
	}
	return &transport.ReportResultResponse{Accepted: true}, nil
}

// dedupeTaskResults collapses results to at most one entry per TaskID: a
// later result for a TaskID already seen overwrites the earlier one in
// place, keeping that TaskID's first-arrival position but its most recent
// value, rather than appearing twice in what feeds the merge. store.PutResult
// does not dedup (a retried task may legitimately report more than once), so
// this is where a duplicate report stops from double-counting toward, or
// corrupting, the merged Aggregate.
func dedupeTaskResults(results []model.TaskResult) []model.TaskResult {
	out := make([]model.TaskResult, 0, len(results))
	index := make(map[model.TaskID]int, len(results))
	for _, r := range results {
		if i, ok := index[r.TaskID]; ok {
			out[i] = r
			continue
		}
		index[r.TaskID] = len(out)
		out = append(out, r)
	}
	return out
}

// Ps reports fleet-wide counts: cells and machines from the registry
// snapshot, jobs from the count of jobs this server has admitted.
func (s *Server) Ps(_ context.Context, _ *transport.PsRequest) (*transport.PsResponse, error) {
	snapshot := registry.Snapshot(s.store.Registry())
	machines := 0
	for _, c := range snapshot {
		machines += c.Size
	}

	s.mu.Lock()
	jobs := len(s.taskTotal)
	s.mu.Unlock()

	return &transport.PsResponse{
		Cells:    int32(len(snapshot)),
		Machines: int32(machines),
		Jobs:     int32(jobs),
	}, nil
}

// JobStatus returns the stored Aggregate for req.JobId, or Done == false
// with an empty Aggregate if the job has not finished (or does not exist).
func (s *Server) JobStatus(_ context.Context, req *transport.JobStatusRequest) (*transport.JobStatusResponse, error) {
	agg, ok, err := s.store.GetAggregate(model.JobID(req.GetJobId()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get aggregate: %v", err)
	}
	if !ok {
		return &transport.JobStatusResponse{Done: false}, nil
	}
	return &transport.JobStatusResponse{Done: agg.Done, Aggregate: agg.Value}, nil
}

// fromProtoCoupling maps the wire Coupling enum to model.Coupling. The two
// enums share ordinal values by construction (see internal/model and
// swarm.proto), but this is written as an explicit switch rather than a raw
// cast so a future divergence between them fails loudly instead of silently
// mismapping.
func fromProtoCoupling(c transport.Coupling) model.Coupling {
	switch c {
	case transport.Coupling_COUPLING_BARRIER:
		return model.Barrier
	case transport.Coupling_COUPLING_LEADER:
		return model.Leader
	case transport.Coupling_COUPLING_MESSAGE_PASSING:
		return model.MessagePassing
	default:
		return model.Independent
	}
}
