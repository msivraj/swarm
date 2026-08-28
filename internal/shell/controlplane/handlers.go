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
	s.mu.Lock()
	jobID := s.newJobIDLocked()
	s.mu.Unlock()

	spec := model.JobSpec{
		ID:       jobID,
		Template: req.GetTemplate(),
		Coupling: fromProtoCoupling(req.GetCoupling()),
		Params:   req.GetParams(),
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
	err := s.drainPendingLocked()
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
		// A joining agent added capacity, so re-run placement over any tasks
		// the ingress pending buffer is holding — one of them may now fit.
		if err := s.drainPendingLocked(); err != nil {
			return nil, status.Errorf(codes.Internal, "place pending tasks: %v", err)
		}
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
		// A new cell appeared, so re-run placement over the pending buffer
		// the same way the Accept branch does.
		if err := s.drainPendingLocked(); err != nil {
			return nil, status.Errorf(codes.Internal, "place pending tasks: %v", err)
		}
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

// Heartbeat refreshes agent's last-contact time, which the reaper loop reads
// to decide whether to evict it.
func (s *Server) Heartbeat(_ context.Context, req *transport.HeartbeatRequest) (*transport.HeartbeatResponse, error) {
	s.mu.Lock()
	s.lastSeen[req.GetAgent()] = s.now()
	s.mu.Unlock()
	return &transport.HeartbeatResponse{Ok: true}, nil
}

// PullTask serves the agent's runner loop from its own cell's queue: it
// looks up which cell req's agent belongs to (learned at JoinAgent) and
// dequeues from that cell's queue only — an agent never receives a task
// placed on another cell. An agent the server does not (or no longer) know
// the cell of (never joined, or reaped) gets HasTask:false, the same
// response as an empty queue.
func (s *Server) PullTask(_ context.Context, req *transport.PullTaskRequest) (*transport.PullTaskResponse, error) {
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
func (s *Server) ReportResult(_ context.Context, req *transport.ReportResultRequest) (*transport.ReportResultResponse, error) {
	taskID := model.TaskID(req.GetTaskId())
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
