package controlplane

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/core/rendezvous"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// SubmitJob decomposes req into a JobSpec via admission.Admit and, on
// acceptance, persists the job and enqueues its tasks.
//
// store.Store's task queue is a single FIFO shared across jobs (see the
// store package doc: "P0 tasks are Independent, so a single FIFO queue
// shared across jobs is sufficient"), not one queue per cell, so there is no
// per-cell destination for placement.Place to route a task into in P0 — any
// agent that calls PullTask may serve any pending task regardless of which
// cell it belongs to. This resolves the ambiguity between the phase doc's
// per-task placement.Place call and the delivered Store shape: tasks are
// enqueued directly, and placement is left for a future phase that gives the
// store per-cell queues.
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
	if err := s.store.EnqueueTasks(tasks); err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue tasks: %v", err)
	}

	s.mu.Lock()
	for _, t := range tasks {
		s.taskJob[t.ID] = t.JobID
	}
	s.taskTotal[jobID] = len(tasks)
	s.mu.Unlock()

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

// PullTask serves the agent's runner loop from the store's pending-task
// queue.
func (s *Server) PullTask(_ context.Context, _ *transport.PullTaskRequest) (*transport.PullTaskResponse, error) {
	t, ok, err := s.store.DequeueTask()
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

// ReportResult records the reported TaskResult and, once every task
// SubmitJob admitted for the owning job has reported, merges the collected
// results into the job's Aggregate via its template's merge function and
// marks it done in the store.
func (s *Server) ReportResult(_ context.Context, req *transport.ReportResultRequest) (*transport.ReportResultResponse, error) {
	taskID := model.TaskID(req.GetTaskId())
	result := model.TaskResult{TaskID: taskID, Output: req.GetOutput(), OK: req.GetOk()}

	if err := s.store.PutResult(result); err != nil {
		return nil, status.Errorf(codes.NotFound, "report result: %v", err)
	}

	s.mu.Lock()
	jobID, known := s.taskJob[taskID]
	total := s.taskTotal[jobID]
	s.mu.Unlock()
	if !known {
		// PutResult already validated the task, so this should not happen;
		// there is nothing more this handler can do about it, and the
		// result the caller reported has already been durably recorded.
		return &transport.ReportResultResponse{Accepted: true}, nil
	}

	if err := s.maybeAggregate(jobID, total); err != nil {
		return nil, status.Errorf(codes.Internal, "aggregate job %s: %v", jobID, err)
	}
	return &transport.ReportResultResponse{Accepted: true}, nil
}

// maybeAggregate computes and persists jobID's Aggregate once at least total
// results have been recorded for it, dispatching to the job's template merge
// function by name.
func (s *Server) maybeAggregate(jobID model.JobID, total int) error {
	results, err := s.store.ResultsForJob(jobID)
	if err != nil {
		return err
	}
	if len(results) < total {
		return nil
	}

	spec, ok, err := s.store.GetJob(jobID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	var agg model.Aggregate
	switch spec.Template {
	case admission.TemplateKeyspaceSearch:
		agg = templates.KeyspaceMerge(results)
	case admission.TemplateMonteCarlo:
		agg = templates.MonteCarloMerge(results)
	default:
		return status.Errorf(codes.Internal, "unknown template %q for job %s", spec.Template, jobID)
	}
	agg.JobID = jobID

	return s.store.PutAggregate(agg)
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
