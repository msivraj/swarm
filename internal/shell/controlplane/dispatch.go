package controlplane

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// DispatchTasks accepts a cross-region task delivery — either the global
// layer spreading a partition to this region, or a peer spilling one or more
// tasks whose origin this region is not (ticket #44, S2). It records
// job/total/result_sink for job.ID exactly as SubmitJob does for a locally
// admitted job, then places every task via the same S1 placement path
// (drainPendingLocked: placement.Place, falling back to
// placement.PlaceAcross/pending in regional mode), recording taskCell for
// each — so a dispatched partition rolls up by cell exactly like a locally
// submitted job.
func (s *Server) DispatchTasks(_ context.Context, req *transport.DispatchTasksRequest) (*transport.DispatchTasksResponse, error) {
	protoSpec := req.GetJob()
	spec := model.JobSpec{
		ID:       model.JobID(protoSpec.GetId()),
		Template: protoSpec.GetTemplate(),
		Coupling: fromProtoCoupling(protoSpec.GetCoupling()),
		Params:   protoSpec.GetParams(),
	}
	if spec.ID == "" {
		return &transport.DispatchTasksResponse{Accepted: false, Reason: "dispatch tasks: missing job id"}, nil
	}

	tasks := make([]model.Task, 0, len(req.GetTasks()))
	for _, pt := range req.GetTasks() {
		tasks = append(tasks, model.Task{
			ID:      model.TaskID(pt.GetId()),
			JobID:   model.JobID(pt.GetJobId()),
			Input:   pt.GetInput(),
			Attempt: int(pt.GetAttempt()),
		})
	}
	if len(tasks) == 0 {
		return &transport.DispatchTasksResponse{Accepted: false, Reason: "dispatch tasks: no tasks"}, nil
	}

	if err := s.store.PutJob(spec); err != nil {
		return nil, status.Errorf(codes.Internal, "put job: %v", err)
	}

	s.mu.Lock()
	for _, t := range tasks {
		s.taskJob[t.ID] = t.JobID
	}
	s.taskTotal[spec.ID] = len(tasks)
	s.resultSink[spec.ID] = req.GetResultSink()
	s.pending = append(s.pending, tasks...)
	err := s.drainPendingLocked()
	s.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "place tasks: %v", err)
	}

	return &transport.DispatchTasksResponse{Accepted: true}, nil
}

// toProtoJobSpec converts spec to the wire JobSpec DispatchTasksRequest
// carries — the counterpart to handlers.go's fromProtoCoupling, needed
// because a spilled/spread task's destination region did not itself admit
// the job and so has no other way to learn its template/coupling/params.
func toProtoJobSpec(spec model.JobSpec) *transport.JobSpec {
	return &transport.JobSpec{
		Id:       string(spec.ID),
		Template: spec.Template,
		Coupling: toProtoCoupling(spec.Coupling),
		Params:   spec.Params,
	}
}

// toProtoCoupling is fromProtoCoupling's inverse: model.Coupling -> the wire
// enum, needed for outbound DispatchTasks requests (fromProtoCoupling only
// ever decodes inbound requests).
func toProtoCoupling(c model.Coupling) transport.Coupling {
	switch c {
	case model.Barrier:
		return transport.Coupling_COUPLING_BARRIER
	case model.Leader:
		return transport.Coupling_COUPLING_LEADER
	case model.MessagePassing:
		return transport.Coupling_COUPLING_MESSAGE_PASSING
	default:
		return transport.Coupling_COUPLING_INDEPENDENT
	}
}

// toProtoTasks converts tasks to their wire representation for an outbound
// DispatchTasksRequest.
func toProtoTasks(tasks []model.Task) []*transport.Task {
	out := make([]*transport.Task, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, &transport.Task{
			Id:      string(t.ID),
			JobId:   string(t.JobID),
			Input:   t.Input,
			Attempt: int32(t.Attempt),
		})
	}
	return out
}
