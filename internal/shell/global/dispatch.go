package global

import (
	"context"
	"fmt"
	"time"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// dialTimeout bounds every outbound regional RPC (DispatchTasks, the To
// route's JobStatus proxy) so a slow or unreachable region cannot hang an
// RPC handler indefinitely. Mirrors controlplane.dialTimeout.
const dialTimeout = 5 * time.Second

// dispatch delivers tasks for spec to the region at target via
// ControlPlane.DispatchTasks, with the given result_sink.
func (s *Server) dispatch(target string, spec model.JobSpec, tasks []model.Task, resultSink string) error {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	client, closer, err := s.cfg.RegionDialer(ctx, target)
	if err != nil {
		return fmt.Errorf("dial %s: %w", target, err)
	}
	defer func() { _ = closer.Close() }()

	resp, err := client.DispatchTasks(ctx, &transport.DispatchTasksRequest{
		Job:        toProtoJobSpec(spec),
		Tasks:      toProtoTasks(tasks),
		ResultSink: resultSink,
	})
	if err != nil {
		return fmt.Errorf("dispatch to %s: %w", target, err)
	}
	if !resp.GetAccepted() {
		return fmt.Errorf("region at %s rejected dispatch: %s", target, resp.GetReason())
	}
	return nil
}

// dispatchTo carries out a routing.To decision: the whole task set goes to
// region, with result_sink "" — region owns roll-up and JobStatus for the
// job as if it were submitted there directly (see controlplane's
// isSpillForward: sink=="" is self). Records jobID -> region for JobStatus's
// proxy on success.
func (s *Server) dispatchTo(jobID model.JobID, spec model.JobSpec, tasks []model.Task, region model.RegionID) error {
	target, ok := s.cfg.RegionTargets[region]
	if !ok || target == "" {
		return fmt.Errorf("no dial address on file for region %s", region)
	}
	if err := s.dispatch(target, spec, tasks, ""); err != nil {
		return err
	}
	s.mu.Lock()
	s.toJobRegion[jobID] = region
	s.mu.Unlock()
	return nil
}

// dispatchSpread carries out a routing.Spread decision: tasks are
// partitioned across spreadRegions proportional to their reported Free
// (partitionTasks), and each nonempty partition is dispatched with
// result_sink == s.cfg.SelfAddress, so the receiving region reports its
// rolled-up partition back via ReportPartial (see controlplane's
// isSpillForward: sink == cfg.GlobalRouter is the global-sink branch — the
// region compares result_sink to its own known GlobalRouter address, which
// operators configure to match this layer's SelfAddress).
//
// The job's spreadJob bookkeeping (template, expected region set) is
// registered before any DispatchTasks call goes out, so a fast region's
// ReportPartial arriving mid-dispatch already finds its job recognized.
func (s *Server) dispatchSpread(jobID model.JobID, spec model.JobSpec, tasks []model.Task, spreadRegions []model.RegionID, allRegions []model.RegionView) error {
	weights := regionWeights(allRegions, spreadRegions)
	partitions := partitionTasks(tasks, weights)

	expected := make(map[model.RegionID]struct{}, len(spreadRegions))
	for _, r := range spreadRegions {
		if len(partitions[r]) > 0 {
			expected[r] = struct{}{}
		}
	}

	s.mu.Lock()
	s.spreadJobs[jobID] = &spreadJob{
		template: spec.Template,
		expected: expected,
		arrived:  make(map[model.RegionID]model.Aggregate),
	}
	s.mu.Unlock()

	for _, r := range spreadRegions {
		part := partitions[r]
		if len(part) == 0 {
			continue
		}
		target, ok := s.cfg.RegionTargets[r]
		if !ok || target == "" {
			return fmt.Errorf("no dial address on file for region %s", r)
		}
		if err := s.dispatch(target, spec, part, s.cfg.SelfAddress); err != nil {
			return err
		}
	}
	return nil
}

// regionWeights extracts the (region, Free) pairs partitionTasks needs for
// ids, looking each up in all — the projected region views the routing
// decision was itself made from, so the weights partitionTasks divides by
// are exactly the capacities routing.Decide already judged eligible.
func regionWeights(all []model.RegionView, ids []model.RegionID) []regionWeight {
	byID := make(map[model.RegionID]int, len(all))
	for _, r := range all {
		byID[r.ID] = r.Free
	}
	out := make([]regionWeight, 0, len(ids))
	for _, id := range ids {
		out = append(out, regionWeight{Region: id, Weight: byID[id]})
	}
	return out
}

// toProtoJobSpec converts spec to the wire JobSpec DispatchTasksRequest
// carries — a dispatched region did not itself admit the job, so it has no
// other way to learn its template/coupling/params. Mirrors
// controlplane.toProtoJobSpec.
func toProtoJobSpec(spec model.JobSpec) *transport.JobSpec {
	return &transport.JobSpec{
		Id:       string(spec.ID),
		Template: spec.Template,
		Coupling: toProtoCoupling(spec.Coupling),
		Params:   spec.Params,
	}
}

// toProtoTasks converts tasks to their wire representation for an outbound
// DispatchTasksRequest. Mirrors controlplane.toProtoTasks.
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

// toProtoCoupling maps model.Coupling to the wire enum. Mirrors
// controlplane.toProtoCoupling; written as an explicit switch (rather than a
// raw cast) so a future divergence between the two enums fails loudly.
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

// fromProtoCoupling maps the wire Coupling enum to model.Coupling. Mirrors
// controlplane.fromProtoCoupling.
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

// toProtoHealth converts a model.Health to its wire enum. Mirrors
// controlplane.toProtoHealth.
func toProtoHealth(h model.Health) transport.Health {
	switch h {
	case model.Degraded:
		return transport.Health_HEALTH_DEGRADED
	case model.Unreachable:
		return transport.Health_HEALTH_UNREACHABLE
	default:
		return transport.Health_HEALTH_HEALTHY
	}
}

// fromProtoHealth converts a wire Health enum to model.Health.
func fromProtoHealth(h transport.Health) model.Health {
	switch h {
	case transport.Health_HEALTH_DEGRADED:
		return model.Degraded
	case transport.Health_HEALTH_UNREACHABLE:
		return model.Unreachable
	default:
		return model.Healthy
	}
}
