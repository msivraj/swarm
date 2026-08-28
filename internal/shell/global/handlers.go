package global

import (
	"context"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/aggregate"
	"github.com/msivraj/swarm/internal/core/routing"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// Submit admits req via admission.Admit, routes the resulting tasks via
// routing.Decide over the current (diverged-downgraded) region view, and
// executes that route:
//   - NoRegion: reject with codes.ResourceExhausted.
//   - To{region}: dispatch the whole task set to region with result_sink ""
//     (region owns roll-up and JobStatus for it — see dispatchTo).
//   - Spread{regions}: partition tasks proportional to Free and dispatch
//     each partition with result_sink naming this layer (region reports a
//     partial back via ReportPartial — see dispatchSpread). This layer owns
//     roll-up for a Spread job.
func (s *Server) Submit(_ context.Context, req *transport.SubmitJobRequest) (*transport.SubmitJobResponse, error) {
	s.mu.Lock()
	jobID := s.newJobIDLocked()
	regions, _ := s.projectRegionsLocked(s.now())
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

	route := routing.Decide(spec, regions)
	switch route.Kind {
	case routing.NoRegion:
		return nil, status.Errorf(codes.ResourceExhausted, "global: no eligible region for job %s", jobID)

	case routing.To:
		if err := s.dispatchTo(jobID, spec, tasks, route.Region); err != nil {
			return nil, status.Errorf(codes.Unavailable, "dispatch to region %s: %v", route.Region, err)
		}

	case routing.Spread:
		if err := s.dispatchSpread(jobID, spec, tasks, route.Regions, regions); err != nil {
			return nil, status.Errorf(codes.Unavailable, "spread dispatch: %v", err)
		}

	default:
		return nil, status.Error(codes.Internal, "global: unknown route kind")
	}

	return &transport.SubmitJobResponse{JobId: string(jobID)}, nil
}

// PublishSummary folds req's RegionalSummary into the held GlobalView via
// routing.MergeGlobal — commutative, associative, and idempotent, so arrival
// order and duplicate publishes are safe (issue #45's headline law).
func (s *Server) PublishSummary(_ context.Context, req *transport.PublishSummaryRequest) (*transport.PublishSummaryResponse, error) {
	sum := fromProtoSummary(req.GetSummary())

	s.mu.Lock()
	s.view = routing.MergeGlobal(s.view, sum)
	s.mu.Unlock()

	return &transport.PublishSummaryResponse{Ok: true}, nil
}

// GetGlobalView projects the held GlobalView into []RegionView, downgrading
// every RegionID routing.Diverged flags stale (as of now) to Unreachable,
// and reports that same diverged set.
func (s *Server) GetGlobalView(_ context.Context, _ *transport.GlobalViewRequest) (*transport.GlobalViewResponse, error) {
	s.mu.Lock()
	regions, diverged := s.projectRegionsLocked(s.now())
	s.mu.Unlock()

	protoRegions := make([]*transport.RegionView, 0, len(regions))
	for _, r := range regions {
		protoRegions = append(protoRegions, &transport.RegionView{
			Id:     string(r.ID),
			Free:   int32(r.Free),
			Cells:  int32(r.Cells),
			Health: toProtoHealth(r.Health),
		})
	}
	protoDiverged := make([]string, 0, len(diverged))
	for _, id := range diverged {
		protoDiverged = append(protoDiverged, string(id))
	}

	return &transport.GlobalViewResponse{Regions: protoRegions, Diverged: protoDiverged}, nil
}

// ReportPartial records region's rolled-up partial for a Spread job,
// replacing (never accumulating) any prior partial from the same region —
// the idempotency issue #45 requires. Once every expected region's done
// partial has arrived, it folds them via aggregate.MergeAll into the job's
// final Aggregate, stamps Done, persists it (store.PutAggregate — JobStatus's
// read path), and latches the job done so a later duplicate/retry report
// cannot re-run the fold or re-persist.
func (s *Server) ReportPartial(_ context.Context, req *transport.ReportPartialRequest) (*transport.ReportPartialResponse, error) {
	p := req.GetPartial()
	jobID := model.JobID(p.GetJobId())
	region := model.RegionID(p.GetRegion())

	s.mu.Lock()
	job, ok := s.spreadJobs[jobID]
	if !ok {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "report partial: unknown spread job %s", jobID)
	}

	job.arrived[region] = model.Aggregate{JobID: jobID, Value: p.GetValue(), Done: p.GetDone()}

	if job.done || !everyExpectedDoneLocked(job) {
		s.mu.Unlock()
		return &transport.ReportPartialResponse{Ok: true}, nil
	}

	final := aggregate.MergeAll(job.template, sortedPartialsLocked(job))
	final.JobID = jobID
	final.Done = true
	job.final = final
	job.done = true
	s.mu.Unlock()

	if err := s.store.PutAggregate(final); err != nil {
		return nil, status.Errorf(codes.Internal, "put aggregate for job %s: %v", jobID, err)
	}
	return &transport.ReportPartialResponse{Ok: true}, nil
}

// everyExpectedDoneLocked reports whether every region in job.expected has
// an arrived partial with Done == true. Callers must hold s.mu.
func everyExpectedDoneLocked(job *spreadJob) bool {
	for r := range job.expected {
		p, ok := job.arrived[r]
		if !ok || !p.Done {
			return false
		}
	}
	return true
}

// sortedPartialsLocked returns job's arrived partials in ascending RegionID
// order — deterministic, though aggregate.MergeAll's result does not depend
// on it (Merge is commutative and associative). Callers must hold s.mu.
func sortedPartialsLocked(job *spreadJob) []model.Aggregate {
	regions := make([]model.RegionID, 0, len(job.arrived))
	for r := range job.arrived {
		regions = append(regions, r)
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i] < regions[j] })

	out := make([]model.Aggregate, 0, len(regions))
	for _, r := range regions {
		out = append(out, job.arrived[r])
	}
	return out
}

// JobStatus serves a Spread job's folded Aggregate from the store once
// ReportPartial has completed it, and proxies JobStatus to the owning region
// for a To job (the global layer never stores that job's result itself — the
// region does). A jobID this layer never routed reports Done: false, the
// same "not found" contract controlplane.Server.JobStatus uses.
func (s *Server) JobStatus(ctx context.Context, req *transport.JobStatusRequest) (*transport.JobStatusResponse, error) {
	jobID := model.JobID(req.GetJobId())

	s.mu.Lock()
	_, isSpread := s.spreadJobs[jobID]
	region, isTo := s.toJobRegion[jobID]
	s.mu.Unlock()

	if isSpread {
		agg, ok, err := s.store.GetAggregate(jobID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get aggregate for job %s: %v", jobID, err)
		}
		if !ok {
			return &transport.JobStatusResponse{Done: false}, nil
		}
		return &transport.JobStatusResponse{Done: agg.Done, Aggregate: agg.Value}, nil
	}

	if !isTo {
		return &transport.JobStatusResponse{Done: false}, nil
	}
	return s.proxyJobStatus(ctx, region, req.GetJobId())
}

// proxyJobStatus dials region's ControlPlane and forwards JobStatus to it —
// a To job's completion is owned entirely by the region it was routed to. A
// dial or RPC failure (region unreachable, no dial address on file) reports
// Done: false rather than erroring the caller: the job may simply not be
// done yet, and there is no durable state at this layer to fall back to for
// a job it never itself aggregates.
func (s *Server) proxyJobStatus(ctx context.Context, region model.RegionID, jobID string) (*transport.JobStatusResponse, error) {
	target, ok := s.cfg.RegionTargets[region]
	if !ok || target == "" {
		return &transport.JobStatusResponse{Done: false}, nil
	}

	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	client, closer, err := s.cfg.RegionDialer(dctx, target)
	if err != nil {
		return &transport.JobStatusResponse{Done: false}, nil
	}
	defer func() { _ = closer.Close() }()

	resp, err := client.JobStatus(dctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		return &transport.JobStatusResponse{Done: false}, nil
	}
	return &transport.JobStatusResponse{Done: resp.GetDone(), Aggregate: resp.GetAggregate()}, nil
}

// fromProtoSummary converts a wire RegionalSummary to routing.RegionalSummary.
func fromProtoSummary(sum *transport.RegionalSummary) routing.RegionalSummary {
	return routing.RegionalSummary{
		Region: model.RegionID(sum.GetRegion()),
		Free:   int(sum.GetFree()),
		Cells:  int(sum.GetCells()),
		Health: fromProtoHealth(sum.GetHealth()),
		At:     model.Instant(sum.GetAt()),
	}
}
