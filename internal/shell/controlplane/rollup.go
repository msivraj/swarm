package controlplane

import (
	"context"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/aggregate"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// mergeFuncs maps a known JobSpec.Template to the function that reduces one
// cell's []TaskResult to a per-cell partial Aggregate — the cell tier of the
// two-tier roll-up (ticket #44). Mirrors aggregate.combiners' shape.
var mergeFuncs = map[string]func([]model.TaskResult) model.Aggregate{
	admission.TemplateKeyspaceSearch: templates.KeyspaceMerge,
	admission.TemplateMonteCarlo:     templates.MonteCarloMerge,
}

// maybeRollup computes and, once every DISTINCT task admitted/dispatched for
// jobID at this region has reported (the same reportedTasks gate P0's
// maybeAggregate used), routes this region's rolled-up partial according to
// jobID's result_sink:
//   - self (""): the region partial IS the final Aggregate — stamp Done and
//     store it, exposed via JobStatus (unchanged P0/S1 behavior).
//   - GlobalRouter: forward the region partial up via ReportPartial. Do not
//     store it locally — the global layer owns this job's final aggregate.
//
// A spill-forward sink never reaches this function: ReportResult forwards
// that job's per-task raw results immediately (see isSpillForward) and skips
// the roll-up gate entirely, since this region does not own that job's
// completion.
//
// finalized latches the completion action (PutAggregate / reportPartialUp)
// to run exactly once per job: the distinct>=total gate alone stays true
// forever once a job completes, and ReportResult explicitly supports a
// duplicate/retry report arriving after completion (see dedupeTaskResults) —
// without this latch, such a report would re-run the completion action.
// P0's self-sink action (PutAggregate) was an idempotent overwrite, so a
// re-run was harmless; S2's global-sink action (reportPartialUp) is a
// non-idempotent network call, so the gate this latch adds is required for
// both branches to keep the "exactly once" contract. The check-and-set
// happens under s.mu, atomically with the distinct>=total read, so two
// concurrent ReportResult calls that both observe completion cannot both
// pass the latch either.
func (s *Server) maybeRollup(jobID model.JobID, total int) error {
	s.mu.Lock()
	distinct := len(s.reportedTasks[jobID])
	if distinct < total {
		s.mu.Unlock()
		return nil
	}
	if _, done := s.finalized[jobID]; done {
		s.mu.Unlock()
		return nil
	}
	s.finalized[jobID] = struct{}{}
	sink := s.resultSink[jobID]
	s.mu.Unlock()

	spec, ok, err := s.store.GetJob(jobID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	results, err := s.store.ResultsForJob(jobID)
	if err != nil {
		return err
	}
	results = dedupeTaskResults(results)

	regionPartial, err := s.rollupByCellLocked(jobID, spec.Template, results)
	if err != nil {
		return err
	}
	regionPartial.JobID = jobID

	switch {
	case sink == "":
		regionPartial.Done = true
		return s.store.PutAggregate(regionPartial)
	case sink == s.cfg.GlobalRouter:
		return s.reportPartialUp(jobID, spec.Template, regionPartial)
	default:
		// Should not be reachable: a non-empty, non-GlobalRouter sink is a
		// spill forward, which ReportResult routes away from maybeRollup
		// before this point. Nothing to do if it ever is.
		return nil
	}
}

// rollupByCellLocked implements the ticket's two-tier hierarchical roll-up:
// group results by the cell (or spillCellID) each task was placed on
// (taskCell, recorded at placement — see drainPendingLocked/
// dispatchToPeerLocked), reduce each cell's group to a per-cell partial via
// the job's template merge (mergeFuncs — the SAME KeyspaceMerge/
// MonteCarloMerge a flat P0 merge would use), then combine the per-cell
// partials into one region partial via aggregate.MergeAll. Because
// aggregate.Merge is commutative/associative with the zero Aggregate as its
// identity, this equals a flat merge of the same results — the acceptance
// criterion the by-cell rollup test exercises — while making the per-cell
// tiering observable (the caller/test can read s.taskCell directly, same
// package).
//
// Groups are visited in ascending CellID order so the region partial is
// deterministic regardless of ResultsForJob's/taskCell's map iteration
// order.
func (s *Server) rollupByCellLocked(jobID model.JobID, template string, results []model.TaskResult) (model.Aggregate, error) {
	merge, ok := mergeFuncs[template]
	if !ok {
		return model.Aggregate{}, status.Errorf(codes.Internal, "unknown template %q for job %s", template, jobID)
	}

	s.mu.Lock()
	groups := make(map[model.CellID][]model.TaskResult, len(results))
	cells := make([]model.CellID, 0, len(results))
	for _, r := range results {
		cell := s.taskCell[r.TaskID] // zero value ("") if never recorded — still a valid, stable group key
		if _, seen := groups[cell]; !seen {
			cells = append(cells, cell)
		}
		groups[cell] = append(groups[cell], r)
	}
	s.mu.Unlock()

	sort.Slice(cells, func(i, j int) bool { return cells[i] < cells[j] })

	cellPartials := make([]model.Aggregate, 0, len(cells))
	for _, cell := range cells {
		cellPartials = append(cellPartials, merge(groups[cell]))
	}
	return aggregate.MergeAll(template, cellPartials), nil
}

// reportPartialUp forwards partial (this region's completed, rolled-up
// roll-up for jobID) to GlobalRouter.ReportPartial, carrying the region
// identity and template a receiver needs to fold it into the global
// aggregate via aggregate.Merge. Called once, when jobID's region partition
// completes (maybeRollup's gate) — never per task, and never with raw
// per-task results (ticket #44's "do not forward raw results upward").
func (s *Server) reportPartialUp(jobID model.JobID, template string, partial model.Aggregate) error {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	client, closer, err := s.cfg.GlobalRouterDialer(ctx, s.cfg.GlobalRouter)
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()

	_, err = client.ReportPartial(ctx, &transport.ReportPartialRequest{
		Partial: &transport.PartialAggregate{
			JobId:    string(jobID),
			Region:   string(s.cfg.RegionID),
			Template: template,
			Value:    partial.Value,
			Done:     true,
		},
	})
	return err
}
