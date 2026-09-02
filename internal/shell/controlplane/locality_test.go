package controlplane

import (
	"context"
	"testing"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// fakeLocality is a LocalitySource test double: cellTopo maps each cell's
// coordinates (a cell absent from the map is left out of the built
// LocalityGraph.Zone, matching the interface's documented contract), origin
// is the single fixed model.Topology every TaskOrigin call returns, and
// noOrigin forces TaskOrigin to report ok==false regardless of cellTopo —
// the "no locality info for this task" case placeLocked must fall back on.
type fakeLocality struct {
	cellTopo map[model.CellID]model.Topology
	origin   model.Topology
	noOrigin bool
}

func (f *fakeLocality) CellTopology(cell model.CellID) (model.Topology, bool) {
	topo, ok := f.cellTopo[cell]
	return topo, ok
}

func (f *fakeLocality) TaskOrigin(model.Task) (model.Topology, bool) {
	if f.noOrigin {
		return model.Topology{}, false
	}
	return f.origin, true
}

// twoRoomyCells joins two agents, each into its own brand-new cell with
// spare Free capacity, by requesting Caps == DefaultCellCapacity so the
// first cell is exactly full (Free 0) by the time the second agent's
// rendezvous.AdmitAgent call runs — forcing it into a second, distinct cell
// (mirroring TestJoinAgentOverflowsIntoNewCell's technique) — rather than
// both agents landing in the same cell. It returns the two cell IDs in
// join order (first, second); with cap=5 each cell ends up with Size=1,
// Free=4, identical capacity/Free on both sides so a locality test's only
// differentiator is Distance, never the Free tie-break.
func twoRoomyCells(t *testing.T, ctx context.Context, client transport.ControlPlaneClient) (first, second model.CellID) {
	t.Helper()
	r1 := joinAgent(t, ctx, client, "agent-1", 5)
	r2 := joinAgent(t, ctx, client, "agent-2", 5)
	if r1.GetCellId() == r2.GetCellId() {
		t.Fatalf("agent-1 and agent-2 landed on the same cell %s, want two distinct cells", r1.GetCellId())
	}
	return model.CellID(r1.GetCellId()), model.CellID(r2.GetCellId())
}

// submitOneTask submits a monte-carlo job decomposing into exactly one task
// (trials=1, blockSize=1) and returns its job id — a minimal placement probe
// that puts exactly one task through drainPendingLocked per call.
func submitOneTask(t *testing.T, ctx context.Context, client transport.ControlPlaneClient) string {
	t.Helper()
	resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "monte-carlo",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"trials": "1", "blockSize": "1", "seed": "1"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if resp.GetJobId() == "" {
		t.Fatalf("SubmitJob returned empty job id")
	}
	return resp.GetJobId()
}

// taskCellFor returns the single cell taskCell records for jobID's one task,
// failing the test if the job has anything other than exactly one recorded
// task-to-cell mapping (submitOneTask's contract).
func taskCellFor(t *testing.T, srv *Server, jobID string) model.CellID {
	t.Helper()
	srv.mu.Lock()
	defer srv.mu.Unlock()

	var found model.CellID
	count := 0
	for taskID, job := range srv.taskJob {
		if string(job) != jobID {
			continue
		}
		cell, ok := srv.taskCell[taskID]
		if !ok {
			t.Fatalf("task %s has no recorded taskCell (job %s)", taskID, jobID)
		}
		found = cell
		count++
	}
	if count != 1 {
		t.Fatalf("job %s has %d recorded tasks, want exactly 1", jobID, count)
	}
	return found
}

// TestPlacementFallbackTransparentWithNoLocalityGraph asserts the #223
// regression: with cfg.Locality left at its nil zero value, drainPendingLocked's
// placeLocked skips BestFit entirely and a task lands exactly where the
// unchanged placement.Place would put it — the first cell in slice
// (CellID) order with Free capacity — even though a second, equally
// capable/roomy cell also exists. This is the fallback-transparency
// acceptance criterion: existing dispatch behaves byte-for-byte as before
// #223 when locality isn't configured.
func TestPlacementFallbackTransparentWithNoLocalityGraph(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 5
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	first, _ := twoRoomyCells(t, ctx, client)

	jobID := submitOneTask(t, ctx, client)

	got := taskCellFor(t, srv, jobID)
	if got != first {
		t.Fatalf("task landed on cell %s, want %s (placement.Place's first-fit pick, cfg.Locality unset)", got, first)
	}
}

// TestLocalityPreferredPlacementChoosesCloserCell asserts the #223 headline
// behavior: with a LocalityGraph in which the SECOND cell (not the one
// placement.Place's first-fit scan would pick) shares the task's origin
// rack, the task lands on that closer cell instead — proving BestFit, not
// Place, decided this placement.
func TestLocalityPreferredPlacementChoosesCloserCell(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 5
	origin := model.Topology{Region: "us-east", AZ: "az1", Rack: "rack1"}
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	first, second := twoRoomyCells(t, ctx, client)

	srv.cfg.Locality = &fakeLocality{
		origin: origin,
		cellTopo: map[model.CellID]model.Topology{
			first:  {Region: "us-west", AZ: "az9", Rack: "rack9"}, // cross-region: Distance 3
			second: origin,                                        // same rack as origin: Distance 0
		},
	}

	jobID := submitOneTask(t, ctx, client)

	got := taskCellFor(t, srv, jobID)
	if got != second {
		t.Fatalf("task landed on cell %s, want %s (the locality-closer cell BestFit should have preferred over Place's first-fit %s)", got, second, first)
	}
}

// TestBestFitFallsBackToPlaceWhenNoCellFits asserts the #223 fallback
// contract's other half: with a LocalityGraph configured but every cell at
// capacity (Free==0, so BestFit's own Free>0 check disqualifies every
// candidate and it returns NoCapacity), the task falls back to
// placement.Place's own NoCapacity result and holds pending exactly as it
// would have before #223 — dispatch is not left in a different state just
// because a locality graph happens to be configured.
func TestBestFitFallsBackToPlaceWhenNoCellFits(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 1
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	// A single agent fills the only cell's capacity-1 room completely
	// (Free 0), so no cell — under BestFit or Place — has anywhere to put
	// a task.
	joinResp := joinAgent(t, ctx, client, "agent-1", 1)
	full := model.CellID(joinResp.GetCellId())

	srv.cfg.Locality = &fakeLocality{
		origin:   model.Topology{Region: "us-east", AZ: "az1", Rack: "rack1"},
		cellTopo: map[model.CellID]model.Topology{full: {Region: "us-east", AZ: "az1", Rack: "rack1"}},
	}

	jobID := submitOneTask(t, ctx, client)

	srv.mu.Lock()
	pendingCount := len(srv.pending)
	_, taskPlaced := func() (model.CellID, bool) {
		for taskID, job := range srv.taskJob {
			if string(job) != jobID {
				continue
			}
			cell, ok := srv.taskCell[taskID]
			return cell, ok
		}
		return "", false
	}()
	srv.mu.Unlock()

	if taskPlaced {
		t.Fatalf("task for job %s was placed despite the only cell being full", jobID)
	}
	if pendingCount != 1 {
		t.Fatalf("s.pending has %d entries, want 1 (the task should be held pending, same as pre-#223 Place-only behavior)", pendingCount)
	}

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = true for a job whose only task is still pending")
	}
}
