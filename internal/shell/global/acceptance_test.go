package global

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// This file covers issue #45 (S3, global routing layer)'s six acceptance
// behaviors: merge convergence under shuffle+duplicates, Route To, Route
// Spread proportional, the Spread partial roll-up, NoRegion rejection, and
// Diverged health downgrade — plus one extra covering the To route's
// JobStatus proxy, part of the same owner-decided semantics.

// --- acceptance criterion 1: merge convergence ---------------------------

// TestMergeConvergenceUnderShuffleAndDuplicates publishes the same multiset
// of RegionalSummaries (including duplicates and an out-of-order At) in two
// different orders against two independent Server instances, and asserts
// GetGlobalView reflects the same last-writer-wins fixed point regardless of
// arrival order — routing.MergeGlobal's commutativity/associativity/
// idempotency (phase doc §02), exercised through the shell's RPC surface.
func TestMergeConvergenceUnderShuffleAndDuplicates(t *testing.T) {
	events := []*transport.RegionalSummary{
		{Region: "a", Free: 5, Cells: 1, Health: transport.Health_HEALTH_HEALTHY, At: 10},
		{Region: "a", Free: 9, Cells: 2, Health: transport.Health_HEALTH_HEALTHY, At: 20}, // newest wins for "a"
		{Region: "b", Free: 3, Cells: 1, Health: transport.Health_HEALTH_HEALTHY, At: 15},
		{Region: "b", Free: 7, Cells: 4, Health: transport.Health_HEALTH_DEGRADED, At: 5}, // older: must lose despite more Free/Cells
		{Region: "c", Free: 1, Cells: 1, Health: transport.Health_HEALTH_HEALTHY, At: 1},
	}
	// Duplicate a few entries so the fold sees repeats, not just a permutation.
	multiset := append(append([]*transport.RegionalSummary{}, events...), events[0], events[2], events[4], events[1])

	orderA := shuffled(multiset, 1)
	orderB := shuffled(multiset, 2)
	if reflect.DeepEqual(orderA, orderB) {
		t.Fatalf("test bug: the two shuffles produced identical orders, this test would be vacuous")
	}

	viewA := publishAllAndGetView(t, orderA)
	viewB := publishAllAndGetView(t, orderB)

	if !reflect.DeepEqual(viewA.GetRegions(), viewB.GetRegions()) {
		t.Fatalf("GetGlobalView.Regions differs by publish order:\n  A: %+v\n  B: %+v", viewA.GetRegions(), viewB.GetRegions())
	}
	if !reflect.DeepEqual(viewA.GetDiverged(), viewB.GetDiverged()) {
		t.Fatalf("GetGlobalView.Diverged differs by publish order:\n  A: %+v\n  B: %+v", viewA.GetDiverged(), viewB.GetDiverged())
	}

	want := map[string]*transport.RegionView{
		"a": {Id: "a", Free: 9, Cells: 2, Health: transport.Health_HEALTH_HEALTHY},
		"b": {Id: "b", Free: 3, Cells: 1, Health: transport.Health_HEALTH_HEALTHY},
		"c": {Id: "c", Free: 1, Cells: 1, Health: transport.Health_HEALTH_HEALTHY},
	}
	if len(viewA.GetRegions()) != len(want) {
		t.Fatalf("GetGlobalView returned %d regions, want %d", len(viewA.GetRegions()), len(want))
	}
	for _, got := range viewA.GetRegions() {
		w, ok := want[got.GetId()]
		if !ok {
			t.Fatalf("unexpected region %q in GetGlobalView", got.GetId())
		}
		if got.GetFree() != w.GetFree() || got.GetCells() != w.GetCells() || got.GetHealth() != w.GetHealth() {
			t.Fatalf("region %q = %+v, want %+v", got.GetId(), got, w)
		}
	}
}

// shuffled returns a copy of in, deterministically permuted by seed (a fixed
// math/rand seed, not real randomness — this is shell test code, not core).
func shuffled(in []*transport.RegionalSummary, seed int64) []*transport.RegionalSummary {
	out := make([]*transport.RegionalSummary, len(in))
	copy(out, in)
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// publishAllAndGetView starts a fresh Server, publishes each summary in
// order, and returns the resulting GetGlobalView response.
func publishAllAndGetView(t *testing.T, summaries []*transport.RegionalSummary) *transport.GlobalViewResponse {
	t.Helper()
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(nil, nil), clock)
	defer teardown()
	ctx := context.Background()

	for _, sum := range summaries {
		resp, err := client.PublishSummary(ctx, &transport.PublishSummaryRequest{Summary: sum})
		if err != nil {
			t.Fatalf("PublishSummary: %v", err)
		}
		if !resp.GetOk() {
			t.Fatalf("PublishSummary: Ok = false")
		}
	}

	view, err := client.GetGlobalView(ctx, &transport.GlobalViewRequest{})
	if err != nil {
		t.Fatalf("GetGlobalView: %v", err)
	}
	sort.Slice(view.Regions, func(i, j int) bool { return view.Regions[i].GetId() < view.Regions[j].GetId() })
	return view
}

// --- acceptance criterion 2: Route To -------------------------------------

// TestRouteToDispatchesWholeSetToOneRegionSelfSink covers a single-eligible
// routing.To decision: the whole admitted task set goes to exactly one
// region, result_sink "" (that region owns roll-up and JobStatus for it —
// see dispatchTo), and no other known region ever sees a DispatchTasks call.
func TestRouteToDispatchesWholeSetToOneRegionSelfSink(t *testing.T) {
	fakes, targets, dial := startFakeRegions(t, "eligible", "full")
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(targets, dial), clock)
	defer teardown()
	ctx := context.Background()

	publishSummary(t, ctx, client, "eligible", 5, 1, model.Healthy, 100)
	publishSummary(t, ctx, client, "full", 0, 1, model.Healthy, 100) // present, but ineligible (no capacity)

	jobID := submitMonteCarlo(t, ctx, client, 4)

	got := fakes["eligible"].dispatched()
	if len(got) != 1 {
		t.Fatalf("region %q received %d DispatchTasks call(s), want exactly 1", "eligible", len(got))
	}
	req := got[0]
	if req.GetJob().GetId() != jobID {
		t.Fatalf("dispatched job id = %q, want %q", req.GetJob().GetId(), jobID)
	}
	if len(req.GetTasks()) != 4 {
		t.Fatalf("dispatched task count = %d, want 4 (the whole admitted set)", len(req.GetTasks()))
	}
	if req.GetResultSink() != "" {
		t.Fatalf("dispatched result_sink = %q, want \"\" (region owns roll-up)", req.GetResultSink())
	}

	if other := fakes["full"].dispatched(); len(other) != 0 {
		t.Fatalf("ineligible region received %d DispatchTasks call(s), want 0", len(other))
	}
}

// --- acceptance criterion 3: Route Spread proportional --------------------

// spreadTestRegions publishes three Independent-eligible regions with
// Free = 1/2/1 and submits an 8-task monte-carlo job (evenly divisible by
// the weight sum, so the largest-remainder split is exact and unambiguous:
// 2/4/2). Shared by the proportional-split and partial-roll-up tests.
func spreadTestRegions(t *testing.T, ctx context.Context, client transport.GlobalRouterClient) {
	t.Helper()
	publishSummary(t, ctx, client, "r1", 1, 1, model.Healthy, 100)
	publishSummary(t, ctx, client, "r2", 2, 1, model.Healthy, 100)
	publishSummary(t, ctx, client, "r3", 1, 1, model.Healthy, 100)
}

func TestRouteSpreadProportional(t *testing.T) {
	fakes, targets, dial := startFakeRegions(t, "r1", "r2", "r3")
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(targets, dial), clock)
	defer teardown()
	ctx := context.Background()

	spreadTestRegions(t, ctx, client)
	jobID := submitMonteCarlo(t, ctx, client, 8)

	wantCounts := map[model.RegionID]int{"r1": 2, "r2": 4, "r3": 2}
	union := map[string]int{}
	for region, wantCount := range wantCounts {
		reqs := fakes[region].dispatched()
		if len(reqs) != 1 {
			t.Fatalf("region %s received %d DispatchTasks call(s), want exactly 1", region, len(reqs))
		}
		req := reqs[0]
		if req.GetJob().GetId() != jobID {
			t.Fatalf("region %s dispatched job id = %q, want %q", region, req.GetJob().GetId(), jobID)
		}
		if len(req.GetTasks()) != wantCount {
			t.Fatalf("region %s dispatched %d task(s), want %d (proportional to Free)", region, len(req.GetTasks()), wantCount)
		}
		if req.GetResultSink() != "global-self-addr" {
			t.Fatalf("region %s dispatched result_sink = %q, want the global layer's SelfAddress", region, req.GetResultSink())
		}
		for _, task := range req.GetTasks() {
			union[task.GetId()]++
		}
	}

	// The union of every region's dispatched tasks equals the admitted set:
	// none dropped, none duplicated.
	wantTasks, _ := admission.Admit(model.JobSpec{
		ID: model.JobID(jobID), Template: "monte-carlo", Coupling: model.Independent,
		Params: map[string]string{"trials": "8", "blockSize": "1", "seed": "0"},
	})
	if len(union) != len(wantTasks) {
		t.Fatalf("union has %d distinct dispatched task(s), want %d (the admitted set)", len(union), len(wantTasks))
	}
	for _, task := range wantTasks {
		if n := union[string(task.ID)]; n != 1 {
			t.Fatalf("task %s appears in dispatched partitions %d time(s), want exactly 1", task.ID, n)
		}
	}
}

// --- acceptance criterion 4: Spread rolls up via partials (headline) -----

// TestSpreadRollsUpViaPartials submits the same 8-task Spread job as
// TestRouteSpreadProportional, has each of the three participating regions
// report exactly ONE ReportPartial (the rolled-up partial for its own
// partition — never a raw per-task result, since the global layer's
// GlobalRouter surface has no raw-result RPC at all), and asserts JobStatus
// returns aggregate.MergeAll of those three partials: byte-identical to a
// flat, single-region merge of the very same raw per-task results (P0's own
// merge, over the whole job at once) — proving the hierarchical roll-up is
// equivalent to a flat one, per aggregate.Merge's associativity.
func TestSpreadRollsUpViaPartials(t *testing.T) {
	fakes, targets, dial := startFakeRegions(t, "r1", "r2", "r3")
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(targets, dial), clock)
	defer teardown()
	ctx := context.Background()

	spreadTestRegions(t, ctx, client)
	jobID := submitMonteCarlo(t, ctx, client, 8)

	// admission.Admit is a pure function of the JobSpec (including the
	// server-assigned JobID echoed back by Submit), so reconstructing it
	// here yields byte-identical tasks/IDs to what the server itself
	// admitted and partitioned — the same task list a single-region P0 run
	// of this job would have decomposed.
	tasks, rej := admission.Admit(model.JobSpec{
		ID: model.JobID(jobID), Template: "monte-carlo", Coupling: model.Independent,
		Params: map[string]string{"trials": "8", "blockSize": "1", "seed": "0"},
	})
	if rej.Rejected {
		t.Fatalf("admission.Admit: rejected: %s", rej.Reason)
	}

	rawByID := make(map[string]model.TaskResult, len(tasks))
	for i, task := range tasks {
		sum := float64(10 * (i + 1))
		rawByID[string(task.ID)] = model.TaskResult{
			TaskID: task.ID,
			Output: encodeMCResultLocal(1, sum, sum*sum),
			OK:     true,
		}
	}

	var allRaw []model.TaskResult
	for _, region := range []model.RegionID{"r1", "r2", "r3"} {
		reqs := fakes[region].dispatched()
		if len(reqs) != 1 {
			t.Fatalf("region %s received %d DispatchTasks call(s), want exactly 1", region, len(reqs))
		}
		var regionRaw []model.TaskResult
		for _, pt := range reqs[0].GetTasks() {
			r, ok := rawByID[pt.GetId()]
			if !ok {
				t.Fatalf("dispatched task %s not in the reconstructed admitted set", pt.GetId())
			}
			regionRaw = append(regionRaw, r)
		}
		allRaw = append(allRaw, regionRaw...)

		partial := templates.MonteCarloMerge(regionRaw)
		resp, err := client.ReportPartial(ctx, &transport.ReportPartialRequest{
			Partial: &transport.PartialAggregate{
				JobId:    jobID,
				Region:   string(region),
				Template: "monte-carlo",
				Value:    partial.Value,
				Done:     true,
			},
		})
		if err != nil {
			t.Fatalf("ReportPartial(%s): %v", region, err)
		}
		if !resp.GetOk() {
			t.Fatalf("ReportPartial(%s): Ok = false", region)
		}
	}

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true (every expected region reported)")
	}

	wantFlat := templates.MonteCarloMerge(allRaw)
	if statusResp.GetAggregate() == nil || string(statusResp.GetAggregate()) != string(wantFlat.Value) {
		gotCount, gotSum, _, _ := decodeMCAggregateLocal(statusResp.GetAggregate())
		wantCount, wantSum, _, _ := decodeMCAggregateLocal(wantFlat.Value)
		t.Fatalf("global aggregate (Count=%d Sum=%v) != flat single-region merge (Count=%d Sum=%v)", gotCount, gotSum, wantCount, wantSum)
	}
}

// TestReportPartialDuplicateReplacesNotAccumulates is a regression guard for
// the exact bug the ticket calls out (S2's double-report bug): a region that
// calls ReportPartial twice for the same job must have its second call
// REPLACE the first, never be folded in on top of it. A doubled r2 partial
// (reported once, then again) must not change the final aggregate versus
// reporting it once.
func TestReportPartialDuplicateReplacesNotAccumulates(t *testing.T) {
	fakes, targets, dial := startFakeRegions(t, "r1", "r2", "r3")
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(targets, dial), clock)
	defer teardown()
	ctx := context.Background()

	spreadTestRegions(t, ctx, client)
	jobID := submitMonteCarlo(t, ctx, client, 8)

	tasks, _ := admission.Admit(model.JobSpec{
		ID: model.JobID(jobID), Template: "monte-carlo", Coupling: model.Independent,
		Params: map[string]string{"trials": "8", "blockSize": "1", "seed": "0"},
	})
	rawByID := make(map[string]model.TaskResult, len(tasks))
	for i, task := range tasks {
		sum := float64(10 * (i + 1))
		rawByID[string(task.ID)] = model.TaskResult{TaskID: task.ID, Output: encodeMCResultLocal(1, sum, sum*sum), OK: true}
	}

	reportRegion := func(region model.RegionID) model.Aggregate {
		reqs := fakes[region].dispatched()
		var regionRaw []model.TaskResult
		for _, pt := range reqs[0].GetTasks() {
			regionRaw = append(regionRaw, rawByID[pt.GetId()])
		}
		partial := templates.MonteCarloMerge(regionRaw)
		_, err := client.ReportPartial(ctx, &transport.ReportPartialRequest{
			Partial: &transport.PartialAggregate{JobId: jobID, Region: string(region), Template: "monte-carlo", Value: partial.Value, Done: true},
		})
		if err != nil {
			t.Fatalf("ReportPartial(%s): %v", region, err)
		}
		return partial
	}

	reportRegion("r1")
	reportRegion("r2")
	reportRegion("r2") // duplicate: must replace, not accumulate
	reportRegion("r3")

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true")
	}

	var allRaw []model.TaskResult
	for _, task := range tasks {
		allRaw = append(allRaw, rawByID[string(task.ID)])
	}
	wantFlat := templates.MonteCarloMerge(allRaw)
	gotCount, gotSum, _, _ := decodeMCAggregateLocal(statusResp.GetAggregate())
	wantCount, wantSum, _, _ := decodeMCAggregateLocal(wantFlat.Value)
	if gotCount != wantCount || gotSum != wantSum {
		t.Fatalf("aggregate after duplicate ReportPartial (Count=%d Sum=%v) != expected (Count=%d Sum=%v) — a duplicate report double-counted", gotCount, gotSum, wantCount, wantSum)
	}
}

// --- acceptance criterion 5: NoRegion rejects -----------------------------

// TestNoRegionRejectsWithResourceExhausted covers Submit when every known
// region is unhealthy or out of capacity: routing.Decide returns NoRegion,
// and Submit must fail with codes.ResourceExhausted rather than dispatching
// anywhere.
func TestNoRegionRejectsWithResourceExhausted(t *testing.T) {
	_, targets, dial := startFakeRegions(t, "r1", "r2")
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(targets, dial), clock)
	defer teardown()
	ctx := context.Background()

	publishSummary(t, ctx, client, "r1", 0, 1, model.Healthy, 100)     // healthy but no capacity
	publishSummary(t, ctx, client, "r2", 5, 1, model.Unreachable, 100) // capacity but unreachable

	_, err := client.Submit(ctx, &transport.SubmitJobRequest{
		Template: "monte-carlo",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"trials": "1", "blockSize": "1", "seed": "0"},
	})
	if err == nil {
		t.Fatalf("Submit: no error, want codes.ResourceExhausted")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Submit error code = %v, want %v", status.Code(err), codes.ResourceExhausted)
	}
}

// TestNoRegionRejectsWithNoRegionsPublished covers the same rejection with
// no GlobalView data at all — the zero-value GlobalView.
func TestNoRegionRejectsWithNoRegionsPublished(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(nil, nil), clock)
	defer teardown()
	ctx := context.Background()

	_, err := client.Submit(ctx, &transport.SubmitJobRequest{
		Template: "monte-carlo",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"trials": "1", "blockSize": "1", "seed": "0"},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Submit error code = %v, want %v", status.Code(err), codes.ResourceExhausted)
	}
}

// --- acceptance criterion 6: Diverged -------------------------------------

// TestDivergedRegionReportsUnreachable covers a region that stops publishing
// past routing.StalenessWindow: it must appear in GetGlobalView.Diverged and
// be reported Health_HEALTH_UNREACHABLE, even though its last known summary
// itself said Healthy.
func TestDivergedRegionReportsUnreachable(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(nil, nil), clock)
	defer teardown()
	ctx := context.Background()

	publishSummary(t, ctx, client, "stale", 5, 1, model.Healthy, clock.now())
	publishSummary(t, ctx, client, "fresh", 5, 1, model.Healthy, clock.now())

	// Before the staleness window elapses, neither region is diverged.
	before, err := client.GetGlobalView(ctx, &transport.GlobalViewRequest{})
	if err != nil {
		t.Fatalf("GetGlobalView: %v", err)
	}
	if len(before.GetDiverged()) != 0 {
		t.Fatalf("Diverged = %v before the staleness window elapsed, want empty", before.GetDiverged())
	}

	clock.advance(31 * 1_000_000_000) // > routing.StalenessWindow (30s of Instant ns)
	publishSummary(t, ctx, client, "fresh", 5, 1, model.Healthy, clock.now())

	after, err := client.GetGlobalView(ctx, &transport.GlobalViewRequest{})
	if err != nil {
		t.Fatalf("GetGlobalView: %v", err)
	}
	if len(after.GetDiverged()) != 1 || after.GetDiverged()[0] != "stale" {
		t.Fatalf("Diverged = %v, want [\"stale\"]", after.GetDiverged())
	}

	for _, r := range after.GetRegions() {
		switch r.GetId() {
		case "stale":
			if r.GetHealth() != transport.Health_HEALTH_UNREACHABLE {
				t.Fatalf("stale region Health = %v, want HEALTH_UNREACHABLE", r.GetHealth())
			}
		case "fresh":
			if r.GetHealth() != transport.Health_HEALTH_HEALTHY {
				t.Fatalf("fresh region Health = %v, want HEALTH_HEALTHY", r.GetHealth())
			}
		}
	}
}

// --- bonus: To route's JobStatus proxy ------------------------------------

// TestJobStatusProxiesToRegionForToRoute covers the owner-decided semantics
// that a To route's JobStatus is proxied to the owning region, unchanged.
func TestJobStatusProxiesToRegionForToRoute(t *testing.T) {
	fakes, targets, dial := startFakeRegions(t, "only")
	clock := &testClock{}
	client, _, teardown := newTestServer(t, testConfig(targets, dial), clock)
	defer teardown()
	ctx := context.Background()

	publishSummary(t, ctx, client, "only", 5, 1, model.Healthy, 100)
	jobID := submitMonteCarlo(t, ctx, client, 2)

	fakes["only"].setJobStatus(jobID, &transport.JobStatusResponse{Done: true, Aggregate: []byte("region-final")})

	resp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !resp.GetDone() || string(resp.GetAggregate()) != "region-final" {
		t.Fatalf("JobStatus = (Done=%v, Aggregate=%q), want (Done=true, Aggregate=%q) proxied from the owning region", resp.GetDone(), resp.GetAggregate(), "region-final")
	}
}

// --- local codecs (mirrors controlplane's test codecs for mcResult/mcAggregate) ---

func encodeMCResultLocal(count int64, sum, sumSq float64) []byte {
	out := make([]byte, 24)
	binary.BigEndian.PutUint64(out[0:8], uint64(count))
	binary.BigEndian.PutUint64(out[8:16], math.Float64bits(sum))
	binary.BigEndian.PutUint64(out[16:24], math.Float64bits(sumSq))
	return out
}

func decodeMCAggregateLocal(b []byte) (count int64, sum, mean, variance float64) {
	if len(b) != 32 {
		return 0, 0, 0, 0
	}
	count = int64(binary.BigEndian.Uint64(b[0:8]))
	sum = math.Float64frombits(binary.BigEndian.Uint64(b[8:16]))
	mean = math.Float64frombits(binary.BigEndian.Uint64(b[16:24]))
	variance = math.Float64frombits(binary.BigEndian.Uint64(b[24:32]))
	return count, sum, mean, variance
}
