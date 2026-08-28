package controlplane

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/core/routing"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// This file covers ticket #44 (S2, regional control plane)'s six acceptance
// behaviors: correct summary publish, by-cell roll-up == flat merge (with
// the tiering observable), a dispatched self-sink partition aggregating
// locally, a global-sink partition reporting exactly one partial up, spill
// to a healthy peer (and holding pending when none qualifies), and never
// spilling a tight-coupling job. Every test is in-process (bufconn), with a
// fake GlobalRouter and a fake peer ControlPlane defined below — no real
// network, no wall-clock sleeping beyond waitFor's bounded polling for the
// two genuinely timer-driven behaviors (the publish loop; SubmitJob/
// DispatchTasks's own placement pass is synchronous, so the rest need no
// waiting at all).

// --- fake GlobalRouter ------------------------------------------------

// fakeGlobalRouter is an in-process stand-in for the P1 global routing
// layer's GlobalRouter service: it records every PublishSummary and
// ReportPartial call it receives and serves a configurable GetGlobalView
// response.
type fakeGlobalRouter struct {
	transport.UnimplementedGlobalRouterServer

	mu           sync.Mutex
	summaries    []*transport.RegionalSummary
	partials     []*transport.PartialAggregate
	viewRegions  []*transport.RegionView
	viewDiverged []string
}

func (f *fakeGlobalRouter) PublishSummary(_ context.Context, req *transport.PublishSummaryRequest) (*transport.PublishSummaryResponse, error) {
	f.mu.Lock()
	f.summaries = append(f.summaries, req.GetSummary())
	f.mu.Unlock()
	return &transport.PublishSummaryResponse{Ok: true}, nil
}

func (f *fakeGlobalRouter) GetGlobalView(_ context.Context, _ *transport.GlobalViewRequest) (*transport.GlobalViewResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &transport.GlobalViewResponse{Regions: f.viewRegions, Diverged: f.viewDiverged}, nil
}

func (f *fakeGlobalRouter) ReportPartial(_ context.Context, req *transport.ReportPartialRequest) (*transport.ReportPartialResponse, error) {
	f.mu.Lock()
	f.partials = append(f.partials, req.GetPartial())
	f.mu.Unlock()
	return &transport.ReportPartialResponse{Ok: true}, nil
}

func (f *fakeGlobalRouter) publishedSummaries() []*transport.RegionalSummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*transport.RegionalSummary, len(f.summaries))
	copy(out, f.summaries)
	return out
}

func (f *fakeGlobalRouter) reportedPartials() []*transport.PartialAggregate {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*transport.PartialAggregate, len(f.partials))
	copy(out, f.partials)
	return out
}

// startFakeGlobalRouter runs a real gRPC server backed by fakeGlobalRouter
// over an in-process bufconn listener, and returns a GlobalRouterDialer that
// connects to it regardless of the target string it is dialed with — these
// tests care about which fake receives a call, not about routing by address
// (that is exercised by the real gRPC dialers in production).
func startFakeGlobalRouter(t *testing.T) (*fakeGlobalRouter, GlobalRouterDialer) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	fake := &fakeGlobalRouter{}
	transport.RegisterGlobalRouterServer(srv, fake)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dial := func(ctx context.Context, _ string) (transport.GlobalRouterClient, io.Closer, error) {
		conn, err := grpc.NewClient("passthrough:///bufnet-global",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return transport.NewGlobalRouterClient(conn), conn, nil
	}
	return fake, dial
}

// --- fake peer ControlPlane --------------------------------------------

// fakePeerControlPlane is an in-process stand-in for a peer region's
// ControlPlane service: it records every DispatchTasks and ReportResult
// call it receives (spill delivery, and a spill origin receiving a
// forwarded raw result), and accepts every DispatchTasks unless
// rejectDispatch is set.
type fakePeerControlPlane struct {
	transport.UnimplementedControlPlaneServer

	mu              sync.Mutex
	dispatchedReqs  []*transport.DispatchTasksRequest
	reportedResults []*transport.ReportResultRequest
	rejectDispatch  bool
}

func (f *fakePeerControlPlane) DispatchTasks(_ context.Context, req *transport.DispatchTasksRequest) (*transport.DispatchTasksResponse, error) {
	f.mu.Lock()
	f.dispatchedReqs = append(f.dispatchedReqs, req)
	reject := f.rejectDispatch
	f.mu.Unlock()
	if reject {
		return &transport.DispatchTasksResponse{Accepted: false, Reason: "rejected by fake peer"}, nil
	}
	return &transport.DispatchTasksResponse{Accepted: true}, nil
}

func (f *fakePeerControlPlane) ReportResult(_ context.Context, req *transport.ReportResultRequest) (*transport.ReportResultResponse, error) {
	f.mu.Lock()
	f.reportedResults = append(f.reportedResults, req)
	f.mu.Unlock()
	return &transport.ReportResultResponse{Accepted: true}, nil
}

func (f *fakePeerControlPlane) dispatched() []*transport.DispatchTasksRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*transport.DispatchTasksRequest, len(f.dispatchedReqs))
	copy(out, f.dispatchedReqs)
	return out
}

// startFakePeerControlPlane runs a real gRPC server backed by
// fakePeerControlPlane over an in-process bufconn listener, and returns a
// PeerDialer that connects to it regardless of the target string.
func startFakePeerControlPlane(t *testing.T) (*fakePeerControlPlane, PeerDialer) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	fake := &fakePeerControlPlane{}
	transport.RegisterControlPlaneServer(srv, fake)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dial := func(ctx context.Context, _ string) (transport.ControlPlaneClient, io.Closer, error) {
		conn, err := grpc.NewClient("passthrough:///bufnet-peer",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return transport.NewControlPlaneClient(conn), conn, nil
	}
	return fake, dial
}

// --- acceptance criterion 1: publishes a correct summary ----------------

// TestPublishLoopReportsCorrectSummary asserts the publish loop's summary
// matches routing.Summarize over the region's own registry, stamped with
// RegionID and a monotonically increasing At across ticks.
func TestPublishLoopReportsCorrectSummary(t *testing.T) {
	clock := &testClock{}
	fake, dial := startFakeGlobalRouter(t)

	cfg := fastConfig()
	cfg.RegionID = "region-a"
	cfg.GlobalRouter = "global-addr"
	cfg.GlobalRouterDialer = dial
	cfg.SummaryInterval = 10 * time.Millisecond
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 3)

	// testClock starts at 0; advance it so the first publish's At is
	// observably nonzero (stamped from the injected clock, not left zeroed).
	clock.advance(time.Second)

	waitFor(t, func() bool { return len(fake.publishedSummaries()) >= 1 })

	srv.mu.Lock()
	want := routing.Summarize(srv.store.Registry())
	srv.mu.Unlock()

	got := fake.publishedSummaries()[0]
	if got.GetRegion() != "region-a" {
		t.Fatalf("published summary Region = %q, want %q", got.GetRegion(), "region-a")
	}
	if int(got.GetFree()) != want.Free {
		t.Fatalf("published summary Free = %d, want %d", got.GetFree(), want.Free)
	}
	if int(got.GetCells()) != want.Cells {
		t.Fatalf("published summary Cells = %d, want %d", got.GetCells(), want.Cells)
	}
	if got.GetAt() <= 0 {
		t.Fatalf("published summary At = %d, want > 0 (stamped from the injected clock)", got.GetAt())
	}

	// A later publish is stamped with a strictly later At (the clock only
	// ever advances), proving At is genuinely re-stamped per tick rather
	// than a fixed value copied from the first summary.
	clock.advance(time.Second)
	firstAt := got.GetAt()
	waitFor(t, func() bool {
		s := fake.publishedSummaries()
		return len(s) >= 2 && s[len(s)-1].GetAt() > firstAt
	})
}

// --- acceptance criterion 2: by-cell roll-up == flat merge --------------

// TestByCellRollupEqualsFlatMerge submits a monte-carlo job whose tasks are
// placed across (at least) two cells, reports each task's result, and
// asserts the region's final Aggregate is byte-identical to a flat
// templates.MonteCarloMerge over the same raw results — proving the by-cell
// -> aggregate.MergeAll roll-up is equivalent to a flat merge — while also
// asserting the tiering actually happened (at least two distinct
// srv.taskCell groups were formed), so the equality is not vacuously true
// because everything landed on one cell.
func TestByCellRollupEqualsFlatMerge(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 4
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-a", 1)
	joinAgent(t, ctx, client, "agent-b", 4)

	jobID := submitMonteCarlo(t, ctx, client, 4) // 4 independent single-trial tasks

	pulledA := drainAllTasks(t, ctx, client, "agent-a")
	pulledB := drainAllTasks(t, ctx, client, "agent-b")
	all := append(append([]*transport.Task{}, pulledA...), pulledB...)
	if len(all) != 4 {
		t.Fatalf("pulled %d tasks total, want 4", len(all))
	}

	srv.mu.Lock()
	cellsUsed := map[model.CellID]struct{}{}
	for _, task := range all {
		cellsUsed[srv.taskCell[model.TaskID(task.GetId())]] = struct{}{}
	}
	srv.mu.Unlock()
	if len(cellsUsed) < 2 {
		t.Fatalf("tasks landed on %d distinct cell group(s), want >= 2 for the by-cell tiering to be observable", len(cellsUsed))
	}

	sums := []float64{10, 20, 30, 40}
	rawResults := make([]model.TaskResult, len(all))
	for i, task := range all {
		out := encodeMCResult(1, sums[i], sums[i]*sums[i])
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{TaskId: task.GetId(), Output: out, Ok: true})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", task.GetId(), err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", task.GetId())
		}
		rawResults[i] = model.TaskResult{TaskID: model.TaskID(task.GetId()), Output: out, OK: true}
	}

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true")
	}

	wantFlat := templates.MonteCarloMerge(rawResults)
	if !bytes.Equal(statusResp.GetAggregate(), wantFlat.Value) {
		gotCount, gotSum, _, _ := decodeMCAggregate(statusResp.GetAggregate())
		wantCount, wantSum, _, _ := decodeMCAggregate(wantFlat.Value)
		t.Fatalf("region aggregate (Count=%d Sum=%v) != flat merge (Count=%d Sum=%v)", gotCount, gotSum, wantCount, wantSum)
	}
}

// --- acceptance criterion 3: dispatched partition, self sink -------------

// TestDispatchTasksSelfSinkAggregatesLocally covers DispatchTasks with an
// empty result_sink: the region places the dispatched tasks onto its own
// per-cell queues, an agent runs them, and the region aggregates locally to
// the expected Aggregate, exposed via JobStatus — exactly like a locally
// submitted job.
func TestDispatchTasksSelfSinkAggregatesLocally(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 2)

	jobID := "dispatched-self-job"
	dispatchResp, err := client.DispatchTasks(ctx, &transport.DispatchTasksRequest{
		Job: &transport.JobSpec{Id: jobID, Template: "monte-carlo", Coupling: transport.Coupling_COUPLING_INDEPENDENT},
		Tasks: []*transport.Task{
			{Id: "dt-1", JobId: jobID},
			{Id: "dt-2", JobId: jobID},
		},
		ResultSink: "",
	})
	if err != nil {
		t.Fatalf("DispatchTasks: %v", err)
	}
	if !dispatchResp.GetAccepted() {
		t.Fatalf("DispatchTasks not accepted: %s", dispatchResp.GetReason())
	}

	pulled := drainAllTasks(t, ctx, client, "agent-1")
	if len(pulled) != 2 {
		t.Fatalf("pulled %d tasks, want 2", len(pulled))
	}

	report := func(taskID string, sum float64) {
		t.Helper()
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: taskID, Output: encodeMCResult(1, sum, sum*sum), Ok: true,
		})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", taskID, err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", taskID)
		}
	}
	report(pulled[0].GetId(), 10)
	report(pulled[1].GetId(), 20)

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true")
	}
	gotCount, gotSum, _, _ := decodeMCAggregate(statusResp.GetAggregate())
	if gotCount != 2 || gotSum != 30 {
		t.Fatalf("aggregate (Count=%d, Sum=%v), want (Count=2, Sum=30)", gotCount, gotSum)
	}
}

// --- acceptance criterion 4: partial reported up, global sink -----------

// TestDispatchTasksGlobalSinkReportsOnePartialUp covers DispatchTasks with
// result_sink set to this region's configured GlobalRouter address: once
// the dispatched partition completes, the fake GlobalRouter's ReportPartial
// receives exactly ONE region partial — never raw per-task results —
// carrying region, template, and the rolled-up value.
func TestDispatchTasksGlobalSinkReportsOnePartialUp(t *testing.T) {
	clock := &testClock{}
	fake, dial := startFakeGlobalRouter(t)

	cfg := fastConfig()
	cfg.RegionID = "region-b"
	cfg.GlobalRouter = "fake-global-addr"
	cfg.GlobalRouterDialer = dial
	cfg.SummaryInterval = time.Hour // keep the publish loop from also touching ReportPartial's fake during this test
	client, _, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 2)

	jobID := "dispatched-global-job"
	dispatchResp, err := client.DispatchTasks(ctx, &transport.DispatchTasksRequest{
		Job: &transport.JobSpec{Id: jobID, Template: "monte-carlo", Coupling: transport.Coupling_COUPLING_INDEPENDENT},
		Tasks: []*transport.Task{
			{Id: "gt-1", JobId: jobID},
			{Id: "gt-2", JobId: jobID},
		},
		ResultSink: cfg.GlobalRouter,
	})
	if err != nil {
		t.Fatalf("DispatchTasks: %v", err)
	}
	if !dispatchResp.GetAccepted() {
		t.Fatalf("DispatchTasks not accepted: %s", dispatchResp.GetReason())
	}

	pulled := drainAllTasks(t, ctx, client, "agent-1")
	if len(pulled) != 2 {
		t.Fatalf("pulled %d tasks, want 2", len(pulled))
	}

	report := func(taskID string, sum float64) {
		t.Helper()
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: taskID, Output: encodeMCResult(1, sum, sum*sum), Ok: true,
		})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", taskID, err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", taskID)
		}
	}
	report(pulled[0].GetId(), 10)
	report(pulled[1].GetId(), 20)

	partials := fake.reportedPartials()
	if len(partials) != 1 {
		t.Fatalf("GlobalRouter received %d ReportPartial call(s), want exactly 1 (one region partial, not raw per-task results)", len(partials))
	}
	got := partials[0]
	if got.GetJobId() != jobID {
		t.Fatalf("partial JobId = %q, want %q", got.GetJobId(), jobID)
	}
	if got.GetRegion() != "region-b" {
		t.Fatalf("partial Region = %q, want %q", got.GetRegion(), "region-b")
	}
	if got.GetTemplate() != "monte-carlo" {
		t.Fatalf("partial Template = %q, want %q", got.GetTemplate(), "monte-carlo")
	}
	if !got.GetDone() {
		t.Fatalf("partial Done = false, want true")
	}
	gotCount, gotSum, _, _ := decodeMCAggregate(got.GetValue())
	if gotCount != 2 || gotSum != 30 {
		t.Fatalf("partial value (Count=%d, Sum=%v), want (Count=2, Sum=30)", gotCount, gotSum)
	}

	// JobStatus at this region must NOT report the job done: the global
	// layer, not this region, owns this job's final aggregate.
	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = true for a global-sink job, want false (this region does not own its completion)")
	}
}

// --- regression: a duplicate/late report must not re-finalize a job -----

// TestDuplicateReportAfterGlobalSinkCompletionReportsPartialOnce is a
// regression test for a real bug the auditor caught: ReportResult's
// distinct-task gate (distinct >= total) stays true forever once a job
// completes, but a duplicate or late-retried report for an
// already-reported task is an explicitly supported input (see
// dedupeTaskResults) and re-enters maybeRollup after completion. For a
// global-sink job, maybeRollup's completion action is the non-idempotent
// network call GlobalRouter.ReportPartial — without a once-only latch, a
// duplicate report after completion fires a SECOND ReportPartial, violating
// "receives ONE region partial". This asserts a duplicate report delivered
// after the job has already completed still yields exactly one
// ReportPartial call.
func TestDuplicateReportAfterGlobalSinkCompletionReportsPartialOnce(t *testing.T) {
	clock := &testClock{}
	fake, dial := startFakeGlobalRouter(t)

	cfg := fastConfig()
	cfg.RegionID = "region-c"
	cfg.GlobalRouter = "fake-global-addr"
	cfg.GlobalRouterDialer = dial
	cfg.SummaryInterval = time.Hour
	client, _, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 2)

	jobID := "dispatched-global-job-dup"
	dispatchResp, err := client.DispatchTasks(ctx, &transport.DispatchTasksRequest{
		Job: &transport.JobSpec{Id: jobID, Template: "monte-carlo", Coupling: transport.Coupling_COUPLING_INDEPENDENT},
		Tasks: []*transport.Task{
			{Id: "dup-gt-1", JobId: jobID},
			{Id: "dup-gt-2", JobId: jobID},
		},
		ResultSink: cfg.GlobalRouter,
	})
	if err != nil {
		t.Fatalf("DispatchTasks: %v", err)
	}
	if !dispatchResp.GetAccepted() {
		t.Fatalf("DispatchTasks not accepted: %s", dispatchResp.GetReason())
	}

	pulled := drainAllTasks(t, ctx, client, "agent-1")
	if len(pulled) != 2 {
		t.Fatalf("pulled %d tasks, want 2", len(pulled))
	}

	report := func(taskID string, sum float64) {
		t.Helper()
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: taskID, Output: encodeMCResult(1, sum, sum*sum), Ok: true,
		})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", taskID, err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", taskID)
		}
	}
	report(pulled[0].GetId(), 10)
	report(pulled[1].GetId(), 20) // completes the job: distinct (2) >= total (2)

	if got := len(fake.reportedPartials()); got != 1 {
		t.Fatalf("GlobalRouter received %d ReportPartial call(s) after initial completion, want exactly 1", got)
	}

	// A duplicate/retry report for an already-reported task, delivered after
	// the job has already completed — distinct>=total is (still) true, but
	// the completion action must not run again.
	report(pulled[1].GetId(), 20)

	if got := len(fake.reportedPartials()); got != 1 {
		t.Fatalf("GlobalRouter received %d ReportPartial call(s) after a duplicate post-completion report, want exactly 1 (the bug: a duplicate re-triggered a second upward report)", got)
	}
}

// TestDuplicateReportAfterSelfSinkCompletionKeepsOneAggregate is the
// self-sink analogue of the regression above: a duplicate/late report
// delivered after a locally-owned job has already completed must not change
// (or fail to keep) the stored final Aggregate. P0's self-sink completion
// action (store.PutAggregate) is an idempotent overwrite, so this was never
// user-visibly broken, but it exercises the same once-only latch path the
// global-sink fix added, guarding against a future non-idempotent self-sink
// completion action regressing this silently.
func TestDuplicateReportAfterSelfSinkCompletionKeepsOneAggregate(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 2)
	jobID := submitMonteCarlo(t, ctx, client, 2)

	pulled := drainAllTasks(t, ctx, client, "agent-1")
	if len(pulled) != 2 {
		t.Fatalf("pulled %d tasks, want 2", len(pulled))
	}

	report := func(taskID string, sum float64) {
		t.Helper()
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: taskID, Output: encodeMCResult(1, sum, sum*sum), Ok: true,
		})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", taskID, err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", taskID)
		}
	}
	report(pulled[0].GetId(), 10)
	report(pulled[1].GetId(), 20) // completes the job

	// A duplicate/retry report delivered after completion.
	report(pulled[1].GetId(), 20)

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true")
	}
	gotCount, gotSum, _, _ := decodeMCAggregate(statusResp.GetAggregate())
	if gotCount != 2 || gotSum != 30 {
		t.Fatalf("aggregate (Count=%d, Sum=%v), want (Count=2, Sum=30) — unchanged by the post-completion duplicate", gotCount, gotSum)
	}
}

// --- acceptance criterion 5: spills when full ----------------------------

// spillTestConfig returns a fastConfig tuned for the spill tests: a
// single-slot starting cell (so one agent fills the whole region), regional
// mode enabled (GlobalRouter set — spill is disabled otherwise), a
// well-known SelfAddress the peer's forwarded DispatchTasksRequest's
// result_sink is asserted against, and the publish loop parked far in the
// future so it cannot race these tests' own direct peerView writes.
func spillTestConfig(peerDial PeerDialer) Config {
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 1
	cfg.GlobalRouter = "global-enabled"
	cfg.SummaryInterval = time.Hour
	cfg.SelfAddress = "origin-addr"
	cfg.PeerTargets = map[model.RegionID]string{"region-peer": "peer-addr"}
	cfg.PeerDialer = peerDial
	return cfg
}

// TestSpillsToHealthyPeerWhenFull fills the region's only cell, seeds a
// healthy, free peer in the cached global view, and submits an Independent
// job: the peer's fake ControlPlane must receive the spilled task via
// DispatchTasks with result_sink naming this region (SelfAddress).
func TestSpillsToHealthyPeerWhenFull(t *testing.T) {
	clock := &testClock{}
	peer, peerDial := startFakePeerControlPlane(t)
	cfg := spillTestConfig(peerDial)
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-a", 1) // fills the region's one cell: Free == 0

	srv.mu.Lock()
	srv.peerView = []model.RegionView{{ID: "region-peer", Free: 5, Health: model.Healthy}}
	srv.mu.Unlock()

	jobID := submitMonteCarlo(t, ctx, client, 1) // one Independent task

	// SubmitJob's own placement pass (drainPendingLocked) runs the spill
	// synchronously before the RPC returns — no waiting needed.
	dispatched := peer.dispatched()
	if len(dispatched) != 1 {
		t.Fatalf("peer received %d DispatchTasks call(s), want exactly 1", len(dispatched))
	}
	req := dispatched[0]
	if req.GetJob().GetId() != jobID {
		t.Fatalf("spilled job id = %q, want %q", req.GetJob().GetId(), jobID)
	}
	if len(req.GetTasks()) != 1 {
		t.Fatalf("spilled task count = %d, want 1", len(req.GetTasks()))
	}
	if req.GetResultSink() != "origin-addr" {
		t.Fatalf("spilled result_sink = %q, want %q (this region's SelfAddress)", req.GetResultSink(), "origin-addr")
	}

	// The spilled task never lands on the local (full) agent.
	if resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-a"}); err != nil {
		t.Fatalf("PullTask(agent-a): %v", err)
	} else if resp.GetHasTask() {
		t.Fatalf("agent-a pulled the spilled task locally, want it to have gone to the peer")
	}
}

// TestSpillNoQualifyingPeerHoldsPending covers the "no qualifying peer"
// branch: with the region full and the cached peer view empty (no peer has
// ever been observed as healthy/free), a submitted Independent task must
// stay pending — never lost, never forced through — exactly like S1's
// region-full behavior.
func TestSpillNoQualifyingPeerHoldsPending(t *testing.T) {
	clock := &testClock{}
	peer, peerDial := startFakePeerControlPlane(t)
	cfg := spillTestConfig(peerDial)
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-a", 1) // fills the region's one cell: Free == 0
	// srv.peerView is left at its zero value (nil): no peer has ever been observed.

	submitMonteCarlo(t, ctx, client, 1)

	if got := peer.dispatched(); len(got) != 0 {
		t.Fatalf("peer received %d DispatchTasks call(s), want 0 (no qualifying peer)", len(got))
	}
	if resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-a"}); err != nil {
		t.Fatalf("PullTask(agent-a): %v", err)
	} else if resp.GetHasTask() {
		t.Fatalf("agent-a pulled a task while the region is full and no peer qualifies")
	}

	srv.mu.Lock()
	pendingCount := len(srv.pending)
	srv.mu.Unlock()
	if pendingCount != 1 {
		t.Fatalf("s.pending holds %d task(s), want 1 (held, not lost)", pendingCount)
	}
}

// --- acceptance criterion 6: no tight-coupling spill ---------------------

// TestSpillNeverCrossesForTightCoupling injects a Barrier-coupled job
// directly via DispatchTasks (admission.Admit itself rejects any
// non-Independent Coupling — see internal/core/admission — so a tight job
// can only reach a region this way, e.g. a future P2 driver) into a full
// region with an otherwise-qualifying healthy peer, and asserts the task is
// never spilled: it stays pending, and the peer never sees it.
func TestSpillNeverCrossesForTightCoupling(t *testing.T) {
	clock := &testClock{}
	peer, peerDial := startFakePeerControlPlane(t)
	cfg := spillTestConfig(peerDial)
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-a", 1) // fills the region's one cell: Free == 0

	srv.mu.Lock()
	srv.peerView = []model.RegionView{{ID: "region-peer", Free: 5, Health: model.Healthy}}
	srv.mu.Unlock()

	dispatchResp, err := client.DispatchTasks(ctx, &transport.DispatchTasksRequest{
		Job:        &transport.JobSpec{Id: "barrier-job", Template: "monte-carlo", Coupling: transport.Coupling_COUPLING_BARRIER},
		Tasks:      []*transport.Task{{Id: "bt-1", JobId: "barrier-job"}},
		ResultSink: "",
	})
	if err != nil {
		t.Fatalf("DispatchTasks: %v", err)
	}
	if !dispatchResp.GetAccepted() {
		t.Fatalf("DispatchTasks not accepted: %s", dispatchResp.GetReason())
	}

	if got := peer.dispatched(); len(got) != 0 {
		t.Fatalf("peer received %d DispatchTasks call(s) for a Barrier job, want 0 — coupling must never cross a region boundary", len(got))
	}

	srv.mu.Lock()
	pendingCount := len(srv.pending)
	srv.mu.Unlock()
	if pendingCount != 1 {
		t.Fatalf("s.pending holds %d task(s), want 1 (the Barrier task, held rather than spilled)", pendingCount)
	}
}
