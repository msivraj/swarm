package controlplane

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// sleepRecorder is a Config.Sleep fake: it records every requested delay
// instead of actually waiting, so a test that drives a request into the
// Throttle band resolves instantly and deterministically — see Config.Sleep's
// doc for why real time.Sleep never runs in this package's tests.
type sleepRecorder struct {
	mu    sync.Mutex
	calls []model.Duration
}

func (r *sleepRecorder) sleep(d model.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, d)
}

func (r *sleepRecorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// highLoadLimits is a small Limits configuration this file's tests drive
// past its ShedThreshold with a handful of in-flight requests, rather than
// DefaultConfig's generous 1000-capacity default — that keeps the arithmetic
// (and the load values below) easy to follow.
var highLoadLimits = model.Limits{Capacity: 100, ShedThreshold: 0.90}

// setLoad sets srv's live load snapshot directly — same-package test
// access, exactly like existing tests already reach into srv.lastSeen etc.
// (see New's doc) — so a test can drive load straight to a chosen ratio
// without needing dozens of real concurrent RPCs in flight.
func setLoad(srv *Server, load model.LoadState) {
	srv.loadMu.Lock()
	srv.load = load
	srv.loadMu.Unlock()
}

// TestBackpressureLowLoadIsTransparent proves the middleware is inert when
// there is headroom (the regression guard for P0-P3's existing behavior):
// with DefaultConfig's generous Limits and no load pushed onto the server,
// SubmitJob and JoinAgent both succeed immediately, and no delay is ever
// recorded on the fake Sleep.
func TestBackpressureLowLoadIsTransparent(t *testing.T) {
	clock := &testClock{}
	sleeps := &sleepRecorder{}
	cfg := fastConfig()
	cfg.Sleep = sleeps.sleep
	client, _, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 4)

	if _, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"start": "0", "end": "1", "shards": "1"},
	}); err != nil {
		t.Fatalf("SubmitJob under low load: %v", err)
	}

	if got := sleeps.callCount(); got != 0 {
		t.Fatalf("Sleep called %d times under low load, want 0", got)
	}
}

// TestBackpressureIngressShedsLowPriorityAdmitsHighPriority drives the
// server's load past highLoadLimits' shed threshold and asserts SubmitJob
// and JoinAgent each faithfully enforce the full Admit/Throttle/Shed
// decision (fork (b) of #157): a low-priority request is rejected with
// ResourceExhausted, while a higher-priority request at the exact same load
// is admitted (or throttled, per the returned Delay, waited out on the fake
// Sleep — never a real one).
func TestBackpressureIngressShedsLowPriorityAdmitsHighPriority(t *testing.T) {
	clock := &testClock{}
	sleeps := &sleepRecorder{}
	cfg := fastConfig()
	cfg.Limits = highLoadLimits
	cfg.Sleep = sleeps.sleep
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	// ratio = 95/100 = 0.95: >= effectiveShed(priority 0) = 0.90, so priority
	// 0 sheds; < effectiveShed(priority 20) = 0.90 + 20*0.01 = 1.10, so
	// priority 20 does not.
	setLoad(srv, model.LoadState{InFlight: 95})

	t.Run("SubmitJob low priority sheds", func(t *testing.T) {
		_, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
			Template: "keyspace-search",
			Coupling: transport.Coupling_COUPLING_INDEPENDENT,
			Params:   map[string]string{"start": "0", "end": "1", "shards": "1", "priority": "0"},
		})
		assertResourceExhausted(t, err)
	})

	t.Run("SubmitJob high priority admits or throttles", func(t *testing.T) {
		setLoad(srv, model.LoadState{InFlight: 95})
		if _, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
			Template: "keyspace-search",
			Coupling: transport.Coupling_COUPLING_INDEPENDENT,
			Params:   map[string]string{"start": "0", "end": "1", "shards": "1", "priority": "20"},
		}); err != nil {
			t.Fatalf("SubmitJob(priority 20) under high load: %v", err)
		}
	})

	t.Run("JoinAgent low priority sheds", func(t *testing.T) {
		setLoad(srv, model.LoadState{InFlight: 95})
		srv.cfg.JoinPriority = 0
		_, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-low", Region: "us", Caps: 4})
		assertResourceExhausted(t, err)
	})

	t.Run("JoinAgent high priority admits or throttles", func(t *testing.T) {
		setLoad(srv, model.LoadState{InFlight: 95})
		srv.cfg.JoinPriority = 20
		resp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-high", Region: "us", Caps: 4})
		if err != nil {
			t.Fatalf("JoinAgent(priority 20) under high load: %v", err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("JoinAgent(priority 20) rejected: %s", resp.GetReason())
		}
	})

	if got := sleeps.callCount(); got == 0 {
		t.Fatalf("Sleep never called across the high-priority Throttle cases, want at least 1")
	}
}

// TestBackpressurePullTaskNeverShedsUnderHighLoad proves PullTask is a
// throttle-only lever (fork (b) of #157): it enqueues a task at low load,
// then drives the server well past its shed threshold and calls PullTask —
// the very load a SubmitJob/JoinAgent at the same priority would be shed
// at — and asserts the agent still gets its task back rather than an error,
// after waiting out a fake (never real) delay.
func TestBackpressurePullTaskNeverShedsUnderHighLoad(t *testing.T) {
	clock := &testClock{}
	sleeps := &sleepRecorder{}
	cfg := fastConfig()
	cfg.Limits = highLoadLimits
	cfg.Sleep = sleeps.sleep
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 4)
	if _, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"start": "0", "end": "1", "shards": "1"},
	}); err != nil {
		t.Fatalf("SubmitJob (queueing a task) at low load: %v", err)
	}

	// Drive load well past what would Shed a priority-0 SubmitJob/JoinAgent
	// (see TestBackpressureIngressShedsLowPriorityAdmitsHighPriority).
	setLoad(srv, model.LoadState{InFlight: 95})

	before := sleeps.callCount()
	resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-1"})
	if err != nil {
		t.Fatalf("PullTask under high load returned an error (should only ever throttle): %v", err)
	}
	if !resp.GetHasTask() {
		t.Fatalf("PullTask under high load: HasTask = false, want true (task was queued)")
	}
	if got := sleeps.callCount(); got <= before {
		t.Fatalf("PullTask's degraded Shed->Throttle never waited out a delay (Sleep call count %d -> %d)", before, got)
	}
}

// TestBackpressureReportResultNeverGatedUnderHighLoad proves ReportResult is
// exempt from backpressure entirely (fork (b) of #157): even at the same
// crushing load a SubmitJob/JoinAgent would be shed at, a reported result is
// always accepted, and no delay is ever recorded on the fake Sleep for this
// call.
func TestBackpressureReportResultNeverGatedUnderHighLoad(t *testing.T) {
	clock := &testClock{}
	sleeps := &sleepRecorder{}
	cfg := fastConfig()
	cfg.Limits = highLoadLimits
	cfg.Sleep = sleeps.sleep
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 4)
	if _, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"start": "0", "end": "1", "shards": "1"},
	}); err != nil {
		t.Fatalf("SubmitJob at low load: %v", err)
	}

	pullResp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-1"})
	if err != nil || !pullResp.GetHasTask() {
		t.Fatalf("PullTask at low load: resp=%+v err=%v", pullResp, err)
	}

	setLoad(srv, model.LoadState{InFlight: 95})

	before := sleeps.callCount()
	reportResp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
		TaskId: pullResp.GetTask().GetId(),
		Output: []byte("ok"),
		Ok:     true,
	})
	if err != nil {
		t.Fatalf("ReportResult under high load: %v", err)
	}
	if !reportResp.GetAccepted() {
		t.Fatalf("ReportResult under high load: Accepted = false, want true")
	}
	if got := sleeps.callCount(); got != before {
		t.Fatalf("ReportResult waited out a delay (Sleep call count %d -> %d), want no backpressure call at all", before, got)
	}
}

// assertResourceExhausted fails the test unless err is a gRPC status with
// codes.ResourceExhausted, the wire signal Shed maps to.
func assertResourceExhausted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("got no error, want codes.ResourceExhausted")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("err = %v, want codes.ResourceExhausted", err)
	}
}
