package controlplane

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/core/mitosis"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/store"
	"github.com/msivraj/swarm/internal/shell/transport"
)

const bufSize = 1 << 20

// testClock is a controllable, monotonic fake clock for tests: it never
// reads time.Now, so advancing it deterministically drives the reaper and
// mitosis loops (which read it via Server.now) without any wall-clock
// sleeping in the test.
type testClock struct {
	ns atomic.Int64
}

func (c *testClock) now() model.Instant { return model.Instant(c.ns.Load()) }
func (c *testClock) advance(d time.Duration) {
	c.ns.Add(int64(d))
}

// newTestServer starts a Server over an in-process bufconn listener and
// returns a connected client plus teardown. cfg lets each test tune
// heartbeat/mitosis timing without any real waiting: the background loops'
// tickers still fire on wall-clock time, but the *decisions* they drive
// (reap/split/merge) are computed from clock.now(), which the test controls.
func newTestServer(t *testing.T, cfg Config, clock *testClock) (transport.ControlPlaneClient, *Server, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := New(store.NewMemStore(), cfg, clock.now)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Serve: %v", err)
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	teardown := func() {
		_ = conn.Close()
		srv.Stop()
		wg.Wait()
	}
	return transport.NewControlPlaneClient(conn), srv, teardown
}

// fastConfig is a Config whose loop intervals are short enough for a test to
// wait a few ticks in real time without dragging out the suite, while
// leaving the *decisions* those loops make driven by the injected testClock.
func fastConfig() Config {
	cfg := DefaultConfig()
	cfg.HeartbeatSweep = 10 * time.Millisecond
	cfg.MitosisInterval = 10 * time.Millisecond
	return cfg
}

func TestSubmitJobPullReportJobStatus(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()

	ctx := context.Background()

	submitResp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"start": "0", "end": "4", "shards": "4"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if submitResp.GetJobId() == "" {
		t.Fatalf("SubmitJob returned empty job id")
	}
	jobID := submitResp.GetJobId()

	joinResp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-1", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent: %v", err)
	}
	if !joinResp.GetAccepted() {
		t.Fatalf("JoinAgent rejected: %s", joinResp.GetReason())
	}

	// keyspace-search's first-hit merge needs a hit; the task covering the
	// key that hits is whichever shard the search range [0,4) with shards=4
	// puts key 2 in — with KeyspaceDecompose's even split, that's shard 2.
	var hitTaskID string
	var pulled []*transport.Task
	for {
		pullResp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-1"})
		if err != nil {
			t.Fatalf("PullTask: %v", err)
		}
		if !pullResp.GetHasTask() {
			break
		}
		pulled = append(pulled, pullResp.GetTask())
	}
	if len(pulled) != 4 {
		t.Fatalf("got %d tasks, want 4", len(pulled))
	}

	for _, task := range pulled {
		start, _ := decodeRange(task.GetInput())
		ok := start == 2 // the task covering key 2 is the "hit"
		if ok {
			hitTaskID = task.GetId()
		}
		reportResp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: task.GetId(),
			Output: task.GetInput(),
			Ok:     ok,
		})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", task.GetId(), err)
		}
		if !reportResp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", task.GetId())
		}
	}
	if hitTaskID == "" {
		t.Fatalf("no task covered the hit key")
	}

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true")
	}
}

// decodeRange decodes a keyspace task's Input (two big-endian uint64s) to
// its start bound, mirroring internal/core/templates' private wire layout.
func decodeRange(b []byte) (start, end uint64) {
	if len(b) != 16 {
		return 0, 0
	}
	return uint64(binary.BigEndian.Uint64(b[0:8])), uint64(binary.BigEndian.Uint64(b[8:16]))
}

func TestJoinAgentRejectsEmptyIdentity(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()

	resp, err := client.JoinAgent(context.Background(), &transport.JoinAgentRequest{Agent: "", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent: %v", err)
	}
	if resp.GetAccepted() {
		t.Fatalf("JoinAgent(empty agent) accepted, want rejected")
	}
	if resp.GetReason() == "" {
		t.Fatalf("JoinAgent(empty agent) rejected with no reason")
	}
}

func TestJoinAgentOverflowsIntoNewCell(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 1
	client, _, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()

	first, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-1", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent(agent-1): %v", err)
	}
	if !first.GetAccepted() {
		t.Fatalf("JoinAgent(agent-1) rejected: %s", first.GetReason())
	}

	// The first cell (capacity 1) is now full; a second agent overflowing it
	// must be admitted into a new cell rather than reusing the first.
	second, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-2", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent(agent-2): %v", err)
	}
	if !second.GetAccepted() {
		t.Fatalf("JoinAgent(agent-2) rejected: %s", second.GetReason())
	}
	if second.GetCellId() == first.GetCellId() {
		t.Fatalf("JoinAgent(agent-2) landed on the full cell %s, want a new cell", first.GetCellId())
	}

	psResp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if psResp.GetCells() != 2 {
		t.Fatalf("Ps: Cells = %d, want 2", psResp.GetCells())
	}
	if psResp.GetMachines() != 2 {
		t.Fatalf("Ps: Machines = %d, want 2", psResp.GetMachines())
	}
}

func TestHeartbeatTimeoutEvictsAgent(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.HeartbeatTimeout = 100 * time.Millisecond
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	joinResp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-1", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent: %v", err)
	}
	if !joinResp.GetAccepted() {
		t.Fatalf("JoinAgent rejected: %s", joinResp.GetReason())
	}

	psResp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if psResp.GetMachines() != 1 {
		t.Fatalf("Ps before timeout: Machines = %d, want 1", psResp.GetMachines())
	}

	// Push the clock past the heartbeat timeout and give the reaper loop's
	// wall-clock ticker (fastConfig's 10ms sweep) a few ticks to observe it.
	clock.advance(200 * time.Millisecond)
	waitFor(t, func() bool {
		psResp, err := client.Ps(ctx, &transport.PsRequest{})
		if err != nil {
			t.Fatalf("Ps: %v", err)
		}
		return psResp.GetMachines() == 0
	})

	srv.mu.Lock()
	_, stillTracked := srv.lastSeen["agent-1"]
	srv.mu.Unlock()
	if stillTracked {
		t.Fatalf("agent-1 still tracked in lastSeen after eviction")
	}
}

func TestMitosisSplitsOversizedCell(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 10
	cfg.MitosisThresholds = mitosis.Thresholds{Target: 2, CooldownNS: 0}
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()

	ctx := context.Background()
	joinResp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-1", Region: "us", Caps: 1})
	if err != nil {
		t.Fatalf("JoinAgent(agent-1): %v", err)
	}
	if !joinResp.GetAccepted() {
		t.Fatalf("JoinAgent(agent-1) rejected: %s", joinResp.GetReason())
	}
	original := joinResp.GetCellId()

	for _, agent := range []string{"agent-2", "agent-3", "agent-4", "agent-5"} {
		resp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: agent, Region: "us", Caps: 1})
		if err != nil {
			t.Fatalf("JoinAgent(%s): %v", agent, err)
		}
		if !resp.GetAccepted() || resp.GetCellId() != original {
			t.Fatalf("JoinAgent(%s) = %+v, want accepted into cell %s", agent, resp, original)
		}
	}
	// The cell now holds 5 agents against a Target of 2: Size(5) > 2*Target(4)
	// triggers mitosis.Decide's Split rule.

	waitFor(t, func() bool {
		psResp, err := client.Ps(ctx, &transport.PsRequest{})
		if err != nil {
			t.Fatalf("Ps: %v", err)
		}
		return psResp.GetCells() == 2
	})

	psResp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if psResp.GetMachines() != 5 {
		t.Fatalf("Ps after split: Machines = %d, want 5 (no agent lost across the split)", psResp.GetMachines())
	}

	srv.mu.Lock()
	_, originalStillPresent := srv.cellAgents[model.CellID(original)]
	srv.mu.Unlock()
	if originalStillPresent {
		t.Fatalf("original cell %s still tracked after split", original)
	}
}

// waitFor polls cond every 5ms for up to 2s (real wall-clock time, bounding
// how long a background loop's wall-clock ticker takes to fire and observe
// the injected testClock's already-advanced value) and fails the test if
// cond never becomes true.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within deadline")
}
