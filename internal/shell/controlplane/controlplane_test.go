package controlplane

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"strconv"
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

// TestReportResultDuplicateDoesNotTriggerPrematureAggregation reports one
// task's result twice (a stale first report, then an overwriting second
// report) while the job's other tasks are still outstanding, and asserts
// aggregation only fires once every DISTINCT task has reported — with the
// merge fed exactly one, non-doubled result per task (the second, most
// recent report for the duplicated task, not both).
func TestReportResultDuplicateDoesNotTriggerPrematureAggregation(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()

	ctx := context.Background()

	// trials=4, blockSize=1 decomposes into 4 single-trial monte-carlo
	// tasks; MonteCarloMerge sums each task's block into the aggregate, so
	// a duplicate that isn't deduped would either double-count (wrong sum
	// and count) or let a stale value win, both of which this test catches.
	submitResp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "monte-carlo",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params:   map[string]string{"trials": "4", "blockSize": "1", "seed": "1"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := submitResp.GetJobId()

	joinResp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-1", Region: "us", Caps: 4})
	if err != nil {
		t.Fatalf("JoinAgent: %v", err)
	}
	if !joinResp.GetAccepted() {
		t.Fatalf("JoinAgent rejected: %s", joinResp.GetReason())
	}

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

	report := func(taskID string, count int64, sum, sumSq float64) {
		t.Helper()
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: taskID,
			Output: encodeMCResult(count, sum, sumSq),
			Ok:     true,
		})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", taskID, err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", taskID)
		}
	}
	jobDone := func() bool {
		t.Helper()
		statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
		if err != nil {
			t.Fatalf("JobStatus: %v", err)
		}
		return statusResp.GetDone()
	}

	// A stale first report for task 0, then an overwriting duplicate. Only
	// task 0 has reported (once, logically) so far — 1 of 4 distinct tasks.
	report(pulled[0].GetId(), 1, 100, 10_000)
	if jobDone() {
		t.Fatalf("job done after 1 of 4 distinct tasks reported")
	}
	report(pulled[0].GetId(), 1, 500, 250_000)
	if jobDone() {
		t.Fatalf("job done after a duplicate report of the same task; only 1 of 4 distinct tasks has reported")
	}

	report(pulled[1].GetId(), 1, 20, 400)
	if jobDone() {
		t.Fatalf("job done after 2 of 4 distinct tasks reported")
	}
	report(pulled[2].GetId(), 1, 30, 900)
	if jobDone() {
		t.Fatalf("job done after 3 of 4 distinct tasks reported")
	}

	// The 4th and final distinct task reports; aggregation must now fire,
	// over exactly one result per distinct task.
	report(pulled[3].GetId(), 1, 40, 1_600)

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true after all 4 distinct tasks reported")
	}

	wantCount := int64(4)           // one block per distinct task, not 5
	wantSum := 500.0 + 20 + 30 + 40 // task 0's overwriting report counts, not its stale first report
	gotCount, gotSum, _, _ := decodeMCAggregate(statusResp.GetAggregate())
	if gotCount != wantCount {
		t.Fatalf("aggregate Count = %d, want %d (a duplicate report must not double-count)", gotCount, wantCount)
	}
	if gotSum != wantSum {
		t.Fatalf("aggregate Sum = %v, want %v", gotSum, wantSum)
	}
}

// TestDedupeTaskResults is a table-driven test of dedupeTaskResults: it must
// collapse a job's raw, possibly-duplicated result list to exactly one entry
// per distinct TaskID, keeping each TaskID's first-arrival position but the
// value of its most recent (last in the slice) report.
func TestDedupeTaskResults(t *testing.T) {
	tests := []struct {
		name string
		in   []model.TaskResult
		want []model.TaskResult
	}{
		{
			name: "empty",
			in:   nil,
			want: []model.TaskResult{},
		},
		{
			name: "no duplicates preserves order",
			in: []model.TaskResult{
				{TaskID: "t1", Output: []byte("a"), OK: true},
				{TaskID: "t2", Output: []byte("b"), OK: true},
				{TaskID: "t3", Output: []byte("c"), OK: false},
			},
			want: []model.TaskResult{
				{TaskID: "t1", Output: []byte("a"), OK: true},
				{TaskID: "t2", Output: []byte("b"), OK: true},
				{TaskID: "t3", Output: []byte("c"), OK: false},
			},
		},
		{
			name: "duplicate overwrites in place with the later value",
			in: []model.TaskResult{
				{TaskID: "t1", Output: []byte("stale"), OK: true},
				{TaskID: "t2", Output: []byte("b"), OK: true},
				{TaskID: "t1", Output: []byte("fresh"), OK: true},
			},
			want: []model.TaskResult{
				{TaskID: "t1", Output: []byte("fresh"), OK: true},
				{TaskID: "t2", Output: []byte("b"), OK: true},
			},
		},
		{
			name: "every result duplicated collapses to one",
			in: []model.TaskResult{
				{TaskID: "t1", Output: []byte("1st"), OK: false},
				{TaskID: "t1", Output: []byte("2nd"), OK: true},
				{TaskID: "t1", Output: []byte("3rd"), OK: true},
			},
			want: []model.TaskResult{
				{TaskID: "t1", Output: []byte("3rd"), OK: true},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := dedupeTaskResults(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("dedupeTaskResults(%v) = %v, want %v", tc.in, got, tc.want)
			}
			for i := range got {
				if got[i].TaskID != tc.want[i].TaskID || string(got[i].Output) != string(tc.want[i].Output) || got[i].OK != tc.want[i].OK {
					t.Fatalf("dedupeTaskResults(%v)[%d] = %+v, want %+v", tc.in, i, got[i], tc.want[i])
				}
			}
		})
	}
}

// encodeMCResult encodes a monte-carlo block result the way an agent's
// runner loop would, mirroring internal/core/templates' private mcResult
// wire layout (Count, Sum, SumSq as big-endian values).
func encodeMCResult(count int64, sum, sumSq float64) []byte {
	out := make([]byte, 24)
	binary.BigEndian.PutUint64(out[0:8], uint64(count))
	binary.BigEndian.PutUint64(out[8:16], math.Float64bits(sum))
	binary.BigEndian.PutUint64(out[16:24], math.Float64bits(sumSq))
	return out
}

// decodeMCAggregate decodes a monte-carlo job's Aggregate.Value, mirroring
// internal/core/templates' private mcAggregate wire layout (Count, Sum,
// Mean, Variance as big-endian values).
func decodeMCAggregate(b []byte) (count int64, sum, mean, variance float64) {
	if len(b) != 32 {
		return 0, 0, 0, 0
	}
	count = int64(binary.BigEndian.Uint64(b[0:8]))
	sum = math.Float64frombits(binary.BigEndian.Uint64(b[8:16]))
	mean = math.Float64frombits(binary.BigEndian.Uint64(b[16:24]))
	variance = math.Float64frombits(binary.BigEndian.Uint64(b[24:32]))
	return count, sum, mean, variance
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

// twoCellConfig returns a fastConfig with a DefaultCellCapacity large enough
// (2) that a solo agent joining a brand-new cell leaves that cell with
// spare Free capacity for placement.Place to assign tasks to, rather than
// immediately consuming the whole cell the way a capacity-1 cell would.
func twoCellConfig() Config {
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 2
	return cfg
}

// joinAgent is a small SubmitJob/JoinAgent test helper: it joins agent with
// the given Caps and fails the test if the join is rejected.
func joinAgent(t *testing.T, ctx context.Context, client transport.ControlPlaneClient, agent string, caps int32) *transport.JoinAgentResponse {
	t.Helper()
	resp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: agent, Region: "us", Caps: caps})
	if err != nil {
		t.Fatalf("JoinAgent(%s): %v", agent, err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("JoinAgent(%s) rejected: %s", agent, resp.GetReason())
	}
	return resp
}

// submitMonteCarlo submits a monte-carlo job decomposing into exactly
// `blocks` independent tasks (Trials == blocks, BlockSize == 1) and returns
// its job id.
func submitMonteCarlo(t *testing.T, ctx context.Context, client transport.ControlPlaneClient, blocks int) string {
	t.Helper()
	resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "monte-carlo",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{
			"trials":    strconv.Itoa(blocks),
			"blockSize": "1",
			"seed":      "7",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	return resp.GetJobId()
}

// drainAllTasks repeatedly calls PullTask(agent) until HasTask is false and
// returns every task pulled, in pull order.
func drainAllTasks(t *testing.T, ctx context.Context, client transport.ControlPlaneClient, agent string) []*transport.Task {
	t.Helper()
	var out []*transport.Task
	for {
		resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: agent})
		if err != nil {
			t.Fatalf("PullTask(%s): %v", agent, err)
		}
		if !resp.GetHasTask() {
			return out
		}
		out = append(out, resp.GetTask())
	}
}

// TestPerCellQueuesIsolateTasksByCell is the ticket's core acceptance
// criterion: two cells A and B, each with a joined agent, share one job's
// tasks between them (placement.Place spreads a batch across cells as each
// cell's local capacity is consumed — see Server.drainPendingLocked), and
// each agent's PullTask must only ever surface the tasks placed on its own
// cell, never the other cell's.
func TestPerCellQueuesIsolateTasksByCell(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, twoCellConfig(), clock)
	defer teardown()
	ctx := context.Background()

	// agent-a (Caps 1) becomes the sole member of a fresh cell with spare
	// Free (capacity 2, size 1). agent-b requests Caps 2, which that cell's
	// remaining Free (1) cannot satisfy, so rendezvous routes it to a second,
	// brand-new cell instead — two distinct cells, each with one agent and
	// spare capacity for a task.
	joinA := joinAgent(t, ctx, client, "agent-a", 1)
	joinB := joinAgent(t, ctx, client, "agent-b", 2)
	if joinA.GetCellId() == joinB.GetCellId() {
		t.Fatalf("agent-a and agent-b landed on the same cell %s, want two distinct cells", joinA.GetCellId())
	}

	// Two independent tasks: with cell A first in sorted CellID order and
	// both cells starting at Free==1, the first task lands on A (consuming
	// its local Free) and the second spills to B.
	jobID := submitMonteCarlo(t, ctx, client, 2)

	pulledA := drainAllTasks(t, ctx, client, "agent-a")
	pulledB := drainAllTasks(t, ctx, client, "agent-b")

	if len(pulledA) != 1 {
		t.Fatalf("agent-a pulled %d tasks, want exactly 1", len(pulledA))
	}
	if len(pulledB) != 1 {
		t.Fatalf("agent-b pulled %d tasks, want exactly 1", len(pulledB))
	}
	if pulledA[0].GetId() == pulledB[0].GetId() {
		t.Fatalf("agent-a and agent-b both pulled task %s", pulledA[0].GetId())
	}
	for _, task := range append(append([]*transport.Task{}, pulledA...), pulledB...) {
		if task.GetJobId() != jobID {
			t.Fatalf("pulled task %s has JobId %s, want %s", task.GetId(), task.GetJobId(), jobID)
		}
	}

	// Neither agent has anything left to pull, and re-pulling never crosses
	// into the other cell's (already-drained) queue.
	if resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-a"}); err != nil {
		t.Fatalf("PullTask(agent-a): %v", err)
	} else if resp.GetHasTask() {
		t.Fatalf("agent-a pulled an extra task %s after draining its cell", resp.GetTask().GetId())
	}
	if resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-b"}); err != nil {
		t.Fatalf("PullTask(agent-b): %v", err)
	} else if resp.GetHasTask() {
		t.Fatalf("agent-b pulled an extra task %s after draining its cell", resp.GetTask().GetId())
	}

	srv.mu.Lock()
	cellA, cellB := srv.agentCell["agent-a"], srv.agentCell["agent-b"]
	srv.mu.Unlock()
	if cellA == cellB {
		t.Fatalf("server bookkeeping shows agent-a and agent-b on the same cell %s", cellA)
	}
}

// TestPlacementIsDeterministicAcrossIdenticalRuns asserts the ticket's
// determinism criterion: an identical submit sequence against an identical
// registry snapshot places the same (decoded) task payloads on the agents
// belonging to the same (relatively-ordered) cells across independent runs.
func TestPlacementIsDeterministicAcrossIdenticalRuns(t *testing.T) {
	run := func() (agentAInput, agentBInput []byte) {
		clock := &testClock{}
		client, _, teardown := newTestServer(t, twoCellConfig(), clock)
		defer teardown()
		ctx := context.Background()

		joinAgent(t, ctx, client, "agent-a", 1)
		joinAgent(t, ctx, client, "agent-b", 2)
		submitMonteCarlo(t, ctx, client, 2)

		pulledA := drainAllTasks(t, ctx, client, "agent-a")
		pulledB := drainAllTasks(t, ctx, client, "agent-b")
		if len(pulledA) != 1 || len(pulledB) != 1 {
			t.Fatalf("run: pulled %d tasks for agent-a, %d for agent-b, want 1 each", len(pulledA), len(pulledB))
		}
		return pulledA[0].GetInput(), pulledB[0].GetInput()
	}

	a1, b1 := run()
	a2, b2 := run()

	if string(a1) != string(a2) {
		t.Fatalf("agent-a's task Input differs across identical runs: %x vs %x", a1, a2)
	}
	if string(b1) != string(b2) {
		t.Fatalf("agent-b's task Input differs across identical runs: %x vs %x", b1, b2)
	}
}

// TestRegionFullHoldsPendingTasksUntilCapacityAppears covers the ticket's
// "region-full holds, does not lose" criterion: with every existing cell at
// Free==0, newly submitted tasks are held pending — no agent can pull them —
// and once a cell with spare capacity appears (a new agent join forming a
// cell whose capacity exceeds its founding member), the drain that JoinAgent
// triggers places the held tasks so they become pullable, with no task lost
// and none delivered twice.
//
// registry.Snapshot's Free is agent headroom (capacity minus member count),
// unaffected by task placement (store.EnqueueTask never touches the
// registry) — so a cell keeps offering room to newly-pending tasks in every
// later drain until a real agent actually fills that headroom. The test
// exploits that directly: agent-c tops off agent-b's cell for real (driving
// its registry Free to 0) before agent-d forms the next new cell, which is
// what makes the second pending task deterministically land on agent-d's
// cell rather than agent-b's.
func TestRegionFullHoldsPendingTasksUntilCapacityAppears(t *testing.T) {
	clock := &testClock{}
	cfg := twoCellConfig()
	client, _, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	// Fill the one cell exactly to capacity (two Caps-1 agents against a
	// capacity-2 cell), leaving Free==0 fleet-wide.
	joinAgent(t, ctx, client, "agent-a1", 1)
	joinAgent(t, ctx, client, "agent-a2", 1)

	jobID := submitMonteCarlo(t, ctx, client, 2)

	// Region full: neither existing agent can pull anything, and the tasks
	// are not dropped from the submit (SubmitJob itself succeeded).
	if resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-a1"}); err != nil {
		t.Fatalf("PullTask(agent-a1): %v", err)
	} else if resp.GetHasTask() {
		t.Fatalf("agent-a1 pulled a task while the region is full")
	}
	if resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-a2"}); err != nil {
		t.Fatalf("PullTask(agent-a2): %v", err)
	} else if resp.GetHasTask() {
		t.Fatalf("agent-a2 pulled a task while the region is full")
	}

	// A new agent forms a brand-new (spare-capacity) cell: capacity appears,
	// and joining drains one of the two pending tasks into it.
	joinAgent(t, ctx, client, "agent-b", 1)
	firstBatch := drainAllTasks(t, ctx, client, "agent-b")
	if len(firstBatch) != 1 {
		t.Fatalf("agent-b pulled %d tasks after its cell gained capacity, want exactly 1", len(firstBatch))
	}

	// The other pending task is still held — not lost, not yet placed
	// anywhere — until more capacity appears.
	if resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-a1"}); err != nil {
		t.Fatalf("PullTask(agent-a1): %v", err)
	} else if resp.GetHasTask() {
		t.Fatalf("agent-a1 pulled a task that was never placed on its cell")
	}

	// agent-c fills agent-b's cell to its real capacity (Free 0), so it can
	// no longer be picked for the still-pending task.
	joinAgent(t, ctx, client, "agent-c", 1)
	if resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-b"}); err != nil {
		t.Fatalf("PullTask(agent-b): %v", err)
	} else if resp.GetHasTask() {
		t.Fatalf("agent-b pulled a second task after its cell's Free hit 0, want none placed there")
	}

	// One more brand-new cell appears; the last pending task drains into it.
	joinAgent(t, ctx, client, "agent-d", 1)
	secondBatch := drainAllTasks(t, ctx, client, "agent-d")
	if len(secondBatch) != 1 {
		t.Fatalf("agent-d pulled %d tasks after its cell gained capacity, want exactly 1", len(secondBatch))
	}

	// No task lost, none delivered twice: exactly the two admitted tasks
	// were pulled in total, and their ids are distinct.
	all := append(append([]*transport.Task{}, firstBatch...), secondBatch...)
	if len(all) != 2 {
		t.Fatalf("got %d tasks total across both drains, want 2", len(all))
	}
	if all[0].GetId() == all[1].GetId() {
		t.Fatalf("the same task %s was delivered to two different agents", all[0].GetId())
	}
	for _, task := range all {
		if task.GetJobId() != jobID {
			t.Fatalf("pulled task %s has JobId %s, want %s", task.GetId(), task.GetJobId(), jobID)
		}
	}
}

// TestAggregationCompletesOverPerCellQueues covers the ticket's "aggregation
// still completes" criterion: a full submit -> pull -> report -> aggregate
// cycle, with the job's tasks spread across two cells' independent queues,
// still produces a correct Aggregate — per-cell queueing is a routing
// change, not a behavior change to job completion.
func TestAggregationCompletesOverPerCellQueues(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, twoCellConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-a", 1)
	joinAgent(t, ctx, client, "agent-b", 2)

	jobID := submitMonteCarlo(t, ctx, client, 2)

	pulledA := drainAllTasks(t, ctx, client, "agent-a")
	pulledB := drainAllTasks(t, ctx, client, "agent-b")
	if len(pulledA) != 1 || len(pulledB) != 1 {
		t.Fatalf("pulled %d tasks for agent-a, %d for agent-b, want 1 each", len(pulledA), len(pulledB))
	}

	report := func(taskID string, count int64, sum, sumSq float64) {
		t.Helper()
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: taskID,
			Output: encodeMCResult(count, sum, sumSq),
			Ok:     true,
		})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", taskID, err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", taskID)
		}
	}
	report(pulledA[0].GetId(), 1, 10, 100)
	report(pulledB[0].GetId(), 1, 20, 400)

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true after both cells' tasks reported")
	}
	gotCount, gotSum, _, _ := decodeMCAggregate(statusResp.GetAggregate())
	if gotCount != 2 {
		t.Fatalf("aggregate Count = %d, want 2", gotCount)
	}
	if gotSum != 30 {
		t.Fatalf("aggregate Sum = %v, want 30", gotSum)
	}
}

// TestMitosisMergeMigratesRetiredCellsQueuedTasks is a regression test for a
// real task-loss bug the auditor caught: executeSplit/executeMerge move
// agent membership and CellDown the retired cell(s), but never migrated
// that cell's per-cell store queue. A task already EnqueueTask'd on a cell
// that a merge (or split) retires had no agent mapped to it anymore (so
// PullTask could never reach it) and was never added to s.pending (so no
// drain would ever re-place it) — the owning job hung forever.
//
// This mirrors the auditor's exact repro: two agents, each the sole member
// of its own cell, each cell holding one queued (not yet pulled) task, and
// a mitosis merge under the default Target:4 threshold (both cells' size 1
// is under Target, and their combined size 2 stays under Target too, so
// mitosis.Decide's merge rule fires deterministically for this shape). It
// asserts both tasks survive the merge, are still deliverable, and the job
// still reaches Aggregate completion.
func TestMitosisMergeMigratesRetiredCellsQueuedTasks(t *testing.T) {
	clock := &testClock{}
	cfg := twoCellConfig()
	// Keep the background mitosis ticker from firing mid-test (it would
	// otherwise race the test's own direct mitosisOnce call below); the
	// merge decision itself still comes from the same core, cooldowns, and
	// clock the background loop would use, just invoked deterministically
	// instead of waited for over wall-clock time.
	cfg.MitosisInterval = time.Hour
	cfg.MitosisThresholds = mitosis.Thresholds{Target: 4, CooldownNS: 0}
	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	// Two agents, two distinct cells (same shape as
	// TestPerCellQueuesIsolateTasksByCell): each cell's size (1) is under
	// Target (4), and their combined size (2) stays under Target too, so
	// mitosis.Decide will merge them.
	joinA := joinAgent(t, ctx, client, "agent-a", 1)
	joinB := joinAgent(t, ctx, client, "agent-b", 2)
	if joinA.GetCellId() == joinB.GetCellId() {
		t.Fatalf("agent-a and agent-b landed on the same cell %s, want two distinct cells", joinA.GetCellId())
	}

	// One task lands on each cell — queued, not yet pulled by either agent —
	// exactly the state a retiring cell's queue must not lose.
	jobID := submitMonteCarlo(t, ctx, client, 2)

	srv.mitosisOnce()

	// The merge retired both original cells into one; confirm via Ps that no
	// agent was lost across the reshape (mirrors TestMitosisSplitsOversizedCell).
	psResp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if psResp.GetCells() != 1 {
		t.Fatalf("Ps after merge: Cells = %d, want 1 (agent-a's and agent-b's cells should have merged)", psResp.GetCells())
	}
	if psResp.GetMachines() != 2 {
		t.Fatalf("Ps after merge: Machines = %d, want 2 (no agent lost across the merge)", psResp.GetMachines())
	}

	// The bug: both cells' queued tasks must survive the merge and still be
	// deliverable — the merge's own end-of-tick drain (mitosisOnce ->
	// drainPendingLocked) must have re-placed them onto the merged cell.
	// Both agents now share that one cell, so either PullTask call may
	// surface either task; what must hold is that both are still there and
	// neither was dropped or delivered twice.
	var pulled []*transport.Task
	pulled = append(pulled, drainAllTasks(t, ctx, client, "agent-a")...)
	pulled = append(pulled, drainAllTasks(t, ctx, client, "agent-b")...)
	if len(pulled) != 2 {
		t.Fatalf("pulled %d tasks after the merge, want 2 — a merge must not orphan a retired cell's queued tasks", len(pulled))
	}
	if pulled[0].GetId() == pulled[1].GetId() {
		t.Fatalf("the same task %s was delivered twice after the merge", pulled[0].GetId())
	}
	for _, task := range pulled {
		if task.GetJobId() != jobID {
			t.Fatalf("pulled task %s has JobId %s, want %s", task.GetId(), task.GetJobId(), jobID)
		}
	}

	// The job must still reach completion — proving the merge does not hang
	// it forever, which is exactly what the orphaned-queue bug did.
	report := func(taskID string, count int64, sum, sumSq float64) {
		t.Helper()
		resp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: taskID,
			Output: encodeMCResult(count, sum, sumSq),
			Ok:     true,
		})
		if err != nil {
			t.Fatalf("ReportResult(%s): %v", taskID, err)
		}
		if !resp.GetAccepted() {
			t.Fatalf("ReportResult(%s) not accepted", taskID)
		}
	}
	report(pulled[0].GetId(), 1, 10, 100)
	report(pulled[1].GetId(), 1, 20, 400)

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus: Done = false, want true — the job must complete despite the mitosis merge")
	}
	gotCount, gotSum, _, _ := decodeMCAggregate(statusResp.GetAggregate())
	if gotCount != 2 {
		t.Fatalf("aggregate Count = %d, want 2", gotCount)
	}
	if gotSum != 30 {
		t.Fatalf("aggregate Sum = %v, want 30", gotSum)
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
