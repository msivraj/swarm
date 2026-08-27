package controlplane

import (
	"context"
	"encoding/binary"
	"math"
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
