package e2e

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/mitosis"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/agent"
	"github.com/msivraj/swarm/internal/shell/controlplane"
	"github.com/msivraj/swarm/internal/shell/store"
	"github.com/msivraj/swarm/internal/shell/transport"
)

const bufSize = 1 << 20

// testClock is a controllable clock for the control plane's background
// loops (the reaper and the mitosis ticker): the loops' tickers still fire
// on wall-clock time, but the *decisions* they drive (evict, split, merge)
// are computed from now(), which the test controls, matching the pattern
// internal/shell/controlplane's own tests use.
type testClock struct {
	ns atomic.Int64
}

func (c *testClock) now() model.Instant { return model.Instant(c.ns.Load()) }

// fastConfig is a controlplane.Config whose background-loop intervals are
// short enough for a test to observe a few ticks without dragging out the
// suite.
func fastConfig() controlplane.Config {
	cfg := controlplane.DefaultConfig()
	cfg.HeartbeatSweep = 10 * time.Millisecond
	cfg.MitosisInterval = 10 * time.Millisecond
	return cfg
}

// buildWorker compiles the worker main package at the relative import path
// pkg (e.g. "./workers/keyspace") into a temp directory and returns the
// resulting binary's path. Building and exec'ing a real process — rather
// than faking exec.Cmd — is what proves internal/shell/agent's native
// process driver (Task.Input on stdin, TaskResult.Output from stdout, exit
// code as ok/not-ok) works end to end, not just against a fake.
func buildWorker(t *testing.T, pkg, name string) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", out, pkg)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, output)
	}
	return out
}

// newControlPlane starts a controlplane.Server over an in-process bufconn
// listener. It returns a transport client for the test's own RPCs (SubmitJob,
// JobStatus, Ps, ...), an agent.Dialer that reaches the same server (for
// agents to dial in), and a teardown func.
func newControlPlane(t *testing.T, cfg controlplane.Config, clock *testClock) (transport.ControlPlaneClient, agent.Dialer, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := controlplane.New(store.NewMemStore(), cfg, clock.now)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	dial := func(ctx context.Context, _ string) (transport.ControlPlaneClient, io.Closer, error) {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return transport.NewControlPlaneClient(conn), conn, nil
	}

	testClient, testConn, err := dial(context.Background(), "bufnet")
	if err != nil {
		t.Fatalf("dial control plane: %v", err)
	}

	teardown := func() {
		_ = testConn.Close()
		srv.Stop()
		if err := <-serveErr; err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Serve: %v", err)
		}
	}
	return testClient, dial, teardown
}

// startAgent constructs and runs an Agent against dial, exec'ing argv (if
// any) for every task it pulls. It registers a t.Cleanup that cancels the
// agent and waits for its Run loop to exit.
func startAgent(t *testing.T, id string, dial agent.Dialer, argv []string) {
	t.Helper()

	a := agent.New(agent.Config{
		AgentID:           id,
		Region:            "us",
		Caps:              1,
		Targets:           []string{"bufnet"},
		Dialer:            dial,
		Jitter:            func() float64 { return 0 },
		HeartbeatInterval: 20 * time.Millisecond,
		PullInterval:      10 * time.Millisecond,
		Process:           agent.ProcessSpec{Argv: argv},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("agent %s Run: %v", id, err)
		}
	})
}

// waitFor polls cond every 5ms until it returns true or timeout elapses,
// failing the test in the latter case.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// waitForJobDone polls JobStatus until Done, returning the final response.
func waitForJobDone(t *testing.T, client transport.ControlPlaneClient, jobID string) *transport.JobStatusResponse {
	t.Helper()

	var status *transport.JobStatusResponse
	waitFor(t, 20*time.Second, func() bool {
		resp, err := client.JobStatus(context.Background(), &transport.JobStatusRequest{JobId: jobID})
		if err != nil {
			t.Fatalf("JobStatus: %v", err)
		}
		status = resp
		return status.GetDone()
	})
	return status
}

// TestKeyspaceSearch brings up a control plane and three agents exec'ing
// the keyspace worker, submits a keyspace-search job over the real gRPC
// transport, and asserts the job's Aggregate reports the exact target key —
// the P0 "first-hit" template merge exercised through the whole pipeline:
// SubmitJob -> admission.Admit -> templates.KeyspaceDecompose -> agents
// pulling/exec'ing/reporting -> templates.KeyspaceMerge -> JobStatus.
func TestKeyspaceSearch(t *testing.T) {
	worker := buildWorker(t, "./workers/keyspace", "keyspace")

	const targetKey = 424242
	t.Setenv("SWARM_E2E_TARGET_KEY", strconv.FormatUint(targetKey, 10))

	clock := &testClock{}
	client, dial, teardown := newControlPlane(t, fastConfig(), clock)
	defer teardown()

	for i := 0; i < 3; i++ {
		startAgent(t, fmt.Sprintf("ks-agent-%d", i), dial, []string{worker})
	}

	ctx := context.Background()
	submitResp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: admission.TemplateKeyspaceSearch,
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{
			"start":  "0",
			"end":    "1000000",
			"shards": "37",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := submitResp.GetJobId()
	if jobID == "" {
		t.Fatalf("SubmitJob returned empty job id")
	}

	status := waitForJobDone(t, client, jobID)

	key, ok := DecodeKeyspaceHit(status.GetAggregate())
	if !ok {
		t.Fatalf("Aggregate = %x, not a valid 8-byte keyspace hit", status.GetAggregate())
	}
	if key != targetKey {
		t.Fatalf("Aggregate key = %d, want %d", key, targetKey)
	}
}

// TestMonteCarlo brings up a control plane and three agents exec'ing the
// monte-carlo worker, submits a monte-carlo job over the real gRPC
// transport, and asserts the job's Aggregate matches an independently
// computed expected value: expectedMCAggregate calls the real
// templates.MonteCarloDecompose to get the same task blocks admission.Admit
// would produce, then sums internal/e2e.NextValue over each block itself —
// it never calls templates.MonteCarloMerge, so a bug in the merge's
// summation would fail this comparison even though decompose is shared.
func TestMonteCarlo(t *testing.T) {
	worker := buildWorker(t, "./workers/montecarlo", "montecarlo")

	clock := &testClock{}
	client, dial, teardown := newControlPlane(t, fastConfig(), clock)
	defer teardown()

	for i := 0; i < 3; i++ {
		startAgent(t, fmt.Sprintf("mc-agent-%d", i), dial, []string{worker})
	}

	const (
		trials    = int64(500)
		blockSize = int64(60)
		baseSeed  = int64(42)
	)

	ctx := context.Background()
	submitResp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: admission.TemplateMonteCarlo,
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{
			"trials":    strconv.FormatInt(trials, 10),
			"blockSize": strconv.FormatInt(blockSize, 10),
			"seed":      strconv.FormatInt(baseSeed, 10),
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := submitResp.GetJobId()
	if jobID == "" {
		t.Fatalf("SubmitJob returned empty job id")
	}

	status := waitForJobDone(t, client, jobID)

	got, ok := DecodeMCAggregate(status.GetAggregate())
	if !ok {
		t.Fatalf("Aggregate = %x, not a valid 32-byte monte-carlo aggregate", status.GetAggregate())
	}

	want := expectedMCAggregate(trials, blockSize, baseSeed)
	if got.Count != want.Count {
		t.Fatalf("Count = %d, want %d", got.Count, want.Count)
	}
	const epsilon = 1e-6
	if math.Abs(got.Sum-want.Sum) > epsilon {
		t.Fatalf("Sum = %v, want %v", got.Sum, want.Sum)
	}
	if math.Abs(got.Mean-want.Mean) > epsilon {
		t.Fatalf("Mean = %v, want %v", got.Mean, want.Mean)
	}
	if math.Abs(got.Variance-want.Variance) > epsilon {
		t.Fatalf("Variance = %v, want %v", got.Variance, want.Variance)
	}
}

// expectedMCAggregate independently computes the count/sum/mean/variance a
// monte-carlo job with these params should aggregate to. It calls the real
// templates.MonteCarloDecompose (so it splits into the exact same blocks
// admission.Admit will) but sums NextValue itself rather than calling
// templates.MonteCarloMerge, so this check does not just re-run the code
// under test on itself.
func expectedMCAggregate(trials, blockSize, baseSeed int64) MCAggregate {
	tasks := templates.MonteCarloDecompose(templates.MCJob{
		JobID:     "expected",
		Trials:    trials,
		BlockSize: blockSize,
		BaseSeed:  baseSeed,
	})

	var count int64
	var sum, sumSq float64
	for _, task := range tasks {
		seed, n, ok := DecodeMCTaskInput(task.Input)
		if !ok {
			continue
		}
		for i := int64(0); i < n; i++ {
			v := NextValue(seed, i)
			sum += v
			sumSq += v * v
			count++
		}
	}

	agg := MCAggregate{Count: count, Sum: sum}
	if count > 0 {
		agg.Mean = sum / float64(count)
		agg.Variance = sumSq/float64(count) - agg.Mean*agg.Mean
	}
	return agg
}

// TestCellSplitsUnderLoad joins enough agents into one cell to cross
// mitosis.Decide's split threshold (Size > 2*Target) and asserts the
// control plane's registry snapshot reflects a resulting split: two cells,
// with every agent still accounted for. Unlike the job tests, these agents
// need no worker process — mitosis reacts to registry membership, not task
// execution — so ProcessSpec is left zero.
func TestCellSplitsUnderLoad(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	cfg.DefaultCellCapacity = 10
	cfg.MitosisThresholds = mitosis.Thresholds{Target: 2, CooldownNS: 0}
	client, dial, teardown := newControlPlane(t, cfg, clock)
	defer teardown()

	// Target=2 splits a cell once its Size exceeds 2*Target=4; five agents
	// joining the same (initially empty) cell crosses that threshold.
	for i := 0; i < 5; i++ {
		startAgent(t, fmt.Sprintf("split-agent-%d", i), dial, nil)
	}

	ctx := context.Background()
	waitFor(t, 10*time.Second, func() bool {
		resp, err := client.Ps(ctx, &transport.PsRequest{})
		if err != nil {
			t.Fatalf("Ps: %v", err)
		}
		return resp.GetCells() == 2
	})

	resp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		t.Fatalf("Ps: %v", err)
	}
	if resp.GetCells() != 2 {
		t.Fatalf("Cells = %d, want 2", resp.GetCells())
	}
	if resp.GetMachines() != 5 {
		t.Fatalf("Machines = %d, want 5 (no agent lost across the split)", resp.GetMachines())
	}
}
