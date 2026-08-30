package controlplane

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// This file covers ticket #98 (coupled-gang cell activation)'s acceptance
// criteria: TestActivateCoupledCell asserts that admitting a Barrier
// dist-training gang onto a cell distributes one CellAssignment per member
// agent, each carrying the shard templates.DistTrainingDecompose computes
// for it, the full raft peer set (from the registry's cellAgents), exactly
// one bootstrap agent, and k/min_members/steps taken straight from the
// job's Params. TestCoupledCompletion asserts the completion half (D6):
// feeding ReportResult a coupled cell's final combined gradient, keyed by
// the job id, writes the Aggregate and flips JobStatus.Done.

// joinCoupledAgent joins agent with caps and the P2 raft/cell-leader
// addresses activateCoupledCellLocked reads back out of s.agentAddrs — a
// local helper (rather than extending the package's shared joinAgent) so
// every other test's JoinAgentRequest stays exactly as it was before this
// ticket.
func joinCoupledAgent(t *testing.T, ctx context.Context, client transport.ControlPlaneClient, agent string, caps int32) *transport.JoinAgentResponse {
	t.Helper()
	resp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{
		Agent:          agent,
		Region:         "us",
		Caps:           caps,
		RaftAddr:       agent + "-raft:7000",
		CellLeaderAddr: agent + "-leader:7001",
	})
	if err != nil {
		t.Fatalf("JoinAgent(%s): %v", agent, err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("JoinAgent(%s) rejected: %s", agent, resp.GetReason())
	}
	return resp
}

// TestActivateCoupledCell submits a Barrier dist-training gang onto a cell
// that already has exactly as many joined agents as the gang's shard count,
// then asserts every member's CellAssignment response is fully and
// correctly populated.
func TestActivateCoupledCell(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	// cell-1 forms with capacity 100 (agent-1 requests it), leaving Free
	// comfortably above the 3-member gang this test submits — see
	// cellactivation_test.go's file doc: AdmitGang's free-capacity check
	// (room for MORE agents to join) and the "real, already-joined agents"
	// cellAgents peer set this ticket reads are two different numbers, and
	// this setup keeps both satisfied at once.
	joinCoupledAgent(t, ctx, client, "agent-1", 100)
	joinCoupledAgent(t, ctx, client, "agent-2", 1)
	joinCoupledAgent(t, ctx, client, "agent-3", 1)

	resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "dist-training",
		Coupling: transport.Coupling_COUPLING_BARRIER,
		Params: map[string]string{
			"min_members": "3",
			"samples":     "30",
			"shards":      "3",
			"steps":       "5",
			"k":           "2",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := resp.GetJobId()
	if jobID == "" {
		t.Fatalf("SubmitJob returned empty job id")
	}

	srv.mu.Lock()
	if _, placed := srv.gangJobs[model.JobID(jobID)]; !placed {
		srv.mu.Unlock()
		t.Fatalf("gang job %s was not placed", jobID)
	}
	srv.mu.Unlock()

	wantTasks, err := templates.DistTrainingDecompose(templates.DistTrainingJob{
		JobID: model.JobID(jobID), Samples: 30, Shards: 3,
	})
	if err != nil {
		t.Fatalf("DistTrainingDecompose: %v", err)
	}

	agents := []string{"agent-1", "agent-2", "agent-3"}
	wantPeers := make([]*transport.CellPeer, len(agents))
	for i, a := range agents {
		wantPeers[i] = &transport.CellPeer{AgentId: a, RaftAddr: a + "-raft:7000", CellLeaderAddr: a + "-leader:7001"}
	}

	bootstraps := 0
	for i, agent := range agents {
		assignResp, err := client.CellAssignment(ctx, &transport.CellAssignmentRequest{Agent: agent})
		if err != nil {
			t.Fatalf("CellAssignment(%s): %v", agent, err)
		}
		if !assignResp.GetHasAssignment() {
			t.Fatalf("CellAssignment(%s).HasAssignment = false, want true", agent)
		}
		if assignResp.GetJobId() != jobID {
			t.Fatalf("CellAssignment(%s).JobId = %q, want %q", agent, assignResp.GetJobId(), jobID)
		}
		if got, want := assignResp.GetWorkerId(), string(wantTasks[i].ID); got != want {
			t.Fatalf("CellAssignment(%s).WorkerId = %q, want %q", agent, got, want)
		}
		if !reflect.DeepEqual(assignResp.GetShardInput(), wantTasks[i].Input) {
			t.Fatalf("CellAssignment(%s).ShardInput = %v, want %v", agent, assignResp.GetShardInput(), wantTasks[i].Input)
		}
		if assignResp.GetK() != 2 {
			t.Fatalf("CellAssignment(%s).K = %d, want 2", agent, assignResp.GetK())
		}
		if assignResp.GetMinMembers() != 3 {
			t.Fatalf("CellAssignment(%s).MinMembers = %d, want 3", agent, assignResp.GetMinMembers())
		}
		if assignResp.GetSteps() != 5 {
			t.Fatalf("CellAssignment(%s).Steps = %d, want 5", agent, assignResp.GetSteps())
		}
		if assignResp.GetBootstrap() {
			bootstraps++
			if agent != "agent-1" {
				t.Fatalf("bootstrap = %s, want agent-1 (lowest agent id)", agent)
			}
		}

		gotPeers := append([]*transport.CellPeer(nil), assignResp.GetPeers()...)
		sort.Slice(gotPeers, func(i, j int) bool { return gotPeers[i].GetAgentId() < gotPeers[j].GetAgentId() })
		if len(gotPeers) != len(wantPeers) {
			t.Fatalf("CellAssignment(%s).Peers = %d peers, want %d", agent, len(gotPeers), len(wantPeers))
		}
		for i := range gotPeers {
			if gotPeers[i].GetAgentId() != wantPeers[i].GetAgentId() ||
				gotPeers[i].GetRaftAddr() != wantPeers[i].GetRaftAddr() ||
				gotPeers[i].GetCellLeaderAddr() != wantPeers[i].GetCellLeaderAddr() {
				t.Fatalf("CellAssignment(%s).Peers[%d] = %+v, want %+v", agent, i, gotPeers[i], wantPeers[i])
			}
		}
	}
	if bootstraps != 1 {
		t.Fatalf("bootstrap count = %d, want exactly 1", bootstraps)
	}
}

// TestActivateCoupledCellNoAssignmentForUnknownAgent asserts an agent that
// was never activated (never joined, or a plain P0/P1 agent) polls
// CellAssignment and gets has_assignment=false — never an error.
func TestActivateCoupledCellNoAssignmentForUnknownAgent(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	resp, err := client.CellAssignment(ctx, &transport.CellAssignmentRequest{Agent: "never-joined"})
	if err != nil {
		t.Fatalf("CellAssignment: %v", err)
	}
	if resp.GetHasAssignment() {
		t.Fatalf("CellAssignment(never-joined).HasAssignment = true, want false")
	}
}

// TestCoupledCompletion is #98's D6 acceptance criterion: feeding the
// completion path (ReportResult, reused and keyed by the job id) a coupled
// cell's final combined gradient writes the Aggregate and flips
// JobStatus.Done.
func TestCoupledCompletion(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 1) // capacity for the gang below to actually Place, not just queue

	jobID := submitGangJob(t, ctx, client, 1) // Barrier, min_members=1: enough to land in s.gangJobs

	statusBefore, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if statusBefore.GetDone() {
		t.Fatalf("JobStatus.Done = true before completion, want false")
	}

	combined := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	reportResp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
		TaskId: jobID,
		Output: combined,
		Ok:     true,
	})
	if err != nil {
		t.Fatalf("ReportResult: %v", err)
	}
	if !reportResp.GetAccepted() {
		t.Fatalf("ReportResult not accepted")
	}

	statusAfter, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusAfter.GetDone() {
		t.Fatalf("JobStatus.Done = false after completion, want true")
	}
	if !reflect.DeepEqual(statusAfter.GetAggregate(), combined) {
		t.Fatalf("JobStatus.Aggregate = %v, want %v", statusAfter.GetAggregate(), combined)
	}
}

// TestActivateCoupledCellSkipsWithoutDistTrainingParams guards the
// no-regression requirement: a Barrier gang that never sets samples/shards
// (P0/P1's existing gang tests, gang_test.go) is admitted exactly as before
// and gets no CellAssignment distributed to anyone — activateCoupledCellLocked
// is a no-op, not an error, so SubmitJob still succeeds.
func TestActivateCoupledCellSkipsWithoutDistTrainingParams(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinCoupledAgent(t, ctx, client, "agent-1", 11)

	jobID := submitGangJob(t, ctx, client, 10)

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if _, placed := srv.gangJobs[model.JobID(jobID)]; !placed {
		t.Fatalf("gang job %s was not placed", jobID)
	}
	if len(srv.cellAssignments) != 0 {
		t.Fatalf("cellAssignments = %+v, want empty (no samples/shards Params, nothing to activate)", srv.cellAssignments)
	}
}

// syncLogSink is a Config.Logger sink safe for concurrent RPC handlers
// (matching how a real logger would be used), and lets a test read back
// every formatted line it received.
type syncLogSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *syncLogSink) log(format string, args ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, fmt.Sprintf(format, args...))
}

func (s *syncLogSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

// TestActivateCoupledCellMalformedGangSurfacesError is #113's acceptance
// criterion: a Barrier dist-training gang whose shard count does not match
// its cell's member-agent count is still admitted — gang.go's "SubmitJob
// never rejects a well-formed gang for lack of capacity" contract holds,
// this is not a capacity problem — but activation for it fails, and that
// failure is now visible in two places instead of an invisible, permanent
// Done=false hang: the injected Logger sees a line naming the job id and
// the activation error, and JobStatus reports an "activation failed: ..."
// reason.
func TestActivateCoupledCellMalformedGangSurfacesError(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	sink := &syncLogSink{}
	cfg.Logger = sink.log

	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	// cell-1 has exactly one member agent, but the job below asks for 3
	// shards — a shard/agent-count mismatch activateCoupledCellLocked
	// reports as an error rather than silently activating a fraction of
	// the cell (see its doc).
	joinCoupledAgent(t, ctx, client, "agent-1", 11)

	resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "dist-training",
		Coupling: transport.Coupling_COUPLING_BARRIER,
		Params: map[string]string{
			"min_members": "1",
			"samples":     "30",
			"shards":      "3",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := resp.GetJobId()
	if jobID == "" {
		t.Fatalf("SubmitJob returned empty job id")
	}

	srv.mu.Lock()
	_, placed := srv.gangJobs[model.JobID(jobID)]
	assignments := len(srv.cellAssignments)
	srv.mu.Unlock()
	if !placed {
		t.Fatalf("gang job %s was not placed, want admission to still succeed", jobID)
	}
	if assignments != 0 {
		t.Fatalf("cellAssignments = %d, want 0 (activation failed, nothing distributed)", assignments)
	}

	logs := sink.snapshot()
	found := false
	for _, l := range logs {
		if strings.Contains(l, jobID) && strings.Contains(l, "shard(s)") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Logger never logged an activation failure for job %s, got %v", jobID, logs)
	}

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if statusResp.GetDone() {
		t.Fatalf("JobStatus.Done = true, want false (activation failed, job never completes)")
	}
	reason := string(statusResp.GetAggregate())
	if !strings.HasPrefix(reason, "activation failed:") {
		t.Fatalf("JobStatus.Aggregate = %q, want prefix %q", reason, "activation failed:")
	}
	if !strings.Contains(reason, "shard(s)") {
		t.Fatalf("JobStatus.Aggregate = %q, want it to name the shard/agent mismatch", reason)
	}
}

// TestActivateCoupledCellWellFormedGangUnaffected is #113's no-regression
// acceptance criterion: a well-formed dist-training gang still activates
// exactly as before — no Logger line, and JobStatus reports the ordinary
// "not done yet" empty-Aggregate shape, never a failure reason.
func TestActivateCoupledCellWellFormedGangUnaffected(t *testing.T) {
	clock := &testClock{}
	cfg := fastConfig()
	sink := &syncLogSink{}
	cfg.Logger = sink.log

	client, srv, teardown := newTestServer(t, cfg, clock)
	defer teardown()
	ctx := context.Background()

	joinCoupledAgent(t, ctx, client, "agent-1", 11)

	resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "dist-training",
		Coupling: transport.Coupling_COUPLING_BARRIER,
		Params: map[string]string{
			"min_members": "1",
			"samples":     "30",
			"shards":      "1",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := resp.GetJobId()

	srv.mu.Lock()
	_, hasAssignment := srv.cellAssignments["agent-1"]
	srv.mu.Unlock()
	if !hasAssignment {
		t.Fatalf("agent-1 has no CellAssignment, want one (well-formed gang activates)")
	}

	if logs := sink.snapshot(); len(logs) != 0 {
		t.Fatalf("Logger = %v, want no lines for a well-formed gang", logs)
	}

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if statusResp.GetDone() {
		t.Fatalf("JobStatus.Done = true, want false (job not complete yet)")
	}
	if len(statusResp.GetAggregate()) != 0 {
		t.Fatalf("JobStatus.Aggregate = %q, want empty (no activation failure)", statusResp.GetAggregate())
	}
}
