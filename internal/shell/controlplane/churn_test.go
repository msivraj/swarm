package controlplane

import (
	"context"
	"sort"
	"testing"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// This file covers ticket #116 (control-plane member-churn: reservation
// release + requeue + refill re-decompose + ReportCellStatus): the two
// reservation-release paths (completion, TestOnCoupledCompleteReleases...,
// and stall, TestReportCellStatusStalled...), the same-cell refill
// re-decompose that recovers from a lost member
// (TestRefillReDecomposesOverCurrentCellMembers), and the no-churn
// no-regression guard (TestCoupledCompletionNoChurnReleasesReservation).

// TestReleaseGangReservationLocked table-drives releaseGangReservationLocked
// directly (white-box, same package): releasing a live reservation always
// gives back its gangReserved slots and drops the gangJobs entry, and
// requeue controls whether the released JobSpec lands back on gangPending;
// releasing a jobID with no live reservation (never a gang, or already
// released) is a no-op either way — nothing to release, nothing to requeue.
func TestReleaseGangReservationLocked(t *testing.T) {
	tests := []struct {
		name        string
		requeue     bool
		seedJob     bool
		wantPending int
	}{
		{name: "requeue=true releases and requeues", requeue: true, seedJob: true, wantPending: 1},
		{name: "requeue=false releases without requeuing", requeue: false, seedJob: true, wantPending: 0},
		{name: "no live reservation is a no-op", requeue: true, seedJob: false, wantPending: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := &testClock{}
			_, srv, teardown := newTestServer(t, fastConfig(), clock)
			defer teardown()

			srv.mu.Lock()
			defer srv.mu.Unlock()

			srv.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellUp, Cell: "cell-1", Capacity: 20})
			jobID := model.JobID("job-x")
			if tt.seedJob {
				spec := model.JobSpec{ID: jobID, Coupling: model.Barrier, MinMembers: 5}
				assignments := []admission.Assignment{{Cell: "cell-1", Members: 5}}
				srv.gangJobs[jobID] = gangReservation{job: spec, assignments: assignments}
				srv.gangReserved["cell-1"] = 5
			}

			srv.releaseGangReservationLocked(jobID, tt.requeue)

			if _, stillThere := srv.gangJobs[jobID]; stillThere {
				t.Fatalf("gangJobs[%s] still present after release", jobID)
			}
			if got := srv.gangReserved["cell-1"]; tt.seedJob && got != 0 {
				t.Fatalf("gangReserved[cell-1] = %d, want 0 (fully released)", got)
			}
			if len(srv.gangPending) != tt.wantPending {
				t.Fatalf("gangPending = %+v, want %d entr(y/ies)", srv.gangPending, tt.wantPending)
			}
			if tt.wantPending == 1 && srv.gangPending[0].ID != jobID {
				t.Fatalf("gangPending[0].ID = %s, want %s", srv.gangPending[0].ID, jobID)
			}
		})
	}
}

// TestOnCoupledCompleteReleasesReservationAndAdmitsPendingGang is #116's
// completion-release acceptance criterion (closing the #71 remainder): a
// coupled gang's normal completion (ReportResult, keyed by job id) frees the
// gangReserved slots its Place decision held, and a second gang that was
// queued waiting on exactly that capacity is then admitted — asserted
// before and after, on both s.gangReserved and s.gangJobs/s.gangPending.
func TestOnCoupledCompleteReleasesReservationAndAdmitsPendingGang(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 10) // cell-1: capacity 10, Free 9

	jobA := model.JobID(submitGangJob(t, ctx, client, 9)) // uses all 9 free slots
	jobB := model.JobID(submitGangJob(t, ctx, client, 1)) // nothing left -> queued

	srv.mu.Lock()
	if _, placed := srv.gangJobs[jobA]; !placed {
		srv.mu.Unlock()
		t.Fatalf("gang A (job %s) was not placed", jobA)
	}
	if len(srv.gangPending) != 1 || srv.gangPending[0].ID != jobB {
		srv.mu.Unlock()
		t.Fatalf("gang B (job %s) was not queued: gangPending = %+v", jobB, srv.gangPending)
	}
	if got := srv.gangReserved["cell-1"]; got != 9 {
		srv.mu.Unlock()
		t.Fatalf("gangReserved[cell-1] before completion = %d, want 9", got)
	}
	srv.mu.Unlock()

	reportResp, err := client.ReportResult(ctx, &transport.ReportResultRequest{
		TaskId: string(jobA),
		Output: []byte{9, 9, 9},
		Ok:     true,
	})
	if err != nil {
		t.Fatalf("ReportResult(%s): %v", jobA, err)
	}
	if !reportResp.GetAccepted() {
		t.Fatalf("ReportResult(%s) not accepted", jobA)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if _, stillPlaced := srv.gangJobs[jobA]; stillPlaced {
		t.Fatalf("gangJobs still holds completed job %s, want released", jobA)
	}
	if _, placed := srv.gangJobs[jobB]; !placed {
		t.Fatalf("gang B (job %s) was never admitted after A's completion freed capacity", jobB)
	}
	if len(srv.gangPending) != 0 {
		t.Fatalf("gangPending = %+v, want empty (B admitted)", srv.gangPending)
	}
	if got := srv.gangReserved["cell-1"]; got != 1 {
		t.Fatalf("gangReserved[cell-1] after completion = %d, want 1 (only B's reservation)", got)
	}
}

// TestReportCellStatusStalledReleasesAndRequeues is #116's stall
// acceptance criterion: a leader's ReportCellStatus{Stalled:true} releases
// the reported job's gang reservation and re-enqueues it on the pending gang
// queue, freeing the capacity it held rather than stranding it while the job
// is parked.
//
// Cell-1's whole free capacity is exactly A's reservation, with a second gang
// B already queued behind it (Wait, no room). retryPendingGangsLocked runs
// FIFO from the pending queue's head (see its doc), so once A's stall frees
// its slots and requeues A behind B, the retry this handler triggers admits
// B first — the freed capacity is put back to work rather than left idle —
// and, B's admission having spent it again, stops at A: A is left pending,
// demonstrating both halves of the criterion at once (freed, and now
// pending) without needing an artificial capacity change of its own.
func TestReportCellStatusStalledReleasesAndRequeues(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 5) // cell-1: capacity 5, Free 4

	jobA := model.JobID(submitGangJob(t, ctx, client, 4)) // uses all 4 free slots
	jobB := model.JobID(submitGangJob(t, ctx, client, 4)) // nothing left -> queued

	srv.mu.Lock()
	if _, placed := srv.gangJobs[jobA]; !placed {
		srv.mu.Unlock()
		t.Fatalf("gang A (job %s) was not placed", jobA)
	}
	if len(srv.gangPending) != 1 || srv.gangPending[0].ID != jobB {
		srv.mu.Unlock()
		t.Fatalf("gang B (job %s) was not queued: gangPending = %+v", jobB, srv.gangPending)
	}
	if got := srv.gangReserved["cell-1"]; got != 4 {
		srv.mu.Unlock()
		t.Fatalf("gangReserved[cell-1] before stall = %d, want 4", got)
	}
	srv.mu.Unlock()

	resp, err := client.ReportCellStatus(ctx, &transport.CellStatusRequest{
		JobId:   string(jobA),
		AgentId: "agent-1",
		Stalled: true,
		Have:    3,
		Need:    4,
	})
	if err != nil {
		t.Fatalf("ReportCellStatus: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("ReportCellStatus.Accepted = false, want true")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if _, stillPlaced := srv.gangJobs[jobA]; stillPlaced {
		t.Fatalf("gangJobs still holds stalled job %s, want released", jobA)
	}
	if _, placed := srv.gangJobs[jobB]; !placed {
		t.Fatalf("gang B (job %s) was never admitted with A's freed capacity", jobB)
	}
	if got := srv.gangReserved["cell-1"]; got != 4 {
		t.Fatalf("gangReserved[cell-1] after stall = %d, want 4 (B's reservation, using A's freed slots)", got)
	}
	if len(srv.gangPending) != 1 || srv.gangPending[0].ID != jobA {
		t.Fatalf("gangPending = %+v, want exactly [%s] (A requeued, now waiting behind B)", srv.gangPending, jobA)
	}
}

// TestReportCellStatusNotStalledIsNoop guards the other half of
// ReportCellStatus's contract: a status report with Stalled==false takes no
// CP-side action (the leader's "runnable again" signal is re-polling
// CellAssignment, not a downward push from this RPC — see the proto doc) but
// still always accepts the report.
func TestReportCellStatusNotStalledIsNoop(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 5)
	jobID := model.JobID(submitGangJob(t, ctx, client, 4))

	resp, err := client.ReportCellStatus(ctx, &transport.CellStatusRequest{
		JobId:   string(jobID),
		AgentId: "agent-1",
		Stalled: false,
		Have:    4,
		Need:    4,
	})
	if err != nil {
		t.Fatalf("ReportCellStatus: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("ReportCellStatus.Accepted = false, want true")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if _, placed := srv.gangJobs[jobID]; !placed {
		t.Fatalf("gangJobs no longer holds job %s after a non-stalled report, want untouched", jobID)
	}
	if len(srv.gangPending) != 0 {
		t.Fatalf("gangPending = %+v, want empty (no stall, nothing requeued)", srv.gangPending)
	}
}

// TestRefillReDecomposesOverCurrentCellMembers is #116's refill acceptance
// criterion (absorbing H2): a coupled dist-training gang activates over its
// cell's 3 joined members, one of them (agent-3) then leaves the cell
// (member churn) and a replacement (agent-4) joins the SAME cell in its
// place (the refill) before the leader's stall report reaches the control
// plane — a fully plausible ordering, since the report only travels as fast
// as the network between them. ReportCellStatus{Stalled:true} releases and
// re-admits the gang immediately (ample free capacity, see gang.go's
// gangFreeCapacityLocked — capacity headroom and real cellAgents membership
// are two independent numbers, per cellactivation_test.go's file doc), and
// because the cell already has exactly 3 current members again by the time
// activateCoupledCellLocked re-runs, it re-decomposes the full dataset over
// them: shard count equals member count, the union of shard inputs is the
// full [0,samples) range with the shard agent-3 dropped now covered by
// agent-4, and the refreshed CellAssignments are stored for the existing
// CellAssignment RPC to pick up (no new downward RPC).
func TestRefillReDecomposesOverCurrentCellMembers(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

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
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := resp.GetJobId()

	// Sanity: the initial activation covered exactly 3 shards over 3 peers.
	srv.mu.Lock()
	if len(srv.cellAssignments) != 3 {
		srv.mu.Unlock()
		t.Fatalf("cellAssignments before churn = %d, want 3", len(srv.cellAssignments))
	}
	srv.mu.Unlock()

	// Member churn: agent-3 leaves cell-1 (exactly what the reaper's
	// AgentLeft eviction would do), then a replacement, agent-4, joins the
	// same cell in its place — the refill.
	srv.mu.Lock()
	srv.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.AgentLeft, Cell: "cell-1", Agent: "agent-3"})
	delete(srv.agentCell, "agent-3")
	delete(srv.cellAgents["cell-1"], "agent-3")
	srv.mu.Unlock()

	joinCoupledAgent(t, ctx, client, "agent-4", 1)

	// The leader's stall report arrives after the refill has already
	// happened, releasing and immediately re-admitting the gang.
	statusResp, err := client.ReportCellStatus(ctx, &transport.CellStatusRequest{
		JobId:   jobID,
		AgentId: "agent-1",
		Stalled: true,
		Have:    2,
		Need:    3,
	})
	if err != nil {
		t.Fatalf("ReportCellStatus: %v", err)
	}
	if !statusResp.GetAccepted() {
		t.Fatalf("ReportCellStatus.Accepted = false, want true")
	}

	srv.mu.Lock()
	if _, placed := srv.gangJobs[model.JobID(jobID)]; !placed {
		srv.mu.Unlock()
		t.Fatalf("gang job %s was not re-admitted after the refill", jobID)
	}
	if len(srv.gangPending) != 0 {
		srv.mu.Unlock()
		t.Fatalf("gangPending = %+v, want empty (refill re-admission succeeded)", srv.gangPending)
	}
	srv.mu.Unlock()

	wantMembers := []string{"agent-1", "agent-2", "agent-4"}
	var union []idRange
	for _, agent := range wantMembers {
		assignResp, err := client.CellAssignment(ctx, &transport.CellAssignmentRequest{Agent: agent})
		if err != nil {
			t.Fatalf("CellAssignment(%s): %v", agent, err)
		}
		if !assignResp.GetHasAssignment() {
			t.Fatalf("CellAssignment(%s).HasAssignment = false, want true (refreshed after refill)", agent)
		}
		if assignResp.GetJobId() != jobID {
			t.Fatalf("CellAssignment(%s).JobId = %q, want %q", agent, assignResp.GetJobId(), jobID)
		}
		start, end := decodeRange(assignResp.GetShardInput())
		union = append(union, idRange{start: start, end: end})
	}

	if len(union) != len(wantMembers) {
		t.Fatalf("shard count = %d, want %d (== current member count)", len(union), len(wantMembers))
	}
	assertContiguousFullCoverage(t, union, 30)
}

// idRange is a decoded [start, end) sample-index shard range, mirroring
// templates' private wire layout via controlplane_test.go's decodeRange.
type idRange struct{ start, end uint64 }

// assertContiguousFullCoverage sorts got by start and asserts it tiles
// [0, total) exactly once each — the "union of shard inputs equals the full
// dataset, no dropped shard, no overlap" acceptance check.
func assertContiguousFullCoverage(t *testing.T, got []idRange, total uint64) {
	t.Helper()
	sort.Slice(got, func(i, j int) bool { return got[i].start < got[j].start })
	var cursor uint64
	for _, r := range got {
		if r.start != cursor {
			t.Fatalf("shard ranges = %+v, want contiguous coverage of [0,%d) with no gap/overlap at %d", got, total, cursor)
		}
		if r.end <= r.start {
			t.Fatalf("shard range %+v is empty or inverted", r)
		}
		cursor = r.end
	}
	if cursor != total {
		t.Fatalf("shard ranges = %+v, cover up to %d, want full coverage up to %d", got, cursor, total)
	}
}

// TestCoupledCompletionNoChurnReleasesReservation is #116's no-regression
// acceptance criterion: a well-formed, normally-completing dist-training
// gang with no churn at all still activates and completes exactly as #98's
// TestCoupledCompletion already asserts (Done flips, Aggregate carries the
// combined gradient) — this ticket adds only that its gang reservation is
// now correctly released afterward instead of leaking forever (the #71
// remainder), asserted directly on gangReserved/gangJobs.
func TestCoupledCompletionNoChurnReleasesReservation(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinCoupledAgent(t, ctx, client, "agent-1", 10)

	resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "dist-training",
		Coupling: transport.Coupling_COUPLING_BARRIER,
		Params: map[string]string{
			"min_members": "1",
			"samples":     "10",
			"shards":      "1",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := resp.GetJobId()

	srv.mu.Lock()
	if got := srv.gangReserved["cell-1"]; got != 1 {
		srv.mu.Unlock()
		t.Fatalf("gangReserved[cell-1] before completion = %d, want 1", got)
	}
	srv.mu.Unlock()

	combined := templates.DistTrainingCombine([][]byte{make([]byte, 8)})
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

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if !statusResp.GetDone() {
		t.Fatalf("JobStatus.Done = false, want true")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if _, placed := srv.gangJobs[model.JobID(jobID)]; placed {
		t.Fatalf("gangJobs still holds completed job %s, want released", jobID)
	}
	if got := srv.gangReserved["cell-1"]; got != 0 {
		t.Fatalf("gangReserved[cell-1] after completion = %d, want 0 (fully released)", got)
	}
}
