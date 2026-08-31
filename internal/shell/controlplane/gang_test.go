package controlplane

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// This file covers ticket #71 (gang reservation shell)'s acceptance
// criteria: a gang that fits is atomically reserved (TestGangPlaced...), a
// gang that does not fit is queued and admitted once a capacity-change
// event frees enough slots (TestGangQueuedThenAdmittedOnCapacityChange,
// pinning the phase doc's own "128 needed, 100 free -> queued; +28 freed ->
// placed" headline), and two competing gangs never double-book overlapping
// capacity (TestGangRaceNeverDoubleBooksOverlappingCapacity, run under
// -race with real goroutines).
//
// TestGangReserveRollsBackPartialOnRace is white-box: this shell's
// SubmitJob/JoinAgent handlers hold s.mu continuously across a gang's
// decide-then-commit (admitGangLocked calls admission.AdmitGang and
// reserveGangLocked back to back, no yield in between), so no concurrent
// RPC can ever actually make reserveGangLocked's live check disagree with
// the free capacity AdmitGang just decided against — the race the design
// (#71) describes ("if reservation loses a race mid-Place") cannot arise
// through the public API by construction. This test instead exercises
// reserveGangLocked directly, manufacturing exactly that disagreement, to
// prove its rollback contract holds: a failed commit never leaves a
// partial reservation behind, for this call or for reservations that
// predate it.

func submitGangJob(t *testing.T, ctx context.Context, client transport.ControlPlaneClient, minMembers int) string {
	t.Helper()
	resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "dist-training",
		Coupling: transport.Coupling_COUPLING_BARRIER,
		Params:   map[string]string{"min_members": itoa(minMembers)},
	})
	if err != nil {
		t.Fatalf("SubmitJob(min_members=%d): %v", minMembers, err)
	}
	if resp.GetJobId() == "" {
		t.Fatalf("SubmitJob(min_members=%d) returned empty job id", minMembers)
	}
	return resp.GetJobId()
}

func itoa(n int) string {
	// Avoids importing strconv into the test just for this one call site's
	// worth of use; kept trivial and non-negative-only on purpose (every
	// caller here passes a positive min_members).
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// TestGangPlacedAtomicallyWhenItFits is the "gang that fits" acceptance
// criterion: MinMembers <= free capacity reserves in full, in one shot, and
// leaves nothing in the pending gang queue.
func TestGangPlacedAtomicallyWhenItFits(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 11) // NewCell capacity 11, agent-1 joins -> Free 10

	jobID := model.JobID(submitGangJob(t, ctx, client, 10))

	srv.mu.Lock()
	defer srv.mu.Unlock()

	r, placed := srv.gangJobs[jobID]
	if !placed {
		t.Fatalf("gang job %s was not placed even though MinMembers exactly matched free capacity", jobID)
	}
	if len(srv.gangPending) != 0 {
		t.Fatalf("gangPending = %+v, want empty", srv.gangPending)
	}
	want := []admission.Assignment{{Cell: "cell-1", Members: 10}}
	if !reflect.DeepEqual(r.assignments, want) {
		t.Fatalf("assignments = %+v, want %+v", r.assignments, want)
	}
	if got := srv.gangReserved["cell-1"]; got != 10 {
		t.Fatalf("gangReserved[cell-1] = %d, want 10", got)
	}
}

// TestGangQueuedThenAdmittedOnCapacityChange is the phase doc's headline
// example ("128 needed, 100 free -> Wait", here: queued) plus its "then
// capacity appears -> placed" half: a gang short of its floor is held
// pending, and a capacity-change event (JoinAgent forming a new cell)
// supplying exactly the missing members admits and reserves it, across
// both cells.
//
// The two capacity chunks arrive smaller-first (28, then 100), not
// 100-then-28 as the doc's prose orders them: rendezvous.AdmitAgent (P0)
// always prefers an existing cell with room over forming a new one (see its
// doc), so once a 100-free cell exists, a 29-slot join would land inside it
// rather than forming a second, independent 28-free cell — smaller-first is
// the only join order that actually produces two distinct cells here. The
// arithmetic (28 + 100 = 128, queued until both exist) is unchanged.
func TestGangQueuedThenAdmittedOnCapacityChange(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 29) // cell-1: capacity 29, Free 28

	jobID := model.JobID(submitGangJob(t, ctx, client, 128))

	srv.mu.Lock()
	if _, placed := srv.gangJobs[jobID]; placed {
		srv.mu.Unlock()
		t.Fatalf("gang job %s was placed with only 28 of the 128 needed members free", jobID)
	}
	if len(srv.gangPending) != 1 || srv.gangPending[0].ID != jobID {
		srv.mu.Unlock()
		t.Fatalf("gang job %s not queued: gangPending = %+v", jobID, srv.gangPending)
	}
	srv.mu.Unlock()

	// cell-1's Free (28) is less than agent-2's requested Caps (101), so
	// rendezvous.AdmitAgent forms a new cell rather than accepting agent-2
	// into cell-1 -- cell-2: capacity 101, Free 100 -> 28+100 = 128.
	joinAgent(t, ctx, client, "agent-2", 101)

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if len(srv.gangPending) != 0 {
		t.Fatalf("gangPending still holds %d job(s) after capacity freed, want 0", len(srv.gangPending))
	}
	r, placed := srv.gangJobs[jobID]
	if !placed {
		t.Fatalf("gang job %s was never admitted after the capacity-change event", jobID)
	}
	sum := 0
	for _, a := range r.assignments {
		sum += a.Members
	}
	if sum != 128 {
		t.Fatalf("reserved sum = %d, want 128", sum)
	}
	if srv.gangReserved["cell-1"] != 28 || srv.gangReserved["cell-2"] != 100 {
		t.Fatalf("gangReserved = %+v, want cell-1:28 cell-2:100", srv.gangReserved)
	}
}

// TestGangRaceNeverDoubleBooksOverlappingCapacity races two equally sized
// gangs (via real goroutines, so -race exercises the actual concurrent
// SubmitJob path) against one cell that has room for only one of them:
// exactly one is placed and reserved, the other is queued, and the total
// reserved on the cell never exceeds what it actually has free — no
// partial or overlapping reservation, regardless of which goroutine's
// SubmitJob call the scheduler runs first.
func TestGangRaceNeverDoubleBooksOverlappingCapacity(t *testing.T) {
	clock := &testClock{}
	client, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	joinAgent(t, ctx, client, "agent-1", 51) // cell-1: capacity 51, Free 50

	const gangSize = 40 // two of these (80) cannot both fit in 50 free
	ids := make([]string, 2)
	errs := make([]error, 2)

	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
				Template: "dist-training",
				Coupling: transport.Coupling_COUPLING_BARRIER,
				Params:   map[string]string{"min_members": itoa(gangSize)},
			})
			if err != nil {
				errs[i] = err
				return
			}
			ids[i] = resp.GetJobId()
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("SubmitJob gang %d: %v", i, err)
		}
	}
	if ids[0] == ids[1] {
		t.Fatalf("both gangs got the same job id %s", ids[0])
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	placed, waiting, reservedTotal := 0, 0, 0
	for _, id := range ids {
		jid := model.JobID(id)
		if r, ok := srv.gangJobs[jid]; ok {
			placed++
			for _, a := range r.assignments {
				reservedTotal += a.Members
			}
			continue
		}
		for _, spec := range srv.gangPending {
			if spec.ID == jid {
				waiting++
				break
			}
		}
	}

	if placed != 1 {
		t.Fatalf("placed = %d, want exactly 1 (two %d-member gangs racing for 50 free slots)", placed, gangSize)
	}
	if waiting != 1 {
		t.Fatalf("waiting = %d, want exactly 1", waiting)
	}
	if reservedTotal != gangSize {
		t.Fatalf("total reserved across the two gangs = %d, want %d (no double booking)", reservedTotal, gangSize)
	}
	if got := srv.gangReserved["cell-1"]; got != gangSize {
		t.Fatalf("gangReserved[cell-1] = %d, want %d", got, gangSize)
	}
}

// TestGangReserveRollsBackPartialOnRace exercises reserveGangLocked
// directly (see the file doc for why a real race cannot be produced
// through the public API): it manufactures a Place decision whose second
// assignment demands more than the cell's current free-minus-reserved
// capacity actually allows — as if the capacity AdmitGang decided against
// had gone stale before commit — and asserts the whole call rolls back: the
// first assignment it had already committed is released, and a
// reservation that predates this call (simulating an earlier, unrelated
// admitted gang) is left untouched.
func TestGangReserveRollsBackPartialOnRace(t *testing.T) {
	clock := &testClock{}
	_, srv, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()

	srv.mu.Lock()
	srv.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellUp, Cell: "cell-a", Capacity: 50})
	srv.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellUp, Cell: "cell-b", Capacity: 10})
	// A pre-existing reservation on cell-b, standing in for an
	// earlier-admitted gang already holding 5 of its 10 free slots.
	srv.gangReserved["cell-b"] = 5
	srv.mu.Unlock()

	// cell-a's assignment fits (30 <= 50 free); cell-b's does not (8 >
	// 10-5=5 free-minus-reserved) -- exactly the race the design describes.
	assignments := []admission.Assignment{
		{Cell: "cell-a", Members: 30},
		{Cell: "cell-b", Members: 8},
	}

	srv.mu.Lock()
	ok := srv.reserveGangLocked(assignments)
	reservedA := srv.gangReserved["cell-a"]
	reservedB := srv.gangReserved["cell-b"]
	srv.mu.Unlock()

	if ok {
		t.Fatalf("reserveGangLocked = true, want false (cell-b only has 5 free-minus-reserved of the 8 demanded)")
	}
	if reservedA != 0 {
		t.Fatalf("cell-a reservation after rollback = %d, want 0 (no partial reservation left behind)", reservedA)
	}
	if reservedB != 5 {
		t.Fatalf("cell-b reservation after rollback = %d, want 5 (the pre-existing reservation, untouched)", reservedB)
	}
}

// TestNonGangSubmitPathUnchanged guards that MinMembers == 0 (no
// min_members param) still routes through P0's admission.Admit unchanged:
// a Barrier-coupled job with no gang floor is still rejected exactly as it
// was before this ticket, since admission.Admit only accepts Independent
// coupling and this ticket must not alter that for non-gang jobs.
func TestNonGangSubmitPathUnchanged(t *testing.T) {
	clock := &testClock{}
	client, _, teardown := newTestServer(t, fastConfig(), clock)
	defer teardown()
	ctx := context.Background()

	_, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "monte-carlo",
		Coupling: transport.Coupling_COUPLING_BARRIER,
		Params:   map[string]string{"trials": "1", "blockSize": "1", "seed": "1"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SubmitJob(Barrier, no min_members) code = %v, want InvalidArgument (P0's admission.Admit path, unchanged)", status.Code(err))
	}
}

// TestRetryPendingGangsPermanentActivationFailureStaysPendingNotDropped is
// #126's "permanent" half: a structurally malformed dist-training gang
// (shards > samples, so templates.DistTrainingDecompose can never succeed —
// no membership change fixes it, unlike a shard/agent-count mismatch) is
// still admitted (SubmitJob never rejects a well-formed-capacity-wise gang,
// #71/#113) but never actually activates. Per #126's fix goal this is
// surfaced (not silently dropped) and left sitting on the pending gang
// queue rather than being popped and lost — retried again, and failing
// again, on every later capacity-change event, without ever leaking or
// double-counting its reservation, and without spinning inside a single
// retryPendingGangsLocked call.
func TestRetryPendingGangsPermanentActivationFailureStaysPendingNotDropped(t *testing.T) {
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
			"samples":     "2",
			"shards":      "3", // shards > samples: partitionRange can never succeed
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := model.JobID(resp.GetJobId())

	srv.mu.Lock()
	if _, placed := srv.gangJobs[jobID]; !placed {
		srv.mu.Unlock()
		t.Fatalf("gang job %s was not placed, want admission to still succeed", jobID)
	}
	if len(srv.gangPending) != 1 || srv.gangPending[0].ID != jobID {
		srv.mu.Unlock()
		t.Fatalf("gangPending = %+v, want exactly [%s] (activation failed, must retry, not be dropped)", srv.gangPending, jobID)
	}
	reserved := srv.gangReserved["cell-1"]
	srv.mu.Unlock()
	if reserved != 1 {
		t.Fatalf("gangReserved[cell-1] = %d, want 1", reserved)
	}

	statusResp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: string(jobID)})
	if err != nil {
		t.Fatalf("JobStatus: %v", err)
	}
	if statusResp.GetDone() {
		t.Fatalf("JobStatus.Done = true, want false (activation can never succeed)")
	}
	if !strings.HasPrefix(string(statusResp.GetAggregate()), "activation failed:") {
		t.Fatalf("JobStatus.Aggregate = %q, want an activation-failed reason, not a silent drop", statusResp.GetAggregate())
	}

	// Two more capacity-change events (unrelated joins) each retry the
	// queue's head — this must neither pop/drop the job, nor leak or
	// double-count its reservation, nor hang: each retryPendingGangsLocked
	// call must return promptly (a permanently-failing head must stop the
	// scan, not spin).
	for i := 0; i < 2; i++ {
		joinCoupledAgent(t, ctx, client, "agent-extra-"+strconv.Itoa(i), 1)

		srv.mu.Lock()
		if len(srv.gangPending) != 1 || srv.gangPending[0].ID != jobID {
			srv.mu.Unlock()
			t.Fatalf("retry %d: gangPending = %+v, want exactly [%s] (still stuck, still visible, never dropped)", i, srv.gangPending, jobID)
		}
		if _, placed := srv.gangJobs[jobID]; !placed {
			srv.mu.Unlock()
			t.Fatalf("retry %d: gangJobs no longer holds job %s", i, jobID)
		}
		if got := srv.gangReserved["cell-1"]; got != 1 {
			srv.mu.Unlock()
			t.Fatalf("retry %d: gangReserved[cell-1] = %d, want 1 (no leak, no double-count across repeated failed retries)", i, got)
		}
		srv.mu.Unlock()
	}

	logs := sink.snapshot()
	found := false
	for _, l := range logs {
		if strings.Contains(l, string(jobID)) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Logger never surfaced the permanent activation failure for job %s, got %v", jobID, logs)
	}
}

// TestParseMinMembers table-drives parseMinMembers's parsing rules: only a
// present, positive, numeric "min_members" param yields a gang floor;
// everything else (absent, zero, negative, non-numeric) means "not a gang".
func TestParseMinMembers(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		want   int
	}{
		{name: "absent params map", params: nil, want: 0},
		{name: "key absent", params: map[string]string{"other": "1"}, want: 0},
		{name: "positive", params: map[string]string{"min_members": "42"}, want: 42},
		{name: "zero", params: map[string]string{"min_members": "0"}, want: 0},
		{name: "negative", params: map[string]string{"min_members": "-3"}, want: 0},
		{name: "non-numeric", params: map[string]string{"min_members": "abc"}, want: 0},
		{name: "empty string", params: map[string]string{"min_members": ""}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseMinMembers(tt.params); got != tt.want {
				t.Fatalf("parseMinMembers(%+v) = %d, want %d", tt.params, got, tt.want)
			}
		})
	}
}
