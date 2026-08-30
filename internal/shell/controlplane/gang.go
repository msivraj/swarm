package controlplane

import (
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// gangMinMembersParam is the JobSpec.Params key SubmitJob reads to learn a
// job's gang admission floor (B4). The wire SubmitJobRequest has no native
// MinMembers field (see internal/shell/transport's generated proto), so —
// mirroring how admission's own decomposeKeyspace/decomposeMonteCarlo
// already treat Params as the typed-parameter channel from the wire — a
// coupled job requests gang admission by setting this key to a positive
// integer. Missing, empty, non-numeric, or non-positive values all mean
// "not a gang" (MinMembers stays 0), which is exactly admission.AdmitGang's
// own "not a gang" convention and keeps every existing submit that never
// sets this key on P0/P1's unchanged single-job admission.Admit path.
const gangMinMembersParam = "min_members"

// gangReservation is the shell-side record of one admitted gang's claim on
// the fleet: the job it belongs to and the assignments admission.AdmitGang
// placed it under. It exists purely so a future release (the barrier
// runtime floor, #59) has something to look up and give back; this ticket
// only ever adds to it.
type gangReservation struct {
	job         model.JobSpec
	assignments []admission.Assignment
}

// parseMinMembers extracts spec.MinMembers from a SubmitJobRequest's Params
// map (see gangMinMembersParam's doc for why the wire carries it there
// rather than as a native field). Any value that does not parse as a
// positive integer maps to 0 ("not a gang").
func parseMinMembers(params map[string]string) int {
	v, ok := params[gangMinMembersParam]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// submitGang is SubmitJob's gang-admission path (extends P0's admission,
// per #71): it decides Place-or-Wait for spec via admitGangLocked — Place
// atomically reserves every assignment, Wait enqueues spec on the pending
// gang queue for a later capacity-change retry (retryPendingGangsLocked) —
// and then persists spec exactly as the Independent path does, so
// JobStatus/GetJob see a submitted gang job whether it is running or still
// queued. SubmitJob itself never rejects a well-formed gang job for lack of
// capacity; that is exactly what Wait/the pending queue is for.
func (s *Server) submitGang(spec model.JobSpec) (*transport.SubmitJobResponse, error) {
	s.mu.Lock()
	g := s.admitGangLocked(spec)
	if g.Kind != admission.Place {
		s.gangPending = append(s.gangPending, spec)
	}
	s.mu.Unlock()

	if err := s.store.PutJob(spec); err != nil {
		return nil, status.Errorf(codes.Internal, "put job: %v", err)
	}

	return &transport.SubmitJobResponse{JobId: string(spec.ID)}, nil
}

// admitGangLocked calls admission.AdmitGang against the free capacity this
// shell currently has to offer a gang (registry capacity net of every slot
// already reserved for an earlier-admitted gang, see gangFreeCapacityLocked)
// and, on Place, commits the decision via reserveGangLocked.
//
// reserveGangLocked re-validates each assignment against the live registry
// at commit time rather than trusting the free numbers AdmitGang decided
// against, so if capacity it read goes stale before the shell commits it —
// a race between the read and the write, in a future implementation that
// does not hold s.mu across both — the commit fails cleanly: every
// assignment reserveGangLocked had already committed for this call is
// rolled back (see its doc), and admitGangLocked hands the caller a Wait
// exactly as if AdmitGang itself had returned one. No caller of
// admitGangLocked ever observes a partially reserved gang. Callers must
// hold s.mu.
func (s *Server) admitGangLocked(job model.JobSpec) admission.Gang {
	free := s.gangFreeCapacityLocked()
	g := admission.AdmitGang(job, free)
	if g.Kind != admission.Place {
		return g
	}
	if !s.reserveGangLocked(g.Assignments) {
		return admission.Gang{Kind: admission.Wait}
	}
	s.gangJobs[job.ID] = gangReservation{job: job, assignments: g.Assignments}
	// A Barrier gang whose Params ask for cell activation (#98) gets its
	// CellAssignments built right here, the single choke point every Place
	// decision passes through whether it came from SubmitJob or a later
	// retryPendingGangsLocked capacity-change retry. Activation failure
	// (e.g. a malformed dist-training request) is not this gang's admission
	// failing — the reservation above already committed, and gang.go's own
	// contract ("SubmitJob never rejects a well-formed gang job for lack of
	// capacity") extends to a request activation can't service either; there
	// is no caller here (retry runs off the background loops, not an RPC) to
	// return the error to. It is instead made visible without changing that
	// admission outcome — logged, and recorded against the job so
	// JobStatus/Ps can report why it will otherwise hang Done=false forever
	// (see surfaceActivationFailureLocked, #113).
	if err := s.activateCoupledCellLocked(job, g); err != nil {
		s.surfaceActivationFailureLocked(job, err)
	}
	return g
}

// gangFreeCapacityLocked projects the registry snapshot into the free
// capacity admission.AdmitGang reasons over, net of every slot this shell
// has already reserved for an earlier-admitted gang (s.gangReserved) — so a
// second gang's admission decision can never see capacity a first gang's
// Place already claimed, without admission.AdmitGang itself needing any
// notion of reservations. Callers must hold s.mu.
func (s *Server) gangFreeCapacityLocked() []model.CellCapacity {
	snap := registry.Snapshot(s.store.Registry())
	free := make([]model.CellCapacity, 0, len(snap))
	for _, c := range snap {
		f := c.Free - s.gangReserved[c.ID]
		if f < 0 {
			f = 0
		}
		free = append(free, model.CellCapacity{ID: c.ID, Free: f, Caps: c.Caps})
	}
	return free
}

// reserveGangLocked commits assignments to s.gangReserved, all-or-nothing:
// it walks them in order, and the moment one would push a cell's
// reservation past what the registry currently reports free for it, it
// rolls back every reservation this call had already made (via
// releaseAssignmentsLocked) and returns false — no assignment from a
// failed call is ever left partially committed. Callers must hold s.mu.
func (s *Server) reserveGangLocked(assignments []admission.Assignment) bool {
	snap := registry.Snapshot(s.store.Registry())
	committed := make([]admission.Assignment, 0, len(assignments))
	for _, a := range assignments {
		free := cellFree(snap, a.Cell) - s.gangReserved[a.Cell]
		if a.Members > free {
			s.releaseAssignmentsLocked(committed)
			return false
		}
		s.gangReserved[a.Cell] += a.Members
		committed = append(committed, a)
	}
	return true
}

// releaseAssignmentsLocked gives back every slot assignments claims in
// s.gangReserved, deleting a cell's entry once its reservation returns to
// zero rather than leaving a zero-value entry behind. Callers must hold
// s.mu.
func (s *Server) releaseAssignmentsLocked(assignments []admission.Assignment) {
	for _, a := range assignments {
		s.gangReserved[a.Cell] -= a.Members
		if s.gangReserved[a.Cell] <= 0 {
			delete(s.gangReserved, a.Cell)
		}
	}
}

// releaseGangReservationLocked gives back the fleet capacity jobID's gang
// reservation holds (via releaseAssignmentsLocked over the assignments
// admitGangLocked committed for it, see gangReservation) and removes its
// gangJobs entry — the single place both member-churn release paths (#116)
// meet: a coupled gang's normal completion (onCoupledComplete) and a
// leader's stall report (ReportCellStatus) each free capacity a Place
// decision reserved but no longer needs, and neither must let it sit
// stranded in gangReserved forever (the #71 remainder this ticket closes).
//
// requeue controls what happens to jobID's JobSpec once its reservation is
// gone: completion passes false (the job is finished; there is nothing left
// to admit), the stall path passes true (H1-A: the job is not done, it lost
// members and must retry from the pending gang queue once capacity, or a
// refilled cell, makes it admissible again) — appending it to the tail of
// s.gangPending for retryPendingGangsLocked to pick back up.
//
// A jobID with no live reservation (never a gang, or already released by an
// earlier call — e.g. a duplicate stall report) is a no-op either way: there
// is nothing to release, and since gangJobs no longer carries its JobSpec,
// nothing here to requeue either. Callers must hold s.mu.
func (s *Server) releaseGangReservationLocked(jobID model.JobID, requeue bool) {
	r, ok := s.gangJobs[jobID]
	if !ok {
		return
	}
	s.releaseAssignmentsLocked(r.assignments)
	delete(s.gangJobs, jobID)
	if requeue {
		s.gangPending = append(s.gangPending, r.job)
	}
}

// cellFree returns id's Free from snap, or 0 if id is not present.
func cellFree(snap []model.CellView, id model.CellID) int {
	for _, c := range snap {
		if c.ID == id {
			return c.Free
		}
	}
	return 0
}

// retryPendingGangsLocked re-runs gang admission for the pending gang
// queue's head, in FIFO order, admitting (and reserving) every job at the
// front that now fits and stopping at the first one that still Waits — a
// later-queued gang cutting ahead of an earlier one that still does not fit
// would let a small gang starve a large one indefinitely, so this only ever
// peels from the front, exactly as the design (#71) calls for ("retry on
// capacity change... re-run AdmitGang for the head of the queue"). Callers
// must hold s.mu, and callers trigger this the same way drainPendingLocked
// is already triggered for the Independent pending buffer: after any event
// that can grow free capacity (JoinAgent, the mitosis tick).
func (s *Server) retryPendingGangsLocked() {
	for len(s.gangPending) > 0 {
		head := s.gangPending[0]
		g := s.admitGangLocked(head)
		if g.Kind != admission.Place {
			return
		}
		s.gangPending = s.gangPending[1:]
	}
}
