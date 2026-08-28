package admission

import "github.com/msivraj/swarm/internal/model"

// GangKind is the tag of a Gang's tagged union.
type GangKind int

const (
	// Wait means the job's MinMembers floor cannot be satisfied by the free
	// capacity given right now; the shell holds the job in a pending queue
	// and retries on capacity change.
	Wait GangKind = iota
	// Place means the job's MinMembers floor is satisfiable now; Gang.
	// Assignments carries the full, all-or-nothing placement.
	Place
)

// Assignment is one cell's contribution to a gang placement: Members slots
// on Cell.
type Assignment struct {
	Cell    model.CellID
	Members int
}

// Gang is a gang-admission decision the shell will execute. It is a tagged
// union: Place{[]Assignment} | Wait. AdmitGang returns Gangs; it never
// carries them out.
type Gang struct {
	Kind GangKind
	// Assignments is set iff Kind == Place. It always sums to at least
	// job.MinMembers (see AdmitGang) and never over-allocates a cell beyond
	// its Free.
	Assignments []Assignment
}

// AdmitGang decides all-or-nothing admission for a coupled job (B4): it
// returns Place with a full assignment covering job.MinMembers iff the free
// capacity can satisfy it right now; otherwise it returns Wait with no
// assignments. AdmitGang never returns a partial Place — a gang either
// starts fully staffed or it does not start at all.
//
// job.MinMembers == 0 is documented here as "not a gang" (P0/P1 behavior
// unchanged, per model.JobSpec.MinMembers's doc comment): AdmitGang returns
// Place with empty Assignments, since there is no floor to satisfy. This
// mirrors P0's Admit, which never gates on a gang floor for such jobs.
//
// Fill order is deterministic first-fit over free's slice order — the same
// tie-break convention placement.Place uses (decide on slice order, not on
// cell identity or load): AdmitGang walks free in order, taking as many of
// each cell's Free slots as still needed, until MinMembers is reached or
// free is exhausted.
//
// Capability filtering (CapSet) is out of scope here, per the phase doc:
// only cells whose Caps satisfy the job's requirement should count toward
// the fit, but that predicate lives in the placement core. AdmitGang takes
// free as already filtered by the caller (the shell, or a caller composing
// with placement's capability predicate) — it treats every entry in free as
// eligible.
func AdmitGang(job model.JobSpec, free []model.CellCapacity) Gang {
	if job.MinMembers <= 0 {
		return Gang{Kind: Place}
	}

	remaining := job.MinMembers
	var assignments []Assignment
	for _, c := range free {
		if remaining <= 0 {
			break
		}
		if c.Free <= 0 {
			continue
		}
		take := c.Free
		if take > remaining {
			take = remaining
		}
		assignments = append(assignments, Assignment{Cell: c.ID, Members: take})
		remaining -= take
	}

	if remaining > 0 {
		return Gang{Kind: Wait}
	}
	return Gang{Kind: Place, Assignments: assignments}
}
