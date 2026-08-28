// Package leader is a pure core: it runs coordinated supersteps for
// graph-compute — assign work to followers, collect their reports, fold the
// results, advance the superstep. It performs no I/O and reads no clock: the
// phase doc omits `now` for this driver entirely (round timeouts arrive as a
// RoundTimeout event, not a clock read), so Step takes no Instant param —
// unlike internal/core/barrier, which keeps an unused `now` for a future
// timing-aware rule, this driver has no such placeholder because the doc
// never names one. This package follows the shape of internal/core/mitosis
// (the reference core) and internal/core/barrier (the sibling driver): take
// data, return commands, never execute an effect.
package leader

import "sort"

// FollowerID identifies one follower under this leader.
type FollowerID string

// Super is the driver's whole state — plain data.
type Super struct {
	Superstep int
	Assigns   map[FollowerID][]byte // work handed to each follower this superstep
	Results   map[FollowerID][]byte // reports collected this superstep
}

// EventKind tags Event's sum type.
type EventKind int

const (
	// Report{follower, result} — a follower reported its result for the
	// current superstep.
	Report EventKind = iota
	// RoundTimeout — the per-superstep deadline fired.
	RoundTimeout
)

// Event is the sum type folded by Step: Report | RoundTimeout.
type Event struct {
	Kind     EventKind
	Follower FollowerID // Report
	Result   []byte     // Report
}

// CmdOp tags Command's sum type.
type CmdOp int

const (
	// Assign{follower, work} — hand a follower its work for a superstep.
	// Part of the doc's Command vocabulary, but Step never constructs one:
	// Event only carries Report and RoundTimeout, neither of which tells
	// this core what a *new* round's work should be (that's graph-compute
	// application logic, not this driver's concern — see the ticket's scope
	// boundary: "Fan-out of Assign ... is the per-cell leader shell's job").
	// The constant is kept so the shell and any future ticket that starts a
	// round share the same vocabulary as Fold/Advance/Reassign below.
	Assign CmdOp = iota
	// Fold{results} — combine every follower's reported result for the
	// superstep that just completed.
	Fold
	// Advance{superstep} — move the leader's followers on to superstep.
	Advance
	// Reassign{follower, work} — re-hand a follower its (unchanged) work
	// after it failed to report by RoundTimeout.
	Reassign
)

// Command is a description of an effect the shell will execute. Cores return
// Commands; they never carry them out.
type Command struct {
	Op        CmdOp
	Follower  FollowerID            // Assign, Reassign
	Work      []byte                // Assign, Reassign
	Results   map[FollowerID][]byte // Fold
	Superstep int                   // Advance
}

// Step folds one event into new state and the commands the shell must run.
// Pure: no I/O, no clock read.
//
// Resolved ambiguities (issue #60 notes, flagged for auditor + human):
//
//   - A. A completing Report emits Fold{results} then Advance{Superstep+1},
//     matching the doc's order and mirroring barrier's AllReduce-before-
//     Release: fold the round that just finished, then move on.
//   - B. Assigns is the round's membership (mirrors barrier's Members): a
//     Report from a follower not in Assigns is ignored; a duplicate Report
//     from an already-reported follower overwrites its Result and, since it
//     already counted toward completion, does not double-complete.
//   - C. On round completion Results is cleared and Superstep is
//     incremented, but Assigns is left untouched — this driver has no event
//     that supplies the next round's work (see the Assign note above), so
//     the shell (or a future ticket) is responsible for setting Assigns
//     before the next round begins.
//   - D. RoundTimeout emits one Reassign per follower in Assigns that has
//     not reported, in Assigns key-sorted order (stable, deterministic
//     command order), and emits no Fold/Advance.
//   - E. RoundTimeout arriving when every assigned follower has already
//     reported (no stragglers) still completes the round — Fold+Advance —
//     rather than doing nothing, mirroring barrier's decision C: a timeout
//     that arrives after everyone reported must not be able to deadlock a
//     round waiting for a completion that would otherwise never come.
//   - F. RoundTimeout on an empty Assigns (nothing was ever handed out) is a
//     no-op — mirrors barrier's decision D: no work assigned means nothing
//     to reassign and nothing to fold.
func Step(s Super, ev Event) (Super, []Command) {
	switch ev.Kind {
	case Report:
		return stepReport(s, ev.Follower, ev.Result)
	case RoundTimeout:
		return stepRoundTimeout(s)
	default:
		// Unknown EventKind: no-op rather than panic — a pure core must
		// never crash on unexpected input.
		return s, nil
	}
}

// stepReport folds a Report{f, result} event (decision B). A Report from a
// follower not in Assigns is ignored; a duplicate Report overwrites its
// Result without double-completing. When every assigned follower has
// reported, the round completes (decision A).
func stepReport(s Super, f FollowerID, result []byte) (Super, []Command) {
	if !isAssigned(s.Assigns, f) {
		return s, nil
	}

	results := cloneResults(s.Results)
	results[f] = result
	next := s
	next.Results = results

	if !allReported(s.Assigns, results) {
		return next, nil
	}
	return completeRound(next, results)
}

// stepRoundTimeout folds a RoundTimeout event: it reassigns the work of
// every follower in Assigns that has not reported (decision D). If no
// followers were ever assigned, it is a no-op (decision F). If every
// assigned follower has already reported, it completes the round instead of
// doing nothing (decision E).
func stepRoundTimeout(s Super) (Super, []Command) {
	if len(s.Assigns) == 0 {
		return s, nil
	}

	outstanding := outstandingFollowers(s.Assigns, s.Results)
	if len(outstanding) == 0 {
		return completeRound(s, s.Results)
	}

	cmds := make([]Command, 0, len(outstanding))
	for _, f := range outstanding {
		cmds = append(cmds, Command{Op: Reassign, Follower: f, Work: s.Assigns[f]})
	}
	return s, cmds
}

// completeRound emits the round-completion commands for s given the (fully
// reported) results collected for it, and returns the advanced state:
// Fold{results} then Advance{Superstep+1} (decisions A, C).
func completeRound(s Super, results map[FollowerID][]byte) (Super, []Command) {
	next := s
	next.Results = nil
	next.Superstep = s.Superstep + 1

	return next, []Command{
		{Op: Fold, Results: results},
		{Op: Advance, Superstep: next.Superstep},
	}
}

// isAssigned reports whether f was handed work for the current superstep.
func isAssigned(assigns map[FollowerID][]byte, f FollowerID) bool {
	_, ok := assigns[f]
	return ok
}

// allReported reports whether every follower in assigns has a recorded
// result.
func allReported(assigns, results map[FollowerID][]byte) bool {
	for f := range assigns {
		if _, ok := results[f]; !ok {
			return false
		}
	}
	return true
}

// outstandingFollowers returns the assigned followers with no recorded
// result, in key-sorted order — the deterministic command order this
// package documents for Reassign.
func outstandingFollowers(assigns, results map[FollowerID][]byte) []FollowerID {
	var out []FollowerID
	for f := range assigns {
		if _, ok := results[f]; !ok {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// cloneResults copies m so folding a Report event never mutates a Results
// map a caller is still holding — copy-on-write, the same discipline as
// internal/core/barrier's clonePartials.
func cloneResults(m map[FollowerID][]byte) map[FollowerID][]byte {
	out := make(map[FollowerID][]byte, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}
