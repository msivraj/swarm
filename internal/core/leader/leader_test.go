package leader

import (
	"reflect"
	"testing"
)

func assigns(pairs ...any) map[FollowerID][]byte {
	if len(pairs) == 0 {
		return nil
	}
	out := make(map[FollowerID][]byte, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out[FollowerID(pairs[i].(string))] = []byte(pairs[i+1].(string))
	}
	return out
}

func results(pairs ...any) map[FollowerID][]byte {
	return assigns(pairs...)
}

func followers(ids ...string) []FollowerID {
	out := make([]FollowerID, len(ids))
	for i, id := range ids {
		out[i] = FollowerID(id)
	}
	return out
}

// -----------------------------------------------------------------------
// Step — table-driven, one case per rule and resolved edge case
// -----------------------------------------------------------------------

func TestStep(t *testing.T) {
	tests := []struct {
		name     string
		s        Super
		ev       Event
		wantS    Super
		wantCmds []Command
	}{
		{
			name: "Report that does not yet complete the round",
			s:    Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb")},
			ev:   Event{Kind: Report, Follower: "a", Result: []byte("ra")},
			wantS: Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb"),
				Results: results("a", "ra")},
			wantCmds: nil,
		},
		{
			name: "the completing Report: Fold then Advance",
			s: Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb"),
				Results: results("a", "ra")},
			ev: Event{Kind: Report, Follower: "b", Result: []byte("rb")},
			wantS: Super{Superstep: 1, Assigns: assigns("a", "wa", "b", "wb"),
				Results: nil},
			wantCmds: []Command{
				{Op: Fold, Results: results("a", "ra", "b", "rb")},
				{Op: Advance, Superstep: 1},
			},
		},
		{
			name: "duplicate Report overwrites and does not double-complete",
			s: Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb"),
				Results: results("a", "ra-first")},
			ev: Event{Kind: Report, Follower: "a", Result: []byte("ra-second")},
			wantS: Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb"),
				Results: results("a", "ra-second")},
			wantCmds: nil,
		},
		{
			name: "Report from a non-assigned follower is ignored",
			s:    Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb")},
			ev:   Event{Kind: Report, Follower: "c", Result: []byte("rc")},
			wantS: Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb"),
				Results: nil},
			wantCmds: nil,
		},
		{
			name: "RoundTimeout with some followers missing: one Reassign per un-reported follower",
			s: Super{Superstep: 2, Assigns: assigns("a", "wa", "b", "wb", "c", "wc"),
				Results: results("a", "ra")},
			ev: Event{Kind: RoundTimeout},
			wantS: Super{Superstep: 2, Assigns: assigns("a", "wa", "b", "wb", "c", "wc"),
				Results: results("a", "ra")},
			wantCmds: []Command{
				{Op: Reassign, Follower: "b", Work: []byte("wb")},
				{Op: Reassign, Follower: "c", Work: []byte("wc")},
			},
		},
		{
			name: "RoundTimeout with nobody reported: Reassign every follower, key-sorted",
			s:    Super{Superstep: 0, Assigns: assigns("z", "wz", "a", "wa", "m", "wm")},
			ev:   Event{Kind: RoundTimeout},
			wantS: Super{Superstep: 0, Assigns: assigns("z", "wz", "a", "wa", "m", "wm"),
				Results: nil},
			wantCmds: []Command{
				{Op: Reassign, Follower: "a", Work: []byte("wa")},
				{Op: Reassign, Follower: "m", Work: []byte("wm")},
				{Op: Reassign, Follower: "z", Work: []byte("wz")},
			},
		},
		{
			name: "RoundTimeout with everyone already reported completes the round instead of deadlocking",
			s: Super{Superstep: 4, Assigns: assigns("a", "wa", "b", "wb"),
				Results: results("a", "ra", "b", "rb")},
			ev: Event{Kind: RoundTimeout},
			wantS: Super{Superstep: 5, Assigns: assigns("a", "wa", "b", "wb"),
				Results: nil},
			wantCmds: []Command{
				{Op: Fold, Results: results("a", "ra", "b", "rb")},
				{Op: Advance, Superstep: 5},
			},
		},
		{
			name:     "RoundTimeout on empty Assigns is a no-op",
			s:        Super{Superstep: 3},
			ev:       Event{Kind: RoundTimeout},
			wantS:    Super{Superstep: 3},
			wantCmds: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotS, gotCmds := Step(tt.s, tt.ev)
			if !reflect.DeepEqual(gotS, tt.wantS) {
				t.Fatalf("Step() state = %+v, want %+v", gotS, tt.wantS)
			}
			if !reflect.DeepEqual(gotCmds, tt.wantCmds) {
				t.Fatalf("Step() cmds = %+v, want %+v", gotCmds, tt.wantCmds)
			}
		})
	}
}

// TestStepUnknownEventKindIsNoop asserts an EventKind outside the sum type's
// two variants is a safe no-op — a pure core must never panic on unexpected
// input.
func TestStepUnknownEventKindIsNoop(t *testing.T) {
	s := Super{Superstep: 1, Assigns: assigns("a", "wa"), Results: results("a", "ra")}
	gotS, gotCmds := Step(s, Event{Kind: EventKind(99)})
	if !reflect.DeepEqual(gotS, s) {
		t.Fatalf("Step() with unknown EventKind changed state: got %+v, want unchanged %+v", gotS, s)
	}
	if gotCmds != nil {
		t.Fatalf("Step() with unknown EventKind emitted commands: %+v, want none", gotCmds)
	}
}

// TestStepDoesNotMutateInputResults ensures Step never mutates a Results map
// the caller still holds — copy-on-write, the same discipline barrier and
// routing follow.
func TestStepDoesNotMutateInputResults(t *testing.T) {
	before := results("a", "ra")
	s := Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb"), Results: before}
	beforeCopy := results("a", "ra")

	_, _ = Step(s, Event{Kind: Report, Follower: "b", Result: []byte("rb")})

	if !reflect.DeepEqual(before, beforeCopy) {
		t.Fatalf("Step mutated its input Results map: got %+v, want unchanged %+v", before, beforeCopy)
	}
}

// TestStepIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestStepIsDeterministic(t *testing.T) {
	s := Super{Superstep: 4, Assigns: assigns("a", "wa", "b", "wb", "c", "wc"),
		Results: results("a", "ra", "b", "rb")}
	ev := Event{Kind: Report, Follower: "c", Result: []byte("rc")}

	firstS, firstCmds := Step(s, ev)
	for i := 0; i < 100; i++ {
		gotS, gotCmds := Step(s, ev)
		if !reflect.DeepEqual(gotS, firstS) || !reflect.DeepEqual(gotCmds, firstCmds) {
			t.Fatalf("non-deterministic output on run %d: state=%+v cmds=%+v, want state=%+v cmds=%+v",
				i, gotS, gotCmds, firstS, firstCmds)
		}
	}
}

// -----------------------------------------------------------------------
// Property: a follower that never reports gets its work Reassigned on
// timeout — checked against every possible reported/not-reported split of
// the membership, mirroring barrier's TestDeadlineAlwaysEvictsExactlyTheNotDone.
// -----------------------------------------------------------------------

// reportedSubsets enumerates every subset of fs (as a bitmask, deterministic
// — no math/rand) so the property is checked against every possible
// reported/not-reported split of the assigned followers.
func reportedSubsets(fs []FollowerID) [][]FollowerID {
	n := len(fs)
	var out [][]FollowerID
	for mask := 0; mask < (1 << n); mask++ {
		var subset []FollowerID
		for i, f := range fs {
			if mask&(1<<i) != 0 {
				subset = append(subset, f)
			}
		}
		out = append(out, subset)
	}
	return out
}

func TestRoundTimeoutAlwaysReassignsExactlyTheUnreported(t *testing.T) {
	all := followers("a", "b", "c", "d")
	work := assigns("a", "work-a", "b", "work-b", "c", "work-c", "d", "work-d")

	for _, reported := range reportedSubsets(all) {
		reportedSet := map[FollowerID]bool{}
		rs := map[FollowerID][]byte{}
		for _, f := range reported {
			reportedSet[f] = true
			rs[f] = []byte("result-" + string(f))
		}

		s := Super{Superstep: 9, Assigns: cloneResults(work), Results: rs}
		_, cmds := Step(s, Event{Kind: RoundTimeout})

		if len(reported) == len(all) {
			// Everyone reported: the round completes instead (decision E),
			// so no Reassign is emitted for anyone.
			for _, c := range cmds {
				if c.Op == Reassign {
					t.Fatalf("reported subset %+v: Reassign emitted for %q even though everyone reported", reported, c.Follower)
				}
			}
			continue
		}

		reassigned := map[FollowerID]bool{}
		for _, c := range cmds {
			if c.Op != Reassign {
				continue
			}
			if reportedSet[c.Follower] {
				t.Fatalf("reported subset %+v: Reassign emitted for %q, which reported", reported, c.Follower)
			}
			if !reflect.DeepEqual(c.Work, work[c.Follower]) {
				t.Fatalf("reported subset %+v: Reassign for %q carries work %q, want its assigned work %q",
					reported, c.Follower, c.Work, work[c.Follower])
			}
			reassigned[c.Follower] = true
		}
		for _, f := range all {
			if !reportedSet[f] && !reassigned[f] {
				t.Fatalf("reported subset %+v: follower %q never reported and was not reassigned", reported, f)
			}
		}
	}
}

// -----------------------------------------------------------------------
// Property: all-reports-in completes the round with exactly one Fold and
// one Advance, regardless of how many followers are in play.
// -----------------------------------------------------------------------

func TestAllReportsYieldExactlyOneFoldAndAdvance(t *testing.T) {
	for n := 1; n <= 5; n++ {
		var all []FollowerID
		work := map[FollowerID][]byte{}
		for i := 0; i < n; i++ {
			f := FollowerID(string(rune('a' + i)))
			all = append(all, f)
			work[f] = []byte("work-" + string(f))
		}

		s := Super{Superstep: 0, Assigns: work}
		var lastCmds []Command
		for i, f := range all {
			s, lastCmds = Step(s, Event{Kind: Report, Follower: f, Result: []byte("result-" + string(f))})
			if i < len(all)-1 {
				if lastCmds != nil {
					t.Fatalf("n=%d: report %d of %d emitted commands before the round completed: %+v", n, i+1, len(all), lastCmds)
				}
				continue
			}
		}

		foldCount, advanceCount := 0, 0
		for _, c := range lastCmds {
			switch c.Op {
			case Fold:
				foldCount++
			case Advance:
				advanceCount++
				if c.Superstep != 1 {
					t.Fatalf("n=%d: Advance carries superstep %d, want 1", n, c.Superstep)
				}
			default:
				t.Fatalf("n=%d: unexpected command op %+v on round completion", n, c)
			}
		}
		if foldCount != 1 || advanceCount != 1 {
			t.Fatalf("n=%d: got %d Fold and %d Advance, want exactly one each", n, foldCount, advanceCount)
		}
		if s.Superstep != 1 {
			t.Fatalf("n=%d: state Superstep = %d, want 1", n, s.Superstep)
		}
		if s.Results != nil {
			t.Fatalf("n=%d: state Results = %+v, want cleared", n, s.Results)
		}
	}
}

// -----------------------------------------------------------------------
// Property: report order-tolerance within a superstep — the same set of
// Reports for a round, in any permutation (and with a duplicate re-delivery),
// yields the same folded state and the same commands.
// -----------------------------------------------------------------------

// permutations returns every ordering of xs, via Heap's algorithm — a
// deterministic enumeration (no randomness), matching
// internal/core/barrier's and internal/core/routing's approach.
func permutations(xs []Event) [][]Event {
	var out [][]Event
	n := len(xs)
	buf := make([]Event, n)
	copy(buf, xs)
	c := make([]int, n)

	snapshot := func() []Event {
		cp := make([]Event, n)
		copy(cp, buf)
		return cp
	}

	out = append(out, snapshot())
	for i := 0; i < n; {
		if c[i] < i {
			if i%2 == 0 {
				buf[0], buf[i] = buf[i], buf[0]
			} else {
				buf[c[i]], buf[i] = buf[i], buf[c[i]]
			}
			out = append(out, snapshot())
			c[i]++
			i = 0
		} else {
			c[i] = 0
			i++
		}
	}
	return out
}

// foldEvents applies evs in order via Step, starting from s, and returns the
// final state plus every non-nil command emitted along the way.
func foldEvents(s Super, evs []Event) (Super, []Command) {
	var all []Command
	for _, ev := range evs {
		var cmds []Command
		s, cmds = Step(s, ev)
		all = append(all, cmds...)
	}
	return s, all
}

func TestStepOrderTolerantWithinASuperstep(t *testing.T) {
	initial := Super{Superstep: 0, Assigns: assigns("a", "wa", "b", "wb", "c", "wc")}
	base := []Event{
		{Kind: Report, Follower: "a", Result: []byte("ra")},
		{Kind: Report, Follower: "b", Result: []byte("rb")},
		{Kind: Report, Follower: "c", Result: []byte("rc")},
	}

	want, wantCmds := foldEvents(initial, base)

	for _, order := range permutations(base) {
		// Also duplicate the first event of this ordering (re-delivery),
		// replayed immediately before the rest of the sequence completes
		// the round, to check idempotence under permutation.
		withDup := append([]Event{order[0]}, order...)

		gotS, gotCmds := foldEvents(initial, order)
		if !reflect.DeepEqual(gotS, want) {
			t.Fatalf("order %+v: state = %+v, want %+v", order, gotS, want)
		}
		if !reflect.DeepEqual(gotCmds, wantCmds) {
			t.Fatalf("order %+v: cmds = %+v, want %+v", order, gotCmds, wantCmds)
		}

		gotDupS, gotDupCmds := foldEvents(initial, withDup)
		if !reflect.DeepEqual(gotDupS, want) {
			t.Fatalf("order+dup %+v: state = %+v, want %+v", withDup, gotDupS, want)
		}
		if !reflect.DeepEqual(gotDupCmds, wantCmds) {
			t.Fatalf("order+dup %+v: cmds = %+v, want %+v", withDup, gotDupCmds, wantCmds)
		}
	}
}
