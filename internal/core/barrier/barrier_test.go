package barrier

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func members(ids ...string) []WorkerID {
	out := make([]WorkerID, len(ids))
	for i, id := range ids {
		out[i] = WorkerID(id)
	}
	return out
}

func partials(pairs ...any) map[WorkerID][]byte {
	if len(pairs) == 0 {
		return nil
	}
	out := make(map[WorkerID][]byte, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out[WorkerID(pairs[i].(string))] = []byte(pairs[i+1].(string))
	}
	return out
}

// -----------------------------------------------------------------------
// step — table-driven, one case per rule and resolved edge case
// -----------------------------------------------------------------------

func TestStep(t *testing.T) {
	tests := []struct {
		name     string
		s        State
		ev       Event
		wantS    State
		wantCmds []Command
	}{
		{
			name: "Done that does not yet complete the step",
			s:    State{Step: 0, K: 0, Members: members("a", "b")},
			ev:   Event{Kind: Done, Worker: "a", Partial: []byte("pa")},
			wantS: State{Step: 0, K: 0, Members: members("a", "b"),
				Partials: partials("a", "pa")},
			wantCmds: nil,
		},
		{
			name: "the completing Done, no checkpoint (K disabled)",
			s: State{Step: 0, K: 0, Members: members("a", "b"),
				Partials: partials("a", "pa")},
			ev: Event{Kind: Done, Worker: "b", Partial: []byte("pb")},
			wantS: State{Step: 1, K: 0, Members: members("a", "b"),
				Partials: nil},
			wantCmds: []Command{
				{Op: AllReduce, Partials: partials("a", "pa", "b", "pb")},
				{Op: Release, Step: 1},
			},
		},
		{
			name: "the completing Done on a cadence step: AllReduce, Checkpoint, Release in order",
			s: State{Step: 0, K: 2, Members: members("a", "b"),
				Partials: partials("a", "pa")},
			ev: Event{Kind: Done, Worker: "b", Partial: []byte("pb")},
			wantS: State{Step: 1, K: 2, Members: members("a", "b"),
				Partials: nil, LastCheckpoint: Checkpoint{Step: 0}},
			wantCmds: []Command{
				{Op: AllReduce, Partials: partials("a", "pa", "b", "pb")},
				{Op: CheckpointOp, Step: 0},
				{Op: Release, Step: 1},
			},
		},
		{
			name: "duplicate Done overwrites and emits nothing new",
			s: State{Step: 0, K: 0, Members: members("a", "b"),
				Partials: partials("a", "pa-first")},
			ev: Event{Kind: Done, Worker: "a", Partial: []byte("pa-second")},
			wantS: State{Step: 0, K: 0, Members: members("a", "b"),
				Partials: partials("a", "pa-second")},
			wantCmds: nil,
		},
		{
			name: "Done from a non-member is ignored",
			s:    State{Step: 0, K: 0, Members: members("a", "b")},
			ev:   Event{Kind: Done, Worker: "c", Partial: []byte("pc")},
			wantS: State{Step: 0, K: 0, Members: members("a", "b"),
				Partials: nil},
			wantCmds: nil,
		},
		{
			name: "Deadline with stragglers and no survivors: Evicts only, no Release",
			s:    State{Step: 3, K: 2, Members: members("a", "b")},
			ev:   Event{Kind: Deadline},
			wantS: State{Step: 3, K: 2, Members: nil,
				Partials: nil},
			wantCmds: []Command{
				{Op: Evict, Worker: "a"},
				{Op: Evict, Worker: "b"},
			},
		},
		{
			name: "Deadline evicts stragglers then completes on the survivors (decision C)",
			s: State{Step: 5, K: 0, Members: members("a", "b", "c"),
				Partials: partials("a", "pa")},
			ev: Event{Kind: Deadline},
			wantS: State{Step: 6, K: 0, Members: members("a"),
				Partials: nil},
			wantCmds: []Command{
				{Op: Evict, Worker: "b"},
				{Op: Evict, Worker: "c"},
				{Op: AllReduce, Partials: partials("a", "pa")},
				{Op: Release, Step: 6},
			},
		},
		{
			name: "Deadline eviction-completion on a cadence step also checkpoints",
			s: State{Step: 4, K: 2, Members: members("a", "b"),
				Partials: partials("a", "pa")},
			ev: Event{Kind: Deadline},
			wantS: State{Step: 5, K: 2, Members: members("a"),
				Partials: nil, LastCheckpoint: Checkpoint{Step: 4}},
			wantCmds: []Command{
				{Op: Evict, Worker: "b"},
				{Op: AllReduce, Partials: partials("a", "pa")},
				{Op: CheckpointOp, Step: 4},
				{Op: Release, Step: 5},
			},
		},
		{
			name:     "Deadline on empty membership is a no-op",
			s:        State{Step: 2, K: 3, Members: nil},
			ev:       Event{Kind: Deadline},
			wantS:    State{Step: 2, K: 3, Members: nil},
			wantCmds: nil,
		},
		{
			name: "Lost rolls back to the last checkpoint and drops the member",
			s: State{Step: 15, K: 5, Members: members("a", "b", "c"),
				Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 10}},
			ev: Event{Kind: Lost, Worker: "b"},
			wantS: State{Step: 10, K: 5, Members: members("a", "c"),
				Partials: nil, LastCheckpoint: Checkpoint{Step: 10}},
			wantCmds: []Command{{Op: Rollback, Ckpt: Checkpoint{Step: 10}}},
		},
		{
			name: "Lost with no checkpoint yet rolls back to genesis (step 0)",
			s: State{Step: 3, K: 2, Members: members("a", "b"),
				Partials: partials("a", "pa")},
			ev: Event{Kind: Lost, Worker: "a"},
			wantS: State{Step: 0, K: 2, Members: members("b"),
				Partials: nil, LastCheckpoint: Checkpoint{Step: 0}},
			wantCmds: []Command{{Op: Rollback, Ckpt: Checkpoint{Step: 0}}},
		},
		{
			name: "Restored resets Step and LastCheckpoint, clears Partials, emits no command",
			s: State{Step: 7, K: 3, Members: members("a", "b"),
				Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 3}},
			ev: Event{Kind: Restored, Ckpt: Checkpoint{Step: 12}},
			wantS: State{Step: 12, K: 3, Members: members("a", "b"),
				Partials: nil, LastCheckpoint: Checkpoint{Step: 12}},
			wantCmds: nil,
		},
		{
			name: "K<=0 never checkpoints even at step%K==0-shaped steps",
			s:    State{Step: 0, K: 0, Members: members("a")},
			ev:   Event{Kind: Done, Worker: "a", Partial: []byte("pa")},
			wantS: State{Step: 1, K: 0, Members: members("a"),
				Partials: nil},
			wantCmds: []Command{
				{Op: AllReduce, Partials: partials("a", "pa")},
				{Op: Release, Step: 1},
			},
		},
		{
			name: "Lost from a non-member still rolls back (membership unaffected by removing an absent worker)",
			s: State{Step: 8, K: 4, Members: members("a", "b"),
				Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 4}},
			ev: Event{Kind: Lost, Worker: "ghost"},
			wantS: State{Step: 4, K: 4, Members: members("a", "b"),
				Partials: nil, LastCheckpoint: Checkpoint{Step: 4}},
			wantCmds: []Command{{Op: Rollback, Ckpt: Checkpoint{Step: 4}}},
		},
		{
			name: "negative K also never checkpoints",
			s:    State{Step: 0, K: -5, Members: members("a")},
			ev:   Event{Kind: Done, Worker: "a", Partial: []byte("pa")},
			wantS: State{Step: 1, K: -5, Members: members("a"),
				Partials: nil},
			wantCmds: []Command{
				{Op: AllReduce, Partials: partials("a", "pa")},
				{Op: Release, Step: 1},
			},
		},

		// -- issue #59: MinMembers floor -------------------------------

		{
			name: "the completing Done under the floor stalls: Rollback + Stall, no AllReduce",
			s: State{Step: 3, K: 2, Members: members("a", "b"),
				Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 2},
				MinMembers: 3},
			ev: Event{Kind: Done, Worker: "b", Partial: []byte("pb")},
			wantS: State{Step: 2, K: 2, Members: members("a", "b"),
				Partials: nil, LastCheckpoint: Checkpoint{Step: 2}, MinMembers: 3},
			wantCmds: []Command{
				{Op: Rollback, Ckpt: Checkpoint{Step: 2}},
				{Op: Stall, Have: 2, Need: 3},
			},
		},
		{
			name: "the completing Done at exactly the floor completes normally",
			s: State{Step: 0, K: 0, Members: members("a", "b"),
				Partials: partials("a", "pa"), MinMembers: 2},
			ev: Event{Kind: Done, Worker: "b", Partial: []byte("pb")},
			wantS: State{Step: 1, K: 0, Members: members("a", "b"),
				Partials: nil, MinMembers: 2},
			wantCmds: []Command{
				{Op: AllReduce, Partials: partials("a", "pa", "b", "pb")},
				{Op: Release, Step: 1},
			},
		},
		{
			name: "Deadline evicting stragglers to below the floor: Evicts + Rollback + Stall, never AllReduce",
			s: State{Step: 5, K: 0, Members: members("a", "b", "c"),
				Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 3},
				MinMembers: 2},
			ev: Event{Kind: Deadline},
			wantS: State{Step: 3, K: 0, Members: members("a"),
				Partials: nil, LastCheckpoint: Checkpoint{Step: 3}, MinMembers: 2},
			wantCmds: []Command{
				{Op: Evict, Worker: "b"},
				{Op: Evict, Worker: "c"},
				{Op: Rollback, Ckpt: Checkpoint{Step: 3}},
				{Op: Stall, Have: 1, Need: 2},
			},
		},
		{
			name: "Deadline evicting stragglers to exactly the floor completes normally",
			s: State{Step: 5, K: 0, Members: members("a", "b", "c"),
				Partials: partials("a", "pa", "b", "pb"), MinMembers: 2},
			ev: Event{Kind: Deadline},
			wantS: State{Step: 6, K: 0, Members: members("a", "b"),
				Partials: nil, MinMembers: 2},
			wantCmds: []Command{
				{Op: Evict, Worker: "c"},
				{Op: AllReduce, Partials: partials("a", "pa", "b", "pb")},
				{Op: Release, Step: 6},
			},
		},
		{
			name: "Deadline to zero survivors under a configured floor also stalls (extends decision D)",
			s: State{Step: 5, K: 0, Members: members("a", "b"),
				LastCheckpoint: Checkpoint{Step: 1}, MinMembers: 2},
			ev: Event{Kind: Deadline},
			wantS: State{Step: 1, K: 0, Members: nil,
				Partials: nil, LastCheckpoint: Checkpoint{Step: 1}, MinMembers: 2},
			wantCmds: []Command{
				{Op: Evict, Worker: "a"},
				{Op: Evict, Worker: "b"},
				{Op: Rollback, Ckpt: Checkpoint{Step: 1}},
				{Op: Stall, Have: 0, Need: 2},
			},
		},
		{
			name: "GiveUp emits a single Fail{LastCheckpoint} and marks the state terminal",
			s: State{Step: 5, K: 2, Members: members("a"),
				Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 4},
				MinMembers: 3},
			ev: Event{Kind: GiveUp},
			wantS: State{Step: 5, K: 2, Members: members("a"),
				Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 4},
				MinMembers: 3, Failed: true},
			wantCmds: []Command{{Op: Fail, Ckpt: Checkpoint{Step: 4}}},
		},
		{
			name: "GiveUp with no checkpoint yet preserves the zero checkpoint",
			s:    State{Step: 0, K: 0, Members: members("a", "b")},
			ev:   Event{Kind: GiveUp},
			wantS: State{Step: 0, K: 0, Members: members("a", "b"),
				Failed: true},
			wantCmds: []Command{{Op: Fail, Ckpt: Checkpoint{}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotS, gotCmds := step(tt.s, tt.ev, model.Instant(0))
			if !reflect.DeepEqual(gotS, tt.wantS) {
				t.Fatalf("step() state = %+v, want %+v", gotS, tt.wantS)
			}
			if !reflect.DeepEqual(gotCmds, tt.wantCmds) {
				t.Fatalf("step() cmds = %+v, want %+v", gotCmds, tt.wantCmds)
			}
		})
	}
}

// TestStepUnknownEventKindIsNoop asserts an EventKind outside the sum type's
// four variants is a safe no-op — a pure core must never panic on
// unexpected input.
func TestStepUnknownEventKindIsNoop(t *testing.T) {
	s := State{Step: 1, K: 2, Members: members("a"), Partials: partials("a", "pa")}
	gotS, gotCmds := step(s, Event{Kind: EventKind(99)}, 0)
	if !reflect.DeepEqual(gotS, s) {
		t.Fatalf("step() with unknown EventKind changed state: got %+v, want unchanged %+v", gotS, s)
	}
	if gotCmds != nil {
		t.Fatalf("step() with unknown EventKind emitted commands: %+v, want none", gotCmds)
	}
}

// TestStepExportedMatchesUnexported asserts Step is exactly step's exported
// entry point — same output for the same input — mirroring
// internal/core/routing's Decide/route split.
func TestStepExportedMatchesUnexported(t *testing.T) {
	s := State{Step: 0, K: 2, Members: members("a", "b"), Partials: partials("a", "pa")}
	ev := Event{Kind: Done, Worker: "b", Partial: []byte("pb")}

	wantS, wantCmds := step(s, ev, model.Instant(7))
	gotS, gotCmds := Step(s, ev, model.Instant(7))

	if !reflect.DeepEqual(gotS, wantS) || !reflect.DeepEqual(gotCmds, wantCmds) {
		t.Fatalf("Step() = %+v, %+v, want %+v, %+v (step()'s own output)", gotS, gotCmds, wantS, wantCmds)
	}
}

// TestStepIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestStepIsDeterministic(t *testing.T) {
	s := State{Step: 4, K: 2, Members: members("a", "b", "c"),
		Partials: partials("a", "pa", "b", "pb")}
	ev := Event{Kind: Done, Worker: "c", Partial: []byte("pc")}

	firstS, firstCmds := step(s, ev, model.Instant(42))
	for i := 0; i < 100; i++ {
		gotS, gotCmds := step(s, ev, model.Instant(42))
		if !reflect.DeepEqual(gotS, firstS) || !reflect.DeepEqual(gotCmds, firstCmds) {
			t.Fatalf("non-deterministic output on run %d: state=%+v cmds=%+v, want state=%+v cmds=%+v",
				i, gotS, gotCmds, firstS, firstCmds)
		}
	}
}

// TestStepDoesNotMutateInputPartials ensures step never mutates a Partials
// map the caller still holds — copy-on-write, the same discipline routing
// and mitosis follow.
func TestStepDoesNotMutateInputPartials(t *testing.T) {
	before := partials("a", "pa")
	s := State{Step: 0, K: 0, Members: members("a", "b"), Partials: before}
	beforeCopy := partials("a", "pa")

	_, _ = step(s, Event{Kind: Done, Worker: "b", Partial: []byte("pb")}, 0)

	if !reflect.DeepEqual(before, beforeCopy) {
		t.Fatalf("step mutated its input Partials map: got %+v, want unchanged %+v", before, beforeCopy)
	}
}

// -----------------------------------------------------------------------
// B1 — a straggler still not-Done at Deadline is ALWAYS evicted
// -----------------------------------------------------------------------

// doneSubsets enumerates every subset of ws (as a bitmask, deterministic —
// no math/rand) so B1 is checked against every possible Done/not-Done split
// of the membership.
func doneSubsets(ws []WorkerID) [][]WorkerID {
	n := len(ws)
	var out [][]WorkerID
	for mask := 0; mask < (1 << n); mask++ {
		var subset []WorkerID
		for i, w := range ws {
			if mask&(1<<i) != 0 {
				subset = append(subset, w)
			}
		}
		out = append(out, subset)
	}
	return out
}

func TestDeadlineAlwaysEvictsExactlyTheNotDone(t *testing.T) {
	all := members("a", "b", "c", "d")

	for _, done := range doneSubsets(all) {
		doneSet := map[WorkerID]bool{}
		p := map[WorkerID][]byte{}
		for _, w := range done {
			doneSet[w] = true
			p[w] = []byte("payload-" + string(w))
		}

		s := State{Step: 9, K: 1000, Members: append([]WorkerID{}, all...), Partials: p}
		_, cmds := step(s, Event{Kind: Deadline}, 0)

		evicted := map[WorkerID]bool{}
		for _, c := range cmds {
			if c.Op != Evict {
				continue
			}
			if doneSet[c.Worker] {
				t.Fatalf("done subset %+v: Evict emitted for %q, which reported Done", done, c.Worker)
			}
			evicted[c.Worker] = true
		}
		for _, w := range all {
			if !doneSet[w] && !evicted[w] {
				t.Fatalf("done subset %+v: straggler %q was not evicted", done, w)
			}
		}
	}
}

// -----------------------------------------------------------------------
// B2 — a Checkpoint is emitted on a completing step iff Step%K==0 (K>0)
// -----------------------------------------------------------------------

func TestCheckpointOnlyOnCadence(t *testing.T) {
	for k := -2; k <= 5; k++ {
		for n := 0; n <= 10; n++ {
			s := State{Step: n, K: k, Members: members("a", "b"),
				Partials: partials("a", "pa")}
			_, cmds := step(s, Event{Kind: Done, Worker: "b", Partial: []byte("pb")}, 0)

			gotCheckpoint := false
			var gotStep int
			for _, c := range cmds {
				if c.Op == CheckpointOp {
					gotCheckpoint = true
					gotStep = c.Step
				}
			}

			want := k > 0 && n%k == 0
			if gotCheckpoint != want {
				t.Fatalf("K=%d Step=%d: CheckpointOp emitted=%v, want %v", k, n, gotCheckpoint, want)
			}
			if gotCheckpoint && gotStep != n {
				t.Fatalf("K=%d Step=%d: CheckpointOp carries step %d, want the completed step %d", k, n, gotStep, n)
			}
		}
	}
}

// -----------------------------------------------------------------------
// Order-tolerance — the same set of Done reports for a step, in any
// permutation (and with a duplicate), yields the same released state and
// the same AllReduce partial set.
// -----------------------------------------------------------------------

// permutations returns every ordering of xs, via Heap's algorithm — a
// deterministic enumeration (no randomness), matching routing_test.go's
// approach.
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

// foldEvents applies evs in order via step, starting from s, and returns the
// final state plus every non-nil command emitted along the way.
func foldEvents(s State, evs []Event) (State, []Command) {
	var all []Command
	for _, ev := range evs {
		var cmds []Command
		s, cmds = step(s, ev, 0)
		all = append(all, cmds...)
	}
	return s, all
}

func TestStepOrderTolerantWithinAStep(t *testing.T) {
	initial := State{Step: 0, K: 2, Members: members("a", "b", "c")}
	base := []Event{
		{Kind: Done, Worker: "a", Partial: []byte("pa")},
		{Kind: Done, Worker: "b", Partial: []byte("pb")},
		{Kind: Done, Worker: "c", Partial: []byte("pc")},
	}

	want, wantCmds := foldEvents(initial, base)

	for _, order := range permutations(base) {
		// Also duplicate the first event of this ordering (re-delivery),
		// replayed immediately before the rest of the sequence completes
		// the step, to check idempotence under permutation.
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

// -----------------------------------------------------------------------
// issue #59 — MinMembers floor: property tests
//
// "A sub-floor fraction can never be all-reduced." These enumerate every
// possible survivor split (via doneSubsets, deterministically — no
// math/rand) and every floor from 0 (no floor) up past the full membership
// size, on both paths that can complete a step: the all-Done (stepDone) path
// and the Deadline-eviction (stepDeadline) path.
// -----------------------------------------------------------------------

// hasOp reports whether cmds contains a command with the given op.
func hasOp(cmds []Command, op CmdOp) bool {
	for _, c := range cmds {
		if c.Op == op {
			return true
		}
	}
	return false
}

func TestFloorPropertyDeadlineNeverAllReducesBelowFloor(t *testing.T) {
	all := members("a", "b", "c", "d")

	for minMembers := 0; minMembers <= len(all)+1; minMembers++ {
		for _, done := range doneSubsets(all) {
			p := map[WorkerID][]byte{}
			for _, w := range done {
				p[w] = []byte("payload-" + string(w))
			}

			s := State{Step: 9, K: 1000, Members: append([]WorkerID{}, all...),
				Partials: p, MinMembers: minMembers, LastCheckpoint: Checkpoint{Step: 3}}
			_, cmds := step(s, Event{Kind: Deadline}, 0)

			survivors := len(done)
			underFloor := minMembers > 0 && survivors < minMembers

			if underFloor && hasOp(cmds, AllReduce) {
				t.Fatalf("MinMembers=%d survivors=%d (done=%+v): AllReduce emitted below the floor: %+v",
					minMembers, survivors, done, cmds)
			}
			if underFloor && !hasOp(cmds, Rollback) {
				t.Fatalf("MinMembers=%d survivors=%d (done=%+v): no Rollback emitted below the floor: %+v",
					minMembers, survivors, done, cmds)
			}
			if underFloor && !hasOp(cmds, Stall) {
				t.Fatalf("MinMembers=%d survivors=%d (done=%+v): no Stall emitted below the floor: %+v",
					minMembers, survivors, done, cmds)
			}

			// At or above the floor with at least one survivor, the step
			// always completes (AllReduce present). Zero survivors with no
			// floor configured is decision D (no command), not a floor case.
			atOrAboveWithSurvivors := !underFloor && survivors > 0
			if atOrAboveWithSurvivors && !hasOp(cmds, AllReduce) {
				t.Fatalf("MinMembers=%d survivors=%d (done=%+v): step did not complete at/above the floor: %+v",
					minMembers, survivors, done, cmds)
			}
		}
	}
}

func TestFloorPropertyDoneNeverAllReducesBelowFloor(t *testing.T) {
	all := members("a", "b", "c", "d", "e")

	for minMembers := 0; minMembers <= len(all)+1; minMembers++ {
		for n := 1; n <= len(all); n++ {
			membership := append([]WorkerID{}, all[:n]...)

			p := map[WorkerID][]byte{}
			for _, w := range membership[:n-1] {
				p[w] = []byte("payload-" + string(w))
			}
			last := membership[n-1]

			s := State{Step: 2, K: 1000, Members: membership, Partials: p,
				MinMembers: minMembers, LastCheckpoint: Checkpoint{Step: 1}}
			_, cmds := step(s, Event{Kind: Done, Worker: last, Partial: []byte("p-last")}, 0)

			underFloor := minMembers > 0 && n < minMembers

			if underFloor {
				if hasOp(cmds, AllReduce) {
					t.Fatalf("MinMembers=%d n=%d: AllReduce emitted below the floor: %+v", minMembers, n, cmds)
				}
				if !hasOp(cmds, Rollback) || !hasOp(cmds, Stall) {
					t.Fatalf("MinMembers=%d n=%d: missing Rollback/Stall below the floor: %+v", minMembers, n, cmds)
				}
				continue
			}
			if !hasOp(cmds, AllReduce) {
				t.Fatalf("MinMembers=%d n=%d: step did not complete at/above the floor: %+v", minMembers, n, cmds)
			}
		}
	}
}

// TestFloorZeroReproducesSpikeExactly is the regression guard from the
// ticket: MinMembers==0 must behave exactly like the merged spike (which had
// no floor field at all) across every survivor split at Deadline.
func TestFloorZeroReproducesSpikeExactly(t *testing.T) {
	all := members("a", "b", "c", "d")

	for _, done := range doneSubsets(all) {
		p := map[WorkerID][]byte{}
		for _, w := range done {
			p[w] = []byte("payload-" + string(w))
		}

		withFloor := State{Step: 9, K: 1000, Members: append([]WorkerID{}, all...), Partials: p, MinMembers: 0}
		withoutFloorField := State{Step: 9, K: 1000, Members: append([]WorkerID{}, all...), Partials: p}

		gotS, gotCmds := step(withFloor, Event{Kind: Deadline}, 0)
		wantS, wantCmds := step(withoutFloorField, Event{Kind: Deadline}, 0)

		if !reflect.DeepEqual(gotS, wantS) || !reflect.DeepEqual(gotCmds, wantCmds) {
			t.Fatalf("done=%+v: MinMembers=0 diverged from the zero-value State: state=%+v cmds=%+v, want state=%+v cmds=%+v",
				done, gotS, gotCmds, wantS, wantCmds)
		}
	}
}

// -----------------------------------------------------------------------
// issue #59 — determinism under the new Stall/GiveUp branches
// -----------------------------------------------------------------------

func TestStepIsDeterministicUnderFloorStall(t *testing.T) {
	s := State{Step: 5, K: 0, Members: members("a", "b", "c"),
		Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 3}, MinMembers: 2}
	ev := Event{Kind: Deadline}

	firstS, firstCmds := step(s, ev, model.Instant(11))
	for i := 0; i < 100; i++ {
		gotS, gotCmds := step(s, ev, model.Instant(11))
		if !reflect.DeepEqual(gotS, firstS) || !reflect.DeepEqual(gotCmds, firstCmds) {
			t.Fatalf("non-deterministic output on run %d: state=%+v cmds=%+v, want state=%+v cmds=%+v",
				i, gotS, gotCmds, firstS, firstCmds)
		}
	}
}

func TestStepIsDeterministicUnderGiveUp(t *testing.T) {
	s := State{Step: 5, K: 2, Members: members("a"),
		Partials: partials("a", "pa"), LastCheckpoint: Checkpoint{Step: 4}, MinMembers: 3}
	ev := Event{Kind: GiveUp}

	firstS, firstCmds := step(s, ev, model.Instant(3))
	for i := 0; i < 100; i++ {
		gotS, gotCmds := step(s, ev, model.Instant(3))
		if !reflect.DeepEqual(gotS, firstS) || !reflect.DeepEqual(gotCmds, firstCmds) {
			t.Fatalf("non-deterministic output on run %d: state=%+v cmds=%+v, want state=%+v cmds=%+v",
				i, gotS, gotCmds, firstS, firstCmds)
		}
	}
}
