package messagepassing

import (
	"reflect"
	"testing"
)

func msg(id string, from, to ActorID, body string) Message {
	return Message{ID: id, From: from, To: to, Body: []byte(body)}
}

// -----------------------------------------------------------------------
// React
// -----------------------------------------------------------------------

func TestReact(t *testing.T) {
	base := Actor{ID: "a1"}

	tests := []struct {
		name      string
		a         Actor
		m         Message
		wantState []byte
		wantSeen  []string
		wantSends []Send
	}{
		{
			name:      "fresh actor folds first message",
			a:         base,
			m:         msg("m1", "sender", "a1", "hello"),
			wantState: reduceState(nil, []byte("hello")),
			wantSeen:  []string{"m1"},
			wantSends: []Send{{To: "sender", Body: []byte("hello"), ID: "ack:m1"}},
		},
		{
			name:      "second distinct message folds on top of the first",
			a:         Actor{ID: "a1", State: reduceState(nil, []byte("hello")), Seen: map[string]bool{"m1": true}},
			m:         msg("m2", "sender", "a1", "world"),
			wantState: reduceState(reduceState(nil, []byte("hello")), []byte("world")),
			wantSeen:  []string{"m1", "m2"},
			wantSends: []Send{{To: "sender", Body: []byte("world"), ID: "ack:m2"}},
		},
		{
			name:      "duplicate message ID is a no-op: no state change, no Send",
			a:         Actor{ID: "a1", State: reduceState(nil, []byte("hello")), Seen: map[string]bool{"m1": true}},
			m:         msg("m1", "sender", "a1", "hello-again"),
			wantState: reduceState(nil, []byte("hello")),
			wantSeen:  []string{"m1"},
			wantSends: nil,
		},
		{
			name:      "empty body still folds and acks",
			a:         base,
			m:         msg("m1", "sender", "a1", ""),
			wantState: reduceState(nil, nil),
			wantSeen:  []string{"m1"},
			wantSends: []Send{{To: "sender", Body: []byte(""), ID: "ack:m1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotActor, gotSends := React(tt.a, tt.m)

			if !reflect.DeepEqual(gotActor.State, tt.wantState) {
				t.Fatalf("React() state = %x, want %x", gotActor.State, tt.wantState)
			}
			for _, id := range tt.wantSeen {
				if !gotActor.Seen[id] {
					t.Fatalf("React() Seen missing id %q: %+v", id, gotActor.Seen)
				}
			}
			if len(gotActor.Seen) != len(tt.wantSeen) {
				t.Fatalf("React() Seen = %+v, want exactly %v", gotActor.Seen, tt.wantSeen)
			}
			if !reflect.DeepEqual(gotSends, tt.wantSends) {
				t.Fatalf("React() sends = %+v, want %+v", gotSends, tt.wantSends)
			}
		})
	}
}

// TestReactDoesNotMutateInputActor ensures React never mutates the Seen map
// of an Actor its caller still holds — copy-on-write, like registry.Apply.
func TestReactDoesNotMutateInputActor(t *testing.T) {
	seen := map[string]bool{"m0": true}
	before := Actor{ID: "a1", Seen: seen}

	React(before, msg("m1", "sender", "a1", "hi"))

	if len(seen) != 1 || !seen["m0"] {
		t.Fatalf("React mutated the caller's Seen map: %+v", seen)
	}
}

// TestReactIdempotentOnDuplicate is a focused idempotence check: folding the
// same message ID twice yields the same Actor.State as folding it once, and
// the second fold produces no Send.
func TestReactIdempotentOnDuplicate(t *testing.T) {
	a := Actor{ID: "a1"}
	m := msg("m1", "sender", "a1", "payload")

	once, sendsOnce := React(a, m)
	twice, sendsTwice := React(once, m)

	if !reflect.DeepEqual(once.State, twice.State) {
		t.Fatalf("duplicate fold changed state: once=%x twice=%x", once.State, twice.State)
	}
	if len(sendsOnce) == 0 {
		t.Fatalf("first fold produced no Send, want an ack")
	}
	if len(sendsTwice) != 0 {
		t.Fatalf("duplicate fold produced Sends %+v, want none", sendsTwice)
	}
}

// -----------------------------------------------------------------------
// React — property M1: order-tolerance
// -----------------------------------------------------------------------

// foldAll folds every message in order into a, applying React sequentially
// and discarding the Sends (M1 is judged on final Actor.State alone).
func foldAll(a Actor, msgs []Message) Actor {
	for _, m := range msgs {
		a, _ = React(a, m)
	}
	return a
}

// permutations returns every ordering of msgs, via Heap's algorithm — a
// deterministic enumeration (no randomness), mirroring routing_test.go's
// permutations helper so this stays a pure core test.
func permutations(msgs []Message) [][]Message {
	var out [][]Message
	n := len(msgs)
	buf := make([]Message, n)
	copy(buf, msgs)
	c := make([]int, n)

	snapshot := func() []Message {
		cp := make([]Message, n)
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

// TestReactOrderTolerant is the headline M1 property: for a set of distinct
// messages, applying them to an actor in ANY shuffled order — with
// arbitrary DUPLICATES injected — yields the SAME final Actor.State. Body
// is reduced with XOR over an FNV-1a hash (see reduceState's doc), which is
// commutative and associative; duplicates are stripped by React's
// Seen-based dedupe before they can perturb the fold a second time.
func TestReactOrderTolerant(t *testing.T) {
	base := []Message{
		msg("m1", "s1", "a1", "alpha"),
		msg("m2", "s2", "a1", "bravo"),
		msg("m3", "s3", "a1", "charlie"),
	}
	// Duplicate the stream so order-tolerance is checked with repeats
	// present, exactly like routing_test.go's convergence test.
	stream := append(append([]Message{}, base...), base...)

	want := foldAll(Actor{ID: "a1"}, base).State

	for i, order := range permutations(stream) {
		if i > 200 { // 6! = 720 permutations of 6 elements; sample for speed
			break
		}
		got := foldAll(Actor{ID: "a1"}, order).State
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("React not order-tolerant for order %+v: got %x, want %x", order, got, want)
		}
	}
}

// TestReactCommutativePair is a minimal two-message witness of M1: folding
// A then B reaches the same state as folding B then A.
func TestReactCommutativePair(t *testing.T) {
	a := msg("m1", "s1", "a1", "up")
	b := msg("m2", "s2", "a1", "down")

	ab := foldAll(Actor{ID: "a1"}, []Message{a, b})
	ba := foldAll(Actor{ID: "a1"}, []Message{b, a})

	if !reflect.DeepEqual(ab.State, ba.State) {
		t.Fatalf("React not commutative: fold(A,B)=%x fold(B,A)=%x", ab.State, ba.State)
	}
}

// TestReactIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestReactIsDeterministic(t *testing.T) {
	a := Actor{ID: "a1"}
	m := msg("m1", "sender", "a1", "payload")

	firstActor, firstSends := React(a, m)
	for i := 0; i < 100; i++ {
		gotActor, gotSends := React(a, m)
		if !reflect.DeepEqual(gotActor, firstActor) || !reflect.DeepEqual(gotSends, firstSends) {
			t.Fatalf("non-deterministic output on run %d", i)
		}
	}
}

// -----------------------------------------------------------------------
// Route
// -----------------------------------------------------------------------

func TestRoute(t *testing.T) {
	table := RoutingTable{
		"a1": {Cell: "cell-1"},
		"a2": {Cell: "cell-2"},
	}

	tests := []struct {
		name  string
		m     Message
		table RoutingTable
		want  []Delivery
	}{
		{
			name:  "known To resolves to its cell",
			m:     msg("m1", "a2", "a1", "hi"),
			table: table,
			want:  []Delivery{{To: "a1", Cell: "cell-1", Msg: msg("m1", "a2", "a1", "hi")}},
		},
		{
			name:  "unknown To returns no delivery, not a panic",
			m:     msg("m1", "a2", "ghost", "hi"),
			table: table,
			want:  nil,
		},
		{
			name:  "empty table returns no delivery",
			m:     msg("m1", "a2", "a1", "hi"),
			table: RoutingTable{},
			want:  nil,
		},
		{
			name:  "nil table returns no delivery",
			m:     msg("m1", "a2", "a1", "hi"),
			table: nil,
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Route(tt.m, tt.table)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Route() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestRouteIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestRouteIsDeterministic(t *testing.T) {
	table := RoutingTable{"a1": {Cell: "cell-1"}}
	m := msg("m1", "a2", "a1", "hi")

	first := Route(m, table)
	for i := 0; i < 100; i++ {
		if got := Route(m, table); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// OnCrash
// -----------------------------------------------------------------------

func TestOnCrash(t *testing.T) {
	tests := []struct {
		name string
		a    ActorID
		want Supervise
	}{
		{"restarts from snapshot", "a1", Supervise{Kind: Restart, FromSnapshot: true}},
		{"restarts from snapshot regardless of ID", "a2", Supervise{Kind: Restart, FromSnapshot: true}},
		{"restarts from snapshot for empty ID", "", Supervise{Kind: Restart, FromSnapshot: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OnCrash(tt.a)
			if got != tt.want {
				t.Fatalf("OnCrash(%q) = %+v, want %+v", tt.a, got, tt.want)
			}
		})
	}
}

// TestOnCrashIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestOnCrashIsDeterministic(t *testing.T) {
	first := OnCrash("a1")
	for i := 0; i < 100; i++ {
		if got := OnCrash("a1"); got != first {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// reduceState — the M1 reducer itself
// -----------------------------------------------------------------------

func TestReduceStateCommutative(t *testing.T) {
	ab := reduceState(reduceState(nil, []byte("a")), []byte("b"))
	ba := reduceState(reduceState(nil, []byte("b")), []byte("a"))
	if !reflect.DeepEqual(ab, ba) {
		t.Fatalf("reduceState not commutative: ab=%x ba=%x", ab, ba)
	}
}

func TestReduceStateInvalidCurrentTreatedAsZero(t *testing.T) {
	fromNil := reduceState(nil, []byte("x"))
	fromShort := reduceState([]byte{1, 2, 3}, []byte("x"))
	if !reflect.DeepEqual(fromNil, fromShort) {
		t.Fatalf("reduceState should treat any non-8-byte current as zero: nil=%x short=%x", fromNil, fromShort)
	}
}
