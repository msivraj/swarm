package checkpoint

import (
	"reflect"
	"testing"
)

// -----------------------------------------------------------------------
// Due
// -----------------------------------------------------------------------

func TestDue(t *testing.T) {
	tests := []struct {
		name string
		step int
		k    int
		want bool
	}{
		{"step 0 under K>0 is a checkpoint step (decision F)", 0, 4, true},
		{"cadence hit at step%K==0", 8, 4, true},
		{"non-cadence step is not due", 5, 4, false},
		{"first step of a fresh cadence is not due", 1, 4, false},
		{"K==1 checkpoints every step", 3, 1, true},
		{"K<=0 is always false (K==0)", 6, 0, false},
		{"K<=0 is always false (K negative)", 6, -3, false},
		{"K<=0 is always false even at step 0", 0, 0, false},
		{"negative step, cadence hit", -8, 4, true},
		{"negative step, non-cadence", -5, 4, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Due(tt.step, tt.k)
			if got != tt.want {
				t.Fatalf("Due(%d, %d) = %v, want %v", tt.step, tt.k, got, tt.want)
			}
		})
	}
}

// TestDueIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestDueIsDeterministic(t *testing.T) {
	first := Due(12, 4)
	for i := 0; i < 100; i++ {
		if got := Due(12, 4); got != first {
			t.Fatalf("non-deterministic output on run %d: %v vs %v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// Snapshot / Restore — round trip
// -----------------------------------------------------------------------

// states enumerates a set of varied-shape States: nil vs empty vs populated
// Members/Meta/DriverBlob, zero and negative Step values.
func states() []State {
	return []State{
		{},
		{Step: 0, Members: nil, DriverBlob: nil, Meta: nil},
		{Step: 7, Members: []string{}, DriverBlob: []byte{}, Meta: map[string]string{}},
		{Step: 42, Members: []string{"a"}, DriverBlob: []byte{1, 2, 3}, Meta: map[string]string{"k": "v"}},
		{
			Step:       -3,
			Members:    []string{"node-1", "node-2", "node-3"},
			DriverBlob: []byte("opaque driver bytes \x00\xff"),
			Meta: map[string]string{
				"z": "last",
				"a": "first",
				"m": "middle",
			},
		},
		{Step: 1000000, Members: []string{""}, DriverBlob: nil, Meta: map[string]string{"": ""}},
		{Step: 5, DriverBlob: make([]byte, 1024)}, // large zeroed blob
	}
}

// TestRestoreSnapshotRoundTrip is the headline property test: for arbitrary
// State shapes, Restore(Snapshot(s)) == s.
func TestRestoreSnapshotRoundTrip(t *testing.T) {
	for i, s := range states() {
		got := Restore(Snapshot(s))
		if !reflect.DeepEqual(got, s) {
			t.Fatalf("case %d: Restore(Snapshot(s)) = %+v, want %+v", i, got, s)
		}
	}
}

// TestRestoreSnapshotRoundTripVaried widens the round-trip check across many
// programmatically varied shapes (membership sizes, blob sizes, meta key
// counts) — a broader sweep than the fixed table above.
func TestRestoreSnapshotRoundTripVaried(t *testing.T) {
	for n := 0; n < 20; n++ {
		members := make([]string, n)
		for i := range members {
			members[i] = string(rune('a' + i%26))
		}
		blob := make([]byte, n*3)
		for i := range blob {
			blob[i] = byte(i * 7)
		}
		var meta map[string]string
		if n > 0 {
			meta = make(map[string]string, n)
			for i := 0; i < n; i++ {
				meta[string(rune('A'+i%26))] = string(rune('0' + i%10))
			}
		}

		s := State{Step: n*n - 10, Members: members, DriverBlob: blob, Meta: meta}
		got := Restore(Snapshot(s))
		if !reflect.DeepEqual(got, s) {
			t.Fatalf("n=%d: Restore(Snapshot(s)) = %+v, want %+v", n, got, s)
		}
	}
}

// TestRestoreMalformedInputIsZeroState asserts Restore degrades to the zero
// State on undecodable bytes rather than panicking.
func TestRestoreMalformedInputIsZeroState(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{"nil bytes", nil},
		{"empty bytes", []byte{}},
		{"garbage bytes", []byte("not json at all")},
		{"truncated json", []byte(`{"step":`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Restore(tt.in)
			if !reflect.DeepEqual(got, State{}) {
				t.Fatalf("Restore(%q) = %+v, want zero State", tt.in, got)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Snapshot — determinism
// -----------------------------------------------------------------------

// TestSnapshotIsDeterministic guards the core's defining property: repeated
// Snapshot calls on the same State produce byte-identical output, including
// stable ordering for a State whose Meta has multiple keys (no
// encoding/json map nondeterminism leaking through).
func TestSnapshotIsDeterministic(t *testing.T) {
	s := State{
		Step:       9,
		Members:    []string{"x", "y", "z"},
		DriverBlob: []byte{9, 8, 7, 6},
		Meta: map[string]string{
			"zeta":  "26",
			"alpha": "1",
			"mu":    "13",
			"beta":  "2",
			"nu":    "14",
		},
	}

	first := Snapshot(s)
	for i := 0; i < 100; i++ {
		got := Snapshot(s)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %q vs %q", i, got, first)
		}
	}
}

// TestSnapshotStableAcrossMetaConstructionOrder asserts that two States that
// differ only in the order their (identical) Meta map was built in produce
// byte-identical Snapshot output — map keys are sorted on encode, so
// insertion order never leaks into the wire format.
func TestSnapshotStableAcrossMetaConstructionOrder(t *testing.T) {
	a := State{Step: 1, Meta: map[string]string{}}
	a.Meta["zeta"] = "26"
	a.Meta["alpha"] = "1"
	a.Meta["mu"] = "13"

	b := State{Step: 1, Meta: map[string]string{}}
	b.Meta["mu"] = "13"
	b.Meta["zeta"] = "26"
	b.Meta["alpha"] = "1"

	got := Snapshot(a)
	want := Snapshot(b)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot varied by Meta construction order: %q vs %q", got, want)
	}
}
