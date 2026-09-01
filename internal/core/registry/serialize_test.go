package registry

import (
	"encoding/gob"
	"reflect"
	"sort"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// roundTrip runs reg through GobEncode then GobDecode and returns the
// result, failing the test on any encode/decode error.
func roundTrip(t *testing.T, reg Registry) Registry {
	t.Helper()
	data, err := reg.GobEncode()
	if err != nil {
		t.Fatalf("GobEncode: %v", err)
	}
	var out Registry
	if err := out.GobDecode(data); err != nil {
		t.Fatalf("GobDecode: %v", err)
	}
	return out
}

// sortedViews sorts views by CellID for comparisons where map-iteration
// nondeterminism inside GobEncode's intermediate agent slices could
// otherwise reorder equal-value output — Snapshot already sorts, but this
// keeps the helper resilient if that ever changes.
func sortedViews(views []model.CellView) []model.CellView {
	out := append([]model.CellView(nil), views...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func TestSerializeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		reg  func() Registry
	}{
		{
			name: "empty registry",
			reg:  func() Registry { return Registry{} },
		},
		{
			name: "single cell no agents",
			reg: func() Registry {
				reg := Registry{}
				reg, _ = Apply(reg, RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 5})
				return reg
			},
		},
		{
			name: "single cell with agents",
			reg: func() Registry {
				reg := Registry{}
				reg, _ = Apply(reg, RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 5})
				reg, _ = Apply(reg, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"})
				reg, _ = Apply(reg, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "y"})
				return reg
			},
		},
		{
			name: "multi-cell multi-agent",
			reg: func() Registry {
				reg := Registry{}
				reg, _ = Apply(reg, RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 5})
				reg, _ = Apply(reg, RegistryEvent{Kind: CellUp, Cell: "b", Capacity: 3})
				reg, _ = Apply(reg, RegistryEvent{Kind: CellUp, Cell: "c", Capacity: 10})
				reg, _ = Apply(reg, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"})
				reg, _ = Apply(reg, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "y"})
				reg, _ = Apply(reg, RegistryEvent{Kind: AgentJoined, Cell: "b", Agent: "z"})
				reg, _ = Apply(reg, RegistryEvent{Kind: AgentJoined, Cell: "c", Agent: "x"})
				return reg
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := tt.reg()
			decoded := roundTrip(t, orig)

			wantView := sortedViews(Snapshot(orig))
			gotView := sortedViews(Snapshot(decoded))
			if !reflect.DeepEqual(wantView, gotView) {
				t.Fatalf("Snapshot mismatch after round-trip: got %+v, want %+v", gotView, wantView)
			}
		})
	}
}

// TestSerializeRoundTripPreservesAgentIdentity is the property the ticket
// calls for: decode(encode(reg)) must behave identically to reg under a
// SUBSEQUENT Apply, not merely report the same Size/Free. An AgentJoined for
// an already-present agent is a no-op (see Apply's doc); an AgentLeft for a
// present agent succeeds. Both only resolve correctly if the decoded
// Registry actually carries the same per-agent membership, not a
// reconstruction that merely matches the derived counts.
func TestSerializeRoundTripPreservesAgentIdentity(t *testing.T) {
	orig := Registry{}
	orig, _ = Apply(orig, RegistryEvent{Kind: CellUp, Cell: "a", Capacity: 5})
	orig, _ = Apply(orig, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"})
	orig, _ = Apply(orig, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "y"})

	decoded := roundTrip(t, orig)

	// Re-joining an already-member agent is a no-op on the original —
	// GobDecode's reconstruction must agree.
	origAfterRejoin, origChanges := Apply(orig, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"})
	decodedAfterRejoin, decodedChanges := Apply(decoded, RegistryEvent{Kind: AgentJoined, Cell: "a", Agent: "x"})
	if !reflect.DeepEqual(origChanges, decodedChanges) {
		t.Fatalf("AgentJoined-for-existing-member changes differ: orig=%+v decoded=%+v", origChanges, decodedChanges)
	}
	if !reflect.DeepEqual(sortedViews(Snapshot(origAfterRejoin)), sortedViews(Snapshot(decodedAfterRejoin))) {
		t.Fatalf("Snapshot after no-op rejoin differs between orig and decoded")
	}

	// Leaving a member present in both must produce identical Changes and
	// resulting views — this only holds if "y" round-tripped as an actual
	// member, not a synthesized filler agent.
	origAfterLeave, origLeaveChanges := Apply(orig, RegistryEvent{Kind: AgentLeft, Cell: "a", Agent: "y"})
	decodedAfterLeave, decodedLeaveChanges := Apply(decoded, RegistryEvent{Kind: AgentLeft, Cell: "a", Agent: "y"})
	if !reflect.DeepEqual(origLeaveChanges, decodedLeaveChanges) {
		t.Fatalf("AgentLeft changes differ: orig=%+v decoded=%+v", origLeaveChanges, decodedLeaveChanges)
	}
	if !reflect.DeepEqual(sortedViews(Snapshot(origAfterLeave)), sortedViews(Snapshot(decodedAfterLeave))) {
		t.Fatalf("Snapshot after AgentLeft differs between orig and decoded")
	}

	// A non-member leaving is a no-op on both — proves decoded doesn't carry
	// a phantom agent that isn't in the original.
	_, origNoopChanges := Apply(orig, RegistryEvent{Kind: AgentLeft, Cell: "a", Agent: "never-joined"})
	_, decodedNoopChanges := Apply(decoded, RegistryEvent{Kind: AgentLeft, Cell: "a", Agent: "never-joined"})
	if origNoopChanges != nil || decodedNoopChanges != nil {
		t.Fatalf("expected no-op AgentLeft to produce nil changes: orig=%+v decoded=%+v", origNoopChanges, decodedNoopChanges)
	}
}

// TestSerializeGobEncodeIsRegisteredGobEncoder is a compile/interface check:
// Registry must satisfy gob.GobEncoder/GobDecoder so a caller embedding it
// in a larger gob-encoded value (as the FDB adapter does) gets the custom
// projection instead of gob's default (which would fail on the unexported
// fields).
func TestSerializeGobEncodeIsRegisteredGobEncoder(t *testing.T) {
	var _ gob.GobEncoder = Registry{}
	var _ gob.GobDecoder = (*Registry)(nil)
}

func TestSerializeEmptyRegistryDecodesToEmpty(t *testing.T) {
	decoded := roundTrip(t, Registry{})
	if got := Snapshot(decoded); got != nil {
		t.Fatalf("expected empty registry to decode with nil Snapshot, got %+v", got)
	}
}
