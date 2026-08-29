package transport

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// TestCellAssignmentResponseRoundTrip is issue #101's acceptance: a
// CellAssignmentResponse (including its repeated peers) survives a proto
// marshal/unmarshal round-trip with every field intact.
func TestCellAssignmentResponseRoundTrip(t *testing.T) {
	orig := &CellAssignmentResponse{
		HasAssignment: true,
		JobId:         "job-7",
		WorkerId:      "w-2",
		ShardInput:    []byte{0x00, 0x01, 0x02, 0xff},
		K:             4,
		MinMembers:    3,
		Steps:         10,
		Bootstrap:     true,
		Peers: []*CellPeer{
			{AgentId: "a1", RaftAddr: "10.0.0.1:7000", CellLeaderAddr: "10.0.0.1:7001"},
			{AgentId: "a2", RaftAddr: "10.0.0.2:7000", CellLeaderAddr: "10.0.0.2:7001"},
			{AgentId: "a3", RaftAddr: "10.0.0.3:7000", CellLeaderAddr: "10.0.0.3:7001"},
		},
	}

	b, err := proto.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &CellAssignmentResponse{}
	if err := proto.Unmarshal(b, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(orig, got) {
		t.Fatalf("round-trip mismatch:\n orig=%v\n  got=%v", orig, got)
	}
	// Spot-check the peer set survived in order with all three fields.
	if len(got.Peers) != 3 || got.Peers[1].AgentId != "a2" || got.Peers[2].RaftAddr != "10.0.0.3:7000" {
		t.Fatalf("peers not preserved: %v", got.Peers)
	}
}

// TestJoinAgentRequestP2AddrsRoundTrip confirms the two new advertised
// addresses survive the wire and the P0/P1 fields are untouched.
func TestJoinAgentRequestP2AddrsRoundTrip(t *testing.T) {
	orig := &JoinAgentRequest{
		Agent:          "a1",
		Region:         "us-east",
		Caps:           5,
		RaftAddr:       "10.0.0.1:7000",
		CellLeaderAddr: "10.0.0.1:7001",
	}
	b, err := proto.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &JoinAgentRequest{}
	if err := proto.Unmarshal(b, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(orig, got) {
		t.Fatalf("round-trip mismatch: orig=%v got=%v", orig, got)
	}
	// A P0/P1 agent that leaves the new fields empty is unaffected.
	legacy := &JoinAgentRequest{Agent: "a2", Region: "eu-west", Caps: 3}
	lb, _ := proto.Marshal(legacy)
	lgot := &JoinAgentRequest{}
	if err := proto.Unmarshal(lb, lgot); err != nil {
		t.Fatalf("legacy Unmarshal: %v", err)
	}
	if lgot.RaftAddr != "" || lgot.CellLeaderAddr != "" {
		t.Fatalf("legacy request gained P2 addrs: %v", lgot)
	}
}
