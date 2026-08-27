package rendezvous

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestAdmitAgent(t *testing.T) {
	cell := func(id string, free int) model.CellView {
		return model.CellView{ID: model.CellID(id), Free: free}
	}

	tests := []struct {
		name string
		req  JoinReq
		reg  []model.CellView
		want AdmitDecision
	}{
		{
			name: "cell with free capacity accepts deterministic pick",
			req:  JoinReq{Agent: "a1", Caps: 1},
			reg:  []model.CellView{cell("x", 0), cell("y", 3)},
			want: AdmitDecision{Kind: Accept, Cell: "y"},
		},
		{
			name: "first eligible cell in slice order wins over a later one",
			req:  JoinReq{Agent: "a1", Caps: 1},
			reg:  []model.CellView{cell("x", 2), cell("y", 5)},
			want: AdmitDecision{Kind: Accept, Cell: "x"},
		},
		{
			name: "no cells at all forms a new cell",
			req:  JoinReq{Agent: "a1", Caps: 1},
			reg:  nil,
			want: AdmitDecision{Kind: NewCell},
		},
		{
			name: "all cells full forms a new cell",
			req:  JoinReq{Agent: "a1", Caps: 1},
			reg:  []model.CellView{cell("x", 0), cell("y", 0)},
			want: AdmitDecision{Kind: NewCell},
		},
		{
			name: "requested caps exceeding every cell's free space forms a new cell",
			req:  JoinReq{Agent: "a1", Caps: 5},
			reg:  []model.CellView{cell("x", 1), cell("y", 2)},
			want: AdmitDecision{Kind: NewCell},
		},
		{
			name: "empty agent identity is rejected regardless of cell state",
			req:  JoinReq{Agent: "", Caps: 1},
			reg:  []model.CellView{cell("x", 3)},
			want: AdmitDecision{Kind: Reject, Reason: "empty agent identity"},
		},
		{
			name: "zero caps request accepts a full cell",
			req:  JoinReq{Agent: "a1", Caps: 0},
			reg:  []model.CellView{cell("x", 0)},
			want: AdmitDecision{Kind: Accept, Cell: "x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AdmitAgent(tt.req, tt.reg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("AdmitAgent() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestAdmitAgentIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestAdmitAgentIsDeterministic(t *testing.T) {
	req := JoinReq{Agent: "a1", Region: "us-east", Caps: 2}
	reg := []model.CellView{
		{ID: "x", Free: 0},
		{ID: "y", Free: 2},
		{ID: "z", Free: 4},
	}
	first := AdmitAgent(req, reg)
	for i := 0; i < 100; i++ {
		if got := AdmitAgent(req, reg); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
