package placement

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestPlace(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	cell := func(id string, free int) model.CellView {
		return model.CellView{ID: model.CellID(id), Free: free}
	}

	tests := []struct {
		name  string
		cells []model.CellView
		want  Placement
	}{
		{
			name:  "empty slice returns NoCapacity",
			cells: nil,
			want:  Placement{Kind: NoCapacity},
		},
		{
			name:  "all cells full returns NoCapacity",
			cells: []model.CellView{cell("a", 0), cell("b", 0)},
			want:  Placement{Kind: NoCapacity},
		},
		{
			name:  "exactly one cell with capacity is assigned",
			cells: []model.CellView{cell("a", 0), cell("b", 3)},
			want:  Placement{Kind: Assign, Cell: "b"},
		},
		{
			name:  "multiple eligible cells picks first in slice order",
			cells: []model.CellView{cell("a", 0), cell("b", 5), cell("c", 7)},
			want:  Placement{Kind: Assign, Cell: "b"},
		},
		{
			name:  "first cell with free capacity is assigned",
			cells: []model.CellView{cell("a", 1), cell("b", 1)},
			want:  Placement{Kind: Assign, Cell: "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Place(task, tt.cells)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Place() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestPlaceIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestPlaceIsDeterministic(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	cells := []model.CellView{
		{ID: "x", Free: 0},
		{ID: "y", Free: 2},
		{ID: "z", Free: 4},
	}
	first := Place(task, cells)
	for i := 0; i < 100; i++ {
		if got := Place(task, cells); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
