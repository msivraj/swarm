package mitosis

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestDecide(t *testing.T) {
	cfg := Thresholds{Target: 100, CooldownNS: 1000}
	cell := func(id string, size int) model.CellView {
		return model.CellView{ID: model.CellID(id), Size: size}
	}

	tests := []struct {
		name      string
		cells     []model.CellView
		cooldowns map[model.CellID]model.Instant
		now       model.Instant
		want      []Command
	}{
		{
			name:  "in band is a no-op",
			cells: []model.CellView{cell("a", 100), cell("b", 150)},
			want:  nil,
		},
		{
			name:  "over 2T splits",
			cells: []model.CellView{cell("a", 201)},
			want:  []Command{{Op: Split, Cell: "a"}},
		},
		{
			name:      "split suppressed during cooldown",
			cells:     []model.CellView{cell("a", 201)},
			cooldowns: map[model.CellID]model.Instant{"a": 500},
			now:       900, // 900-500 = 400 < 1000 window
			want:      nil,
		},
		{
			name:  "two under-full cells merge",
			cells: []model.CellView{cell("a", 20), cell("b", 30)},
			want:  []Command{{Op: Merge, Cell: "a", Other: "b"}},
		},
		{
			name:  "under-full but combined >= T does not merge",
			cells: []model.CellView{cell("a", 60), cell("b", 60)},
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Decide(tt.cells, cfg, tt.cooldowns, tt.now)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Decide() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestDecideIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestDecideIsDeterministic(t *testing.T) {
	cfg := Thresholds{Target: 50, CooldownNS: 0}
	cells := []model.CellView{{ID: "x", Size: 200}, {ID: "y", Size: 10}, {ID: "z", Size: 10}}
	first := Decide(cells, cfg, nil, 0)
	for i := 0; i < 100; i++ {
		if got := Decide(cells, cfg, nil, 0); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
