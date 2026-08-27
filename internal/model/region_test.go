package model

import "testing"

// TestHealthOrder pins the iota order of the Health constants: routing and
// selectRegion depend on Healthy being the zero value and on this exact
// ordering being stable.
func TestHealthOrder(t *testing.T) {
	tests := []struct {
		name string
		got  Health
		want Health
	}{
		{"Healthy", Healthy, 0},
		{"Degraded", Degraded, 1},
		{"Unreachable", Unreachable, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	if !(Healthy < Degraded && Degraded < Unreachable) {
		t.Fatalf("Health constants out of order: Healthy=%d Degraded=%d Unreachable=%d", Healthy, Degraded, Unreachable)
	}

	var zero Health
	if zero != Healthy {
		t.Fatalf("zero Health = %d, want Healthy (%d)", zero, Healthy)
	}
}

// TestRegionViewZeroAndRoundTrip asserts RegionView's zero value is usable
// and that its fields round-trip once populated — plain data, no behavior
// beyond holding what is put into it.
func TestRegionViewZeroAndRoundTrip(t *testing.T) {
	var zero RegionView
	if zero.ID != "" || zero.Free != 0 || zero.Cells != 0 || zero.Health != Healthy {
		t.Fatalf("zero RegionView = %+v, want all zero", zero)
	}

	rv := RegionView{ID: "region-1", Free: 42, Cells: 7, Health: Degraded}
	if rv.ID != "region-1" || rv.Free != 42 || rv.Cells != 7 || rv.Health != Degraded {
		t.Fatalf("RegionView did not round-trip: %+v", rv)
	}
}
