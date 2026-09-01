package model

import "testing"

// TestSLStateOrder pins the iota order of the SLState constants and the
// zero-value contract: Met must be zero so a fresh, unevaluated window
// reads as healthy rather than paging on startup.
func TestSLStateOrder(t *testing.T) {
	tests := []struct {
		name string
		got  SLState
		want SLState
	}{
		{"Met", Met, 0},
		{"AtRisk", AtRisk, 1},
		{"Breached", Breached, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero SLState
	if zero != Met {
		t.Fatalf("zero SLState = %d, want Met (%d)", zero, Met)
	}
}

// TestSLAZeroAndRoundTrip asserts the SLA types' zero values are usable and
// that fields round-trip once populated.
func TestSLAZeroAndRoundTrip(t *testing.T) {
	t.Run("SLO", func(t *testing.T) {
		var zero SLO
		if zero.Objective != 0 || zero.AtRisk != 0 {
			t.Fatalf("zero SLO = %+v, want all zero", zero)
		}
		slo := SLO{Objective: 0.999, AtRisk: 0.1}
		if slo.Objective != 0.999 || slo.AtRisk != 0.1 {
			t.Fatalf("SLO did not round-trip: %+v", slo)
		}
	})

	t.Run("Metrics", func(t *testing.T) {
		var zero Metrics
		if zero.Good != 0 || zero.Total != 0 {
			t.Fatalf("zero Metrics = %+v, want all zero", zero)
		}
		m := Metrics{Good: 98, Total: 100}
		if m.Good != 98 || m.Total != 100 {
			t.Fatalf("Metrics did not round-trip: %+v", m)
		}
	})

	t.Run("Hysteresis", func(t *testing.T) {
		var zero Hysteresis
		if zero.Margin != 0 {
			t.Fatalf("zero Hysteresis = %+v, want all zero", zero)
		}
		h := Hysteresis{Margin: 0.01}
		if h.Margin != 0.01 {
			t.Fatalf("Hysteresis did not round-trip: %+v", h)
		}
	})
}
