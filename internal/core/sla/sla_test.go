package sla

import (
	"math"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

const epsilon = 1e-9

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < epsilon
}

// TestEvaluateThresholds (evalThresholds) enumerates windows straddling the
// Met/AtRisk/Breached boundaries, including exact-boundary cases and
// Total == 0.
func TestEvaluateThresholds(t *testing.T) {
	tests := []struct {
		name string
		slo  model.SLO
		w    model.Metrics
		want model.SLState
	}{
		{
			name: "empty window is Met regardless of SLO",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0.5},
			w:    model.Metrics{Good: 0, Total: 0},
			want: model.Met,
		},
		{
			name: "just below objective is Breached",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0.5},
			w:    model.Metrics{Good: 98, Total: 100}, // ratio 0.98 < 0.99
			want: model.Breached,
		},
		{
			name: "well below objective is Breached",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0.5},
			w:    model.Metrics{Good: 50, Total: 100},
			want: model.Breached,
		},
		{
			name: "exactly at objective has zero budget: AtRisk",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0.5},
			w:    model.Metrics{Good: 99, Total: 100}, // ratio == 0.99, budget == 0
			want: model.AtRisk,
		},
		{
			name: "budget exactly at the AtRisk line is AtRisk",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0.5},
			w:    model.Metrics{Good: 995, Total: 1000}, // o=0.005, e=0.01, budget=0.5
			want: model.AtRisk,
		},
		{
			name: "budget just above the AtRisk line is Met",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0.5},
			w:    model.Metrics{Good: 996, Total: 1000}, // o=0.004, e=0.01, budget=0.6
			want: model.Met,
		},
		{
			name: "perfect window is Met",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0.5},
			w:    model.Metrics{Good: 100, Total: 100},
			want: model.Met,
		},
		{
			name: "AtRisk of zero: any spare budget is Met",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0},
			w:    model.Metrics{Good: 100, Total: 100},
			want: model.Met,
		},
		{
			name: "AtRisk of zero: exact objective is AtRisk",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0},
			w:    model.Metrics{Good: 99, Total: 100},
			want: model.AtRisk,
		},
		{
			name: "AtRisk of zero: below objective is Breached",
			slo:  model.SLO{Objective: 0.99, AtRisk: 0},
			w:    model.Metrics{Good: 98, Total: 100},
			want: model.Breached,
		},
		{
			name: "objective 1.0 perfect window is Met (no NaN)",
			slo:  model.SLO{Objective: 1.0, AtRisk: 0.5},
			w:    model.Metrics{Good: 100, Total: 100},
			want: model.Met,
		},
		{
			name: "objective 1.0 with any error is Breached (no NaN)",
			slo:  model.SLO{Objective: 1.0, AtRisk: 0.5},
			w:    model.Metrics{Good: 99, Total: 100},
			want: model.Breached,
		},
		{
			name: "empty window with objective 1.0 is still Met",
			slo:  model.SLO{Objective: 1.0, AtRisk: 0.5},
			w:    model.Metrics{Good: 0, Total: 0},
			want: model.Met,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Evaluate(tt.slo, tt.w); got != tt.want {
				t.Fatalf("Evaluate(%+v, %+v) = %v, want %v", tt.slo, tt.w, got, tt.want)
			}
		})
	}
}

// TestErrorBudget (budgetMath) checks the error-budget fraction across
// representative windows: full budget at zero errors, zero budget at the
// objective, clamped to zero when breached, and the Total==0 / Objective==1
// edges handled without NaN/Inf.
func TestErrorBudget(t *testing.T) {
	tests := []struct {
		name string
		slo  model.SLO
		w    model.Metrics
		want float64
	}{
		{
			name: "zero errors is full budget",
			slo:  model.SLO{Objective: 0.99},
			w:    model.Metrics{Good: 100, Total: 100},
			want: 1.0,
		},
		{
			name: "errors equal to allowance is zero budget",
			slo:  model.SLO{Objective: 0.99},
			w:    model.Metrics{Good: 99, Total: 100}, // o=0.01, e=0.01
			want: 0.0,
		},
		{
			name: "errors double the allowance clamp to zero, not negative",
			slo:  model.SLO{Objective: 0.99},
			w:    model.Metrics{Good: 98, Total: 100}, // o=0.02, e=0.01
			want: 0.0,
		},
		{
			name: "errors far beyond the allowance still clamp to zero",
			slo:  model.SLO{Objective: 0.99},
			w:    model.Metrics{Good: 0, Total: 100},
			want: 0.0,
		},
		{
			name: "half the allowance used is half budget remaining",
			slo:  model.SLO{Objective: 0.9},
			w:    model.Metrics{Good: 95, Total: 100}, // o=0.05, e=0.1
			want: 0.5,
		},
		{
			name: "Total==0 is full budget",
			slo:  model.SLO{Objective: 0.99},
			w:    model.Metrics{Good: 0, Total: 0},
			want: 1.0,
		},
		{
			name: "Objective==1 and a perfect window is full budget (no NaN)",
			slo:  model.SLO{Objective: 1.0},
			w:    model.Metrics{Good: 100, Total: 100},
			want: 1.0,
		},
		{
			name: "Objective==1 with any error is zero budget (no Inf)",
			slo:  model.SLO{Objective: 1.0},
			w:    model.Metrics{Good: 99, Total: 100},
			want: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ErrorBudget(tt.slo, tt.w)
			if math.IsNaN(got) || math.IsInf(got, 0) {
				t.Fatalf("ErrorBudget(%+v, %+v) = %v, want a finite number", tt.slo, tt.w, got)
			}
			if !approxEqual(got, tt.want) {
				t.Fatalf("ErrorBudget(%+v, %+v) = %v, want %v", tt.slo, tt.w, got, tt.want)
			}
			if got < 0 || got > 1 {
				t.Fatalf("ErrorBudget(%+v, %+v) = %v, want in [0,1]", tt.slo, tt.w, got)
			}
		})
	}
}

// TestShouldAlert covers the hysteresis rule directly: fire only on a strict
// worsening transition whose severity jump clears h.Margin; never on an
// unchanged or improving transition.
func TestShouldAlert(t *testing.T) {
	tests := []struct {
		name string
		cur  model.SLState
		prev model.SLState
		h    model.Hysteresis
		want bool
	}{
		{"unchanged Met does not alert", model.Met, model.Met, model.Hysteresis{}, false},
		{"unchanged AtRisk does not alert", model.AtRisk, model.AtRisk, model.Hysteresis{}, false},
		{"unchanged Breached does not alert", model.Breached, model.Breached, model.Hysteresis{}, false},
		{"Met->AtRisk worsening alerts", model.AtRisk, model.Met, model.Hysteresis{}, true},
		{"AtRisk->Breached worsening alerts", model.Breached, model.AtRisk, model.Hysteresis{}, true},
		{"Met->Breached worsening alerts", model.Breached, model.Met, model.Hysteresis{}, true},
		{"AtRisk->Met improving does not alert", model.Met, model.AtRisk, model.Hysteresis{}, false},
		{"Breached->AtRisk improving does not alert", model.AtRisk, model.Breached, model.Hysteresis{}, false},
		{"Breached->Met improving does not alert", model.Met, model.Breached, model.Hysteresis{}, false},
		{
			name: "margin of 2 suppresses a single-level jump",
			cur:  model.AtRisk, prev: model.Met, h: model.Hysteresis{Margin: 2}, want: false,
		},
		{
			name: "margin of 2 still fires for a double-level jump",
			cur:  model.Breached, prev: model.Met, h: model.Hysteresis{Margin: 2}, want: true,
		},
		{
			name: "small margin does not block a genuine single-level jump",
			cur:  model.AtRisk, prev: model.Met, h: model.Hysteresis{Margin: 0.01}, want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldAlert(tt.cur, tt.prev, tt.h); got != tt.want {
				t.Fatalf("ShouldAlert(%v, %v, %+v) = %v, want %v", tt.cur, tt.prev, tt.h, got, tt.want)
			}
		})
	}
}

// TestNoFlapStorm is the headline property test (§03, "noFlapStorm"): a
// state oscillating at the threshold pages at most once per genuine
// worsening. ShouldAlert alone only decides one transition; flap
// suppression across a sequence comes from debouncing against the
// last-ALERTED state rather than the raw last-observed state — exactly the
// pattern the shell (#190) uses. This test drives that loop and asserts the
// alert count matches only the genuine escalations, not every flap.
func TestNoFlapStorm(t *testing.T) {
	h := model.Hysteresis{Margin: 1}

	// A window hovering right at the AtRisk/Met boundary, then a genuine
	// escalation to Breached, then flapping back down.
	readings := []model.SLState{
		model.Met,
		model.AtRisk,   // genuine worsening: alert #1
		model.Met,      // improves: no alert
		model.AtRisk,   // re-enters an already-alerted severity: no alert
		model.Met,      // improves: no alert
		model.AtRisk,   // re-enters again: no alert
		model.Breached, // genuine escalation past AtRisk: alert #2
		model.AtRisk,   // improves: no alert
		model.Breached, // re-enters an already-alerted severity: no alert
		model.Met,      // improves: no alert
	}
	wantAlerted := []bool{false, true, false, false, false, false, true, false, false, false}

	lastAlerted := model.Met // the state the last page was sent for
	var alertCount int
	for i, r := range readings {
		got := ShouldAlert(r, lastAlerted, h)
		if got != wantAlerted[i] {
			t.Fatalf("step %d: ShouldAlert(%v, lastAlerted=%v) = %v, want %v", i, r, lastAlerted, got, wantAlerted[i])
		}
		if got {
			alertCount++
			lastAlerted = r
		}
	}

	if alertCount != 2 {
		t.Fatalf("alertCount = %d, want 2 (one per genuine worsening, none for flapping)", alertCount)
	}
}

// TestDeterminism (determinism) mirrors mitosis_test.go: identical inputs
// yield identical outputs across repeated calls, for all three functions.
func TestDeterminism(t *testing.T) {
	slo := model.SLO{Objective: 0.99, AtRisk: 0.5}
	w := model.Metrics{Good: 987, Total: 1000}
	h := model.Hysteresis{Margin: 1}

	wantState := Evaluate(slo, w)
	wantBudget := ErrorBudget(slo, w)
	wantAlert := ShouldAlert(model.Breached, model.AtRisk, h)

	for i := 0; i < 100; i++ {
		if got := Evaluate(slo, w); got != wantState {
			t.Fatalf("Evaluate non-deterministic on run %d: %v vs %v", i, got, wantState)
		}
		if got := ErrorBudget(slo, w); got != wantBudget {
			t.Fatalf("ErrorBudget non-deterministic on run %d: %v vs %v", i, got, wantBudget)
		}
		if got := ShouldAlert(model.Breached, model.AtRisk, h); got != wantAlert {
			t.Fatalf("ShouldAlert non-deterministic on run %d: %v vs %v", i, got, wantAlert)
		}
	}
}
