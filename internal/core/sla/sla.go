// Package sla is a pure core: it turns a metrics window into an SLO verdict,
// computes the remaining error-budget fraction, and decides whether a state
// transition is alert-worthy. It performs no I/O and reads no clock — the
// shell (#190) feeds the rolled-up metrics window and carries out the paging
// decision. See docs/phases/swarm-p5-components.txt §02 (SLAs & ALERTING).
package sla

import "github.com/msivraj/swarm/internal/model"

// Evaluate classifies a metrics window against the SLO.
//
// Thresholds (exact):
//   - Total == 0 → Met (no evidence of failure; a fresh, unevaluated window
//     reads as healthy rather than paging on startup).
//   - success ratio (Good/Total) < Objective → Breached: the SLO is already
//     violated, regardless of budget.
//   - ratio >= Objective and the remaining error budget (see ErrorBudget) is
//     at/below slo.AtRisk → AtRisk: the objective is technically met but with
//     little or no budget to spare (this includes the exact boundary
//     ratio == Objective, where budget is exactly 0).
//   - ratio >= Objective and budget > slo.AtRisk → Met: the objective is met
//     with budget to spare.
func Evaluate(slo model.SLO, w model.Metrics) model.SLState {
	if w.Total == 0 {
		return model.Met
	}

	ratio := float64(w.Good) / float64(w.Total)
	if ratio < slo.Objective {
		return model.Breached
	}

	if ErrorBudget(slo, w) <= slo.AtRisk {
		return model.AtRisk
	}
	return model.Met
}

// ErrorBudget returns the fraction of error budget remaining, clamped to
// [0, 1].
//
// With allowed error e = 1 - Objective and observed error o = 1 - Good/Total,
// budget = clamp((e - o) / e, 0, 1): 1.0 with zero observed errors, 0.0 right
// at the allowance (o == e), and clamped to 0.0 (never negative) once errors
// exceed the allowance.
//
// Edge cases, handled without NaN/Inf:
//   - Total == 0 → 1.0 (no evidence of failure, full budget).
//   - Objective >= 1 (e <= 0, so the division above is undefined): a perfect
//     window (o == 0, i.e. Good == Total) → 1.0; any observed error → 0.0
//     (there is no allowance left to divide into).
func ErrorBudget(slo model.SLO, w model.Metrics) float64 {
	if w.Total == 0 {
		return 1.0
	}

	allowed := 1 - slo.Objective
	observed := 1 - float64(w.Good)/float64(w.Total)

	if allowed <= 0 {
		if observed <= 0 {
			return 1.0
		}
		return 0.0
	}

	return clamp01((allowed - observed) / allowed)
}

func clamp01(x float64) float64 {
	switch {
	case x < 0:
		return 0
	case x > 1:
		return 1
	default:
		return x
	}
}

// severity orders SLState by badness so a transition's direction and size can
// be compared numerically: Met < AtRisk < Breached.
func severity(s model.SLState) int {
	switch s {
	case model.Breached:
		return 2
	case model.AtRisk:
		return 1
	default: // model.Met and any unrecognized zero-like value
		return 0
	}
}

// ShouldAlert reports whether the transition from prev to cur should page.
//
// Rule: fire only on a STRICT WORSENING transition (severity(cur) >
// severity(prev), where Met < AtRisk < Breached) whose severity jump is at
// least h.Margin levels (e.g. Met->AtRisk and AtRisk->Breached are each a
// 1-level jump; Met->Breached is a 2-level jump). An unchanged state
// (cur == prev) never fires, and an improving transition (cur less severe
// than prev) never fires.
//
// This function alone cannot prevent an alert storm from a raw sequence of
// readings that keeps crossing back and forth over a threshold (e.g.
// AtRisk, Met, AtRisk, Met, ...): every Met->AtRisk step in that raw
// sequence is, in isolation, a genuine 1-level worsening. Flap suppression
// across a sequence is a debounce built on top of this pure decision: the
// caller (test or shell) keeps a "last alerted at" state instead of the raw
// "last observed" state, and only updates it when ShouldAlert returns true.
// Fed that way, re-entering a severity that has already been alerted on
// (cur == prev, the last-alerted state) reports false, so a metric
// oscillating at the threshold pages once per genuine worsening, not once
// per flap. See TestShouldAlert's noFlapStorm subtest.
func ShouldAlert(cur, prev model.SLState, h model.Hysteresis) bool {
	delta := severity(cur) - severity(prev)
	if delta <= 0 {
		return false
	}
	return float64(delta) >= h.Margin
}
