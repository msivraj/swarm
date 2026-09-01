package model

// P5 boundary types: SLAs & ALERTING. evaluate() turns an SLO plus a metrics
// window into a state, and shouldAlert() applies hysteresis so a metric
// flapping at the threshold does not page twice. See
// docs/phases/swarm-p5-components.txt §02.

// SLO is the objective a metrics window is judged against.
type SLO struct {
	// Objective is the target success ratio, e.g. 0.999.
	Objective float64
	// AtRisk is the budget-fraction remaining at/below which state is
	// AtRisk (0..1).
	AtRisk float64
}

// Metrics is the SLO evaluation window (good vs total events). Derived by
// the SLA shell from the P4 observability.Reporter's already-rolled-up
// series — kept minimal and SLO-shaped rather than reusing GlobalMetrics,
// whose Count/Gauge/Samples don't cleanly express an availability ratio.
type Metrics struct {
	Good  int64
	Total int64
}

// SLState is Met | AtRisk | Breached. The zero value is Met — a fresh,
// unevaluated window reads as healthy rather than paging on startup.
type SLState int

const (
	// Met means the SLO is satisfied — the zero value.
	Met SLState = iota
	// AtRisk means the error budget is running low.
	AtRisk
	// Breached means the SLO has been violated.
	Breached
)

// Hysteresis are the flap-suppression bands shouldAlert applies so a metric
// oscillating at the threshold does not page twice: it fires only on a
// worsening transition, strictly past Margin.
type Hysteresis struct {
	// Margin is the minimum distance past the threshold a transition must
	// cross before it is considered a genuine (not flapping) change.
	Margin float64
}
