// Package sla is the imperative shell that feeds the P4
// observability.Reporter's already-rolled-up metric series into the pure
// internal/core/sla, and carries its verdict out to a paging pipeline. It
// adds no SLA logic of its own: state, error budget, and alert-worthiness
// all come from the core; this package only reads metrics and fires
// alerts. See docs/phases/swarm-p5-components.txt §02 (SLAs & ALERTING)
// and the #183 fork-(b) ruling: the SLA core never sees raw per-cell data
// and the observability rollup is never redone here.
//
// # Debounce against last-ALERTED, not last-observed (the load-bearing bit)
//
// coresla.ShouldAlert(cur, prev, h) is a single pure decision: does the
// transition from prev to cur cross a genuine (not flapping) worsening? Its
// flap suppression depends entirely on what the caller passes as prev (see
// the core's doc comment and TestNoFlapStorm in internal/core/sla). A
// Watcher in this package tracks lastAlerted — the state the LAST FIRED
// ALERT was for, not the last metrics window it observed — and only
// advances it when Tick actually fires an alert. Passing the last-OBSERVED
// state instead would make every step across the threshold in a flapping
// sequence look like a fresh worsening relative to the immediately
// preceding reading, and re-fire on every flap — an alert storm. See
// TestWatcher_NoFlapStorm for the end-to-end proof.
package sla

import (
	"fmt"
	"math"

	coresla "github.com/msivraj/swarm/internal/core/sla"
	"github.com/msivraj/swarm/internal/model"
)

// MetricsReporter is the seam onto the P4 observability.Reporter this
// package's metrics feed reads. *observability.Reporter satisfies it via
// its existing Global/Region methods — this package never re-does rollup
// math and never sees raw per-cell metrics. Tests wire a fake in-memory
// implementation so the metrics-feed path is exercisable without standing
// up a real OTel pipeline.
type MetricsReporter interface {
	// Global returns the fleet-wide rolled-up series.
	Global() model.GlobalMetrics
	// Region returns the rolled-up series for id and whether it is
	// currently present (it may have been evicted by the observability
	// tier's cardinality budget, or never collected).
	Region(id model.RegionID) (model.RegionMetrics, bool)
}

// AlertSink is the seam onto the notification/paging pipeline a fired
// alert is carried out through (PagerDuty, Slack, an on-call rotation,
// ...). A real implementation does network I/O; tests wire a fake that
// records fired alerts.
type AlertSink interface {
	Alert(a Alert) error
}

// Alert is what a Watcher hands to an AlertSink when coresla.ShouldAlert
// says a transition is page-worthy.
type Alert struct {
	// Name identifies which SLO this alert is for.
	Name string
	// State is the SLState the alert was fired for.
	State model.SLState
	// Budget is the remaining error-budget fraction at the time of firing.
	Budget float64
}

// Watcher evaluates one SLO's metrics window on every Tick and decides
// whether to page. Every answer — state, budget, alert-worthiness — comes
// from internal/core/sla; a Watcher only holds the per-SLO debounce state
// (lastAlerted) the core's ShouldAlert needs as its prev argument.
type Watcher struct {
	// Name identifies the SLO this Watcher tracks — carried on every Alert
	// so a pipeline handling several SLOs can tell them apart.
	Name string
	// SLO is the objective this Watcher's metrics windows are judged
	// against.
	SLO model.SLO
	// Hysteresis is the flap-suppression margin passed to ShouldAlert.
	Hysteresis model.Hysteresis

	// lastAlerted is the state the last FIRED alert was for — the debounce
	// state ShouldAlert's prev argument needs. Its zero value, model.Met,
	// means "nothing has paged yet" (matching model.SLState's own zero
	// value), so a fresh Watcher never fires on an unchanged healthy
	// reading.
	lastAlerted model.SLState
}

// NewWatcher builds a Watcher for one SLO. Its debounce state starts at
// model.Met — nothing has paged yet.
func NewWatcher(name string, slo model.SLO, h model.Hysteresis) *Watcher {
	return &Watcher{Name: name, SLO: slo, Hysteresis: h}
}

// LastAlerted returns the state the Watcher's last fired alert was for
// (model.Met if none has fired yet). It exists for tests/observability;
// nothing in this package treats it as more than the debounce state the
// core's ShouldAlert consumes.
func (w *Watcher) LastAlerted() model.SLState {
	return w.lastAlerted
}

// Tick evaluates m against w's SLO through the pure core and, if
// coresla.ShouldAlert says the transition from w.lastAlerted is
// page-worthy, fires an Alert through sink and advances lastAlerted to the
// newly observed state.
//
// A ShouldAlert == false tick — an unchanged state, an improving one, or a
// re-entered severity that has already been alerted on — never touches
// sink and never advances lastAlerted. That is exactly what stops a
// flapping metric from paging more than once per genuine worsening: see
// the package doc.
func (w *Watcher) Tick(m model.Metrics, sink AlertSink) (state model.SLState, budget float64, err error) {
	state = coresla.Evaluate(w.SLO, m)
	budget = coresla.ErrorBudget(w.SLO, m)

	if !coresla.ShouldAlert(state, w.lastAlerted, w.Hysteresis) {
		return state, budget, nil
	}

	if err := sink.Alert(Alert{Name: w.Name, State: state, Budget: budget}); err != nil {
		return state, budget, fmt.Errorf("sla: watcher %q: alert: %w", w.Name, err)
	}
	w.lastAlerted = state
	return state, budget, nil
}

// EvalGlobal derives the fleet-wide metrics window from reporter.Global()
// (see DeriveGlobalMetrics) and Ticks w against it — the fleet-level SLA
// feed.
func (w *Watcher) EvalGlobal(reporter MetricsReporter, sink AlertSink) (state model.SLState, budget float64, err error) {
	return w.Tick(DeriveGlobalMetrics(reporter.Global()), sink)
}

// EvalRegion derives region's metrics window from reporter.Region(region)
// (see DeriveRegionMetrics) and Ticks w against it. ok is false — and no
// Tick is performed — when region is not currently present in reporter
// (evicted by the observability tier's cardinality budget, or never
// collected).
func (w *Watcher) EvalRegion(reporter MetricsReporter, region model.RegionID, sink AlertSink) (state model.SLState, budget float64, ok bool, err error) {
	rm, ok := reporter.Region(region)
	if !ok {
		return model.Met, 0, false, nil
	}
	state, budget, err = w.Tick(DeriveRegionMetrics(rm), sink)
	return state, budget, true, err
}

// --- metrics derivation (the #183 fork-(b) boundary) ---
//
// model.GlobalMetrics/model.RegionMetrics are the P4 observability
// rollup's generic series shape (Count/Gauge/Samples, see
// internal/model/scale.go) and carry no explicit good/bad split. Per the
// #183 fork-(b) ruling, the SLA shell derives model.Metrics{Good, Total}
// from that existing rollup rather than the observability pipeline
// growing a dedicated success/error counter. The mapping:
//
//	Total = Count    -- the rolled-up event total for the tier (the sum,
//	                     via RollupRegion/RollupGlobal, of each cell's
//	                     additive Count — e.g. total requests observed).
//	Good  = round(Gauge * Count), clamped to [0, Total]
//	                  -- Gauge combines by Samples-weighted average (see
//	                     model.CellMetrics); for the SLA feed, cells
//	                     report their per-request success ratio (in
//	                     [0,1]) as Gauge, so a tier's rolled-up Gauge is
//	                     itself a Samples-weighted success ratio, and
//	                     Good is the success COUNT that ratio implies
//	                     over Total requests. Clamping means a
//	                     misconfigured pipeline (Gauge outside [0,1])
//	                     can never derive a negative or over-Total Good.
//
// A Total of zero (an empty or not-yet-collected rollup) derives
// Metrics{0, 0}: coresla.Evaluate reads that as Met (a fresh window has no
// evidence of failure) — the same rule the core applies directly, not a
// shell-side reinterpretation of it.
func DeriveGlobalMetrics(gm model.GlobalMetrics) model.Metrics {
	return deriveMetrics(gm.Count, gm.Gauge)
}

// DeriveRegionMetrics converts one region's rolled-up series into the
// core's Metrics window. See DeriveGlobalMetrics for the mapping.
func DeriveRegionMetrics(rm model.RegionMetrics) model.Metrics {
	return deriveMetrics(rm.Count, rm.Gauge)
}

func deriveMetrics(count int64, gauge float64) model.Metrics {
	if count <= 0 {
		return model.Metrics{}
	}
	good := int64(math.Round(gauge * float64(count)))
	switch {
	case good < 0:
		good = 0
	case good > count:
		good = count
	}
	return model.Metrics{Good: good, Total: count}
}
