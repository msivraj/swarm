package sla

import (
	"context"
	"errors"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	coresla "github.com/msivraj/swarm/internal/core/sla"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/observability"
)

// --- fakes ---

// fakeReporter is a test-local MetricsReporter — no OTel wiring, no
// network. It exists to prove the metrics-feed path works against any
// MetricsReporter, not just *observability.Reporter.
type fakeReporter struct {
	global  model.GlobalMetrics
	regions map[model.RegionID]model.RegionMetrics
}

func (f fakeReporter) Global() model.GlobalMetrics { return f.global }

func (f fakeReporter) Region(id model.RegionID) (model.RegionMetrics, bool) {
	rm, ok := f.regions[id]
	return rm, ok
}

// fakeSink is a test-local AlertSink: it records every fired alert instead
// of paging a real on-call rotation. This is the "no real network/paging"
// fake the ticket requires.
type fakeSink struct {
	fired []Alert
	err   error // if set, Alert returns this error instead of recording
}

func (f *fakeSink) Alert(a Alert) error {
	if f.err != nil {
		return f.err
	}
	f.fired = append(f.fired, a)
	return nil
}

// --- DeriveGlobalMetrics / DeriveRegionMetrics ---

func TestDeriveMetrics(t *testing.T) {
	tests := []struct {
		name  string
		count int64
		gauge float64
		want  model.Metrics
	}{
		{"zero count derives an empty window", 0, 0.5, model.Metrics{}},
		{"negative count (defensive) derives an empty window", -5, 0.9, model.Metrics{}},
		{"perfect success ratio", 100, 1.0, model.Metrics{Good: 100, Total: 100}},
		{"zero success ratio", 100, 0.0, model.Metrics{Good: 0, Total: 100}},
		{"fractional ratio rounds to nearest", 200, 0.745, model.Metrics{Good: 149, Total: 200}},
		{"rounds up at the half", 3, 0.5, model.Metrics{Good: 2, Total: 3}}, // 1.5 -> 2
		{"gauge above 1 clamps Good to Total", 100, 1.2, model.Metrics{Good: 100, Total: 100}},
		{"negative gauge clamps Good to zero", 100, -0.3, model.Metrics{Good: 0, Total: 100}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DeriveGlobalMetrics(model.GlobalMetrics{Count: tt.count, Gauge: tt.gauge}); got != tt.want {
				t.Fatalf("DeriveGlobalMetrics(Count=%d, Gauge=%v) = %+v, want %+v", tt.count, tt.gauge, got, tt.want)
			}
			if got := DeriveRegionMetrics(model.RegionMetrics{Count: tt.count, Gauge: tt.gauge}); got != tt.want {
				t.Fatalf("DeriveRegionMetrics(Count=%d, Gauge=%v) = %+v, want %+v", tt.count, tt.gauge, got, tt.want)
			}
		})
	}
}

// --- metrics feed: the shell doesn't distort ---

// TestMetricsFeed_FakeReporter drives EvalGlobal/EvalRegion against a fake
// MetricsReporter and checks the derived Metrics{Good,Total} — and the
// resulting Evaluate state — match calling the core directly over the same
// derivation. This is the "the shell doesn't distort" acceptance
// criterion: the shell's job is purely to read a rollup and feed the core,
// never to add its own SLA judgment.
func TestMetricsFeed_FakeReporter(t *testing.T) {
	slo := model.SLO{Objective: 0.99, AtRisk: 0.5}
	region := model.RegionID("us-east")

	reporter := fakeReporter{
		global: model.GlobalMetrics{Count: 1000, Gauge: 0.995}, // Good=995 -> AtRisk (o=0.005,e=0.01,budget=0.5)
		regions: map[model.RegionID]model.RegionMetrics{
			region: {Region: region, Count: 100, Gauge: 0.50}, // Good=50 -> Breached
		},
	}

	sink := &fakeSink{}
	w := NewWatcher("global-availability", slo, model.Hysteresis{Margin: 1})

	gotState, gotBudget, err := w.EvalGlobal(reporter, sink)
	if err != nil {
		t.Fatalf("EvalGlobal: %v", err)
	}
	wantMetrics := model.Metrics{Good: 995, Total: 1000}
	wantState := coresla.Evaluate(slo, wantMetrics)
	wantBudget := coresla.ErrorBudget(slo, wantMetrics)
	if gotState != wantState {
		t.Fatalf("EvalGlobal state = %v, want %v (core over %+v)", gotState, wantState, wantMetrics)
	}
	if gotBudget != wantBudget {
		t.Fatalf("EvalGlobal budget = %v, want %v", gotBudget, wantBudget)
	}

	rw := NewWatcher("us-east-availability", slo, model.Hysteresis{Margin: 1})
	regionState, regionBudget, ok, err := rw.EvalRegion(reporter, region, sink)
	if err != nil {
		t.Fatalf("EvalRegion: %v", err)
	}
	if !ok {
		t.Fatalf("EvalRegion: region %q should be present", region)
	}
	wantRegionMetrics := model.Metrics{Good: 50, Total: 100}
	wantRegionState := coresla.Evaluate(slo, wantRegionMetrics)
	if regionState != wantRegionState {
		t.Fatalf("EvalRegion state = %v, want %v", regionState, wantRegionState)
	}
	if regionBudget != coresla.ErrorBudget(slo, wantRegionMetrics) {
		t.Fatalf("EvalRegion budget = %v, want %v", regionBudget, coresla.ErrorBudget(slo, wantRegionMetrics))
	}

	// Both should have alerted, since a fresh Watcher starts at Met and
	// both windows are a genuine worsening.
	if len(sink.fired) != 2 {
		t.Fatalf("fired alerts = %d, want 2 (one per watcher's first genuine worsening)", len(sink.fired))
	}
}

// TestMetricsFeed_EvalRegion_Absent checks EvalRegion reports ok=false and
// performs no Tick (no alert, no state) when the region never survived the
// observability tier's collection/cardinality cap.
func TestMetricsFeed_EvalRegion_Absent(t *testing.T) {
	reporter := fakeReporter{regions: map[model.RegionID]model.RegionMetrics{}}
	sink := &fakeSink{}
	w := NewWatcher("missing-region", model.SLO{Objective: 0.99, AtRisk: 0.5}, model.Hysteresis{Margin: 1})

	state, budget, ok, err := w.EvalRegion(reporter, model.RegionID("nowhere"), sink)
	if err != nil {
		t.Fatalf("EvalRegion: %v", err)
	}
	if ok {
		t.Fatalf("EvalRegion: ok = true, want false for an absent region")
	}
	if state != model.Met || budget != 0 {
		t.Fatalf("EvalRegion absent = (%v, %v), want (Met, 0)", state, budget)
	}
	if len(sink.fired) != 0 {
		t.Fatalf("fired alerts = %d, want 0 for an absent region", len(sink.fired))
	}
}

// TestMetricsFeed_RealReporter wires an actual *observability.Reporter
// (the P4 rollup, via a manual OTel reader — no network) and checks the
// SLA shell's derived window and resulting state match calling the core
// directly against the mapping this package documents (see
// DeriveGlobalMetrics). This is the "reuses the P4 rollup, doesn't
// re-implement it" acceptance criterion.
func TestMetricsFeed_RealReporter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	reporter, err := observability.NewReporter(provider.Meter(observability.MeterName))
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}

	region := model.RegionID("us-east")
	cellsByRegion := map[model.RegionID][]model.CellMetrics{
		region: {
			{Cell: "cell-a", Count: 100, Gauge: 0.99, Samples: 100}, // 99 successes
			{Cell: "cell-b", Count: 100, Gauge: 0.50, Samples: 100}, // 50 successes
		},
	}
	reporter.Collect(context.Background(), cellsByRegion)
	// RollupRegion: Count=200, weighted Gauge = (0.99*100 + 0.50*100)/200 = 0.745.
	// DeriveGlobalMetrics/DeriveRegionMetrics: Good = round(0.745*200) = 149.
	wantMetrics := model.Metrics{Good: 149, Total: 200}

	slo := model.SLO{Objective: 0.99, AtRisk: 0.5}
	sink := &fakeSink{}
	w := NewWatcher("global-availability", slo, model.Hysteresis{Margin: 1})

	gotState, gotBudget, err := w.EvalGlobal(reporter, sink)
	if err != nil {
		t.Fatalf("EvalGlobal: %v", err)
	}
	if got := DeriveGlobalMetrics(reporter.Global()); got != wantMetrics {
		t.Fatalf("DeriveGlobalMetrics(reporter.Global()) = %+v, want %+v", got, wantMetrics)
	}
	if wantState := coresla.Evaluate(slo, wantMetrics); gotState != wantState {
		t.Fatalf("EvalGlobal state = %v, want %v", gotState, wantState)
	}
	if wantBudget := coresla.ErrorBudget(slo, wantMetrics); gotBudget != wantBudget {
		t.Fatalf("EvalGlobal budget = %v, want %v", gotBudget, wantBudget)
	}

	rw := NewWatcher("us-east-availability", slo, model.Hysteresis{Margin: 1})
	regionState, _, ok, err := rw.EvalRegion(reporter, region, sink)
	if err != nil {
		t.Fatalf("EvalRegion: %v", err)
	}
	if !ok {
		t.Fatalf("EvalRegion: region %q should have survived Collect", region)
	}
	if got := DeriveRegionMetrics(mustRegion(t, reporter, region)); got != wantMetrics {
		t.Fatalf("DeriveRegionMetrics = %+v, want %+v", got, wantMetrics)
	}
	if wantRegionState := coresla.Evaluate(slo, wantMetrics); regionState != wantRegionState {
		t.Fatalf("EvalRegion state = %v, want %v", regionState, wantRegionState)
	}
}

func mustRegion(t *testing.T, reporter *observability.Reporter, id model.RegionID) model.RegionMetrics {
	t.Helper()
	rm, ok := reporter.Region(id)
	if !ok {
		t.Fatalf("Region(%q): not present", id)
	}
	return rm
}

// --- the headline: no flap storm ---

// TestWatcher_NoFlapStorm is the shell-side half of the hysteresis story
// (the core-side half is coresla.TestNoFlapStorm): driving a Watcher with
// a metrics sequence whose derived SLState oscillates Met<->AtRisk<->
// Breached at the threshold must page the fake AlertSink ONCE per genuine
// worsening, never once per flap. A watcher debouncing against the raw
// last-OBSERVED state (instead of lastAlerted) would fire on every
// Met->AtRisk step in this sequence — 4 times instead of 2 — because each
// step is, in isolation, a genuine one-level worsening; only tracking
// last-ALERTED collapses the repeated re-entries into a single page per
// escalation. See the package doc for why lastAlerted (not last-observed)
// is required.
func TestWatcher_NoFlapStorm(t *testing.T) {
	slo := model.SLO{Objective: 0.99, AtRisk: 0.5}

	// Three fixed metrics windows, each landing squarely in one SLState
	// under slo (mirrors internal/core/sla's own threshold fixtures).
	metWindow := model.Metrics{Good: 100, Total: 100}     // ratio 1.0, budget 1.0 -> Met
	atRiskWindow := model.Metrics{Good: 995, Total: 1000} // o=0.005,e=0.01,budget=0.5 -> AtRisk
	breachedWindow := model.Metrics{Good: 50, Total: 100} // ratio 0.5 -> Breached
	sanity := []struct {
		w    model.Metrics
		want model.SLState
	}{
		{metWindow, model.Met},
		{atRiskWindow, model.AtRisk},
		{breachedWindow, model.Breached},
	}
	for _, s := range sanity {
		if got := coresla.Evaluate(slo, s.w); got != s.want {
			t.Fatalf("fixture sanity check: Evaluate(%+v) = %v, want %v", s.w, got, s.want)
		}
	}

	// A metric flapping at the AtRisk/Breached boundary, with one genuine
	// escalation to Breached in the middle — the same shape as
	// coresla.TestNoFlapStorm's readings sequence.
	sequence := []model.Metrics{
		metWindow,
		atRiskWindow,   // genuine worsening: alert #1 (Met -> AtRisk)
		metWindow,      // improves: no alert
		atRiskWindow,   // re-enters an already-alerted severity: no alert
		metWindow,      // improves: no alert
		atRiskWindow,   // re-enters again: no alert
		breachedWindow, // genuine escalation past AtRisk: alert #2
		atRiskWindow,   // improves: no alert
		breachedWindow, // re-enters an already-alerted severity: no alert
		metWindow,      // improves: no alert
	}
	wantFireCount := []int{0, 1, 1, 1, 1, 1, 2, 2, 2, 2} // cumulative fired count after each step

	sink := &fakeSink{}
	w := NewWatcher("flapping-slo", slo, model.Hysteresis{Margin: 1})

	for i, m := range sequence {
		if _, _, err := w.Tick(m, sink); err != nil {
			t.Fatalf("step %d: Tick: %v", i, err)
		}
		if len(sink.fired) != wantFireCount[i] {
			t.Fatalf("step %d: cumulative fired = %d, want %d", i, len(sink.fired), wantFireCount[i])
		}
	}

	if len(sink.fired) != 2 {
		t.Fatalf("total fired = %d, want exactly 2 (one per genuine worsening, none for flapping)", len(sink.fired))
	}
	if sink.fired[0].State != model.AtRisk {
		t.Fatalf("first fired alert state = %v, want AtRisk", sink.fired[0].State)
	}
	if sink.fired[1].State != model.Breached {
		t.Fatalf("second fired alert state = %v, want Breached", sink.fired[1].State)
	}
}

// TestWatcher_NoFlapStorm_LastObservedWouldStorm is a control: it proves
// the flap sequence above WOULD alert-storm if debounced against the raw
// last-observed state instead of lastAlerted, which is exactly the bug the
// #190 ticket calls out. It calls coresla.ShouldAlert directly (bypassing
// Watcher) with prev = the previous reading, showing that pattern fires 5
// times, not 2, on the identical sequence — every Met->AtRisk/AtRisk->
// Breached re-entry looks like a fresh worsening when prev is last-
// observed instead of last-alerted.
func TestWatcher_NoFlapStorm_LastObservedWouldStorm(t *testing.T) {
	h := model.Hysteresis{Margin: 1}
	readings := []model.SLState{
		model.Met,
		model.AtRisk,
		model.Met,
		model.AtRisk,
		model.Met,
		model.AtRisk,
		model.Breached,
		model.AtRisk,
		model.Breached,
		model.Met,
	}

	prev := model.Met // last-OBSERVED, updated every step regardless of alert
	var stormCount int
	for _, cur := range readings {
		if coresla.ShouldAlert(cur, prev, h) {
			stormCount++
		}
		prev = cur
	}

	if stormCount != 5 {
		t.Fatalf("last-observed debounce fired %d times, want 5 (demonstrating the storm the shell must avoid)", stormCount)
	}
}

// --- genuine breach pages, sustained breach does not re-page ---

// TestWatcher_GenuineBreachPages checks a real transition into Breached
// fires exactly one alert, with the correct state and error budget.
func TestWatcher_GenuineBreachPages(t *testing.T) {
	slo := model.SLO{Objective: 0.99, AtRisk: 0.5}
	breached := model.Metrics{Good: 50, Total: 100}

	sink := &fakeSink{}
	w := NewWatcher("availability", slo, model.Hysteresis{Margin: 1})

	state, budget, err := w.Tick(breached, sink)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if state != model.Breached {
		t.Fatalf("state = %v, want Breached", state)
	}
	if len(sink.fired) != 1 {
		t.Fatalf("fired = %d, want exactly 1", len(sink.fired))
	}
	if sink.fired[0].State != model.Breached {
		t.Fatalf("fired alert state = %v, want Breached", sink.fired[0].State)
	}
	if sink.fired[0].Budget != budget {
		t.Fatalf("fired alert budget = %v, want %v", sink.fired[0].Budget, budget)
	}
	if sink.fired[0].Name != "availability" {
		t.Fatalf("fired alert name = %q, want %q", sink.fired[0].Name, "availability")
	}
}

// TestWatcher_SustainedBreachDoesNotRepage drives the SAME Breached
// window through Tick repeatedly and checks only the first tick pages —
// a sustained breach (no further worsening) never re-fires.
func TestWatcher_SustainedBreachDoesNotRepage(t *testing.T) {
	slo := model.SLO{Objective: 0.99, AtRisk: 0.5}
	breached := model.Metrics{Good: 50, Total: 100}

	sink := &fakeSink{}
	w := NewWatcher("availability", slo, model.Hysteresis{Margin: 1})

	for i := 0; i < 10; i++ {
		if _, _, err := w.Tick(breached, sink); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
	}

	if len(sink.fired) != 1 {
		t.Fatalf("fired = %d over 10 sustained-breach ticks, want exactly 1", len(sink.fired))
	}
}

// --- error propagation ---

// TestWatcher_Tick_SinkError checks a failing AlertSink surfaces its error
// and does NOT advance lastAlerted — so a delivery failure is retried on
// the next tick rather than silently swallowed as "already alerted."
func TestWatcher_Tick_SinkError(t *testing.T) {
	slo := model.SLO{Objective: 0.99, AtRisk: 0.5}
	breached := model.Metrics{Good: 50, Total: 100}
	wantErr := errors.New("paging pipeline unavailable")

	sink := &fakeSink{err: wantErr}
	w := NewWatcher("availability", slo, model.Hysteresis{Margin: 1})

	if _, _, err := w.Tick(breached, sink); err == nil {
		t.Fatalf("Tick: want an error when the sink fails, got nil")
	}
	if w.LastAlerted() != model.Met {
		t.Fatalf("LastAlerted = %v after a failed send, want Met (unchanged)", w.LastAlerted())
	}

	// Retrying with a working sink should still fire — lastAlerted was
	// never (incorrectly) advanced by the failed attempt.
	sink.err = nil
	if _, _, err := w.Tick(breached, sink); err != nil {
		t.Fatalf("Tick (retry): %v", err)
	}
	if len(sink.fired) != 1 {
		t.Fatalf("fired = %d after retry, want 1", len(sink.fired))
	}
}
