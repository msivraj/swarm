package observability

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	core "github.com/msivraj/swarm/internal/core/observability"
	"github.com/msivraj/swarm/internal/model"
)

// newTestReporter builds a Reporter wired to an in-memory manual reader —
// no network, no real OTLP collector, per the ticket. The returned collect
// func pulls the reader's current snapshot on demand.
func newTestReporter(t *testing.T) (*Reporter, func() metricdata.ResourceMetrics) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter(MeterName)
	r, err := NewReporter(meter)
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}
	collect := func() metricdata.ResourceMetrics {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("reader.Collect: %v", err)
		}
		return rm
	}
	return r, collect
}

// dataPoints returns the Gauge[float64] data points OTel recorded for the
// named instrument, or nil if that instrument reported nothing yet.
func dataPoints(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[float64] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			if !ok {
				t.Fatalf("metric %s: want Gauge[float64], got %T", name, m.Data)
			}
			return g.DataPoints
		}
	}
	return nil
}

// pointFor returns the single data point tagged with the given level (and,
// for region points, the given region), or fails the test if there isn't
// exactly one.
func pointFor(t *testing.T, points []metricdata.DataPoint[float64], level, region string) metricdata.DataPoint[float64] {
	t.Helper()
	var matches []metricdata.DataPoint[float64]
	for _, p := range points {
		lv, _ := p.Attributes.Value(attribute.Key(attrLevel))
		if lv.AsString() != level {
			continue
		}
		if level == levelRegion {
			rv, _ := p.Attributes.Value(attribute.Key(attrRegion))
			if rv.AsString() != region {
				continue
			}
		}
		matches = append(matches, p)
	}
	if len(matches) != 1 {
		t.Fatalf("level=%s region=%s: want exactly 1 matching point, got %d (of %d total)", level, region, len(matches), len(points))
	}
	return matches[0]
}

func cell(id model.CellID, count int64, gauge float64, samples int64) model.CellMetrics {
	return model.CellMetrics{Cell: id, Count: count, Gauge: gauge, Samples: samples}
}

// TestCollect_MatchesCoreRollup is the ticket's central assertion: the
// shell's emitted region/global series equal exactly what the pure core
// would compute from the same raw inputs — the shell folds, it never
// distorts.
func TestCollect_MatchesCoreRollup(t *testing.T) {
	tests := []struct {
		name          string
		cellsByRegion map[model.RegionID][]model.CellMetrics
	}{
		{
			name:          "empty",
			cellsByRegion: map[model.RegionID][]model.CellMetrics{},
		},
		{
			name: "single region single cell",
			cellsByRegion: map[model.RegionID][]model.CellMetrics{
				"r1": {cell("c1", 10, 2.0, 5)},
			},
		},
		{
			name: "single region multiple cells",
			cellsByRegion: map[model.RegionID][]model.CellMetrics{
				"r1": {cell("c1", 10, 2.0, 5), cell("c2", 20, 4.0, 15)},
			},
		},
		{
			name: "multiple regions multiple cells",
			cellsByRegion: map[model.RegionID][]model.CellMetrics{
				"r1": {cell("c1", 10, 2.0, 5), cell("c2", 20, 4.0, 15)},
				"r2": {cell("c3", 7, 1.0, 3)},
				"r3": {cell("c4", 0, 0, 0), cell("c5", 100, 9.0, 40)},
			},
		},
		{
			name: "region with zero-sample cell contributes zero weight",
			cellsByRegion: map[model.RegionID][]model.CellMetrics{
				"r1": {cell("c1", 5, 0, 0), cell("c2", 3, 6.0, 2)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := newTestReporter(t)
			r.Collect(context.Background(), tt.cellsByRegion)

			wantGlobal := core.RollupGlobal(rollupAllRegions(tt.cellsByRegion))
			if got := r.Global(); got != wantGlobal {
				t.Fatalf("Global() = %+v, want %+v", got, wantGlobal)
			}

			for id, cells := range tt.cellsByRegion {
				want := core.RollupRegion(cells)
				want.Region = id
				got, ok := r.Region(id)
				if !ok {
					t.Fatalf("Region(%s): not stored", id)
				}
				if got != want {
					t.Fatalf("Region(%s) = %+v, want %+v", id, got, want)
				}
			}
		})
	}
}

// TestEmit_ThroughOTelExporter asserts the values read back from the
// in-memory OTel exporter equal the core's rollup — not just the Reporter's
// internal store, but what actually goes out through OpenTelemetry.
func TestEmit_ThroughOTelExporter(t *testing.T) {
	cellsByRegion := map[model.RegionID][]model.CellMetrics{
		"r1": {cell("c1", 10, 2.0, 5), cell("c2", 20, 4.0, 15)},
		"r2": {cell("c3", 7, 1.0, 3)},
	}

	r, collect := newTestReporter(t)
	r.Collect(context.Background(), cellsByRegion)
	rm := collect()

	wantRegion := map[model.RegionID]model.RegionMetrics{
		"r1": core.RollupRegion(cellsByRegion["r1"]),
		"r2": core.RollupRegion(cellsByRegion["r2"]),
	}
	wantGlobal := core.RollupGlobal(rollupAllRegions(cellsByRegion))

	countPoints := dataPoints(t, rm, metricCount)
	gaugePoints := dataPoints(t, rm, metricGauge)
	samplesPoints := dataPoints(t, rm, metricSamples)

	for regionID, want := range wantRegion {
		cp := pointFor(t, countPoints, levelRegion, string(regionID))
		if cp.Value != float64(want.Count) {
			t.Errorf("region %s count = %v, want %v", regionID, cp.Value, want.Count)
		}
		gp := pointFor(t, gaugePoints, levelRegion, string(regionID))
		if gp.Value != want.Gauge {
			t.Errorf("region %s gauge = %v, want %v", regionID, gp.Value, want.Gauge)
		}
		sp := pointFor(t, samplesPoints, levelRegion, string(regionID))
		if sp.Value != float64(want.Samples) {
			t.Errorf("region %s samples = %v, want %v", regionID, sp.Value, want.Samples)
		}
	}

	gcp := pointFor(t, countPoints, levelGlobal, "")
	if gcp.Value != float64(wantGlobal.Count) {
		t.Errorf("global count = %v, want %v", gcp.Value, wantGlobal.Count)
	}
	ggp := pointFor(t, gaugePoints, levelGlobal, "")
	if ggp.Value != wantGlobal.Gauge {
		t.Errorf("global gauge = %v, want %v", ggp.Value, wantGlobal.Gauge)
	}
	gsp := pointFor(t, samplesPoints, levelGlobal, "")
	if gsp.Value != float64(wantGlobal.Samples) {
		t.Errorf("global samples = %v, want %v", gsp.Value, wantGlobal.Samples)
	}

	// No raw per-cell series is ever emitted: only 2 regions + 1 global == 3
	// data points per instrument, regardless of the 3 cells that fed them.
	if got := len(countPoints); got != 3 {
		t.Errorf("count data points = %d, want 3 (2 regions + 1 global)", got)
	}
}

// TestCardinality_DoesNotGrowWithCellCount is the §03 property this
// component exists for: as the number of raw cells reporting into a fixed
// set of regions grows, the emitted series count stays flat.
func TestCardinality_DoesNotGrowWithCellCount(t *testing.T) {
	const regionCount = 4
	cellCounts := []int{1, 50, 5000, 50000}

	var priorSeries int
	for i, n := range cellCounts {
		cellsByRegion := make(map[model.RegionID][]model.CellMetrics, regionCount)
		for ri := 0; ri < regionCount; ri++ {
			region := model.RegionID(fmt.Sprintf("r%d", ri))
			cells := make([]model.CellMetrics, n)
			for ci := 0; ci < n; ci++ {
				cells[ci] = cell(model.CellID(fmt.Sprintf("c%d-%d", ri, ci)), 1, float64(ci%7), 1)
			}
			cellsByRegion[region] = cells
		}

		r, collect := newTestReporter(t)
		r.Collect(context.Background(), cellsByRegion)
		rm := collect()

		points := dataPoints(t, rm, metricCount)
		series := len(points)
		if series != regionCount+1 { // + 1 global point
			t.Fatalf("cells/region=%d: emitted series = %d, want %d (regions + global)", n, series, regionCount+1)
		}
		if i > 0 && series != priorSeries {
			t.Fatalf("cells/region=%d: emitted series changed from %d to %d as cell count grew", n, priorSeries, series)
		}
		priorSeries = series

		if got := r.RegionCount(); got != regionCount {
			t.Fatalf("cells/region=%d: RegionCount() = %d, want %d", n, got, regionCount)
		}
	}
}

// TestCardinality_RegionTierCappedAtBudget grows the number of REGIONS (not
// cells) past core.Budget(model.LevelRegion) and asserts the emitted/stored
// region series never exceeds it, while the global series still folds
// EVERY region — a flat reduce over every input cell — matching the core's
// two-step == flat-reduce associativity property even when the region tier
// itself is over budget.
func TestCardinality_RegionTierCappedAtBudget(t *testing.T) {
	budget := int(core.Budget(model.LevelRegion))
	regionCount := budget + 250

	cellsByRegion := make(map[model.RegionID][]model.CellMetrics, regionCount)
	for i := 0; i < regionCount; i++ {
		region := model.RegionID(fmt.Sprintf("r%04d", i))
		cellsByRegion[region] = []model.CellMetrics{
			cell(model.CellID(fmt.Sprintf("c%04d", i)), int64(i+1), float64(i%11), int64(i%5+1)),
		}
	}

	r, collect := newTestReporter(t)
	r.Collect(context.Background(), cellsByRegion)
	rm := collect()

	points := dataPoints(t, rm, metricCount)
	// 1 global point + at most `budget` region points.
	if got := len(points); got > budget+1 {
		t.Fatalf("emitted series = %d, want <= %d (budget) + 1 (global)", got, budget+1)
	}
	if got := r.RegionCount(); got != budget {
		t.Fatalf("RegionCount() = %d, want exactly the budget %d", got, budget)
	}

	// The global rollup still equals a flat reduce over every input cell —
	// the region-tier cap bounds what's exposed for drill-down, not what
	// feeds the global fold.
	wantGlobal := core.RollupGlobal(rollupAllRegions(cellsByRegion))
	if got := r.Global(); got != wantGlobal {
		t.Fatalf("Global() = %+v, want %+v (flat reduce over all %d regions)", got, wantGlobal, regionCount)
	}
}

// TestTwoStepEqualsFlatReduce is the associativity property the whole
// component leans on (§02/§03): rolling up cell -> region -> global equals
// one flat fold directly over every cell, so nothing is lost by reducing in
// two hierarchical passes instead of one.
func TestTwoStepEqualsFlatReduce(t *testing.T) {
	cellsByRegion := map[model.RegionID][]model.CellMetrics{
		"r1": {cell("c1", 10, 2.0, 5), cell("c2", 20, 4.0, 15), cell("c3", 0, 0, 0)},
		"r2": {cell("c4", 7, 1.0, 3)},
		"r3": {cell("c5", 3, 9.0, 1), cell("c6", 8, 8.0, 8)},
	}

	var allCells []model.CellMetrics
	for _, region := range []model.RegionID{"r1", "r2", "r3"} {
		allCells = append(allCells, cellsByRegion[region]...)
	}

	twoStep := core.RollupGlobal(rollupAllRegions(cellsByRegion))
	flat := core.RollupRegion(allCells) // one flat fold, treated as a single "region"

	if twoStep.Count != flat.Count || twoStep.Samples != flat.Samples || twoStep.Gauge != flat.Gauge {
		t.Fatalf("two-step rollup (Count=%d Gauge=%v Samples=%d) != flat reduce (Count=%d Gauge=%v Samples=%d)",
			twoStep.Count, twoStep.Gauge, twoStep.Samples, flat.Count, flat.Gauge, flat.Samples)
	}

	// The Reporter's Collect must produce the exact same global value as
	// this flat reduce.
	r, _ := newTestReporter(t)
	r.Collect(context.Background(), cellsByRegion)
	got := r.Global()
	if got.Count != flat.Count || got.Samples != flat.Samples || got.Gauge != flat.Gauge {
		t.Fatalf("Reporter.Global() (Count=%d Gauge=%v Samples=%d) != flat reduce (Count=%d Gauge=%v Samples=%d)",
			got.Count, got.Gauge, got.Samples, flat.Count, flat.Gauge, flat.Samples)
	}
}

func TestNewReporter_InstrumentErrorsSurfaced(t *testing.T) {
	// Meter().Float64Gauge only fails on a malformed instrument name;
	// NewReporter uses fixed, valid names, so this documents the happy path
	// return signature stays (*Reporter, nil) for a well-formed meter.
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r, err := NewReporter(provider.Meter(MeterName))
	if err != nil {
		t.Fatalf("NewReporter: unexpected error %v", err)
	}
	if r == nil {
		t.Fatal("NewReporter returned nil Reporter with nil error")
	}
}
