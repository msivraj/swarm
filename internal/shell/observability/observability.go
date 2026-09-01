// Package observability is a shell package: it receives raw per-cell
// metrics from agents, folds them through the pure
// internal/core/observability rollups (RollupRegion, then RollupGlobal),
// and emits ONLY the reduced series through OpenTelemetry — never a raw
// per-cell series. It performs no rollup math of its own; every combine
// decision (sum a Count, Samples-weighted-average a Gauge, how many series
// a tier may keep) is the core's. This package's job is entirely I/O: take
// the metrics agents hand it, call the core, and hand the result to OTel.
//
// This is the mechanism behind the P4 O1 target: keeping metric cardinality
// bounded as the fleet climbs toward a million machines. Because
// RollupRegion always folds a region's cells to exactly one RegionMetrics,
// and RollupGlobal always folds every region to exactly one GlobalMetrics,
// the number of series this package emits never grows with the number of
// cells or agents reporting — only with the number of regions, and even
// that is capped at observability.Budget(model.LevelRegion) (see Collect).
//
// See docs/phases/swarm-p4-components.txt §02 (OBSERVABILITY AGGREGATION)
// and §03 (metric cardinality SLO). This is a distinct concern from the
// job-RESULT rollup in internal/shell/controlplane/rollup.go — that folds
// task results into a job's Aggregate; this folds metric SERIES to keep
// telemetry cardinality bounded. They share the associative-fold shape but
// nothing else, and are kept in separate packages on purpose.
package observability

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	core "github.com/msivraj/swarm/internal/core/observability"
	"github.com/msivraj/swarm/internal/model"
)

// Instrument names and the attribute keys their data points carry. Every
// data point is tagged with a level (region or global) and, at the region
// tier, the region it rolled up — never a cell, since raw cell series are
// never emitted.
const (
	meterName = "github.com/msivraj/swarm/internal/shell/observability"

	metricCount   = "swarm.observability.count"
	metricGauge   = "swarm.observability.gauge"
	metricSamples = "swarm.observability.samples"

	attrLevel  = "swarm.level"
	attrRegion = "swarm.region"

	levelRegion = "region"
	levelGlobal = "global"
)

// MeterName is the instrumentation scope Reporter registers its
// instruments under. Callers building a MeterProvider (production: wired to
// a real OTLP exporter; tests: wired to an in-memory/manual reader) use
// meterProvider.Meter(observability.MeterName) to obtain the Meter NewReporter
// expects.
const MeterName = meterName

// Reporter folds raw per-cell metrics into the bounded rolled-up series (via
// the pure core) and emits them through OpenTelemetry. Each field is
// recorded as an OTel Gauge instrument (a current-value, not a running
// total) because a rollup is a snapshot of the fleet at collection time, not
// a delta to accumulate — recording it as a counter would double-count
// across collection cycles.
//
// A Reporter also keeps the last-collected series in memory so callers can
// read the current rollup directly (Region, Global, RegionCount) without
// round-tripping through an OTel reader; this is the "store only the
// reduced series" half of the ticket. The stored map is replaced wholesale
// on every Collect, so it never grows past the region-tier budget and never
// accumulates stale regions.
type Reporter struct {
	countGauge   otelmetric.Float64Gauge
	valueGauge   otelmetric.Float64Gauge
	samplesGauge otelmetric.Float64Gauge

	mu      sync.Mutex
	regions map[model.RegionID]model.RegionMetrics
	global  model.GlobalMetrics
}

// NewReporter builds a Reporter that records through meter. meter should be
// obtained from an OTel MeterProvider — a real OTLP-backed one in
// production, an in-memory/manual-reader one in tests (see the package
// tests for the pattern). NewReporter itself performs no network I/O; it
// only registers instrument descriptors.
func NewReporter(meter otelmetric.Meter) (*Reporter, error) {
	countGauge, err := meter.Float64Gauge(
		metricCount,
		otelmetric.WithDescription("rolled-up counter total for a region/global metric tier"),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: count instrument: %w", err)
	}
	valueGauge, err := meter.Float64Gauge(
		metricGauge,
		otelmetric.WithDescription("rolled-up gauge (Samples-weighted average) for a region/global metric tier"),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: gauge instrument: %w", err)
	}
	samplesGauge, err := meter.Float64Gauge(
		metricSamples,
		otelmetric.WithDescription("sample count backing the rolled-up gauge for a region/global metric tier"),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: samples instrument: %w", err)
	}
	return &Reporter{
		countGauge:   countGauge,
		valueGauge:   valueGauge,
		samplesGauge: samplesGauge,
		regions:      make(map[model.RegionID]model.RegionMetrics),
	}, nil
}

// Collect takes one cycle's raw per-cell metrics, grouped by the region each
// cell belongs to, and:
//
//  1. folds each region's cells through the core (core.RollupRegion) —
//     always exactly one RegionMetrics per region, regardless of cell count;
//  2. folds every region through the core (core.RollupGlobal) over ALL
//     regions, so the emitted global series always equals a flat reduce over
//     every input cell, matching the two-step/flat-reduce associativity
//     property RollupRegion/RollupGlobal are built on;
//  3. caps how many region series it retains and emits at
//     core.Budget(model.LevelRegion) — bounding cardinality at the region
//     tier even if the number of regions itself grows past budget. Regions
//     are ranked by RegionID so which ones survive a cap is deterministic;
//  4. emits the surviving region series plus the one global series through
//     OTel, and replaces the in-memory store with exactly what it emitted.
//
// Raw CellMetrics never leave this call: no per-cell series is ever stored
// or emitted, which is what keeps cardinality from growing with fleet size
// (§03).
func (r *Reporter) Collect(ctx context.Context, cellsByRegion map[model.RegionID][]model.CellMetrics) {
	regions := rollupAllRegions(cellsByRegion)
	global := core.RollupGlobal(regions)

	emitted := capRegions(regions, core.Budget(model.LevelRegion))

	r.store(emitted, global)
	r.emit(ctx, emitted, global)
}

// rollupAllRegions calls core.RollupRegion once per region, over ALL of
// cellsByRegion, in deterministic RegionID order.
func rollupAllRegions(cellsByRegion map[model.RegionID][]model.CellMetrics) []model.RegionMetrics {
	ids := make([]model.RegionID, 0, len(cellsByRegion))
	for id := range cellsByRegion {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	regions := make([]model.RegionMetrics, 0, len(ids))
	for _, id := range ids {
		rm := core.RollupRegion(cellsByRegion[id])
		rm.Region = id
		regions = append(regions, rm)
	}
	return regions
}

// capRegions bounds regions (already sorted by RegionID) to at most budget
// entries, keeping the lowest-sorted RegionIDs — a deterministic, testable
// eviction rule rather than depending on map iteration order.
func capRegions(regions []model.RegionMetrics, budget model.Cardinality) []model.RegionMetrics {
	if budget < 0 || len(regions) <= int(budget) {
		return regions
	}
	return regions[:int(budget)]
}

func (r *Reporter) store(regions []model.RegionMetrics, global model.GlobalMetrics) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.regions = make(map[model.RegionID]model.RegionMetrics, len(regions))
	for _, rm := range regions {
		r.regions[rm.Region] = rm
	}
	r.global = global
}

func (r *Reporter) emit(ctx context.Context, regions []model.RegionMetrics, global model.GlobalMetrics) {
	for _, rm := range regions {
		r.record(ctx, rm.Count, rm.Gauge, rm.Samples, attribute.NewSet(
			attribute.String(attrLevel, levelRegion),
			attribute.String(attrRegion, string(rm.Region)),
		))
	}
	r.record(ctx, global.Count, global.Gauge, global.Samples, attribute.NewSet(
		attribute.String(attrLevel, levelGlobal),
	))
}

func (r *Reporter) record(ctx context.Context, count int64, gauge float64, samples int64, attrs attribute.Set) {
	opt := otelmetric.WithAttributeSet(attrs)
	r.countGauge.Record(ctx, float64(count), opt)
	r.valueGauge.Record(ctx, gauge, opt)
	r.samplesGauge.Record(ctx, float64(samples), opt)
}

// Region returns the last-collected rolled-up RegionMetrics for id and
// whether it is currently stored. It is present only if id survived the
// region-tier budget cap on the most recent Collect; it is never raw
// per-cell data.
func (r *Reporter) Region(id model.RegionID) (model.RegionMetrics, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rm, ok := r.regions[id]
	return rm, ok
}

// Global returns the last-collected rolled-up GlobalMetrics.
func (r *Reporter) Global() model.GlobalMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.global
}

// RegionCount returns how many region series are currently stored — always
// <= core.Budget(model.LevelRegion), regardless of how many regions or
// cells fed the most recent Collect.
func (r *Reporter) RegionCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.regions)
}
