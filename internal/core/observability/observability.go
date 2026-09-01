// Package observability is a pure core: the associative, commutative metric
// rollups that keep series cardinality bounded as the fleet climbs toward a
// million machines (cell -> region -> global). It performs no I/O and reads
// no clock or randomness — RollupRegion and RollupGlobal are pure functions
// of the metrics passed in. It follows the shape set by
// internal/core/mitosis and mirrors internal/core/aggregate: take data,
// return a value, never execute an effect.
//
// The fold is a monoid over (Count, Gauge, Samples): Count and Samples
// combine by sum, and Gauge combines by a Samples-weighted average. Carrying
// the weight (Samples) forward at every tier is what makes the average
// associative — a plain average of averages is not, but a weighted one is,
// which is what lets RollupGlobal(regions), where each region is
// RollupRegion(itsCells), equal one flat fold over every cell directly. See
// docs/phases/swarm-p4-components.txt §02-03.
package observability

import "github.com/msivraj/swarm/internal/model"

// RollupRegion reduces one region's per-cell metric series into a single
// bounded RegionMetrics. Associative and commutative in cells, with the
// empty slice folding to the zero RegionMetrics (Count=0, Gauge=0,
// Samples=0) — the identity. RollupRegion does not stamp Region: cells carry
// no region tag of their own, so the shell (which already grouped cells by
// region to call this) attaches it.
func RollupRegion(cells []model.CellMetrics) model.RegionMetrics {
	var count, samples int64
	var weightedGauge float64
	for _, c := range cells {
		count += c.Count
		weightedGauge += c.Gauge * float64(c.Samples)
		samples += c.Samples
	}
	return model.RegionMetrics{
		Count:   count,
		Gauge:   weightedAverage(weightedGauge, samples),
		Samples: samples,
	}
}

// RollupGlobal reduces every region's RegionMetrics into one bounded
// GlobalMetrics. Associative and commutative in rs, with the empty slice
// folding to the zero GlobalMetrics — the identity. It applies the exact
// same combine rule as RollupRegion (sum Count and Samples, Samples-weighted
// average for Gauge), which is what lets the two-step hierarchical reduce
// (cell -> region -> global) equal a flat reduce over all cells: each
// region's Gauge*Samples reconstitutes the weighted sum its cells
// contributed, so nothing is lost by rolling up in two passes instead of
// one.
func RollupGlobal(rs []model.RegionMetrics) model.GlobalMetrics {
	var count, samples int64
	var weightedGauge float64
	for _, r := range rs {
		count += r.Count
		weightedGauge += r.Gauge * float64(r.Samples)
		samples += r.Samples
	}
	return model.GlobalMetrics{
		Count:   count,
		Gauge:   weightedAverage(weightedGauge, samples),
		Samples: samples,
	}
}

// weightedAverage divides a Samples-weighted sum by its total weight,
// defining the zero-weight case as 0 rather than dividing by zero: a
// CellMetrics/RegionMetrics with Samples==0 carries no observation to
// average, so it contributes nothing (weight 0) to any fold it takes part
// in, and a fold with total weight 0 has nothing to report.
func weightedAverage(weightedSum float64, totalWeight int64) float64 {
	if totalWeight == 0 {
		return 0
	}
	return weightedSum / float64(totalWeight)
}

// Budget bounds are the maximum number of metric series a level's store is
// allowed to retain, regardless of fleet size. Finer tiers see more raw
// dimensionality (per-cell detail, e.g. broken out by task or job) and so
// are allowed more series; each rollup above it folds that detail into a
// fixed number of series — RollupRegion and RollupGlobal always produce
// exactly one output value no matter how many inputs they fold, which is
// why cardinality stops growing with fleet size the moment it crosses a
// tier boundary. Higher tiers keep strictly less detail: LevelCell >
// LevelRegion > LevelGlobal.
const (
	cellBudget   model.Cardinality = 1000
	regionBudget model.Cardinality = 100
	globalBudget model.Cardinality = 10
)

// Budget returns how many series the given Level is allowed to keep. An
// unrecognized Level gets the most conservative (smallest) bound, the same
// "refuse to grow unbounded" default as globalBudget — a level Budget
// doesn't know is never treated as licence to keep more detail.
func Budget(level model.Level) model.Cardinality {
	switch level {
	case model.LevelCell:
		return cellBudget
	case model.LevelRegion:
		return regionBudget
	case model.LevelGlobal:
		return globalBudget
	default:
		return globalBudget
	}
}
