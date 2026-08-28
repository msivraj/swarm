package global

import (
	"sort"

	"github.com/msivraj/swarm/internal/model"
)

// regionWeight pairs a RegionID with the weight (reported Free capacity)
// partitionTasks divides tasks proportional to.
type regionWeight struct {
	Region model.RegionID
	Weight int
}

// partitionTasks divides tasks across weights proportional to each region's
// Weight, by the largest-remainder method: every region's integer share is
// floor(len(tasks)*Weight/totalWeight), and the len(tasks) - sum(shares)
// tasks left over go one each to the regions with the largest remainder,
// ties broken by ascending RegionID (round-robin). Every task is assigned to
// exactly one region's output slice, in its original relative order; the
// order tasks are split into per-region runs is regions sorted ascending by
// RegionID, so the result depends only on (tasks, weights) — never on
// weights' input order or any map iteration.
//
// A zero totalWeight (every region weight 0 — should not happen for a real
// routing.Spread, whose regions are all Free > 0 by construction, but this
// stays total for it) falls back to plain round-robin by ascending RegionID,
// so every task still lands somewhere rather than being silently dropped.
//
// This is a pure function of its two slice arguments: no I/O, no clock, no
// randomness — issue #45 requires it factored out and unit-tested directly.
func partitionTasks(tasks []model.Task, weights []regionWeight) map[model.RegionID][]model.Task {
	out := make(map[model.RegionID][]model.Task, len(weights))
	if len(tasks) == 0 || len(weights) == 0 {
		return out
	}

	sorted := make([]regionWeight, len(weights))
	copy(sorted, weights)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Region < sorted[j].Region })

	n := len(tasks)
	totalWeight := 0
	for _, w := range sorted {
		totalWeight += w.Weight
	}

	counts := make([]int, len(sorted))
	remainders := make([]int, len(sorted))
	assigned := 0
	if totalWeight > 0 {
		for i, w := range sorted {
			counts[i] = (n * w.Weight) / totalWeight
			remainders[i] = (n * w.Weight) % totalWeight
			assigned += counts[i]
		}
	}
	extra := n - assigned

	// order ranks region indices by remainder descending; a stable sort over
	// the already-RegionID-ascending "sorted" slice keeps equal remainders
	// (including every remainder when totalWeight == 0) in ascending
	// RegionID order — the round-robin tiebreak.
	order := make([]int, len(sorted))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return remainders[order[a]] > remainders[order[b]] })

	for i := 0; i < extra; i++ {
		counts[order[i%len(order)]]++
	}

	pos := 0
	for i, w := range sorted {
		c := counts[i]
		if pos+c > n { // defensive clamp; the arithmetic above never overshoots n
			c = n - pos
		}
		if c > 0 {
			out[w.Region] = append(out[w.Region], tasks[pos:pos+c]...)
		}
		pos += c
	}
	return out
}
