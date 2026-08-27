package region

import (
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

const (
	home  model.RegionID = "home"
	peerA model.RegionID = "peer-a"
	peerB model.RegionID = "peer-b"
)

func TestSelectRegion(t *testing.T) {
	tests := []struct {
		name      string
		known     []model.RegionID
		health    map[model.RegionID]model.Health
		homeFirst bool
		attempt   int
		want      model.RegionID
	}{
		{
			// §03's one-line test: home down, two peers up, prefer nearest.
			name:  "home down, two peers up, prefer nearest",
			known: []model.RegionID{home, peerA, peerB},
			health: map[model.RegionID]model.Health{
				home:  model.Unreachable,
				peerA: model.Healthy,
				peerB: model.Healthy,
			},
			homeFirst: true,
			attempt:   0,
			want:      peerA, // nearest = first in known after home
		},
		{
			name:  "homeFirst true and home healthy selects home",
			known: []model.RegionID{home, peerA},
			health: map[model.RegionID]model.Health{
				home:  model.Healthy,
				peerA: model.Healthy,
			},
			homeFirst: true,
			attempt:   0,
			want:      home,
		},
		{
			name:  "homeFirst false skips home even if healthy",
			known: []model.RegionID{home, peerA, peerB},
			health: map[model.RegionID]model.Health{
				home:  model.Healthy,
				peerA: model.Healthy,
				peerB: model.Healthy,
			},
			homeFirst: false,
			attempt:   0,
			want:      peerA,
		},
		{
			name:  "degraded region is still a candidate",
			known: []model.RegionID{home, peerA},
			health: map[model.RegionID]model.Health{
				home:  model.Unreachable,
				peerA: model.Degraded,
			},
			homeFirst: true,
			attempt:   0,
			want:      peerA,
		},
		{
			name:  "attempt 0 walks the first candidate",
			known: []model.RegionID{home, peerA, peerB},
			health: map[model.RegionID]model.Health{
				home:  model.Healthy,
				peerA: model.Healthy,
				peerB: model.Healthy,
			},
			homeFirst: true,
			attempt:   0,
			want:      home,
		},
		{
			name:  "attempt 1 advances to the next candidate",
			known: []model.RegionID{home, peerA, peerB},
			health: map[model.RegionID]model.Health{
				home:  model.Healthy,
				peerA: model.Healthy,
				peerB: model.Healthy,
			},
			homeFirst: true,
			attempt:   1,
			want:      peerA,
		},
		{
			name:  "attempt cycles back to the first candidate",
			known: []model.RegionID{home, peerA, peerB},
			health: map[model.RegionID]model.Health{
				home:  model.Healthy,
				peerA: model.Healthy,
				peerB: model.Healthy,
			},
			homeFirst: true,
			attempt:   3, // 3 candidates: wraps back to index 0
			want:      home,
		},
		{
			name:  "all regions unhealthy returns the zero RegionID",
			known: []model.RegionID{home, peerA},
			health: map[model.RegionID]model.Health{
				home:  model.Unreachable,
				peerA: model.Unreachable,
			},
			homeFirst: true,
			attempt:   0,
			want:      "",
		},
		{
			name:      "empty known returns the zero RegionID",
			known:     nil,
			health:    nil,
			homeFirst: true,
			attempt:   0,
			want:      "",
		},
		{
			name:  "region missing from health is treated as unreachable",
			known: []model.RegionID{home, peerA},
			health: map[model.RegionID]model.Health{
				home: model.Healthy,
				// peerA has no entry.
			},
			homeFirst: false,
			attempt:   0,
			want:      "", // home excluded by homeFirst=false, peerA fails closed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectRegion(tt.known, tt.health, tt.homeFirst, tt.attempt)
			if got != tt.want {
				t.Fatalf("SelectRegion() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSelectRegionIsDeterministic guards the core's defining property:
// identical inputs always produce identical output.
func TestSelectRegionIsDeterministic(t *testing.T) {
	known := []model.RegionID{home, peerA, peerB}
	health := map[model.RegionID]model.Health{
		home:  model.Unreachable,
		peerA: model.Healthy,
		peerB: model.Degraded,
	}
	first := SelectRegion(known, health, true, 2)
	for i := 0; i < 100; i++ {
		if got := SelectRegion(known, health, true, 2); got != first {
			t.Fatalf("non-deterministic output on run %d: %q vs %q", i, got, first)
		}
	}
}

// TestSelectRegionNeverSelectsUnreachable is a property test: for any
// combination of health assignments, homeFirst, and attempt, SelectRegion
// never returns a region whose health is Unreachable. Swept exhaustively
// over a small fixed region set and health alphabet — core packages may not
// import math/rand (fcischeck), so the sweep is combinatorial, not sampled.
func TestSelectRegionNeverSelectsUnreachable(t *testing.T) {
	known := []model.RegionID{home, peerA, peerB}
	healths := []model.Health{model.Healthy, model.Degraded, model.Unreachable}

	for _, hHome := range healths {
		for _, hA := range healths {
			for _, hB := range healths {
				health := map[model.RegionID]model.Health{
					home:  hHome,
					peerA: hA,
					peerB: hB,
				}
				for _, homeFirst := range []bool{true, false} {
					for attempt := 0; attempt < 5; attempt++ {
						got := SelectRegion(known, health, homeFirst, attempt)
						if got == "" {
							continue
						}
						if health[got] == model.Unreachable {
							t.Fatalf("SelectRegion(%v, homeFirst=%v, attempt=%d) = %q, which is Unreachable",
								health, homeFirst, attempt, got)
						}
					}
				}
			}
		}
	}
}

// TestSelectRegionTieBreakIsDeterministic is a property test: the tie-break
// among equally-ranked candidates is the slice order, not incidental map
// iteration order — repeated calls with the same inputs (including the same
// map value, rebuilt fresh each time) always agree.
func TestSelectRegionTieBreakIsDeterministic(t *testing.T) {
	known := []model.RegionID{home, peerA, peerB}

	for attempt := 0; attempt < 6; attempt++ {
		var want model.RegionID
		for i := 0; i < 20; i++ {
			// Rebuild the map fresh each call so Go's randomized map
			// iteration order can't leak into the result.
			health := map[model.RegionID]model.Health{
				home:  model.Healthy,
				peerA: model.Healthy,
				peerB: model.Healthy,
			}
			got := SelectRegion(known, health, true, attempt)
			if i == 0 {
				want = got
				continue
			}
			if got != want {
				t.Fatalf("attempt %d: tie-break not deterministic: %q vs %q", attempt, got, want)
			}
		}
	}
}
