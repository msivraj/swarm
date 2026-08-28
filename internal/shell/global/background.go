package global

import (
	"time"

	"github.com/msivraj/swarm/internal/core/routing"
	"github.com/msivraj/swarm/internal/model"
)

// divergeSweepLoop periodically recomputes routing.Diverged over the held
// GlobalView on a timer, using the injected clock. GetGlobalView and Submit
// already recompute Diverged live against s.now() on every call, so this
// loop's own recomputation changes no decision — it exists for the
// between-poll health downgrade and observability the ticket calls for
// (issue #45), and mirrors controlplane's reapLoop/mitosisLoop/publishLoop
// shape: a wall-clock ticker driving a *Once method, exiting on s.stop.
func (s *Server) divergeSweepLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.DivergeSweep)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.divergeSweepOnce()
		case <-s.stop:
			return
		}
	}
}

// divergeSweepOnce recomputes routing.Diverged over the current view and
// caches it in s.lastDiverged, so it is available for observability without
// waiting for the next GetGlobalView poll.
func (s *Server) divergeSweepOnce() {
	s.mu.Lock()
	s.lastDiverged = routing.Diverged(s.view, s.now())
	s.mu.Unlock()
}

// projectRegionsLocked projects the held GlobalView into the []model.RegionView
// pure cores reason over, downgrading every RegionID routing.Diverged flags
// stale (as of now) to model.Unreachable — the same projection both Submit
// (to feed routing.Decide) and GetGlobalView (to serve the RPC) need, kept in
// one place so a region is never treated healthy for a routing decision but
// reported diverged (or vice versa) via two different projections. Callers
// must hold s.mu.
func (s *Server) projectRegionsLocked(now model.Instant) ([]model.RegionView, []model.RegionID) {
	summaries := routing.Summaries(s.view)
	diverged := routing.Diverged(s.view, now)

	stale := make(map[model.RegionID]struct{}, len(diverged))
	for _, id := range diverged {
		stale[id] = struct{}{}
	}

	out := make([]model.RegionView, 0, len(summaries))
	for _, sum := range summaries {
		health := sum.Health
		if _, ok := stale[sum.Region]; ok {
			health = model.Unreachable
		}
		out = append(out, model.RegionView{
			ID:     sum.Region,
			Free:   sum.Free,
			Cells:  sum.Cells,
			Health: health,
		})
	}
	return out, diverged
}
