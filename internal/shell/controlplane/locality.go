package controlplane

import (
	"github.com/msivraj/swarm/internal/core/placement"
	"github.com/msivraj/swarm/internal/model"
)

// LocalitySource supplies the network-topology data the P6 locality-
// preferred placement layer (placeLocked) needs to build a
// model.LocalityGraph: each candidate cell's Region/AZ/Rack coordinates, and
// where a task being placed should be near. See Config.Locality's doc for
// the nil-disables-the-layer contract and how a real deployment would wire
// one.
type LocalitySource interface {
	// CellTopology returns cell's network coordinates, and whether one is
	// known for it at all. A cell reported ok==false is simply left out of
	// the built LocalityGraph's Zone map — placement.Rank's own
	// localityDistance already treats an absent cell as maximum distance,
	// never panics on a partial map.
	CellTopology(cell model.CellID) (model.Topology, bool)

	// TaskOrigin returns the coordinates t should be placed near (e.g. its
	// job's or submitter's declared locality), and whether one is known.
	// ok==false means the shell has nothing for locality-preferred
	// placement to prefer for t, so placeLocked skips BestFit for this task
	// and goes straight to placement.Place.
	TaskOrigin(t model.Task) (model.Topology, bool)
}

// localityGraphLocked builds the model.LocalityGraph placeLocked passes to
// placement.BestFit from cfg.Locality, scoped to cells (the same working
// snapshot drainPendingLocked is placing t against). ok is false — and the
// returned graph is the zero value, never used — whenever cfg.Locality is
// nil or t has no known TaskOrigin: both mean "no locality info for this
// task," the same "opt out" signal model.LocalityGraph's own doc documents
// for a nil/empty Zone. Callers must hold s.mu (cells/cfg.Locality access
// mirrors drainPendingLocked's own locking contract).
func (s *Server) localityGraphLocked(t model.Task, cells []model.CellView) (model.LocalityGraph, bool) {
	if s.cfg.Locality == nil {
		return model.LocalityGraph{}, false
	}
	origin, ok := s.cfg.Locality.TaskOrigin(t)
	if !ok {
		return model.LocalityGraph{}, false
	}
	zone := make(map[model.CellID]model.Topology, len(cells))
	for _, c := range cells {
		if topo, ok := s.cfg.Locality.CellTopology(c.ID); ok {
			zone[c.ID] = topo
		}
	}
	return model.LocalityGraph{Origin: origin, Zone: zone}, true
}

// placeLocked is the additive P6 locality-preferred placement layer
// (#223, owner fork c of #218): it tries placement.BestFit against a
// model.LocalityGraph built from cfg.Locality first, and only falls back to
// the untouched placement.Place when either no locality graph is available
// for t (cfg.Locality nil, or TaskOrigin unknown for t — localityGraphLocked
// returns ok==false) or BestFit itself returns NoCapacity (no candidate is
// both capability-capable and has room). placement.Place/PlaceAcross are
// never modified by this layer — placeLocked only decides which of the two
// existing functions a given task's placement call resolves to, exactly as
// drainPendingLocked and trySpillLocked already compose Place with
// PlaceAcross for cross-region spill. A deployment that never sets
// cfg.Locality gets ok==false on every call, so every drainPendingLocked
// placement is byte-for-byte the same placement.Place call it was before
// this ticket.
func (s *Server) placeLocked(t model.Task, cells []model.CellView) placement.Placement {
	if loc, ok := s.localityGraphLocked(t, cells); ok {
		if p := placement.BestFit(t, cells, loc); p.Kind == placement.Assign {
			return p
		}
	}
	return placement.Place(t, cells)
}
