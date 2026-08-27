// Package registry is a pure core: it folds membership events into the
// authoritative view of the fleet's cells and their capacities. It performs
// no I/O and reads no clock — Apply is a pure function of a Registry value
// and a RegistryEvent. This package follows the shape set by
// internal/core/mitosis: take data, return a new value plus a description of
// what changed, never execute an effect. The shell owns the stored Registry
// pointer and swaps it for the value Apply returns.
package registry

import (
	"sort"

	"github.com/msivraj/swarm/internal/model"
)

// AgentID uniquely identifies an agent within a cell. It is local to this
// package: no other P0 component needs to name an individual agent, so it is
// not promoted to model until one does.
type AgentID string

// EventKind enumerates the membership events the registry folds in.
type EventKind int

const (
	// CellUp introduces a new cell with a starting capacity.
	CellUp EventKind = iota
	// CellDown removes a cell and everything it held.
	CellDown
	// CapacityChanged updates a cell's total capacity.
	CapacityChanged
	// AgentJoined adds an agent to a cell's membership.
	AgentJoined
	// AgentLeft removes an agent from a cell's membership.
	AgentLeft
)

// RegistryEvent is a single membership fact for Apply to fold in. Only the
// fields relevant to Kind are read:
//   - CellUp:           Cell, Capacity
//   - CellDown:         Cell
//   - CapacityChanged:  Cell, Capacity
//   - AgentJoined:      Cell, Agent
//   - AgentLeft:        Cell, Agent
type RegistryEvent struct {
	Kind     EventKind
	Cell     model.CellID
	Agent    AgentID
	Capacity int
}

// ChangeKind is the tag of a Change's tagged union.
type ChangeKind int

const (
	// CellAdded reports a new cell entering the registry, at Change.Capacity.
	CellAdded ChangeKind = iota
	// CellRemoved reports a cell leaving the registry.
	CellRemoved
	// CapacityUpdated reports a cell's capacity changing to Change.Capacity.
	CapacityUpdated
	// AgentAdded reports Change.Agent joining Change.Cell.
	AgentAdded
	// AgentRemoved reports Change.Agent leaving Change.Cell.
	AgentRemoved
)

// Change describes one thing Apply altered, for the shell to react to (e.g.
// persist, publish a snapshot, wire an agent into gossip). Apply returns nil
// when an event is a no-op against the current Registry.
type Change struct {
	Kind     ChangeKind
	Cell     model.CellID
	Agent    AgentID // set for AgentAdded / AgentRemoved
	Capacity int     // set for CellAdded / CapacityUpdated: the new capacity
}

// cellState is the immutable per-cell state a Registry holds. Size is
// derived from len(agents) rather than stored redundantly, so it can never
// drift from membership.
type cellState struct {
	capacity int
	agents   map[AgentID]struct{}
}

// Registry is immutable data: Apply never mutates its receiver, it returns a
// new value. The zero value is a valid, empty Registry.
type Registry struct {
	cells map[model.CellID]cellState
}

// Apply folds ev into reg and returns the resulting Registry plus the
// Changes it produced. reg is never mutated: Apply only ever builds new maps
// (copy-on-write), so any Registry value a caller is holding stays valid and
// unchanged after Apply runs.
//
// Ambiguities the phase doc leaves open, resolved here (see issue notes):
//   - CellUp on a cell that already exists is a no-op (idempotent re-delivery
//     of a membership event should not silently reset capacity).
//   - CellDown on an unknown cell, CapacityChanged to the same capacity,
//     AgentJoined for an agent already a member, and AgentLeft for an agent
//     that already is not a member are all no-ops: they return reg unchanged
//     and a nil Change slice, so replaying an event twice is safe.
//   - AgentJoined/AgentLeft against an unknown cell are no-ops: an agent
//     cannot belong to a cell the registry has never seen come up.
func Apply(reg Registry, ev RegistryEvent) (Registry, []Change) {
	switch ev.Kind {
	case CellUp:
		return applyCellUp(reg, ev)
	case CellDown:
		return applyCellDown(reg, ev)
	case CapacityChanged:
		return applyCapacityChanged(reg, ev)
	case AgentJoined:
		return applyAgentJoined(reg, ev)
	case AgentLeft:
		return applyAgentLeft(reg, ev)
	default:
		return reg, nil
	}
}

func applyCellUp(reg Registry, ev RegistryEvent) (Registry, []Change) {
	if _, exists := reg.cells[ev.Cell]; exists {
		return reg, nil
	}
	cells := cloneCells(reg.cells)
	cells[ev.Cell] = cellState{capacity: ev.Capacity, agents: map[AgentID]struct{}{}}
	return Registry{cells: cells}, []Change{{Kind: CellAdded, Cell: ev.Cell, Capacity: ev.Capacity}}
}

func applyCellDown(reg Registry, ev RegistryEvent) (Registry, []Change) {
	if _, exists := reg.cells[ev.Cell]; !exists {
		return reg, nil
	}
	cells := cloneCells(reg.cells)
	delete(cells, ev.Cell)
	return Registry{cells: cells}, []Change{{Kind: CellRemoved, Cell: ev.Cell}}
}

func applyCapacityChanged(reg Registry, ev RegistryEvent) (Registry, []Change) {
	cs, exists := reg.cells[ev.Cell]
	if !exists || cs.capacity == ev.Capacity {
		return reg, nil
	}
	cells := cloneCells(reg.cells)
	cells[ev.Cell] = cellState{capacity: ev.Capacity, agents: cs.agents}
	return Registry{cells: cells}, []Change{{Kind: CapacityUpdated, Cell: ev.Cell, Capacity: ev.Capacity}}
}

func applyAgentJoined(reg Registry, ev RegistryEvent) (Registry, []Change) {
	cs, exists := reg.cells[ev.Cell]
	if !exists {
		return reg, nil
	}
	if _, already := cs.agents[ev.Agent]; already {
		return reg, nil
	}
	agents := cloneAgents(cs.agents)
	agents[ev.Agent] = struct{}{}
	cells := cloneCells(reg.cells)
	cells[ev.Cell] = cellState{capacity: cs.capacity, agents: agents}
	return Registry{cells: cells}, []Change{{Kind: AgentAdded, Cell: ev.Cell, Agent: ev.Agent}}
}

func applyAgentLeft(reg Registry, ev RegistryEvent) (Registry, []Change) {
	cs, exists := reg.cells[ev.Cell]
	if !exists {
		return reg, nil
	}
	if _, present := cs.agents[ev.Agent]; !present {
		return reg, nil
	}
	agents := cloneAgents(cs.agents)
	delete(agents, ev.Agent)
	cells := cloneCells(reg.cells)
	cells[ev.Cell] = cellState{capacity: cs.capacity, agents: agents}
	return Registry{cells: cells}, []Change{{Kind: AgentRemoved, Cell: ev.Cell, Agent: ev.Agent}}
}

// cloneCells shallow-copies the cell map so Apply can insert or remove an
// entry without mutating reg's map. Untouched cellStates are shared by
// reference, which is safe because cellState is never mutated in place —
// every write path builds a fresh cellState (and, when membership changes, a
// fresh agents map via cloneAgents) rather than editing one.
func cloneCells(cells map[model.CellID]cellState) map[model.CellID]cellState {
	out := make(map[model.CellID]cellState, len(cells)+1)
	for id, cs := range cells {
		out[id] = cs
	}
	return out
}

// cloneAgents copies an agent set so a membership change never mutates the
// set a prior Registry value still references.
func cloneAgents(agents map[AgentID]struct{}) map[AgentID]struct{} {
	out := make(map[AgentID]struct{}, len(agents)+1)
	for id := range agents {
		out[id] = struct{}{}
	}
	return out
}

// Snapshot projects reg into the point-in-time cell views the rest of the
// system reasons over. The result is sorted by CellID so it is stable and
// order-deterministic regardless of Go's randomized map iteration order.
func Snapshot(reg Registry) []model.CellView {
	var views []model.CellView
	for id, cs := range reg.cells {
		views = append(views, model.CellView{
			ID:   id,
			Size: len(cs.agents),
			Free: cs.capacity - len(cs.agents),
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
	return views
}
