package controlplane

import (
	"time"

	"github.com/msivraj/swarm/internal/core/mitosis"
	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

// reapLoop evicts agents that have gone quiet for longer than
// cfg.HeartbeatTimeout. This is P0's failure detector: central, on a timer,
// applying registry.AgentLeft — there is no gossip/SWIM dissemination here.
func (s *Server) reapLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.HeartbeatSweep)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reapOnce()
		case <-s.stop:
			return
		}
	}
}

// reapOnce evicts every agent whose last-seen instant is older than
// cfg.HeartbeatTimeout, folding an AgentLeft event for each into the
// registry.
func (s *Server) reapOnce() {
	now := s.now()
	timeoutNS := s.cfg.HeartbeatTimeout.Nanoseconds()

	s.mu.Lock()
	defer s.mu.Unlock()

	for agent, last := range s.lastSeen {
		if int64(now-last) < timeoutNS {
			continue
		}
		cell, ok := s.agentCell[agent]
		if !ok {
			delete(s.lastSeen, agent)
			continue
		}
		s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.AgentLeft, Cell: cell, Agent: registry.AgentID(agent)})
		delete(s.lastSeen, agent)
		delete(s.agentCell, agent)
		if members := s.cellAgents[cell]; members != nil {
			delete(members, agent)
		}
	}
}

// mitosisLoop drives the mitosis core on a timer: read the registry
// snapshot + clock, call mitosis.Decide, execute the returned Split/Merge
// commands.
func (s *Server) mitosisLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.MitosisInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mitosisOnce()
		case <-s.stop:
			return
		}
	}
}

// mitosisOnce reads a registry snapshot, calls mitosis.Decide, executes
// every returned command, and then drains the ingress pending buffer: a
// split or merge can change which cells exist and how much free capacity
// each has, so a task placement.Place could not previously assign may now
// fit. This is also the P1 "dedicated tick" (issue #43) that periodically
// retries pending tasks even when no JoinAgent/SubmitJob call happens to
// trigger a drain — driven by the same injected clock/ticker as the rest of
// this loop, so it stays deterministic and I/O-free beyond the store call.
func (s *Server) mitosisOnce() {
	s.mu.Lock()
	snapshot := registry.Snapshot(s.store.Registry())
	cooldowns := make(map[model.CellID]model.Instant, len(s.cooldowns))
	for id, at := range s.cooldowns {
		cooldowns[id] = at
	}
	s.mu.Unlock()

	now := s.now()
	for _, cmd := range mitosis.Decide(snapshot, s.cfg.MitosisThresholds, cooldowns, now) {
		switch cmd.Op {
		case mitosis.Split:
			s.executeSplit(cmd.Cell, now)
		case mitosis.Merge:
			s.executeMerge(cmd.Cell, cmd.Other, now)
		}
	}

	s.mu.Lock()
	// drainPendingLocked's only error comes from store.EnqueueTask, which
	// only ever fails on an empty TaskID — impossible here, since every
	// pending task was already admitted (and so ID-validated) by SubmitJob.
	// There is no RPC caller to report an error to from a background loop,
	// so this mirrors applyRegistryEventLocked's handling of SetRegistry.
	_ = s.drainPendingLocked()
	s.mu.Unlock()
}

// executeSplit carries out a mitosis.Split command: it forms two new cells,
// dividing cell's current capacity and membership between them, then tears
// down cell. The registry core has no "split" primitive of its own — it
// only knows CellUp/CellDown/CapacityChanged/AgentJoined/AgentLeft — so the
// shell composes those into the split, using its own agent<->cell
// bookkeeping (recordJoinLocked's cellAgents) to know which agents to move,
// since registry.Snapshot exposes only aggregate cell sizes.
func (s *Server) executeSplit(cell model.CellID, now model.Instant) {
	s.mu.Lock()
	defer s.mu.Unlock()

	agents := sortedAgents(s.cellAgents[cell])
	capacity := cellCapacity(registry.Snapshot(s.store.Registry()), cell)
	if capacity == 0 {
		return // cell no longer exists (e.g. raced with a merge); nothing to split
	}

	half := len(agents) / 2
	groupA, groupB := agents[:half], agents[half:]
	capA := capacity / 2
	capB := capacity - capA

	idA, idB := s.newCellIDLocked(), s.newCellIDLocked()
	s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellUp, Cell: idA, Capacity: capA})
	s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellUp, Cell: idB, Capacity: capB})
	s.moveAgentsLocked(groupA, idA)
	s.moveAgentsLocked(groupB, idB)
	// cell's store queue does not migrate itself — drain whatever is still
	// queued on it into the pending buffer before it is torn down, or those
	// tasks would be orphaned (see migrateCellQueueLocked's doc).
	_ = s.migrateCellQueueLocked(cell)
	s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellDown, Cell: cell})

	delete(s.cellAgents, cell)
	delete(s.cooldowns, cell)
	s.cooldowns[idA] = now
	s.cooldowns[idB] = now
}

// executeMerge carries out a mitosis.Merge command: it forms one new cell
// holding a's and b's combined capacity and membership, then tears down a
// and b. See executeSplit's doc for why this is composed from the registry
// core's primitives rather than a single "merge" event.
func (s *Server) executeMerge(a, b model.CellID, now model.Instant) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := registry.Snapshot(s.store.Registry())
	capA, capB := cellCapacity(snapshot, a), cellCapacity(snapshot, b)
	if capA == 0 || capB == 0 {
		return // one side no longer exists (e.g. raced with another split); nothing to merge
	}

	agentsA := sortedAgents(s.cellAgents[a])
	agentsB := sortedAgents(s.cellAgents[b])

	merged := s.newCellIDLocked()
	s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellUp, Cell: merged, Capacity: capA + capB})
	s.moveAgentsLocked(agentsA, merged)
	s.moveAgentsLocked(agentsB, merged)
	// a and b's store queues do not migrate themselves — drain whatever is
	// still queued on each into the pending buffer before either is torn
	// down, or those tasks would be orphaned (see migrateCellQueueLocked's
	// doc).
	_ = s.migrateCellQueueLocked(a)
	_ = s.migrateCellQueueLocked(b)
	s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellDown, Cell: a})
	s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.CellDown, Cell: b})

	delete(s.cellAgents, a)
	delete(s.cellAgents, b)
	delete(s.cooldowns, a)
	delete(s.cooldowns, b)
	s.cooldowns[merged] = now
}

// migrateCellQueueLocked drains every task still queued on cell (via
// repeated store.DequeueTask) into the ingress pending buffer, in that
// queue's own FIFO order. It exists because a cell's store queue does not
// migrate itself when the cell is retired: executeSplit/executeMerge move
// agent membership and CellDown the old cell, but a task already
// EnqueueTask'd on that cell would otherwise be orphaned — no agent maps to
// the defunct cell anymore, so PullTask can never reach it, and it was
// never in s.pending, so no drain would ever re-place it either, hanging
// the owning job forever.
//
// Callers must hold s.mu and must call this before the cell's CellDown
// event (so the migrated tasks are already in s.pending in time for
// mitosisOnce's own end-of-tick drainPendingLocked call to re-run
// placement.Place over them, same mechanism already used for region-full
// pending tasks — placement.Place itself is unmodified and this never
// spills cross-region, only re-places locally onto the reshaped fleet).
func (s *Server) migrateCellQueueLocked(cell model.CellID) error {
	for {
		t, ok, err := s.store.DequeueTask(cell)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		s.pending = append(s.pending, t)
	}
}

// moveAgentsLocked folds an AgentJoined event for each agent into dest and
// updates the shell's agent<->cell bookkeeping to match. Callers must hold
// s.mu.
func (s *Server) moveAgentsLocked(agents []string, dest model.CellID) {
	if len(agents) == 0 {
		return
	}
	if s.cellAgents[dest] == nil {
		s.cellAgents[dest] = make(map[string]struct{})
	}
	for _, agent := range agents {
		s.applyRegistryEventLocked(registry.RegistryEvent{Kind: registry.AgentJoined, Cell: dest, Agent: registry.AgentID(agent)})
		s.cellAgents[dest][agent] = struct{}{}
		s.agentCell[agent] = dest
	}
}
