package controlplane

import (
	"context"
	"time"

	"github.com/msivraj/swarm/internal/core/mitosis"
	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/core/routing"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
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
// snapshot + clock, call mitosis.DecideSignal, execute the returned Split/
// Merge commands.
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

// mitosisOnce reads a registry snapshot, calls mitosis.DecideSignal, executes
// every returned command, and then drains the ingress pending buffer: a
// split or merge can change which cells exist and how much free capacity
// each has, so a task placement.Place could not previously assign may now
// fit. This is also the P1 "dedicated tick" (issue #43) that periodically
// retries pending tasks even when no JoinAgent/SubmitJob call happens to
// trigger a drain — driven by the same injected clock/ticker as the rest of
// this loop, so it stays deterministic and I/O-free beyond the store call.
//
// #222 rewires this from mitosis.Decide to mitosis.DecideSignal: the
// signals fed in (buildCellSignals) carry each cell's Size (the P0 count
// proxy, unchanged) plus its measured P99/Tput from cfg.CellSignals, when
// one is wired. A cell cfg.CellSignals has no measurement for (or a nil
// cfg.CellSignals, e.g. DefaultConfig) gets P99==0, and DecideSignal falls
// back to exactly the count decision Decide made — so this rewiring is
// byte-identical to the old loop until a caller actually wires a signal
// source, the subsumption guarantee end to end.
func (s *Server) mitosisOnce() {
	s.mu.Lock()
	snapshot := registry.Snapshot(s.store.Registry())
	couplings := s.cellCouplingsLocked()
	cooldowns := make(map[model.CellID]model.Instant, len(s.cooldowns))
	for id, at := range s.cooldowns {
		cooldowns[id] = at
	}
	src := s.cfg.CellSignals
	s.mu.Unlock()

	now := s.now()
	signals := buildCellSignals(snapshot, couplings, src)
	for _, cmd := range mitosis.DecideSignal(signals, s.cfg.signalThresholds(), cooldowns, now) {
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
	// A split/merge can also change per-cell free capacity, so this is also
	// the gang pending queue's dedicated retry tick (#71), matching
	// drainPendingLocked's own rationale just above.
	s.retryPendingGangsLocked()
	s.mu.Unlock()
}

// cellCouplingsLocked returns the model.Coupling each currently
// gang-reserved cell is governed by, derived from s.gangJobs — the control
// plane's own record of which job's coupling contract a cell's membership
// must respect while that gang's reservation is live (see gang.go's
// gangReservation). A cell that holds no live gang reservation is absent
// from the returned map; buildCellSignals treats that as model.Independent
// (the zero value) — the loosest, most permissive signalThreshold band,
// which is exactly right for a plain Independent-job cell that coordinates
// nothing and so has no coupling to protect. Callers must hold s.mu.
func (s *Server) cellCouplingsLocked() map[model.CellID]model.Coupling {
	couplings := make(map[model.CellID]model.Coupling, len(s.gangJobs))
	for _, r := range s.gangJobs {
		for _, a := range r.assignments {
			couplings[a.Cell] = r.job.Coupling
		}
	}
	return couplings
}

// buildCellSignals converts a registry snapshot into the []model.CellSignal
// mitosis.DecideSignal reasons over: Cell/Size come straight from snapshot,
// Coupling from couplings (model.Independent when a cell is absent from it),
// and P99/Tput from src's per-cell measured reading when one exists. src ==
// nil (Config.CellSignals unset, e.g. DefaultConfig) or a cell src has no
// measurement for both leave that CellSignal's P99 at its zero value, so
// DecideSignal falls back to exactly the count-based decision for it — the
// subsumption guarantee buildCellSignals exists to preserve.
func buildCellSignals(snapshot []model.CellView, couplings map[model.CellID]model.Coupling, src CellSignalSource) []model.CellSignal {
	signals := make([]model.CellSignal, len(snapshot))
	for i, c := range snapshot {
		sig := model.CellSignal{Cell: c.ID, Coupling: couplings[c.ID], Size: c.Size}
		if src != nil {
			if p99, tput, ok := src.CellSignal(c.ID); ok {
				sig.P99 = p99
				sig.Tput = tput
			}
		}
		signals[i] = sig
	}
	return signals
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

// publishLoop drives the regional summary publish on a timer (S2, issue
// #44): it mirrors reapLoop/mitosisLoop's shape (a wall-clock ticker driving
// a *Once method), but is itself a no-op in standalone P0 mode
// (cfg.GlobalRouter == "") — it still runs (and its wg.Done() still fires on
// exit) so Serve's shutdown sequencing does not need a standalone-mode
// special case, it just never ticks.
func (s *Server) publishLoop() {
	defer s.wg.Done()
	if s.cfg.GlobalRouter == "" {
		<-s.stop
		return
	}

	ticker := time.NewTicker(s.cfg.SummaryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.publishOnce()
		case <-s.stop:
			return
		}
	}
}

// publishOnce computes this region's RegionalSummary via routing.Summarize
// over the current registry, stamps it with cfg.RegionID and the injected
// clock, and calls GlobalRouter.PublishSummary. It then also refreshes the
// cached peer view (GlobalRouter.GetGlobalView) that placement's spill
// decision (trySpillLocked) reads — the same dial/tick drives both, since a
// region already talking to GlobalRouter to publish is exactly the moment it
// should also learn who else is out there to spill to.
//
// A dropped publish (dial or RPC failure) is silently retried next tick, the
// same "logged and retried" contract reapLoop/mitosisLoop apply to their own
// per-tick work — there is no RPC caller here to surface an error to, and
// the clock read stays in this shell method, never inside a core.
func (s *Server) publishOnce() {
	s.mu.Lock()
	reg := s.store.Registry()
	s.mu.Unlock()

	summary := routing.Summarize(reg)
	summary.Region = s.cfg.RegionID
	summary.At = s.now()

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	client, closer, err := s.cfg.GlobalRouterDialer(ctx, s.cfg.GlobalRouter)
	if err != nil {
		return
	}
	defer func() { _ = closer.Close() }()

	if _, err := client.PublishSummary(ctx, &transport.PublishSummaryRequest{Summary: toProtoSummary(summary)}); err != nil {
		return
	}

	viewResp, err := client.GetGlobalView(ctx, &transport.GlobalViewRequest{})
	if err != nil {
		return
	}
	peers := make([]model.RegionView, 0, len(viewResp.GetRegions()))
	for _, rv := range viewResp.GetRegions() {
		id := model.RegionID(rv.GetId())
		if id == s.cfg.RegionID {
			continue // never spill "to" this region itself
		}
		peers = append(peers, model.RegionView{
			ID:     id,
			Free:   int(rv.GetFree()),
			Cells:  int(rv.GetCells()),
			Health: model.Health(rv.GetHealth()),
		})
	}

	s.mu.Lock()
	s.peerView = peers
	s.mu.Unlock()
}

// toProtoSummary converts a routing.RegionalSummary to its wire form.
func toProtoSummary(sum routing.RegionalSummary) *transport.RegionalSummary {
	return &transport.RegionalSummary{
		Region: string(sum.Region),
		Free:   int32(sum.Free),
		Cells:  int32(sum.Cells),
		Health: toProtoHealth(sum.Health),
		At:     int64(sum.At),
	}
}

// toProtoHealth converts a model.Health to its wire enum. The two enums
// share ordinal values by construction (see internal/model and swarm.proto),
// but this is written as an explicit switch for the same reason
// fromProtoCoupling is: a future divergence fails loudly instead of silently
// mismapping.
func toProtoHealth(h model.Health) transport.Health {
	switch h {
	case model.Degraded:
		return transport.Health_HEALTH_DEGRADED
	case model.Unreachable:
		return transport.Health_HEALTH_UNREACHABLE
	default:
		return transport.Health_HEALTH_HEALTHY
	}
}
