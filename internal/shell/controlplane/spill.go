package controlplane

import (
	"context"
	"time"

	"github.com/msivraj/swarm/internal/core/placement"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// spillCellID is the sentinel taskCell group a spilled task (one that ran on
// a peer, not any local cell) is bucketed under for this region's by-cell
// roll-up — the "origin cell" the ticket describes: this region never placed
// the task on a real cell, but the roll-up still needs a grouping key, and
// every spilled task sharing one key keeps the tiering total without
// inventing a fake per-peer cell identity this region has no other use for.
const spillCellID model.CellID = "__spilled__"

// dialTimeout bounds every outbound regional RPC (PublishSummary,
// GetGlobalView, DispatchTasks, the spill-result ReportResult forward,
// ReportPartial) so a slow or unreachable peer/GlobalRouter cannot hang a
// background loop or an RPC handler indefinitely.
const dialTimeout = 5 * time.Second

// trySpillLocked attempts to place t on a peer region when it does not fit
// any local cell: it is drainPendingLocked's region-full branch (S2, issue
// #44), replacing what would otherwise be an unconditional fall-through to
// s.pending. It reports whether t was handed off to a peer (true) or must
// stay pending (false) — the caller is responsible for appending t back to
// the pending buffer on false. Callers must hold s.mu.
//
// Owner-decided semantics (ticket #44): spill is disabled entirely in
// standalone P0 mode (cfg.GlobalRouter == ""), and only Independent tasks
// ever reach placement.PlaceAcross — a Barrier/Leader/MessagePassing job's
// tasks always stay local (or pending), never crossing a region boundary.
func (s *Server) trySpillLocked(t model.Task, working []model.CellView) bool {
	if s.cfg.GlobalRouter == "" {
		return false
	}

	jobID, known := s.taskJob[t.ID]
	if !known {
		return false
	}
	spec, ok, err := s.store.GetJob(jobID)
	if err != nil || !ok || spec.Coupling != model.Independent {
		return false
	}

	p := placement.PlaceAcross(t, working, s.peerView)
	if p.Kind != placement.Spill {
		return false
	}
	target, ok := s.cfg.PeerTargets[p.Region]
	if !ok || target == "" {
		return false // no dial address on file for an otherwise-qualifying peer: hold pending rather than guess
	}

	if !s.dispatchToPeerLocked(target, spec, t) {
		return false
	}
	return true
}

// dispatchToPeerLocked forwards t to the peer ControlPlane at target via
// DispatchTasks, with result_sink set to this region's own SelfAddress so
// the peer's agent can report t's raw result back here via the ordinary
// ControlPlane.ReportResult RPC (see forwardResultToOrigin, called from the
// peer's own ReportResult handler). On success it registers t in the store
// (so this region's later inbound ReportResult for it resolves — see the
// store package doc: PutResult requires a prior EnqueueTask/RequeueTask to
// learn a task's owning job) without ever making it pullable by any local
// agent, and records taskCell[t.ID] = spillCellID for the by-cell roll-up.
// Callers must hold s.mu; this does a network call while holding it — a
// deliberate simplicity/latency trade-off documented here rather than routed
// around, matching the ticket's scope (regional control plane, not a general
// async dispatch queue).
func (s *Server) dispatchToPeerLocked(target string, spec model.JobSpec, t model.Task) bool {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	client, closer, err := s.cfg.PeerDialer(ctx, target)
	if err != nil {
		return false
	}
	defer func() { _ = closer.Close() }()

	resp, err := client.DispatchTasks(ctx, &transport.DispatchTasksRequest{
		Job:        toProtoJobSpec(spec),
		Tasks:      toProtoTasks([]model.Task{t}),
		ResultSink: s.cfg.SelfAddress,
	})
	if err != nil || !resp.GetAccepted() {
		return false
	}

	// Register-and-immediately-drain: this cell never actually holds t (no
	// agent is ever mapped to spillCellID), it only needs the store to learn
	// t's owning job so a later inbound ReportResult for it succeeds.
	_ = s.store.EnqueueTask(spillCellID, t)
	_, _, _ = s.store.DequeueTask(spillCellID)
	s.taskCell[t.ID] = spillCellID
	return true
}

// forwardResultToOrigin sends result's raw TaskID/Output/OK to the
// ControlPlane at target via the ordinary ReportResult RPC — used when this
// region ran a task spilled to it by a peer (result_sink names that peer,
// its spill origin) and must fold the result in there rather than aggregate
// it locally.
func (s *Server) forwardResultToOrigin(target string, result model.TaskResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	client, closer, err := s.cfg.PeerDialer(ctx, target)
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()

	_, err = client.ReportResult(ctx, &transport.ReportResultRequest{
		TaskId: string(result.TaskID),
		Output: result.Output,
		Ok:     result.OK,
	})
	return err
}

// isSpillForward reports whether sink names a spill origin this region must
// forward a raw per-task result to, rather than a job this region owns the
// roll-up for. sink=="" is self (this region owns the roll-up);
// sink==globalRouter is a global-sink job (this region owns the *regional*
// roll-up, reported up as one partial — see maybeRollup); any other non-empty
// sink is a peer ControlPlane address a spilled task's result forwards to
// immediately, per task, bypassing this region's own roll-up gate entirely
// (that job is not this region's to finalize).
func isSpillForward(sink, globalRouter string) bool {
	return sink != "" && sink != globalRouter
}
