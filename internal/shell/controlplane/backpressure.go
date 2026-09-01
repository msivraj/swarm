package controlplane

import (
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/backpressure"
	"github.com/msivraj/swarm/internal/model"
)

// This file wires the P4 backpressure core (internal/core/backpressure) into
// the control-plane ingress, per fork (b) of #157: SubmitJob and JoinAgent
// are gated with the FULL Admit/Throttle/Shed decision; PullTask honors only
// Throttle (a Shed there is degraded, never a hard rejection); ReportResult
// is never gated at all — completed work is precious, so it is not even
// wired into this file. The decision itself stays in the pure core; this
// shell only measures load, calls it, and enforces what it returns.

// requestPriorityParam is the SubmitJobRequest.Params key a caller sets to
// carry model.Req.Priority to the backpressure middleware. The wire
// SubmitJobRequest has no native Priority field, so — mirroring
// gangMinMembersParam's own precedent for MinMembers — Params is the typed
// side channel from the wire. Missing or non-numeric values map to 0,
// model.Req's zero-value (lowest) priority, never a silent high priority.
const requestPriorityParam = "priority"

// pullTaskShedDelay is the fixed backoff PullTask waits out when the
// underlying decision is Shed. LoadDecision.Delay is only meaningful on
// Throttle (Shed's zero value carries no delay), so degrading Shed to
// "wait, then still proceed" needs a delay of the shell's own choosing —
// this is the shell owning the token-bucket/timer side of the decision
// (see the package's phase-doc boundary), not a second admission threshold.
const pullTaskShedDelay = model.Duration(250 * time.Millisecond)

// realSleep is Config.Sleep's production default: a genuine wall-clock
// wait. Tests inject a fake instead (see Config.Sleep's doc), so no test in
// this package ever really sleeps out a throttle delay.
func realSleep(d model.Duration) {
	if d > 0 {
		time.Sleep(time.Duration(d))
	}
}

// requestPriority extracts a SubmitJobRequest's Req.Priority from params
// (see requestPriorityParam's doc). Any absent or non-numeric value is 0.
func requestPriority(params map[string]string) int {
	v, ok := params[requestPriorityParam]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// beginRPC claims one in-flight slot on the server's live load snapshot
// (backpressure.UpdateLoad) and returns the func the caller must defer to
// retire it once the RPC's real work finishes.
func (s *Server) beginRPC() func() {
	s.loadMu.Lock()
	s.load = backpressure.UpdateLoad(s.load, model.LoadEvent{InFlightDelta: 1})
	s.loadMu.Unlock()
	return func() {
		s.loadMu.Lock()
		s.load = backpressure.UpdateLoad(s.load, model.LoadEvent{InFlightDelta: -1})
		s.loadMu.Unlock()
	}
}

// decide reads the live load snapshot and calls backpressure.AdmitUnderLoad
// for a request of the given priority against cfg.Limits. Gathering the
// load snapshot is this shell method's only impure step; the decision
// itself is the pure core call CLAUDE.md's FCIS line requires.
func (s *Server) decide(priority int) model.LoadDecision {
	s.loadMu.Lock()
	load := s.load
	s.loadMu.Unlock()
	return backpressure.AdmitUnderLoad(model.Req{Priority: priority}, load, s.cfg.Limits)
}

// waitQueued waits out d on cfg.Sleep (never a direct time.Sleep — see
// Config.Sleep's doc), first recording the wait as one queued-not-yet-
// admitted request (backpressure.UpdateLoad's QueueDelta) so a request
// waiting out its own delay also raises the load later, concurrent
// requests are decided against — exactly a real admission queue's effect on
// load. A non-positive d is a no-op.
func (s *Server) waitQueued(d model.Duration) {
	if d <= 0 {
		return
	}
	s.loadMu.Lock()
	s.load = backpressure.UpdateLoad(s.load, model.LoadEvent{QueueDelta: 1})
	s.loadMu.Unlock()

	s.cfg.Sleep(d)

	s.loadMu.Lock()
	s.load = backpressure.UpdateLoad(s.load, model.LoadEvent{QueueDelta: -1})
	s.loadMu.Unlock()
}

// admitIngress enforces the FULL Admit/Throttle/Shed decision for an
// ingress RPC (SubmitJob, JoinAgent — fork (b) of #157): Shed returns a
// ResourceExhausted error the handler must return immediately, without
// running its real logic or claiming an in-flight slot (a shed request
// never starts being served, so done is nil); Throttle waits out the
// returned delay (waitQueued) before claiming the slot; Admit claims the
// slot immediately. Callers defer the returned done once err is nil.
func (s *Server) admitIngress(priority int) (done func(), err error) {
	switch d := s.decide(priority); d.Kind {
	case model.Shed:
		return nil, status.Error(codes.ResourceExhausted, "control plane overloaded, try again later")
	case model.Throttle:
		s.waitQueued(d.Delay)
	}
	return s.beginRPC(), nil
}

// admitThrottleOnly applies AdmitUnderLoad but degrades Shed to a fixed
// pullTaskShedDelay wait rather than a rejection (Throttle waits out its
// own returned delay unchanged, Admit proceeds immediately): PullTask is a
// throttle-only pressure-relief lever (fork (b) of #157) — an agent's
// runner loop is slowed under load, never hard-rejected, since a stalled
// puller can only make load worse, never better. Always returns a non-nil
// done (PullTask itself is never shed outright).
func (s *Server) admitThrottleOnly(priority int) (done func()) {
	switch d := s.decide(priority); d.Kind {
	case model.Throttle:
		s.waitQueued(d.Delay)
	case model.Shed:
		s.waitQueued(pullTaskShedDelay)
	}
	return s.beginRPC()
}
