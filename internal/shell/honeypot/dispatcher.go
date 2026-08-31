// Package honeypot is the imperative shell that injects known-answer
// spot-checks into the open-tier verification dispatch path and blacklists
// any identity caught lying about one. See
// docs/phases/swarm-p3-components.txt §02 (HONEYPOT SPOT-CHECKS) and §03
// (the `honeypot` property), and the #132 design ruling (fork b): the
// blacklist a lie feeds is DISTINCT from the P2 reason-agnostic
// liveness-eviction path, consulted at enrollment/dispatch admission.
//
// This package makes no security DECISION of its own — ShouldProbe, Check,
// and OnLie all come from the pure internal/core/honeypot core. It only
// performs the I/O those decisions license: drawing the rng sample that
// decides whether to probe, sending the known-answer task, and writing a
// caught lie into the shared Blacklist.
//
// # Hook shape: a Dispatcher decorator
//
// The verification coordinator (internal/shell/verification, #140) takes a
// Dispatcher interface (Dispatch(ctx, machine, task) (model.Result, error))
// and is otherwise unmodified by this ticket (scope boundary, #141). The
// non-invasive hook is ProbingDispatcher: it WRAPS an inner Dispatcher and
// implements that same interface, so a Coordinator is simply constructed
// with a ProbingDispatcher in front of the real one:
//
//	coordinator := verification.New(verification.Config{
//	    Dispatcher: honeypot.NewProbingDispatcher(honeypot.Config{
//	        Dispatcher: realDispatcher, // the inner seam
//	        Reputation: repStore,       // reputation.Store satisfies ReputationReader
//	        Blacklist:  blacklist,      // *Blacklist satisfies BlacklistWriter
//	        RNG:        rng,
//	        ProbeTask:   knownAnswerTask,
//	        ProbeResult: knownResult,
//	    }),
//	    ...
//	})
//
// On every Dispatch to machine m, ProbingDispatcher:
//
//  1. reads m's current Reputation (zero value — the max probe rate — if
//     Config.Reputation is nil or m has no history);
//  2. asks the pure core ShouldProbe(rep, rng()) — rng is the shell's
//     func() float64 seam (a fake in tests); the core only ever sees the
//     resulting float;
//  3. if probing, sends a SIDE probe — Config.ProbeTask — to m over the
//     INNER Dispatcher (never back through itself, so the probe never
//     recurses) and compares the claim against Config.ProbeResult with the
//     pure core's Check. A Lie applies the pure core's OnLie action to
//     Config.Blacklist. The side probe never touches or corrupts the real
//     task's result;
//  4. always forwards the real task to the inner Dispatcher and returns
//     its real result — probing is additive, not a substitute for the
//     real dispatch.
//
// # Reputation seam
//
// A decorator that only sees (ctx, machine, task) has no reputation of its
// own to consult, so Config.Reputation is a small read-only seam
// (ReputationReader — just Get) rather than the full
// internal/shell/reputation.Store surface: ProbingDispatcher never writes a
// reputation (that stays the coordinator's job, #140, on a real verdict).
// internal/shell/reputation.Store already satisfies ReputationReader.
//
// # MachineID <-> SpiffeID mapping
//
// Like internal/shell/verification's identityOf, this package treats a
// dispatch-pool model.MachineID and the enrolled model.SpiffeID it names as
// the same underlying string — a single open-tier machine enrolls exactly
// once and is dispatched to under that identity. See identityOf.
package honeypot

import (
	"context"

	corehoneypot "github.com/msivraj/swarm/internal/core/honeypot"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/verification"
)

// ReputationReader is the read-only seam ProbingDispatcher uses to size
// each machine's probe rate. internal/shell/reputation.Store satisfies it
// (Get is the only method ProbingDispatcher calls); keeping the interface
// to a single read method decouples this package from Store's write
// surface, which ProbingDispatcher never uses — the reputation update on a
// real verdict stays the coordinator's job (#140), not this one's.
type ReputationReader interface {
	// Get returns the reputation stored for id. An id with no history
	// returns the zero-value model.Reputation{} — the zero-start floor.
	Get(id model.SpiffeID) model.Reputation
}

// BlacklistWriter is the write seam ProbingDispatcher applies a caught
// lie's model.Action to. The concrete Blacklist in this package implements
// it (and also internal/shell/enrollment.Blacklist's read side, so the same
// store both blocks admission/dispatch and receives ProbingDispatcher's
// writes).
type BlacklistWriter interface {
	// Apply performs the effect described by act (a Blacklist action
	// blacklists act.ID; any other Kind is a no-op).
	Apply(act model.Action)
}

// Config configures a ProbingDispatcher.
type Config struct {
	// Dispatcher is the inner seam every Dispatch call — real task and
	// probe task alike — is ultimately forwarded to. Required.
	Dispatcher verification.Dispatcher
	// Reputation reads each machine's current trust to size its probe
	// rate via the pure core's ShouldProbe. Nil is treated as every
	// machine having the zero-value (untrusted, max probe rate)
	// Reputation.
	Reputation ReputationReader
	// Blacklist receives the pure core's OnLie action when a probe catches
	// a lie. Required — without it, a caught lie has nowhere to go.
	Blacklist BlacklistWriter
	// RNG draws a uniform [0,1) sample for ShouldProbe. This is the
	// shell's seam: the core never draws randomness itself. Required.
	RNG func() float64
	// ProbeTask is the known-answer task injected when ShouldProbe says to
	// probe.
	ProbeTask model.Task
	// ProbeResult is the known-good result ProbeTask's claim is checked
	// against.
	ProbeResult model.Result
}

// ProbingDispatcher wraps a verification.Dispatcher, injecting known-answer
// honeypot probes at a sampled rate and blacklisting any identity caught
// lying about one. It implements verification.Dispatcher itself, so it can
// be composed directly into a Coordinator's Config.Dispatcher without any
// change to internal/shell/verification. See the package doc for the exact
// hook shape.
type ProbingDispatcher struct {
	cfg Config
}

// NewProbingDispatcher builds a ProbingDispatcher from cfg.
func NewProbingDispatcher(cfg Config) *ProbingDispatcher {
	return &ProbingDispatcher{cfg: cfg}
}

// identityOf maps a dispatch-pool MachineID to the SpiffeID it is
// attributed to for reputation/blacklist purposes — the same assumption
// internal/shell/verification's identityOf encodes: a single open-tier
// machine enrolls exactly once and is dispatched to under that identity's
// string value.
func identityOf(m model.MachineID) model.SpiffeID {
	return model.SpiffeID(m)
}

// Dispatch implements verification.Dispatcher. It may inject a known-answer
// probe to machine before forwarding task, per the package doc's hook
// shape. The real task is always dispatched and its result always
// returned; a probe never substitutes for or corrupts the real dispatch.
func (p *ProbingDispatcher) Dispatch(ctx context.Context, machine model.MachineID, task model.Task) (model.Result, error) {
	id := identityOf(machine)
	rep := model.Reputation{}
	if p.cfg.Reputation != nil {
		rep = p.cfg.Reputation.Get(id)
	}

	if corehoneypot.ShouldProbe(rep, p.cfg.RNG()) {
		p.probe(ctx, machine, id)
	}

	return p.cfg.Dispatcher.Dispatch(ctx, machine, task)
}

// probe sends Config.ProbeTask to machine over the inner Dispatcher — never
// back through p itself, so a probe never recursively triggers another
// probe — and blacklists id via Config.Blacklist if the pure core's Check
// says the claim is a Lie. A Dispatch error on the probe itself (e.g. the
// machine is unreachable) is not evidence of a lie either way, so it is
// treated as inconclusive and simply skipped — no blacklist action, no
// panic, no effect on the caller's real dispatch that follows.
func (p *ProbingDispatcher) probe(ctx context.Context, machine model.MachineID, id model.SpiffeID) {
	claimed, err := p.cfg.Dispatcher.Dispatch(ctx, machine, p.cfg.ProbeTask)
	if err != nil {
		return
	}
	if corehoneypot.Check(claimed, p.cfg.ProbeResult) != model.Lie {
		return
	}
	if p.cfg.Blacklist != nil {
		p.cfg.Blacklist.Apply(corehoneypot.OnLie(id))
	}
}

// compile-time assertion: ProbingDispatcher implements verification.Dispatcher.
var _ verification.Dispatcher = (*ProbingDispatcher)(nil)
