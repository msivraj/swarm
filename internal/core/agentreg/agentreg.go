// Package agentreg is a pure core: the agent's registration & reconnect state
// machine. It decides how an agent gets and stays joined to the fleet — dial
// out, enroll once, join a cell, heartbeat, and resume the loop on any drop
// (phase doc §05 "AGENT · REGISTRATION & RECONNECT"). It performs no I/O and
// reads no clock — the shell injects `now` and any jitter as data. This
// package follows the shape set by internal/core/mitosis: take data, return
// commands, never execute an effect.
package agentreg

import (
	"math"
	"time"

	"github.com/msivraj/swarm/internal/model"
)

// Phase is the tag of a RegState's four named states.
type Phase int

const (
	Dialing Phase = iota
	Enrolling
	Joining
	Member
)

// RegState is the agent's registration state. The doc names four phases
// (Dialing | Enrolling | Joining | Member); this core widens that into a
// small struct because Step needs two more bits of memory to make its
// documented decisions purely from (state, event) with no clock or I/O:
//
//   - Enrolled records whether the agent has completed enrollment before.
//     The component's responsibility line is "dial out, enroll once, join a
//     cell" — enrollment happens exactly once. On a reconnect (ConnLost ->
//     Dialing -> DialOK) an already-enrolled agent skips Enrolling and goes
//     straight to Joining. The doc says the shell "holds the SPIFFE identity
//     across reconnects"; the core only needs to remember *that* enrollment
//     happened, not the identity's bytes, so a bool is enough and keeps the
//     core free of any credential material.
//   - Attempt counts consecutive DialFail events since the last successful
//     dial (or the last ConnLost, which resets it). It drives Backoff's
//     input and the FailoverThreshold decision below.
//
// Both fields are preserved verbatim through transitions that don't
// explicitly reset them, which is what "keeps identity" means in this core:
// Enrolled survives a Member -> ConnLost -> Dialing round trip unchanged.
type RegState struct {
	Phase    Phase
	Attempt  int
	Enrolled bool
}

// RegEvent is the doc's six named events.
type RegEvent int

const (
	DialOK RegEvent = iota
	DialFail
	Enrolled
	Joined
	ConnLost
	Tick
)

// CmdKind is the tag of a Command's tagged union.
type CmdKind int

const (
	Dial CmdKind = iota
	Enroll
	JoinCell
	Heartbeat
	Wait
	Failover
)

// Command is a registration action the shell will execute. Cores return
// Commands; they never carry them out.
type Command struct {
	Kind CmdKind
	// Region is reserved for Dial in a later phase that picks among regions
	// (mirroring placement.Place's unused Task parameter); P0 always leaves
	// it empty and lets the shell dial its configured default region.
	Region string
	// Wait is set for Kind == Wait: how long the shell should sleep before
	// the next Dial.
	Wait time.Duration
}

// BackoffCfg configures exponential backoff with jitter.
type BackoffCfg struct {
	Base   time.Duration // delay for attempt 0, before jitter
	Max    time.Duration // hard cap on the computed delay
	Factor float64       // exponential growth factor per attempt
}

// DefaultBackoffCfg is the BackoffCfg Step uses internally to size its Wait
// commands. The doc leaves BackoffCfg's fields and its values unspecified;
// these are a reasonable starting point for the auditor to confirm or the
// shell to override by calling Backoff directly with its own cfg.
var DefaultBackoffCfg = BackoffCfg{
	Base:   100 * time.Millisecond,
	Max:    30 * time.Second,
	Factor: 2,
}

// FailoverThreshold is the number of consecutive DialFail events after which
// Step also emits Failover — asking the shell to try a different dial
// target — alongside its usual Wait. The doc names Failover as an available
// command but does not pin its trigger condition; this is the resolution:
// "repeated DialFail" means "still failing after this many tries."
const FailoverThreshold = 5

// Step advances the registration state machine. now and jitter are supplied
// by the shell; Step reads no clock and draws no randomness of its own.
//
// now is part of the boundary contract the doc specifies
// (`step(s, ev, now, jitter)`) but no transition below needs it: every
// decision here is a function of the state and the event alone. It is kept
// in the signature so a later phase (e.g. stamping a last-contact time into
// RegState) has nothing to retrofit.
func Step(s RegState, ev RegEvent, now model.Instant, jitter float64) (RegState, []Command) {
	_ = now

	switch ev {
	case ConnLost:
		// Resume the loop on any drop, from any phase, keeping identity.
		return RegState{Phase: Dialing, Enrolled: s.Enrolled}, []Command{{Kind: Dial}}

	case DialOK:
		if s.Phase != Dialing {
			return s, nil
		}
		if s.Enrolled {
			// Reconnecting with an identity already issued: enroll once,
			// so skip straight to joining a cell.
			return RegState{Phase: Joining, Enrolled: true}, []Command{{Kind: JoinCell}}
		}
		return RegState{Phase: Enrolling}, []Command{{Kind: Enroll}}

	case DialFail:
		if s.Phase != Dialing {
			return s, nil
		}
		next := RegState{Phase: Dialing, Enrolled: s.Enrolled, Attempt: s.Attempt + 1}
		wait := Command{Kind: Wait, Wait: Backoff(next.Attempt, DefaultBackoffCfg, jitter)}
		if next.Attempt >= FailoverThreshold {
			return next, []Command{{Kind: Failover}, wait}
		}
		return next, []Command{wait}

	case Enrolled:
		if s.Phase != Enrolling {
			return s, nil
		}
		return RegState{Phase: Joining, Enrolled: true}, []Command{{Kind: JoinCell}}

	case Joined:
		if s.Phase != Joining {
			return s, nil
		}
		return RegState{Phase: Member, Enrolled: true}, []Command{{Kind: Heartbeat}}

	case Tick:
		if s.Phase != Member {
			return s, nil
		}
		return s, []Command{{Kind: Heartbeat}}

	default:
		return s, nil
	}
}

// Backoff computes the retry delay for the given attempt using equal jitter:
// the delay is exponential(attempt) capped at cfg.Max, and jitter (expected
// in [0,1], clamped otherwise) linearly fills the top half of that cap. This
// keeps every delay at least half of the capped exponential value — so
// backoff still backs off even at jitter 0 — while jitter 1 recovers the
// full uncapped-by-jitter delay. attempt and jitter are supplied by the
// caller; Backoff draws nothing itself.
func Backoff(attempt int, cfg BackoffCfg, jitter float64) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	switch {
	case jitter < 0:
		jitter = 0
	case jitter > 1:
		jitter = 1
	}

	capped := exponentialDelay(attempt, cfg)
	half := capped / 2
	return half + time.Duration(float64(half)*jitter)
}

// exponentialDelay returns cfg.Base * cfg.Factor^attempt, capped at cfg.Max.
func exponentialDelay(attempt int, cfg BackoffCfg) time.Duration {
	d := float64(cfg.Base) * math.Pow(cfg.Factor, float64(attempt))
	if math.IsInf(d, 0) || d > float64(cfg.Max) {
		return cfg.Max
	}
	return time.Duration(d)
}
