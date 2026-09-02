// Package verification is the P3 open-tier dispatch/collect coordinator —
// the imperative shell that ties an open-tier task to the untrusted pool
// and back to a trusted answer. Per the #132 design ruling (fork a), it is
// a SEPARATE coordinator the control plane hands Tier == Open jobs to; it
// does not thread any untrusted-verification logic into the trusted P0
// dispatch path. See docs/phases/swarm-p3-components.txt §01-02 (THE
// OPEN-TIER VERIFICATION PATH / RESULT VERIFICATION).
//
// The coordinator itself performs no security DECISION — every one of those
// (which K machines to assign, how much redundancy a tier/reputation needs,
// which answer a quorum agrees on) comes from the pure
// internal/core/verification core. This package only does the I/O those
// decisions license: dispatching to K machines concurrently, collecting
// their claimed results within a timeout, and — on Agreed — writing the
// resulting trust update back through internal/shell/reputation.Store.
//
// # Control-plane integration hook
//
// This ticket does not rewire the P0 control-plane dispatch path (scope
// boundary, #140/#132 fork a) — only a documented seam. The intended
// integration, wired end to end in #143's capstone, is a single branch at
// the control plane's existing task-dispatch entry point (e.g.
// internal/shell/global.Server.dispatch or the per-region
// internal/shell/controlplane dispatch): before the existing Core-tier
// path runs, check `spec.Tier == model.Open` and, for each task, call
// Coordinator.Verify instead of the native run path. The Core-tier path
// itself is untouched either way.
//
// # MachineID <-> SpiffeID mapping
//
// Assign (internal/core/verification) works over the dispatch-pool key
// model.MachineID; reputation and the blacklist are keyed by the enrolled
// identity model.SpiffeID (internal/model/open.go). This package treats the
// two as the same underlying string: a single open-tier machine enrolls
// exactly once (internal/shell/enrollment, #142) and is issued exactly one
// SpiffeID, so the MachineID a dispatch pool names it by IS that identity's
// string value — see identityOf, the one seam function that encodes this
// assumption. Coordinator.Verify never trusts a Dispatcher's returned
// model.Result.ID for this attribution (an untrusted machine could claim to
// be anyone); it always overwrites it with identityOf(machine), the
// identity of whichever MachineID was actually dialed — mirroring how a
// real mTLS transport attributes a peer identity from the connection it
// dialed, never from the payload the untrusted peer sends.
package verification

import (
	"bytes"
	"context"
	"errors"

	corereputation "github.com/msivraj/swarm/internal/core/reputation"
	coreverification "github.com/msivraj/swarm/internal/core/verification"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/enrollment"
	"github.com/msivraj/swarm/internal/shell/reputation"
)

// Errors Verify can return.
var (
	// ErrNoDispatcher means Config.Dispatcher was nil.
	ErrNoDispatcher = errors.New("verification: Config.Dispatcher must not be nil")
	// ErrNoClock means Config.Clock was nil.
	ErrNoClock = errors.New("verification: Config.Clock must not be nil")
	// ErrEmptyPool means every machine in the supplied pool was excluded —
	// blacklisted, frozen (chronic quorum-loser), or both — leaving nothing
	// to assign.
	ErrEmptyPool = errors.New("verification: pool has no eligible (non-blacklisted, non-frozen) machines")
	// ErrNoQuorum means every attempt was exhausted (MaxAttempts rounds of
	// assign+dispatch+collect) without reaching an Agreed verdict.
	ErrNoQuorum = errors.New("verification: exhausted retries without reaching an Agreed verdict")
)

// Config configures a Coordinator.
type Config struct {
	// Dispatcher sends a task to one machine and collects its claimed
	// result. Required.
	Dispatcher Dispatcher
	// Reputation persists each identity's trust across verdicts: Verify
	// reads it (keyed by requester) to size redundancy, and writes it
	// (keyed by each responding machine) after an Agreed verdict. Nil
	// means every requester is treated as the zero-value (untrusted, max
	// redundancy) Reputation and no verdict updates are ever recorded.
	Reputation reputation.Store
	// Blacklist excludes refused identities from the assignable pool
	// (design fork b, #132) — distinct from P2's liveness eviction. Nil
	// means no blacklist filtering.
	Blacklist enrollment.Blacklist
	// Clock drives a collection round's timeout. Required.
	Clock Clock
	// Timeout bounds how long one collection round waits for the assigned
	// machines to answer before Verify gives up on that round's stragglers
	// and tallies whatever arrived.
	Timeout model.Duration
	// MaxAttempts bounds how many assign-dispatch-collect rounds Verify
	// will run before giving up (ErrNoQuorum) on a Disputed, Insufficient,
	// or below-floor round. Values <= 0 are treated as 1 (no retries).
	MaxAttempts int
}

// Coordinator runs the open-tier verification loop: assign K machines,
// dispatch, collect within a timeout, tally a quorum verdict, and either
// accept (recording each responding machine's trust update) or retry with a
// fresh seed. See the package doc for the full design.
type Coordinator struct {
	cfg Config
}

// New builds a Coordinator from cfg.
func New(cfg Config) *Coordinator {
	return &Coordinator{cfg: cfg}
}

// identityOf maps a dispatch-pool MachineID to the SpiffeID it is
// attributed to for reputation/blacklist purposes. See the package doc's
// "MachineID <-> SpiffeID mapping" section for the assumption this encodes.
func identityOf(m model.MachineID) model.SpiffeID {
	return model.SpiffeID(m)
}

// quorumFloor is the minimum-response floor (mandated by the #137 audit):
// Verify only trusts an Agreed verdict when at least this many of the
// assigned machines actually responded — a majority of K, not merely a
// majority of whoever happened to answer. A round that collects fewer than
// this is treated as Insufficient and retried with a fresh seed, even if
// the few responses that did arrive happen to agree with each other
// (verification.Verdict is K-agnostic and would otherwise call a lone
// response a trivial Agreed-of-one).
func quorumFloor(k int) int {
	return k/2 + 1
}

// Verify runs the open-tier verification loop for task, up to
// Config.MaxAttempts times:
//
//  1. Look up requester's reputation (zero value if Config.Reputation is
//     nil or requester has no history) and use it, with tier, to size K via
//     the pure core's Redundancy.
//  2. Assign K machines from pool, excluding any blacklisted (fork b) or
//     frozen (#211) identity, using a seed that is fresh on every attempt.
//  3. Dispatch to the assigned machines concurrently and collect their
//     results within Config.Timeout.
//  4. Enforce the minimum-response floor (quorumFloor): fewer responses
//     than that is treated as Insufficient regardless of what
//     verification.Verdict would say about the few that arrived.
//  5. Tally the pure core's Verdict. Agreed records each responding
//     machine's trust update (agreeing rise, lying fall) and returns.
//     Disputed/Insufficient (including a below-floor round) retries with a
//     fresh seed.
//
// Verify returns ErrEmptyPool if every machine in pool is blacklisted or
// frozen, and ErrNoQuorum if every attempt is exhausted without reaching
// Agreed — in the latter case the last non-Agreed Verdict observed is also
// returned.
func (c *Coordinator) Verify(ctx context.Context, task model.Task, tier model.Tier, requester model.SpiffeID, pool []model.MachineID, baseSeed uint64) (model.Verdict, error) {
	if c.cfg.Dispatcher == nil {
		return model.Verdict{}, ErrNoDispatcher
	}
	if c.cfg.Clock == nil {
		return model.Verdict{}, ErrNoClock
	}

	eligible := c.eligiblePool(pool)
	if len(eligible) == 0 {
		return model.Verdict{}, ErrEmptyPool
	}

	maxAttempts := c.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	rep := model.Reputation{}
	if c.cfg.Reputation != nil {
		rep = c.cfg.Reputation.Get(requester)
	}
	k := coreverification.Redundancy(tier, rep)

	var last model.Verdict
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// A fresh seed per attempt: deterministic shell data (not a new
		// entropy draw), but enough to make Assign choose a different
		// K-set on retry, exactly as the ticket requires. The caller's
		// baseSeed is free to fold in real entropy/a counter before
		// calling Verify — this per-attempt offset is orthogonal to that.
		seed := baseSeed + uint64(attempt)
		ids := coreverification.Assign(task.ID, eligible, k, seed)

		results := c.collect(ctx, ids, task)
		if len(results) < quorumFloor(len(ids)) {
			last = model.Verdict{Kind: model.Insufficient}
			continue
		}

		v := coreverification.Verdict(results)
		last = v
		if v.Kind != model.Agreed {
			continue
		}

		c.recordVerdict(results, v)
		return v, nil
	}

	return last, ErrNoQuorum
}

// eligiblePool returns the machines in pool that are neither blacklisted
// (fork b, #132) nor frozen (a chronic quorum-loser per the pure
// reputation.Eligible predicate, #209/#211), preserving order. A nil
// Config.Blacklist excludes nothing on the blacklist axis; a nil
// Config.Reputation excludes nothing on the freeze axis — every machine is
// treated as the zero-value (fresh, eligible) Reputation.
func (c *Coordinator) eligiblePool(pool []model.MachineID) []model.MachineID {
	out := make([]model.MachineID, 0, len(pool))
	for _, m := range pool {
		if c.cfg.Blacklist != nil && c.cfg.Blacklist.IsBlacklisted(identityOf(m)) {
			continue
		}
		if c.cfg.Reputation != nil && !corereputation.Eligible(c.cfg.Reputation.Get(identityOf(m))) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// collect dispatches task to every machine in ids concurrently and gathers
// their claimed results until either all have answered, Config.Timeout
// elapses (per Config.Clock), or ctx is done — whichever comes first. Every
// gathered model.Result.ID is set to identityOf(machine), never trusted
// from the Dispatcher's return value (see the package doc's mapping
// section). Machines that error (including a Hang past the round's
// deadline) simply do not contribute a result; collect never blocks past
// the round ending, since ids' dispatch calls all share a context that is
// canceled the moment collect returns.
func (c *Coordinator) collect(ctx context.Context, ids []model.MachineID, task model.Task) []model.Result {
	if len(ids) == 0 {
		return nil
	}

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultsCh := make(chan model.Result, len(ids))
	for _, id := range ids {
		go func(id model.MachineID) {
			res, err := c.cfg.Dispatcher.Dispatch(dispatchCtx, id, task)
			if err != nil {
				return
			}
			res.ID = identityOf(id)
			resultsCh <- res
		}(id)
	}

	timeout := c.cfg.Clock.After(c.cfg.Timeout)
	results := make([]model.Result, 0, len(ids))
	for len(results) < len(ids) {
		select {
		case r := <-resultsCh:
			results = append(results, r)
		case <-timeout:
			return results
		case <-ctx.Done():
			return results
		}
	}
	return results
}

// recordVerdict writes each responding machine's trust update for an
// Agreed verdict v: agreeing with v.Value rises, disagreeing falls. A nil
// Config.Reputation makes this a no-op.
func (c *Coordinator) recordVerdict(results []model.Result, v model.Verdict) {
	if c.cfg.Reputation == nil {
		return
	}
	for _, r := range results {
		c.cfg.Reputation.RecordVerdict(r.ID, bytes.Equal(r.Value, v.Value))
	}
}
