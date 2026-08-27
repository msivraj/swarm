package agent

import (
	"context"
	"time"

	"github.com/msivraj/swarm/internal/core/agentreg"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// runRegistration drives agentreg's registration/reconnect core: it seeds
// the loop with an initial Dial (mirroring what a ConnLost does from any
// later phase), executes every Command the core returns, and feeds the
// resulting events back into Step. It runs until ctx is done or a command
// execution hits a non-recoverable error (only ctx cancellation today).
//
// Ambiguities resolved here (see the PR description for the full write-up):
//   - Enroll has no RPC of its own in the P0 proto (single-cert/trusted
//     machines — see the ticket). It is executed as a local step that always
//     succeeds immediately, matching the doc's framing of "enroll" as
//     establishing identity rather than talking to the network.
//   - JoinCell is the actual JoinAgent RPC. It runs on every Dialing ->
//     Joining transition, including reconnects — which is what makes
//     "enroll once, join a cell every (re)connect" observable.
//   - Neither JoinCell nor Heartbeat has a documented failure event; both
//     failures are folded into ConnLost, which is the only event that always
//     resumes the loop, from any phase.
//   - Wait's job is "sleep, then retry the dial": Step never emits another
//     Dial after a Wait (the state is already Dialing), so the shell issues
//     one directly once the timer fires.
func (a *Agent) runRegistration(ctx context.Context) error {
	state := agentreg.RegState{}
	queue := []agentreg.Command{{Kind: agentreg.Dial}}

	var hb *time.Ticker
	defer func() {
		if hb != nil {
			hb.Stop()
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if len(queue) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tickerChan(hb):
				var cmds []agentreg.Command
				state, cmds = agentreg.Step(state, agentreg.Tick, a.now(), a.jitter())
				queue = append(queue, cmds...)
				hb = a.syncHeartbeatTicker(state, hb)
			}
			continue
		}

		cmd := queue[0]
		queue = queue[1:]

		ev, hasEvent, extra, err := a.execRegCommand(ctx, cmd)
		if err != nil {
			return err
		}
		queue = append(queue, extra...)
		if !hasEvent {
			continue
		}

		if ev == agentreg.ConnLost {
			// The current connection is presumed dead: drop it so the
			// runner loop (and any in-flight Dial) waits for a fresh one
			// rather than using a stale client.
			a.clients.clear()
		}

		var cmds []agentreg.Command
		state, cmds = agentreg.Step(state, ev, a.now(), a.jitter())
		queue = append(queue, cmds...)
		hb = a.syncHeartbeatTicker(state, hb)
	}
}

// tickerChan returns t.C, or nil if t is nil. A nil channel blocks forever
// in a select, which is exactly "no heartbeat ticker armed."
func tickerChan(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

// syncHeartbeatTicker starts the heartbeat ticker on entering Member and
// stops it on leaving, so Tick events (and therefore Heartbeat commands)
// only fire while the agent actually is a member.
func (a *Agent) syncHeartbeatTicker(state agentreg.RegState, hb *time.Ticker) *time.Ticker {
	if state.Phase == agentreg.Member {
		if hb == nil {
			return time.NewTicker(a.cfg.HeartbeatInterval)
		}
		return hb
	}
	if hb != nil {
		hb.Stop()
	}
	return nil
}

// execRegCommand executes one agentreg.Command. It returns the RegEvent to
// feed back into Step (if hasEvent), any extra Commands to enqueue directly
// without going through Step (only Wait uses this, to re-arm Dial), and a
// non-nil error only for a condition the loop cannot recover from (ctx
// cancellation) — RPC failures are reported as events (DialFail/ConnLost),
// never as Go errors.
func (a *Agent) execRegCommand(ctx context.Context, cmd agentreg.Command) (ev agentreg.RegEvent, hasEvent bool, extra []agentreg.Command, err error) {
	switch cmd.Kind {
	case agentreg.Dial:
		return a.execDial(ctx)
	case agentreg.Enroll:
		a.recordEnroll()
		return agentreg.Enrolled, true, nil, nil
	case agentreg.JoinCell:
		return a.execJoinCell(ctx)
	case agentreg.Heartbeat:
		return a.execHeartbeat(ctx)
	case agentreg.Wait:
		return a.execWait(ctx, cmd.Wait)
	case agentreg.Failover:
		a.execFailover()
		return 0, false, nil, nil
	default:
		return 0, false, nil, nil
	}
}

func (a *Agent) execDial(ctx context.Context) (agentreg.RegEvent, bool, []agentreg.Command, error) {
	target := a.currentTarget()
	client, closer, err := probeDial(ctx, a.cfg.Dialer, target)
	if err != nil {
		if ctx.Err() != nil {
			return 0, false, nil, ctx.Err()
		}
		return agentreg.DialFail, true, nil, nil
	}
	a.clients.set(client, closer)
	return agentreg.DialOK, true, nil, nil
}

func (a *Agent) execJoinCell(ctx context.Context) (agentreg.RegEvent, bool, []agentreg.Command, error) {
	client, err := a.clients.get(ctx)
	if err != nil {
		return 0, false, nil, err
	}
	resp, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{
		Agent:  a.cfg.AgentID,
		Region: a.cfg.Region,
		Caps:   a.cfg.Caps,
	})
	if err != nil || !resp.Accepted {
		return agentreg.ConnLost, true, nil, nil
	}
	return agentreg.Joined, true, nil, nil
}

func (a *Agent) execHeartbeat(ctx context.Context) (agentreg.RegEvent, bool, []agentreg.Command, error) {
	client, err := a.clients.get(ctx)
	if err != nil {
		return 0, false, nil, err
	}
	resp, err := client.Heartbeat(ctx, &transport.HeartbeatRequest{Agent: a.cfg.AgentID})
	if err != nil || !resp.Ok {
		return agentreg.ConnLost, true, nil, nil
	}
	// Heartbeat has no success event in the doc's RegEvent set: staying
	// Member is the whole point, and the next Tick will heartbeat again.
	return 0, false, nil, nil
}

func (a *Agent) execWait(ctx context.Context, d time.Duration) (agentreg.RegEvent, bool, []agentreg.Command, error) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return 0, false, nil, ctx.Err()
	case <-t.C:
	}
	// The state is already Dialing; Step has nothing more to say until the
	// next dial attempt resolves, so the shell issues it directly.
	return 0, false, []agentreg.Command{{Kind: agentreg.Dial}}, nil
}
