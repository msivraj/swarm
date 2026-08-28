// Package detection is a pure core: it sets the failure-detection timeout by
// tier + coupling (O4) and decides whether a member has gone silent past that
// timeout. It performs no I/O and reads no clock — the shell tracks lastSeen
// per member from gossip and supplies every Instant as data.
package detection

import "github.com/msivraj/swarm/internal/model"

// Duration table (nanoseconds), by Tier x Coupling.
//
// Two axes set the timeout:
//   - Tier: Core is trusted and low-latency, so it can afford to declare a
//     member dead fast; Open is untrusted/high-latency, so it must wait
//     longer to avoid evicting a merely-slow, honest member.
//   - Coupling: tighter coupling (Barrier) means every other member is
//     blocked on the missing one, so detection must be fast; looser coupling
//     (Independent) means nothing else is waiting, so detection can be
//     patient. Ordered tightest-to-loosest: Barrier < Leader <
//     MessagePassing < Independent.
//
// Resolved ambiguity: the phase doc and ticket only pin the two extremes —
// core+barrier in seconds, open+independent in tens of seconds — and leave
// the other six cells and the exact numbers unspecified. This table fills
// them in with a single deliberate rule: within a tier, each looser coupling
// step adds one tier-scaled increment (Core: +1s per step; Open: +5s per
// step) over the tightest (Barrier) baseline. That keeps the acceptance
// criterion's required ordering exact — strictly increasing with looser
// coupling, and every Core value strictly less than every Open value — while
// giving every combination a concrete, documented number instead of leaving
// six cells to guesswork at the shell layer.
//
//	Tier  Coupling         Deadline
//	----  ---------------  --------
//	Core  Barrier            2s
//	Core  Leader             3s
//	Core  MessagePassing     4s
//	Core  Independent        5s
//	Open  Barrier           15s
//	Open  Leader            20s
//	Open  MessagePassing    25s
//	Open  Independent       30s
const (
	second = model.Duration(1_000_000_000)

	coreBarrier        = 2 * second
	coreLeader         = 3 * second
	coreMessagePassing = 4 * second
	coreIndependent    = 5 * second
	openBarrier        = 15 * second
	openLeader         = 20 * second
	openMessagePassing = 25 * second
	openIndependent    = 30 * second
)

// Deadline returns the failure-detection timeout for a member on tier t
// participating in a job coupled by c. It is pure table lookup: the same
// (t, c) always yields the same Duration, with unrecognized values falling
// back to the most patient entry (Open, Independent) rather than 0 — a
// silent zero-length deadline would make every member instantly "dead",
// which is never the safe default.
func Deadline(t model.Tier, c model.Coupling) model.Duration {
	switch t {
	case model.Core:
		switch c {
		case model.Barrier:
			return coreBarrier
		case model.Leader:
			return coreLeader
		case model.MessagePassing:
			return coreMessagePassing
		default:
			return coreIndependent
		}
	default: // model.Open and any future tier fall back to the patient table.
		switch c {
		case model.Barrier:
			return openBarrier
		case model.Leader:
			return openLeader
		case model.MessagePassing:
			return openMessagePassing
		default:
			return openIndependent
		}
	}
}

// IsDead reports whether a member last seen at lastSeen, with absolute
// deadline instant dl, is dead as of now. dl is the instant the shell
// computes as lastSeen + Deadline(t, c) — Deadline returns the span, IsDead
// consumes the resulting absolute instant plus the current time.
//
// Resolved ambiguity: the boundary is exclusive. now == dl is NOT dead — the
// deadline instant itself is still within budget; only now > dl (past the
// deadline) declares the member dead. This mirrors a standard exclusive
// timeout: a request due "at" its deadline has not yet failed.
//
// lastSeen is accepted (per the ticket's signature) for symmetry with the
// shell's bookkeeping — it computed dl from lastSeen — but deadness here
// depends only on the already-resolved dl vs now; IsDead does no arithmetic
// on lastSeen itself.
func IsDead(lastSeen, dl, now model.Instant) bool {
	return now > dl
}
