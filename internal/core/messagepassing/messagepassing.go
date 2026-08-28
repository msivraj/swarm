// Package messagepassing is a pure core: the message-passing driver that
// folds one message into an actor's state, routes a message to its delivery
// target via a routing table, and decides how a crashed actor is
// supervised. It performs no I/O and reads no clock — the shell owns
// mailboxes, draining inbound messages, snapshotting actors, and redelivery
// (at-least-once). It follows the shape set by internal/core/mitosis: take
// data, return a value or a description of a decision, never execute an
// effect.
//
// There is no global step here (unlike mitosis's Decide over all cells at
// once): React folds exactly one message into one actor, so the driver has
// no notion of a global order in which messages arrive. That absence of
// ordering is the point — React must be order-tolerant (property M1): the
// same set of messages, delivered in any shuffled order and with arbitrary
// duplicates, must fold to the same final Actor.State. See React's doc for
// how the reducer achieves that.
package messagepassing

import (
	"encoding/binary"
	"hash/fnv"
)

// ActorID identifies an actor.
type ActorID string

// Actor is one actor's durable state: its accumulated State plus the set of
// message IDs already folded into it (Seen). Seen is the basis of
// idempotent, order-tolerant React — a message ID already in Seen is a
// no-op, so at-least-once redelivery (the shell's job) can never double
// apply a message, and State is the point on which M1 (order-tolerance) is
// judged.
type Actor struct {
	ID    ActorID
	State []byte
	Seen  map[string]bool // message IDs already applied
}

// Message is one message addressed from one actor to another.
type Message struct {
	ID   string // unique message id, the basis of dedupe
	From ActorID
	To   ActorID
	Body []byte
}

// Send is an outbound message React asks the shell to deliver.
type Send struct {
	To   ActorID
	Body []byte
	ID   string
}

// React folds message m into actor a, returning a's next state and any
// Sends the fold produces. It is:
//
//   - Idempotent: if m.ID is already in a.Seen, React is a no-op — it
//     returns a unchanged and no Sends. This is what makes at-least-once
//     redelivery (the shell's job) safe: a message replayed after a crash
//     can never be applied twice.
//
//   - Commutative over distinct messages (property M1): State is folded
//     with XOR over an FNV-1a 64-bit hash of each distinct message's Body.
//     XOR is commutative and associative, so folding any permutation of a
//     set of distinct message IDs into the same starting Actor converges to
//     the same 8-byte State, regardless of delivery order. Combined with
//     the Seen-based dedupe above (which strips duplicates by ID before
//     they can be folded a second time), a shuffled-and-duplicated stream
//     of messages converges to the same final State as the deduplicated,
//     order-independent set — there is no global ordering to depend on.
//
// On a successful (non-duplicate) fold, React also returns one Send: an
// acknowledgement back to m.From, echoing m's body, so a caller can observe
// that a specific message was actually applied.
func React(a Actor, m Message) (Actor, []Send) {
	if a.Seen[m.ID] {
		return a, nil
	}

	next := Actor{
		ID:    a.ID,
		State: reduceState(a.State, m.Body),
		Seen:  markSeen(a.Seen, m.ID),
	}
	sends := []Send{{To: m.From, Body: m.Body, ID: "ack:" + m.ID}}
	return next, sends
}

// reduceState folds body into current via XOR over an FNV-1a 64-bit hash of
// body. This is the commutative, associative reducer that makes React's
// fold order-tolerant (M1) — see React's doc.
func reduceState(current, body []byte) []byte {
	acc := decodeAcc(current)
	acc ^= hashBody(body)
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, acc)
	return out
}

// decodeAcc reads current as a big-endian uint64 accumulator. Any state
// that isn't exactly 8 bytes (notably a fresh actor's nil State) decodes as
// the zero accumulator — XOR's identity element.
func decodeAcc(current []byte) uint64 {
	if len(current) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(current)
}

// hashBody hashes body with FNV-1a 64-bit. hash/fnv is a pure, deterministic
// function of its input — not a source of randomness or I/O — so it is safe
// inside a core.
func hashBody(body []byte) uint64 {
	h := fnv.New64a()
	h.Write(body) //nolint:errcheck // hash.Hash.Write never returns an error
	return h.Sum64()
}

// markSeen returns a copy of seen with id added, leaving seen itself
// unmutated — copy-on-write, like registry.Apply and routing.MergeGlobal.
func markSeen(seen map[string]bool, id string) map[string]bool {
	out := make(map[string]bool, len(seen)+1)
	for k, v := range seen {
		out[k] = v
	}
	out[id] = true
	return out
}

// CellRef is where an actor's cell lives, as far as routing is concerned.
type CellRef struct {
	Cell string
}

// RoutingTable maps each actor to the cell it currently lives on.
type RoutingTable map[ActorID]CellRef

// Delivery is a message resolved to a concrete delivery target.
type Delivery struct {
	To   ActorID
	Cell string
	Msg  Message
}

// Route resolves message m to its delivery target(s) via routing table t.
// A known m.To resolves to exactly one Delivery, on the cell t records for
// it. An unknown m.To (not present in t) returns no delivery — nil, not a
// panic — documenting that the shell is responsible for deciding what to do
// with an unroutable message (e.g. dead-letter it).
func Route(m Message, t RoutingTable) []Delivery {
	ref, ok := t[m.To]
	if !ok {
		return nil
	}
	return []Delivery{{To: m.To, Cell: ref.Cell, Msg: m}}
}

// SuperviseKind is the tag of a Supervise decision.
type SuperviseKind int

const (
	// Restart restarts the crashed actor from its last snapshot.
	Restart SuperviseKind = iota
	// Escalate hands the failure up to a higher supervisor. No caller in
	// this codebase constructs Escalate yet — see OnCrash's doc — but the
	// tag exists so a future supervision-depth/param can select it without
	// changing Supervise's shape.
	Escalate
)

// Supervise is the shell's instruction for a crashed actor: restart it
// (optionally from a snapshot) or escalate.
type Supervise struct {
	Kind         SuperviseKind
	FromSnapshot bool // set when Kind == Restart
}

// OnCrash decides supervision for a crashed actor. The phase doc names
// Restart{fromSnapshot} | Escalate but gives no escalation trigger (no
// supervision-depth, retry-count, or other parameter is threaded through
// this ticket's signature — OnCrash takes only the crashed actor's ID) so,
// per the ticket's resolution, OnCrash always restarts the actor from its
// last snapshot. A future ticket that adds a supervision-depth parameter
// can introduce the Escalate branch without breaking this signature's
// callers.
func OnCrash(a ActorID) Supervise {
	return Supervise{Kind: Restart, FromSnapshot: true}
}
