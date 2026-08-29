// Package cell's combine.go is issue #73's driver->template combine wiring:
// the ONE place in this shell a gradient/boundary/state payload crosses the
// wire (see cell.go's package doc). The driver (barrier, leader,
// message-passing) decides WHEN to combine — it already gathers the
// per-worker payloads into OpAllReduce/OpFold/OpAggregate, exactly as
// adapter_barrier.go, adapter_leader.go, and adapter_messagepassing.go
// translate from each core's own Command sum type. The template decides HOW
// (internal/core/templates' pure *Combine functions). This file is the
// lookup between the two — a CombineRegistry keyed by (driver, template) —
// so that adding a new coordinated template is data/registration only, per
// phase doc §06: "a new coordinated template is a new decompose/combine pair
// against an existing driver — no shell to write."
package cell

import (
	"sort"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/leader"
	"github.com/msivraj/swarm/internal/core/messagepassing"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
)

// TemplateKey identifies one (driver, template) combine pair — the
// registry's lookup key. Driver names the hosted driver kind ("barrier",
// "leader", "message-passing" — see the DriverName* constants below);
// Template names the job's chosen coordinated template ("dist-training",
// "sci-sim", "graph-compute", "agent-sim", ...). A single driver can host
// more than one template (barrier hosts both dist-training and sci-sim,
// phase doc §06's table), which is exactly why Template, not just Driver,
// is part of the key.
type TemplateKey struct {
	Driver   string
	Template string
}

// The driver names TemplateKey.Driver uses — matching phase doc §06's
// table's left column, not this package's DriverKind (that enum only
// distinguishes StepReport routing, cell.go/server.go). A driver name here
// is plain data, so a registry entry is a literal, readable map key.
const (
	DriverNameBarrier        = "barrier"
	DriverNameLeader         = "leader"
	DriverNameMessagePassing = "message-passing"
)

// CombineFunc is the shape every internal/core/templates combine function
// has: reduce one step's per-worker payloads into a single combined result.
// Every combine this ticket wires (DistTrainingCombine, SciSimCombine,
// GraphComputeCombine, AgentSimCombine) already matches this signature
// exactly, so registering one is a plain assignment — see
// DefaultCombineRegistry.
type CombineFunc func(payloads [][]byte) []byte

// CombineRegistry is the driver->template combine lookup this ticket wires.
// It is plain data (a map), so extending it — the extensibility property
// issue #73 asks for — is registration, not new shell code:
//
//	reg := DefaultCombineRegistry()
//	reg[TemplateKey{Driver: DriverNameBarrier, Template: "my-template"}] = myCombine
//
// CombiningDriver (below) is the only thing that reads a CombineRegistry;
// Loop, the adapters, and TransportExecutor never change when this map
// grows.
type CombineRegistry map[TemplateKey]CombineFunc

// DefaultCombineRegistry returns the registry for the four P2 templates,
// wired straight to internal/core/templates' pure combine functions —
// phase doc §06's table:
//
//	dist-training  + barrier          -> templates.DistTrainingCombine
//	sci-sim        + barrier          -> templates.SciSimCombine
//	graph-compute  + leader           -> templates.GraphComputeCombine
//	agent-sim      + message-passing  -> templates.AgentSimCombine
func DefaultCombineRegistry() CombineRegistry {
	return CombineRegistry{
		{Driver: DriverNameBarrier, Template: "dist-training"}:    templates.DistTrainingCombine,
		{Driver: DriverNameBarrier, Template: "sci-sim"}:          templates.SciSimCombine,
		{Driver: DriverNameLeader, Template: "graph-compute"}:     templates.GraphComputeCombine,
		{Driver: DriverNameMessagePassing, Template: "agent-sim"}: templates.AgentSimCombine,
	}
}

// CombiningDriver wraps another Driver (Inner — BarrierDriver, LeaderDriver,
// or MessagePassingDriver) with issue #73's combine wiring: after Inner.Step
// folds an event, CombiningDriver gathers the per-worker payloads of any
// OpAllReduce/OpFold/OpAggregate command Inner returned and fills in
// Command.Combined with Registry[Key]'s output over them — the pure
// template combine applied to exactly the payloads the driver gathered. Any
// other command (OpRelease, OpAssign, OpSend, ...) passes through unchanged.
//
// Loop itself never changes: it already calls Apply(cmds) — which replicates
// Combined into the log alongside every other field, satisfying "folded
// global state to the replicated log" for leader/graph-compute and
// message-passing/agent-sim with no extra wiring — and then Exec(cmds),
// where TransportExecutor reads a preceding AllReduce/Fold/Aggregate
// command's Combined bytes to distribute them to followers on the very next
// Release/Advance in the same batch (see transportexec.go's execOne). This
// is the "no new shell per template" property: swapping Key (and Inner)
// selects a different (driver, template) pair; CombiningDriver's own code
// never changes.
type CombiningDriver struct {
	Inner    Driver
	Registry CombineRegistry
	Key      TemplateKey
}

var _ Driver = CombiningDriver{}

// Step folds ev through Inner.Step, then combines any AllReduce/Fold/
// Aggregate command in the result.
func (d CombiningDriver) Step(s State, ev Event, now model.Instant) (State, []Command) {
	next, cmds := d.Inner.Step(s, ev, now)
	return next, d.combineAll(cmds)
}

// Snapshot delegates to Inner — CombiningDriver adds no state of its own.
func (d CombiningDriver) Snapshot(s State) []byte { return d.Inner.Snapshot(s) }

// Resume delegates to Inner — the replicated log's Combined field (set by
// Step above before Loop replicated it) is read-only history; no adapter's
// Resume needs it to rebuild state, since replay reconstructs state from
// the same fields it always has.
func (d CombiningDriver) Resume(log []Command, ckpt checkpoint.State) State {
	return d.Inner.Resume(log, ckpt)
}

// combineAll returns cmds with Combined filled in on every
// OpAllReduce/OpFold/OpAggregate command, via Registry[Key]. cmds itself is
// never mutated in place — a fresh slice is returned, matching the
// copy-on-write discipline the cores this package hosts already follow.
func (d CombiningDriver) combineAll(cmds []Command) []Command {
	if len(cmds) == 0 {
		return cmds
	}
	fn := d.Registry[d.Key]
	if fn == nil {
		return cmds
	}
	out := make([]Command, len(cmds))
	for i, c := range cmds {
		out[i] = combineOne(c, fn)
	}
	return out
}

func combineOne(c Command, fn CombineFunc) Command {
	payloads, ok := gatheredPayloads(c)
	if !ok {
		return c
	}
	c.Combined = fn(payloads)
	return c
}

// gatheredPayloads extracts c's per-worker payloads in a deterministic
// (sorted-by-key) order for OpAllReduce, OpFold, and OpAggregate — the three
// combine-triggering commands the phase doc names (§06's "combine (per
// step)" column: barrier all-reduce, leader superstep combine, message-
// passing aggregate state). Every other Op returns ok=false: it carries no
// per-worker payload map to combine.
//
// Sorting is not required for correctness (every wired combine's reduction —
// elementwise sum — is commutative and associative, see e.g.
// disttraining.go's DistTrainingCombine doc), but it makes Combined
// deterministic across runs for the exact same input map, matching this
// shell's own determinism discipline for anything it writes to the
// replicated log.
func gatheredPayloads(c Command) ([][]byte, bool) {
	switch c.Op {
	case OpAllReduce:
		return sortedPayloads(barrierKeys(c.Partials), c.Partials), true
	case OpFold:
		return sortedPayloads(leaderKeys(c.Results), c.Results), true
	case OpAggregate:
		return sortedPayloads(aggregateKeys(c.AggregateStates), c.AggregateStates), true
	default:
		return nil, false
	}
}

func barrierKeys(m map[barrier.WorkerID][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	return keys
}

func leaderKeys(m map[leader.FollowerID][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	return keys
}

func aggregateKeys(m map[messagepassing.ActorID][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, string(k))
	}
	return keys
}

// sortedPayloads sorts keys and returns m's values in that order. K is one
// of the three worker/follower/actor ID string types every driver's
// per-worker map is keyed by; a generic helper avoids repeating the same
// sort-then-project logic three times.
func sortedPayloads[K ~string](keys []string, m map[K][]byte) [][]byte {
	sort.Strings(keys)
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = m[K(k)]
	}
	return out
}
