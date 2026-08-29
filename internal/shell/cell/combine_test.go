package cell

import (
	"context"
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/barrier"
	"github.com/msivraj/swarm/internal/core/checkpoint"
	"github.com/msivraj/swarm/internal/core/leader"
	"github.com/msivraj/swarm/internal/core/messagepassing"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// encodeVec encodes vs as consecutive big-endian float64s — the wire shape
// DistTrainingCombine/AgentSimCombine's sum reduction expects (see
// internal/core/templates/vectorsum.go).
func encodeVec(vs ...float64) []byte {
	out := make([]byte, len(vs)*8)
	for i, v := range vs {
		binary.BigEndian.PutUint64(out[i*8:i*8+8], math.Float64bits(v))
	}
	return out
}

// stubDriver is a Driver whose Step always returns the same fixed cmds,
// ignoring s/ev/now — it isolates combine.go's wiring from any particular
// core's own step logic so these tests can hand CombiningDriver exactly the
// OpAllReduce/OpFold/OpAggregate Command they want combined.
type stubDriver struct{ cmds []Command }

func (d stubDriver) Step(s State, _ Event, _ model.Instant) (State, []Command) { return s, d.cmds }
func (stubDriver) Snapshot(State) []byte                                       { return nil }
func (stubDriver) Resume([]Command, checkpoint.State) State                    { return nil }

// TestCombineRegistry_RoutesToMatchingCombine is issue #73's first
// acceptance criterion: for each of the four templates, a step's per-worker
// payloads are routed to the correct combine and the combined bytes equal
// the pure *Combine output for those same payloads.
func TestCombineRegistry_RoutesToMatchingCombine(t *testing.T) {
	tests := []struct {
		name string
		key  TemplateKey
		cmd  Command
		want []byte
	}{
		{
			name: "barrier/dist-training all-reduces gradients",
			key:  TemplateKey{Driver: DriverNameBarrier, Template: "dist-training"},
			cmd: Command{Op: OpAllReduce, Partials: map[barrier.WorkerID][]byte{
				"a": encodeVec(1, 2), "b": encodeVec(3, 4),
			}},
			want: templates.DistTrainingCombine([][]byte{encodeVec(1, 2), encodeVec(3, 4)}),
		},
		{
			name: "barrier/sci-sim exchanges boundaries",
			key:  TemplateKey{Driver: DriverNameBarrier, Template: "sci-sim"},
			cmd: Command{Op: OpAllReduce, Partials: map[barrier.WorkerID][]byte{
				"a": []byte("left"), "b": []byte("right"),
			}},
			want: templates.SciSimCombine([][]byte{[]byte("left"), []byte("right")}),
		},
		{
			name: "leader/graph-compute superstep-combines",
			key:  TemplateKey{Driver: DriverNameLeader, Template: "graph-compute"},
			cmd: Command{Op: OpFold, Results: map[leader.FollowerID][]byte{
				"f1": templates.EncodeGraphSuperstepPartial(2, []byte("m1")),
				"f2": templates.EncodeGraphSuperstepPartial(3, []byte("m2")),
			}},
			want: templates.GraphComputeCombine([][]byte{
				templates.EncodeGraphSuperstepPartial(2, []byte("m1")),
				templates.EncodeGraphSuperstepPartial(3, []byte("m2")),
			}),
		},
		{
			name: "message-passing/agent-sim aggregates state",
			key:  TemplateKey{Driver: DriverNameMessagePassing, Template: "agent-sim"},
			cmd: Command{Op: OpAggregate, AggregateStates: map[messagepassing.ActorID][]byte{
				"p1": encodeVec(1, 1), "p2": encodeVec(2, 2),
			}},
			want: templates.AgentSimCombine([][]byte{encodeVec(1, 1), encodeVec(2, 2)}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := CombiningDriver{
				Inner:    stubDriver{cmds: []Command{tt.cmd}},
				Registry: DefaultCombineRegistry(),
				Key:      tt.key,
			}
			_, cmds := d.Step(nil, Event{}, 0)
			if len(cmds) != 1 {
				t.Fatalf("Step returned %d commands, want 1", len(cmds))
			}
			if !reflect.DeepEqual(cmds[0].Combined, tt.want) {
				t.Fatalf("Combined = %#v, want %#v", cmds[0].Combined, tt.want)
			}
			// The wiring must not mutate the un-combined fields (Partials,
			// Results, AggregateStates) it read from.
			gotWithoutCombined := cmds[0]
			gotWithoutCombined.Combined = nil
			wantWithoutCombined := tt.cmd
			if !reflect.DeepEqual(gotWithoutCombined, wantWithoutCombined) {
				t.Fatalf("command fields besides Combined changed: got %#v, want %#v", gotWithoutCombined, wantWithoutCombined)
			}
		})
	}
}

// trivialXORCombine is a stand-in 5th (existing-driver) combine used by
// TestCombineRegistry_Extensibility below — it XORs every payload's first
// byte, a deliberately trivial reduction unrelated to any of the four wired
// templates.
func trivialXORCombine(payloads [][]byte) []byte {
	var acc byte
	for _, p := range payloads {
		if len(p) > 0 {
			acc ^= p[0]
		}
	}
	return []byte{acc}
}

// TestCombineRegistry_Extensibility is issue #73's second acceptance
// criterion: registering a trivial 5th combine — a new (existing-driver)
// template — is data/registration only, run through the exact same wiring
// (CombiningDriver) with no new shell code. The only "new code" here is the
// trivial combine function itself and its one registry entry; CombiningDriver,
// Loop, and every adapter are completely unmodified from the four-template
// case above.
func TestCombineRegistry_Extensibility(t *testing.T) {
	reg := DefaultCombineRegistry()
	key := TemplateKey{Driver: DriverNameBarrier, Template: "checksum-xor"}
	reg[key] = trivialXORCombine // registration: the entire "add a template" story

	cmd := Command{Op: OpAllReduce, Partials: map[barrier.WorkerID][]byte{
		"a": {0x0F}, "b": {0xF0},
	}}
	d := CombiningDriver{Inner: stubDriver{cmds: []Command{cmd}}, Registry: reg, Key: key}

	_, cmds := d.Step(nil, Event{}, 0)
	if len(cmds) != 1 {
		t.Fatalf("Step returned %d commands, want 1", len(cmds))
	}
	want := trivialXORCombine([][]byte{{0x0F}, {0xF0}})
	if !reflect.DeepEqual(cmds[0].Combined, want) {
		t.Fatalf("Combined = %#v, want %#v", cmds[0].Combined, want)
	}

	// The four originally-wired templates are untouched by the registration.
	if reg[TemplateKey{Driver: DriverNameBarrier, Template: "dist-training"}] == nil {
		t.Fatalf("registering checksum-xor clobbered dist-training's entry")
	}
}

// TestCombiningDriver_PassesThroughNonCombineCommands asserts that commands
// carrying no per-worker payload map (Release, Assign, Send, ...) are
// returned unchanged — the wiring only ever touches OpAllReduce/OpFold/
// OpAggregate.
func TestCombiningDriver_PassesThroughNonCombineCommands(t *testing.T) {
	cmd := Command{Op: OpRelease, Step: 3}
	d := CombiningDriver{
		Inner:    stubDriver{cmds: []Command{cmd}},
		Registry: DefaultCombineRegistry(),
		Key:      TemplateKey{Driver: DriverNameBarrier, Template: "dist-training"},
	}
	_, cmds := d.Step(nil, Event{}, 0)
	if !reflect.DeepEqual(cmds, []Command{cmd}) {
		t.Fatalf("cmds = %#v, want unchanged %#v", cmds, []Command{cmd})
	}
}

// TestCombiningDriver_UnknownKeyIsNoOp asserts a Key with no registry entry
// leaves every command's Combined nil, rather than panicking — the same
// nil-is-no-op convention TransportExecutor's own hooks use.
func TestCombiningDriver_UnknownKeyIsNoOp(t *testing.T) {
	cmd := Command{Op: OpAllReduce, Partials: map[barrier.WorkerID][]byte{"a": []byte("x")}}
	d := CombiningDriver{
		Inner:    stubDriver{cmds: []Command{cmd}},
		Registry: DefaultCombineRegistry(),
		Key:      TemplateKey{Driver: DriverNameBarrier, Template: "no-such-template"},
	}
	_, cmds := d.Step(nil, Event{}, 0)
	if len(cmds) != 1 || cmds[0].Combined != nil {
		t.Fatalf("cmds = %#v, want Combined nil", cmds)
	}
}

// TestCombiningDriver_SnapshotResumeDelegateToInner asserts Snapshot/Resume
// are plain pass-throughs — CombiningDriver adds no state of its own.
func TestCombiningDriver_SnapshotResumeDelegateToInner(t *testing.T) {
	start := barrier.State{Step: 2, Members: []barrier.WorkerID{"a"}}
	d := CombiningDriver{Inner: BarrierDriver{}, Registry: DefaultCombineRegistry(), Key: TemplateKey{Driver: DriverNameBarrier, Template: "dist-training"}}

	blob := d.Snapshot(start)
	wantBlob := BarrierDriver{}.Snapshot(start)
	if !reflect.DeepEqual(blob, wantBlob) {
		t.Fatalf("Snapshot = %#v, want %#v", blob, wantBlob)
	}

	ckpt := checkpoint.State{DriverBlob: blob}
	got := d.Resume(nil, ckpt)
	want := BarrierDriver{}.Resume(nil, ckpt)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Resume = %#v, want %#v", got, want)
	}
}

// TestDistTrainingEndToEnd is issue #73's third acceptance criterion: a
// dist-training job over the barrier driver runs a multi-step job through
// the cell loop with gradients all-reduced each step, using in-process fake
// followers (bufconn, the same pattern transport_test.go's
// TestServerAndTransportExecutor_BarrierEndToEnd uses) — this time asserting
// the AssignWork payload each follower receives for the next step is the
// combined (all-reduced) gradient, not a raw per-worker one.
func TestDistTrainingEndToEnd(t *testing.T) {
	followerA := &fakeFollower{}
	followerB := &fakeFollower{}
	clientA := dialBufconn(t, followerA)
	clientB := dialBufconn(t, followerB)

	dial := func(w string) (transport.CellLeaderClient, error) {
		switch w {
		case "a":
			return clientA, nil
		case "b":
			return clientB, nil
		}
		return nil, nil
	}
	members := []string{"a", "b"}

	exec := &TransportExecutor{JobID: "job-1", Dial: dial, Members: func() []string { return members }}
	driver := CombiningDriver{
		Inner:    BarrierDriver{},
		Registry: DefaultCombineRegistry(),
		Key:      TemplateKey{Driver: DriverNameBarrier, Template: "dist-training"},
	}
	loop := NewLoop(driver, barrier.State{K: 0, Members: []barrier.WorkerID{"a", "b"}}, exec, nil)

	// Three steps, each with a different pair of per-worker gradients —
	// after each step, both followers must be told to advance with the
	// SAME payload: DistTrainingCombine's sum of that step's two gradients.
	steps := [][2][]float64{
		{{1, 2}, {3, 4}},
		{{0.5, -1}, {2, 2}},
		{{10, 0}, {-5, 5}},
	}

	ctx := context.Background()
	for i, grads := range steps {
		gradA, gradB := encodeVec(grads[0]...), encodeVec(grads[1]...)
		wantCombined := templates.DistTrainingCombine([][]byte{gradA, gradB})

		if _, err := loop.Handle(ctx, Event{Kind: EventDone, Worker: "a", Partial: gradA}, model.Instant(i)); err != nil {
			t.Fatalf("step %d: Handle a: %v", i, err)
		}
		if _, err := loop.Handle(ctx, Event{Kind: EventDone, Worker: "b", Partial: gradB}, model.Instant(i)); err != nil {
			t.Fatalf("step %d: Handle b: %v", i, err)
		}

		assignsA := followerA.snapshotAssigns()
		assignsB := followerB.snapshotAssigns()
		if len(assignsA) != i+1 || len(assignsB) != i+1 {
			t.Fatalf("step %d: assigns = (%d, %d), want (%d, %d)", i, len(assignsA), len(assignsB), i+1, i+1)
		}
		last := assignsA[i]
		if last.GetStep() != int32(i+1) {
			t.Fatalf("step %d: AssignWork.Step = %d, want %d", i, last.GetStep(), i+1)
		}
		if !reflect.DeepEqual(last.GetPayload(), wantCombined) {
			t.Fatalf("step %d: follower a AssignWork payload = %#v, want combined %#v", i, last.GetPayload(), wantCombined)
		}
		if !reflect.DeepEqual(assignsB[i].GetPayload(), wantCombined) {
			t.Fatalf("step %d: follower b AssignWork payload = %#v, want combined %#v", i, assignsB[i].GetPayload(), wantCombined)
		}
	}

	if bs, ok := loop.State().(barrier.State); !ok || bs.Step != len(steps) {
		t.Fatalf("loop state = %#v, want Step=%d", loop.State(), len(steps))
	}
}

// snapshotAssigns is a small race-safe accessor fakeFollower itself does not
// expose (transport_test.go's tests read the unexported fields directly,
// single-threaded); this test drives loop.Handle from the same goroutine but
// AssignWork calls arrive over a real gRPC connection on the executor's own
// goroutine, so guard the read with fakeFollower's own mutex.
func (f *fakeFollower) snapshotAssigns() []*transport.AssignWorkRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*transport.AssignWorkRequest, len(f.assigns))
	copy(out, f.assigns)
	return out
}
