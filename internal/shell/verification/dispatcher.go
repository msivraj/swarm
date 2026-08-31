package verification

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/msivraj/swarm/internal/model"
)

// Dispatcher sends task to a single machine and returns that machine's
// claimed model.Result, or an error if the machine could not be reached or
// did not answer before ctx is done. This is the coordinator's one real I/O
// seam: Coordinator.Verify dispatches to K machines over a Dispatcher
// concurrently and collects into a set of model.Result the pure
// verification core can tally.
//
// The production implementation dials the target machine's agent (gRPC) and
// asks it to run task inside its internal/shell/sandbox.Runner — that
// wiring is later (#143's end-to-end capstone); this ticket only needs the
// seam plus FakeDispatcher below.
//
// Security note: a Dispatch implementation MUST NOT be trusted to say which
// identity it talked to — Coordinator.Verify never reads Result.ID off the
// returned value. It attributes every result to the identity of the
// MachineID it dialed (see identityOf), the same way a real transport
// attributes a peer identity from the mTLS connection it dialed, not from
// anything the untrusted peer claims in its payload.
type Dispatcher interface {
	Dispatch(ctx context.Context, machine model.MachineID, task model.Task) (model.Result, error)
}

// ErrTimeout is returned by a FakeDispatcher's Hang behavior once ctx is
// done — it simulates an unresponsive machine.
var ErrTimeout = errors.New("verification: machine did not respond before the round's context was done")

// FakeBehavior configures how a FakeDispatcher answers Dispatch for one
// machine.
type FakeBehavior struct {
	// Value is the claimed result payload. Set this to the true value for
	// an honest machine, or to anything else to simulate a lie.
	Value []byte
	// OK is the claimed task-level success flag.
	OK bool
	// Err, if non-nil, makes Dispatch return this error immediately instead
	// of a Result.
	Err error
	// Hang, if true, makes Dispatch block until ctx is done (simulating an
	// unresponsive/slow machine) and then return ErrTimeout — no real sleep
	// is involved; it is unblocked purely by ctx cancellation, which the
	// coordinator drives off its Clock (see collect in coordinator.go).
	Hang bool
}

// FakeDispatcher is an in-memory Dispatcher for tests: each machine's
// behavior is configured independently via FakeBehavior, so a test can mix
// honest, lying, erroring, and hanging machines in one pool. Safe for
// concurrent use.
type FakeDispatcher struct {
	mu        sync.Mutex
	behaviors map[model.MachineID]FakeBehavior
	// calls records every machine Dispatch was invoked for, in call order,
	// so a test can assert exactly which machines were dispatched to
	// (e.g. that a blacklisted machine never appears).
	calls []model.MachineID
}

// NewFakeDispatcher returns a FakeDispatcher with no configured machines —
// Dispatch on an unconfigured machine returns an error.
func NewFakeDispatcher() *FakeDispatcher {
	return &FakeDispatcher{behaviors: make(map[model.MachineID]FakeBehavior)}
}

// Set configures machine's behavior for every future Dispatch call.
func (f *FakeDispatcher) Set(machine model.MachineID, b FakeBehavior) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.behaviors[machine] = b
}

// Honest is a convenience for Set(machine, FakeBehavior{Value: value, OK: true}) —
// a machine that always reports the true value.
func (f *FakeDispatcher) Honest(machine model.MachineID, value []byte) {
	f.Set(machine, FakeBehavior{Value: value, OK: true})
}

// Lying configures machine to always report value — the caller is expected
// to pass a value that disagrees with the honest answer, simulating a
// dishonest machine.
func (f *FakeDispatcher) Lying(machine model.MachineID, value []byte) {
	f.Set(machine, FakeBehavior{Value: value, OK: true})
}

// Hanging is a convenience for Set(machine, FakeBehavior{Hang: true}) — a
// machine that never answers until the round's context is done.
func (f *FakeDispatcher) Hanging(machine model.MachineID) {
	f.Set(machine, FakeBehavior{Hang: true})
}

// Calls returns every machine Dispatch has been invoked for so far, in call
// order. Intended for test assertions (e.g. "no blacklisted machine was
// ever dispatched to").
func (f *FakeDispatcher) Calls() []model.MachineID {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.MachineID, len(f.calls))
	copy(out, f.calls)
	return out
}

// Dispatch implements Dispatcher.
func (f *FakeDispatcher) Dispatch(ctx context.Context, machine model.MachineID, task model.Task) (model.Result, error) {
	f.mu.Lock()
	b, ok := f.behaviors[machine]
	f.calls = append(f.calls, machine)
	f.mu.Unlock()

	if !ok {
		return model.Result{}, fmt.Errorf("verification: fake dispatcher has no behavior configured for machine %q", machine)
	}
	if b.Hang {
		<-ctx.Done()
		return model.Result{}, ErrTimeout
	}
	if b.Err != nil {
		return model.Result{}, b.Err
	}
	return model.Result{Value: b.Value, OK: b.OK}, nil
}
