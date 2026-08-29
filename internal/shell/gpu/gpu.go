// Package gpu is the imperative shell that binds a physical GPU device and
// executes the barrier driver's AllReduce command. Per the P2 design ("only the
// shell touches the device"), the DECISION to all-reduce belongs to the barrier
// core (internal/core/barrier); this package only performs the I/O of allocating
// device memory and running the collective.
//
// Two Device implementations exist behind one interface:
//   - fakeDevice (this file): an in-process, deterministic, hardware-free device
//     that the CI gate and dev runs use. Its AllReduce delegates to
//     templates.DistTrainingCombine so the fake collective is byte-identical to
//     the pure combine — letting the barrier+GPU path be validated with no GPU.
//   - ncclDevice (nccl.go, behind the `nccl` build tag): the real CUDA/NCCL
//     binding for GPU nodes, owner-provided and NOT compiled by `make gate-full`.
//
// The cell leader's driver→template combine wiring calls Device.AllReduce for a
// GPU-bound barrier job; placement's capability predicate (Satisfies/PlaceCapable)
// already ensured the job landed on a GPU-capable cell.
package gpu

import (
	"errors"
	"sync"

	"github.com/msivraj/swarm/internal/core/templates"
)

// Spec requests device resources for a job.
type Spec struct {
	JobID string
	GPUs  int // number of GPUs this allocation needs
}

// Handle identifies a live allocation held on a Device.
type Handle struct {
	ID   uint64
	GPUs int
}

// Device allocates GPU resources and executes collectives. All methods are
// safe for concurrent use.
type Device interface {
	// Allocate reserves spec.GPUs on the device, returning a Handle.
	Allocate(spec Spec) (Handle, error)
	// AllReduce executes the barrier's AllReduce{partials}: it combines the
	// per-worker gradient blobs into one reduced blob.
	AllReduce(h Handle, partials [][]byte) ([]byte, error)
	// Free releases the allocation.
	Free(h Handle) error
}

// Sentinel errors.
var (
	ErrBadSpec    = errors.New("gpu: spec must request a positive GPU count")
	ErrNoCapacity = errors.New("gpu: not enough free device capacity")
	ErrBadHandle  = errors.New("gpu: unknown or already-freed handle")
)

// fakeDevice is a deterministic, in-process Device for tests and dev. It tracks
// GPU capacity exactly like a real device would, but its AllReduce is the pure
// gradient sum (templates.DistTrainingCombine) rather than a hardware collective.
type fakeDevice struct {
	mu       sync.Mutex
	capacity int
	used     int
	next     uint64
	live     map[uint64]int // handle id -> GPUs held
}

// NewFakeDevice returns an in-process Device with the given GPU capacity.
func NewFakeDevice(capacity int) Device {
	return &fakeDevice{capacity: capacity, live: make(map[uint64]int)}
}

func (d *fakeDevice) Allocate(spec Spec) (Handle, error) {
	if spec.GPUs <= 0 {
		return Handle{}, ErrBadSpec
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.used+spec.GPUs > d.capacity {
		return Handle{}, ErrNoCapacity
	}
	d.next++
	id := d.next
	d.used += spec.GPUs
	d.live[id] = spec.GPUs
	return Handle{ID: id, GPUs: spec.GPUs}, nil
}

func (d *fakeDevice) AllReduce(h Handle, partials [][]byte) ([]byte, error) {
	d.mu.Lock()
	_, ok := d.live[h.ID]
	d.mu.Unlock()
	if !ok {
		return nil, ErrBadHandle
	}
	// A real NCCL all-reduce sums the gradient vectors across workers;
	// templates.DistTrainingCombine sums them too. Delegating makes the fake
	// device's result byte-identical to the pure combine, so a test can validate
	// the barrier+GPU path without hardware.
	return templates.DistTrainingCombine(partials), nil
}

func (d *fakeDevice) Free(h Handle) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	gpus, ok := d.live[h.ID]
	if !ok {
		return ErrBadHandle
	}
	delete(d.live, h.ID)
	d.used -= gpus
	return nil
}
