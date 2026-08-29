//go:build nccl

// This file is the real CUDA/NCCL device binding. It is compiled ONLY with the
// `nccl` build tag on GPU nodes and is intentionally excluded from
// `make gate-full` (the fake device in gpu.go is the gate default). The method
// bodies are the owner-provided seam where cgo CUDA allocation and NCCL
// collectives are wired; until linked against a real NCCL they return
// ErrNCCLNotLinked. Replacing these bodies with cgo calls is a hardware-node
// concern, reviewed by the owner, and never gates a normal PR.
package gpu

import "errors"

// ErrNCCLNotLinked is returned by the NCCL device until the real CUDA/NCCL
// binding is compiled in.
var ErrNCCLNotLinked = errors.New("gpu: NCCL/CUDA binding not linked in this build")

// ncclDevice is the real GPU-backed Device. Its concrete fields (CUDA context,
// NCCL communicator handles) are added alongside the cgo binding.
type ncclDevice struct{}

// NewNCCLDevice returns a Device backed by real CUDA/NCCL. Available only under
// the `nccl` build tag.
func NewNCCLDevice() Device { return &ncclDevice{} }

func (d *ncclDevice) Allocate(spec Spec) (Handle, error) {
	// TODO(owner): cudaSetDevice + cudaMalloc for spec.GPUs; init the NCCL comm.
	return Handle{}, ErrNCCLNotLinked
}

func (d *ncclDevice) AllReduce(h Handle, partials [][]byte) ([]byte, error) {
	// TODO(owner): ncclAllReduce(sum) over device buffers holding `partials`,
	// then copy the reduced buffer back to a host []byte. Must produce the same
	// sum as templates.DistTrainingCombine so the fake and real paths agree.
	return nil, ErrNCCLNotLinked
}

func (d *ncclDevice) Free(h Handle) error {
	// TODO(owner): cudaFree the buffers + ncclCommDestroy.
	return ErrNCCLNotLinked
}
