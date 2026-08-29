package gpu

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"github.com/msivraj/swarm/internal/core/templates"
)

// encodeVec encodes a float64 vector as consecutive big-endian float64s — the
// same wire shape templates.DistTrainingCombine (sumFloat64Vectors) expects.
func encodeVec(vs []float64) []byte {
	b := make([]byte, len(vs)*8)
	for i, v := range vs {
		binary.BigEndian.PutUint64(b[i*8:i*8+8], math.Float64bits(v))
	}
	return b
}

func decodeVec(b []byte) []float64 {
	out := make([]float64, len(b)/8)
	for i := range out {
		out[i] = math.Float64frombits(binary.BigEndian.Uint64(b[i*8 : i*8+8]))
	}
	return out
}

func TestAllocateFreeBookkeeping(t *testing.T) {
	d := NewFakeDevice(4)

	if _, err := d.Allocate(Spec{JobID: "j", GPUs: 0}); err != ErrBadSpec {
		t.Fatalf("zero GPUs: got %v, want ErrBadSpec", err)
	}
	h1, err := d.Allocate(Spec{JobID: "j", GPUs: 3})
	if err != nil {
		t.Fatalf("Allocate 3: %v", err)
	}
	// 3 of 4 used; a 2-GPU request must not fit.
	if _, err := d.Allocate(Spec{JobID: "j", GPUs: 2}); err != ErrNoCapacity {
		t.Fatalf("over-allocate: got %v, want ErrNoCapacity", err)
	}
	// 1 GPU still fits.
	h2, err := d.Allocate(Spec{JobID: "j", GPUs: 1})
	if err != nil {
		t.Fatalf("Allocate 1: %v", err)
	}
	// Freeing h1 returns its 3 GPUs; now a 3-GPU request fits again.
	if err := d.Free(h1); err != nil {
		t.Fatalf("Free h1: %v", err)
	}
	if _, err := d.Allocate(Spec{JobID: "j", GPUs: 3}); err != nil {
		t.Fatalf("Allocate 3 after free: %v", err)
	}
	// Double-free / unknown handle is an error, not a silent capacity leak.
	if err := d.Free(h1); err != ErrBadHandle {
		t.Fatalf("double free: got %v, want ErrBadHandle", err)
	}
	_ = h2
}

func TestAllReduceMatchesDistTrainingCombine(t *testing.T) {
	d := NewFakeDevice(8)
	h, err := d.Allocate(Spec{JobID: "train", GPUs: 8})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	// Three workers' gradient vectors (exactly-representable integers so the
	// sum is order-independent with no float rounding).
	partials := [][]byte{
		encodeVec([]float64{1, 2, 3}),
		encodeVec([]float64{4, 5, 6}),
		encodeVec([]float64{7, 8, 9}),
	}

	got, err := d.AllReduce(h, partials)
	if err != nil {
		t.Fatalf("AllReduce: %v", err)
	}

	// The fake collective must be byte-identical to the pure combine — this is
	// what lets the barrier+GPU path be validated without hardware.
	want := templates.DistTrainingCombine(partials)
	if !bytes.Equal(got, want) {
		t.Fatalf("AllReduce != DistTrainingCombine:\n got=%v\nwant=%v", got, want)
	}

	// And it is a real elementwise all-reduce (sum): [1,2,3]+[4,5,6]+[7,8,9].
	if sum := decodeVec(got); len(sum) != 3 || sum[0] != 12 || sum[1] != 15 || sum[2] != 18 {
		t.Fatalf("reduced sum = %v, want [12 15 18]", sum)
	}
}

func TestAllReduceRejectsBadHandle(t *testing.T) {
	d := NewFakeDevice(2)
	h, err := d.Allocate(Spec{JobID: "j", GPUs: 1})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := d.Free(h); err != nil {
		t.Fatalf("Free: %v", err)
	}
	// A freed handle can no longer all-reduce.
	if _, err := d.AllReduce(h, [][]byte{encodeVec([]float64{1})}); err != ErrBadHandle {
		t.Fatalf("AllReduce on freed handle: got %v, want ErrBadHandle", err)
	}
	// An unknown handle likewise.
	if _, err := d.AllReduce(Handle{ID: 999, GPUs: 1}, nil); err != ErrBadHandle {
		t.Fatalf("AllReduce on unknown handle: got %v, want ErrBadHandle", err)
	}
}

// TestConcurrentAllocateFree stresses the capacity bookkeeping under -race: many
// goroutines allocate one GPU, all-reduce, then free, and the device must never
// over-commit its capacity or corrupt its accounting.
func TestConcurrentAllocateFree(t *testing.T) {
	const capacity = 8
	d := NewFakeDevice(capacity)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tries := 0; tries < 20; tries++ {
				h, err := d.Allocate(Spec{JobID: "j", GPUs: 1})
				if err == ErrNoCapacity {
					continue // device full right now; retry
				}
				if err != nil {
					t.Errorf("Allocate: %v", err)
					return
				}
				if _, err := d.AllReduce(h, [][]byte{encodeVec([]float64{float64(tries)})}); err != nil {
					t.Errorf("AllReduce: %v", err)
				}
				if err := d.Free(h); err != nil {
					t.Errorf("Free: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	// After every goroutine has freed, capacity must be fully restored.
	fd := d.(*fakeDevice)
	fd.mu.Lock()
	used, live := fd.used, len(fd.live)
	fd.mu.Unlock()
	if used != 0 || live != 0 {
		t.Fatalf("leaked accounting after all frees: used=%d live=%d, want 0 0", used, live)
	}
}
