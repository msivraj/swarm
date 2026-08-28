package templates

import (
	"reflect"
	"testing"
)

func TestSumFloat64Vectors(t *testing.T) {
	vec := func(vs ...float64) []byte { return encodeFloat64Vector(vs) }

	tests := []struct {
		name    string
		vectors [][]byte
		want    []float64
		wantNil bool
	}{
		{
			name:    "nil input",
			vectors: nil,
			wantNil: true,
		},
		{
			name:    "empty input",
			vectors: [][]byte{},
			wantNil: true,
		},
		{
			name:    "single vector",
			vectors: [][]byte{vec(1, 2, 3)},
			want:    []float64{1, 2, 3},
		},
		{
			name:    "two vectors sum elementwise",
			vectors: [][]byte{vec(1, 2, 3), vec(10, 20, 30)},
			want:    []float64{11, 22, 33},
		},
		{
			name:    "three vectors sum elementwise",
			vectors: [][]byte{vec(1, 1, 1), vec(2, 2, 2), vec(3, 3, 3)},
			want:    []float64{6, 6, 6},
		},
		{
			name:    "negative and zero components",
			vectors: [][]byte{vec(5, -5, 0), vec(-5, 5, 0)},
			want:    []float64{0, 0, 0},
		},
		{
			name:    "malformed entry (wrong length) is skipped",
			vectors: [][]byte{vec(1, 2), []byte("bad"), vec(10, 20)},
			want:    []float64{11, 22},
		},
		{
			name:    "mismatched dimension entry is skipped",
			vectors: [][]byte{vec(1, 2), vec(10, 20, 30)},
			want:    []float64{1, 2},
		},
		{
			name:    "all entries malformed yields nil",
			vectors: [][]byte{[]byte("bad"), {}},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sumFloat64Vectors(tt.vectors)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("sumFloat64Vectors() = %v, want nil", got)
				}
				return
			}
			gotVec, ok := decodeFloat64Vector(got)
			if !ok {
				t.Fatalf("sumFloat64Vectors() output is not a valid vector (%d bytes)", len(got))
			}
			if !reflect.DeepEqual(gotVec, tt.want) {
				t.Fatalf("sumFloat64Vectors() = %v, want %v", gotVec, tt.want)
			}
		})
	}
}

func TestDecodeFloat64VectorRejectsMalformed(t *testing.T) {
	if _, ok := decodeFloat64Vector(nil); ok {
		t.Fatal("decodeFloat64Vector accepted an empty input")
	}
	if _, ok := decodeFloat64Vector([]byte("bad")); ok {
		t.Fatal("decodeFloat64Vector accepted a non-multiple-of-8 input")
	}
}

// TestSumFloat64VectorsIsDeterministic guards the core's defining property:
// identical inputs always produce identical output.
func TestSumFloat64VectorsIsDeterministic(t *testing.T) {
	vectors := [][]byte{
		encodeFloat64Vector([]float64{1, 2, 3}),
		encodeFloat64Vector([]float64{4, 5, 6}),
		encodeFloat64Vector([]float64{7, 8, 9}),
	}
	first := sumFloat64Vectors(vectors)
	for i := 0; i < 100; i++ {
		if got := sumFloat64Vectors(vectors); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d", i)
		}
	}
}

// TestSumFloat64VectorsCommutative checks the law dist-training's and
// agent-sim's combine both rely on: any permutation of the same set of
// worker vectors sums to the same combined vector — the driver can reduce
// in whatever order results arrive in.
func TestSumFloat64VectorsCommutative(t *testing.T) {
	vectors := [][]byte{
		encodeFloat64Vector([]float64{1, -2, 3}),
		encodeFloat64Vector([]float64{4, 5, -6}),
		encodeFloat64Vector([]float64{-7, 8, 9}),
		encodeFloat64Vector([]float64{2, 2, 2}),
	}
	want := sumFloat64Vectors(vectors)

	for _, order := range permutationsOfIndices(len(vectors)) {
		permuted := make([][]byte, len(vectors))
		for i, idx := range order {
			permuted[i] = vectors[idx]
		}
		if got := sumFloat64Vectors(permuted); !reflect.DeepEqual(got, want) {
			t.Fatalf("order %v: sumFloat64Vectors() = %v, want %v", order, got, want)
		}
	}
}

// TestSumFloat64VectorsAssociative checks the other half of the law: summing
// two sub-group combines gives the same result as combining every vector at
// once, regardless of how the group is split — what lets a driver combine
// results incrementally as they arrive instead of waiting to collect them
// all first.
func TestSumFloat64VectorsAssociative(t *testing.T) {
	vectors := [][]byte{
		encodeFloat64Vector([]float64{1, 2}),
		encodeFloat64Vector([]float64{3, 4}),
		encodeFloat64Vector([]float64{5, 6}),
		encodeFloat64Vector([]float64{7, 8}),
		encodeFloat64Vector([]float64{9, 10}),
	}
	want := sumFloat64Vectors(vectors)

	for split := 0; split <= len(vectors); split++ {
		left := sumFloat64Vectors(vectors[:split])
		right := sumFloat64Vectors(vectors[split:])
		combined := sumFloat64Vectors([][]byte{left, right})
		if !reflect.DeepEqual(combined, want) {
			t.Fatalf("split at %d: combining sub-sums = %v, want %v", split, combined, want)
		}
	}
}

// permutationsOfIndices returns every permutation of [0, n) via Heap's
// algorithm, deterministically (no math/rand) — mirrors
// internal/core/barrier's permutations helper.
func permutationsOfIndices(n int) [][]int {
	var out [][]int
	buf := make([]int, n)
	for i := range buf {
		buf[i] = i
	}
	c := make([]int, n)

	snapshot := func() []int {
		cp := make([]int, n)
		copy(cp, buf)
		return cp
	}

	out = append(out, snapshot())
	for i := 0; i < n; {
		if c[i] < i {
			if i%2 == 0 {
				buf[0], buf[i] = buf[i], buf[0]
			} else {
				buf[c[i]], buf[i] = buf[i], buf[c[i]]
			}
			out = append(out, snapshot())
			c[i]++
			i = 0
		} else {
			c[i] = 0
			i++
		}
	}
	return out
}
