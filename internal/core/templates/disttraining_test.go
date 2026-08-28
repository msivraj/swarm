package templates

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestDistTrainingDecompose(t *testing.T) {
	tests := []struct {
		name       string
		job        DistTrainingJob
		wantShards int
		wantErr    bool
	}{
		{
			name:       "even split",
			job:        DistTrainingJob{JobID: "j1", Samples: 9, Shards: 3},
			wantShards: 3,
		},
		{
			name:       "uneven split spreads remainder over first shards",
			job:        DistTrainingJob{JobID: "j1", Samples: 10, Shards: 3},
			wantShards: 3,
		},
		{
			name:       "single shard covers all samples",
			job:        DistTrainingJob{JobID: "j1", Samples: 20, Shards: 1},
			wantShards: 1,
		},
		{name: "zero samples is rejected", job: DistTrainingJob{JobID: "j1", Samples: 0, Shards: 3}, wantErr: true},
		{name: "zero shards is rejected", job: DistTrainingJob{JobID: "j1", Samples: 10, Shards: 0}, wantErr: true},
		{name: "negative shards is rejected", job: DistTrainingJob{JobID: "j1", Samples: 10, Shards: -1}, wantErr: true},
		{name: "more shards than samples is rejected", job: DistTrainingJob{JobID: "j1", Samples: 3, Shards: 10}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DistTrainingDecompose(tt.job)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("DistTrainingDecompose() error = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("DistTrainingDecompose() unexpected error: %v", err)
			}
			if len(got) != tt.wantShards {
				t.Fatalf("len(DistTrainingDecompose()) = %d, want %d", len(got), tt.wantShards)
			}
			assertDistTrainingTasks(t, tt.job, got)
		})
	}
}

// assertDistTrainingTasks checks every task has a valid, tiling sample-index
// range and a job-scoped, unique ID.
func assertDistTrainingTasks(t *testing.T, job DistTrainingJob, tasks []model.Task) {
	t.Helper()
	seen := map[model.TaskID]bool{}
	var cur uint64
	for i, task := range tasks {
		if task.JobID != job.JobID {
			t.Fatalf("task %d: JobID = %q, want %q", i, task.JobID, job.JobID)
		}
		if seen[task.ID] {
			t.Fatalf("task %d: ID %q reused by an earlier task", i, task.ID)
		}
		seen[task.ID] = true

		r, ok := decodeIDRange(task.Input)
		if !ok {
			t.Fatalf("task %d: Input is not a valid range (%d bytes)", i, len(task.Input))
		}
		if r.Start != cur {
			t.Fatalf("task %d: range starts at %d, want %d", i, r.Start, cur)
		}
		cur = r.End
	}
	if cur != job.Samples {
		t.Fatalf("shards cover up to %d samples, want %d", cur, job.Samples)
	}
}

func TestDistTrainingDecomposeIsDeterministic(t *testing.T) {
	job := DistTrainingJob{JobID: "j1", Samples: 137, Shards: 11}
	first, err := DistTrainingDecompose(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 100; i++ {
		got, err := DistTrainingDecompose(job)
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// DistTrainingCombine
// -----------------------------------------------------------------------

func TestDistTrainingCombine(t *testing.T) {
	grad := func(vs ...float64) []byte { return encodeFloat64Vector(vs) }

	tests := []struct {
		name      string
		gradients [][]byte
		want      []float64
		wantNil   bool
	}{
		{name: "no gradients", gradients: nil, wantNil: true},
		{
			name:      "single worker's gradient passes through",
			gradients: [][]byte{grad(0.5, -1.5, 2.0)},
			want:      []float64{0.5, -1.5, 2.0},
		},
		{
			name:      "three workers' gradients sum elementwise",
			gradients: [][]byte{grad(1, 0, 1), grad(0, 1, 1), grad(1, 1, 0)},
			want:      []float64{2, 2, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DistTrainingCombine(tt.gradients)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("DistTrainingCombine() = %v, want nil", got)
				}
				return
			}
			gotVec, ok := decodeFloat64Vector(got)
			if !ok {
				t.Fatalf("DistTrainingCombine() output is not a valid vector")
			}
			if !reflect.DeepEqual(gotVec, tt.want) {
				t.Fatalf("DistTrainingCombine() = %v, want %v", gotVec, tt.want)
			}
		})
	}
}

// TestDistTrainingCombineCommutativeAndAssociative checks the law issue #63
// names for the all-reduce combines: DistTrainingCombine does not depend on
// the order gradients arrive in, nor on how they are grouped before being
// combined — sumFloat64Vectors already proves the general law, this pins it
// through the exported DistTrainingCombine entry point specifically.
func TestDistTrainingCombineCommutativeAndAssociative(t *testing.T) {
	gradients := [][]byte{
		encodeFloat64Vector([]float64{1, 2}),
		encodeFloat64Vector([]float64{3, -4}),
		encodeFloat64Vector([]float64{-5, 6}),
	}
	want := DistTrainingCombine(gradients)

	reordered := [][]byte{gradients[2], gradients[0], gradients[1]}
	if got := DistTrainingCombine(reordered); !reflect.DeepEqual(got, want) {
		t.Fatalf("reordered combine = %v, want %v (order-independent)", got, want)
	}

	left := DistTrainingCombine(gradients[:1])
	right := DistTrainingCombine(gradients[1:])
	if got := DistTrainingCombine([][]byte{left, right}); !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped combine = %v, want %v (associative)", got, want)
	}
}

func TestDistTrainingCombineIsDeterministic(t *testing.T) {
	gradients := [][]byte{
		encodeFloat64Vector([]float64{1, 2}),
		encodeFloat64Vector([]float64{3, 4}),
	}
	first := DistTrainingCombine(gradients)
	for i := 0; i < 100; i++ {
		if got := DistTrainingCombine(gradients); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d", i)
		}
	}
}
