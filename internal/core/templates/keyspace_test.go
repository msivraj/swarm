package templates

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestKeyspaceDecompose(t *testing.T) {
	tests := []struct {
		name       string
		job        KeyspaceJob
		wantShards int // expected number of tasks, after any clamping
	}{
		{
			name:       "even split",
			job:        KeyspaceJob{JobID: "j1", Start: 0, End: 9, Shards: 3},
			wantShards: 3,
		},
		{
			name:       "uneven split spreads remainder over first shards",
			job:        KeyspaceJob{JobID: "j1", Start: 0, End: 10, Shards: 3},
			wantShards: 3,
		},
		{
			name:       "single shard covers whole range",
			job:        KeyspaceJob{JobID: "j1", Start: 5, End: 20, Shards: 1},
			wantShards: 1,
		},
		{
			name:       "shards <= 0 treated as 1",
			job:        KeyspaceJob{JobID: "j1", Start: 5, End: 20, Shards: 0},
			wantShards: 1,
		},
		{
			name:       "negative shards treated as 1",
			job:        KeyspaceJob{JobID: "j1", Start: 5, End: 20, Shards: -4},
			wantShards: 1,
		},
		{
			name:       "shards exceeding range size clamps to range size",
			job:        KeyspaceJob{JobID: "j1", Start: 0, End: 3, Shards: 100},
			wantShards: 3,
		},
		{
			name:       "empty range yields no tasks",
			job:        KeyspaceJob{JobID: "j1", Start: 10, End: 10, Shards: 4},
			wantShards: 0,
		},
		{
			name:       "inverted range yields no tasks",
			job:        KeyspaceJob{JobID: "j1", Start: 10, End: 5, Shards: 4},
			wantShards: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := KeyspaceDecompose(tt.job)
			if len(got) != tt.wantShards {
				t.Fatalf("len(KeyspaceDecompose()) = %d, want %d", len(got), tt.wantShards)
			}
			assertKeyspaceTiling(t, tt.job, got)
		})
	}
}

// assertKeyspaceTiling checks the property every keyspace decomposition must
// satisfy: sub-ranges are contiguous, non-overlapping, and together cover
// exactly [job.Start, job.End) with no gaps.
func assertKeyspaceTiling(t *testing.T, job KeyspaceJob, tasks []model.Task) {
	t.Helper()
	if len(tasks) == 0 {
		return
	}
	cur := job.Start
	for i, task := range tasks {
		r, ok := decodeKeyspaceInput(task.Input)
		if !ok {
			t.Fatalf("task %d: Input is not a valid keyspace range (%d bytes)", i, len(task.Input))
		}
		if r.Start != cur {
			t.Fatalf("task %d: range starts at %d, want %d (gap or overlap after previous task)", i, r.Start, cur)
		}
		if r.End <= r.Start {
			t.Fatalf("task %d: empty or inverted sub-range [%d, %d)", i, r.Start, r.End)
		}
		cur = r.End
	}
	if cur != job.End {
		t.Fatalf("tasks cover up to %d, want %d (gap at the end of the range)", cur, job.End)
	}
}

func TestDecodeKeyspaceInputRejectsWrongLength(t *testing.T) {
	if _, ok := decodeKeyspaceInput([]byte("too-short")); ok {
		t.Fatal("decodeKeyspaceInput accepted a malformed input")
	}
}

func TestKeyspaceMerge(t *testing.T) {
	tests := []struct {
		name string
		rs   []model.TaskResult
		want model.Aggregate
	}{
		{
			name: "no results",
			rs:   nil,
			want: model.Aggregate{Done: false},
		},
		{
			name: "no hit among results",
			rs: []model.TaskResult{
				{TaskID: "a", OK: false},
				{TaskID: "b", OK: false},
			},
			want: model.Aggregate{Done: false},
		},
		{
			name: "single hit",
			rs: []model.TaskResult{
				{TaskID: "a", OK: false},
				{TaskID: "b", OK: true, Output: []byte("found-b")},
				{TaskID: "c", OK: false},
			},
			want: model.Aggregate{Value: []byte("found-b"), Done: true},
		},
		{
			name: "first hit wins over a later hit",
			rs: []model.TaskResult{
				{TaskID: "a", OK: true, Output: []byte("found-a")},
				{TaskID: "b", OK: true, Output: []byte("found-b")},
			},
			want: model.Aggregate{Value: []byte("found-a"), Done: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := KeyspaceMerge(tt.rs)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("KeyspaceMerge() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestKeyspaceDecomposeIsDeterministic guards the core's defining property:
// identical inputs always produce identical output — same IDs, order, and
// Input bytes.
func TestKeyspaceDecomposeIsDeterministic(t *testing.T) {
	job := KeyspaceJob{JobID: "j1", Start: 7, End: 1009, Shards: 11}
	first := KeyspaceDecompose(job)
	for i := 0; i < 100; i++ {
		if got := KeyspaceDecompose(job); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
