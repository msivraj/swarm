package templates

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestMonteCarloDecompose(t *testing.T) {
	tests := []struct {
		name       string
		job        MCJob
		wantBlocks []int64 // expected trial count per task, in order
	}{
		{
			name:       "evenly divides",
			job:        MCJob{JobID: "j1", Trials: 9, BlockSize: 3, BaseSeed: 100},
			wantBlocks: []int64{3, 3, 3},
		},
		{
			name:       "remainder becomes a final short block",
			job:        MCJob{JobID: "j1", Trials: 10, BlockSize: 3, BaseSeed: 100},
			wantBlocks: []int64{3, 3, 3, 1},
		},
		{
			name:       "block size exceeds trials yields one short block",
			job:        MCJob{JobID: "j1", Trials: 2, BlockSize: 50, BaseSeed: 100},
			wantBlocks: []int64{2},
		},
		{
			name:       "trials <= 0 yields no tasks",
			job:        MCJob{JobID: "j1", Trials: 0, BlockSize: 3, BaseSeed: 100},
			wantBlocks: nil,
		},
		{
			name:       "block size <= 0 yields no tasks",
			job:        MCJob{JobID: "j1", Trials: 10, BlockSize: 0, BaseSeed: 100},
			wantBlocks: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MonteCarloDecompose(tt.job)
			if len(got) != len(tt.wantBlocks) {
				t.Fatalf("len(MonteCarloDecompose()) = %d, want %d", len(got), len(tt.wantBlocks))
			}

			seen := map[int64]bool{}
			var totalTrials int64
			for i, task := range got {
				in, ok := decodeMCTaskInput(task.Input)
				if !ok {
					t.Fatalf("task %d: Input is not a valid mc task input (%d bytes)", i, len(task.Input))
				}
				if in.Trials != tt.wantBlocks[i] {
					t.Fatalf("task %d: Trials = %d, want %d", i, in.Trials, tt.wantBlocks[i])
				}
				if seen[in.Seed] {
					t.Fatalf("task %d: seed %d reused by an earlier task", i, in.Seed)
				}
				seen[in.Seed] = true
				totalTrials += in.Trials
			}
			var wantTotal int64
			for _, n := range tt.wantBlocks {
				wantTotal += n
			}
			if totalTrials != wantTotal {
				t.Fatalf("blocks total %d trials, want %d", totalTrials, wantTotal)
			}
		})
	}
}

func TestDecodeMCHelpersRejectWrongLength(t *testing.T) {
	if _, ok := decodeMCTaskInput([]byte("bad")); ok {
		t.Fatal("decodeMCTaskInput accepted a malformed input")
	}
	if _, ok := decodeMCAggregate([]byte("bad")); ok {
		t.Fatal("decodeMCAggregate accepted a malformed input")
	}
}

func TestMonteCarloMerge(t *testing.T) {
	block := func(count int64, sum, sumSq float64) []byte {
		return encodeMCResult(mcResult{Count: count, Sum: sum, SumSq: sumSq})
	}

	tests := []struct {
		name string
		rs   []model.TaskResult
		want mcAggregate
	}{
		{
			name: "empty input",
			rs:   nil,
			want: mcAggregate{},
		},
		{
			name: "single block",
			rs: []model.TaskResult{
				{TaskID: "a", OK: true, Output: block(4, 8, 20)},
			},
			want: mcAggregate{Count: 4, Sum: 8, Mean: 2, Variance: 1}, // 20/4 - 2^2 = 5-4=1
		},
		{
			// Two identical blocks of {count:2, sum:4, sumSq:8}: combined
			// count=4 sum=8 mean=2 sumSq=16 -> variance = 16/4 - 2^2 = 0.
			name: "sums across multiple blocks",
			rs: []model.TaskResult{
				{TaskID: "a", OK: true, Output: block(2, 4, 8)},
				{TaskID: "b", OK: true, Output: block(2, 4, 8)},
			},
			want: mcAggregate{Count: 4, Sum: 8, Mean: 2, Variance: 0},
		},
		{
			name: "failed block is skipped",
			rs: []model.TaskResult{
				{TaskID: "a", OK: true, Output: block(4, 8, 20)},
				{TaskID: "b", OK: false, Output: block(100, 1000, 1000)},
			},
			want: mcAggregate{Count: 4, Sum: 8, Mean: 2, Variance: 1},
		},
		{
			name: "malformed output is skipped",
			rs: []model.TaskResult{
				{TaskID: "a", OK: true, Output: block(4, 8, 20)},
				{TaskID: "b", OK: true, Output: []byte("not-valid")},
			},
			want: mcAggregate{Count: 4, Sum: 8, Mean: 2, Variance: 1},
		},
		{
			name: "all blocks fail behaves like empty input",
			rs: []model.TaskResult{
				{TaskID: "a", OK: false, Output: block(4, 8, 20)},
			},
			want: mcAggregate{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MonteCarloMerge(tt.rs)
			if !got.Done {
				t.Fatalf("MonteCarloMerge().Done = false, want true")
			}
			gotAgg, ok := decodeMCAggregate(got.Value)
			if !ok {
				t.Fatalf("MonteCarloMerge().Value is not a valid mcAggregate (%d bytes)", len(got.Value))
			}
			if gotAgg != tt.want {
				t.Fatalf("MonteCarloMerge() aggregate = %+v, want %+v", gotAgg, tt.want)
			}
		})
	}
}

// TestMonteCarloMergeVarianceExample fixes the "sums across multiple blocks"
// case above with hand-computed numbers, so the test table's expectation
// isn't just re-deriving the implementation.
func TestMonteCarloMergeVarianceExample(t *testing.T) {
	// Two blocks of {1,3} and {1,3}: combined values {1,3,1,3}.
	// count=4 sum=8 mean=2 sumSq=1+9+1+9=20 -> variance = 20/4 - 4 = 1.
	rs := []model.TaskResult{
		{TaskID: "a", OK: true, Output: encodeMCResult(mcResult{Count: 2, Sum: 4, SumSq: 10})},
		{TaskID: "b", OK: true, Output: encodeMCResult(mcResult{Count: 2, Sum: 4, SumSq: 10})},
	}
	got := MonteCarloMerge(rs)
	agg, ok := decodeMCAggregate(got.Value)
	if !ok {
		t.Fatalf("Value is not a valid mcAggregate")
	}
	want := mcAggregate{Count: 4, Sum: 8, Mean: 2, Variance: 1}
	if agg != want {
		t.Fatalf("got %+v, want %+v", agg, want)
	}
}

// TestMonteCarloMergePartitionProperty checks the law the issue names for
// monte-carlo: merging results reconstructs the whole regardless of how the
// same underlying trials were partitioned into blocks. All values here are
// small integers, exactly representable as float64, so summing them in any
// grouping is exact — no floating-point rounding to account for.
func TestMonteCarloMergePartitionProperty(t *testing.T) {
	// 8 trial values, each contributing (count=1, sum=v, sumSq=v*v).
	values := []float64{2, 4, 6, 8, 10, 12, 14, 16}

	asBlocks := func(groups [][]float64) []model.TaskResult {
		rs := make([]model.TaskResult, 0, len(groups))
		for _, g := range groups {
			var r mcResult
			for _, v := range g {
				r.Count++
				r.Sum += v
				r.SumSq += v * v
			}
			rs = append(rs, model.TaskResult{OK: true, Output: encodeMCResult(r)})
		}
		return rs
	}

	partitions := [][][]float64{
		{values},                 // one block holding everything
		{values[:4], values[4:]}, // two even blocks
		{values[:1], values[1:3], values[3:6], values[6:]},                                                       // four uneven blocks
		{{values[0]}, {values[1]}, {values[2]}, {values[3]}, {values[4]}, {values[5]}, {values[6]}, {values[7]}}, // one trial per block
	}

	var want *mcAggregate
	for i, p := range partitions {
		got := MonteCarloMerge(asBlocks(p))
		agg, ok := decodeMCAggregate(got.Value)
		if !ok {
			t.Fatalf("partition %d: Value is not a valid mcAggregate", i)
		}
		if want == nil {
			want = &agg
			continue
		}
		if agg != *want {
			t.Fatalf("partition %d: merge = %+v, want %+v (same as partition 0)", i, agg, *want)
		}
	}
}

// TestMonteCarloDecomposeIsDeterministic guards the core's defining
// property: identical inputs always produce identical output — same IDs,
// order, and Input bytes.
func TestMonteCarloDecomposeIsDeterministic(t *testing.T) {
	job := MCJob{JobID: "j1", Trials: 137, BlockSize: 10, BaseSeed: 42}
	first := MonteCarloDecompose(job)
	for i := 0; i < 100; i++ {
		if got := MonteCarloDecompose(job); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
