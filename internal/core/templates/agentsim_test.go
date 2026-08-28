package templates

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestAgentSimDecompose(t *testing.T) {
	tests := []struct {
		name           string
		job            AgentSimJob
		wantPartitions int
		wantErr        bool
	}{
		{
			name:           "even split",
			job:            AgentSimJob{JobID: "j1", NumAgents: 12, Partitions: 4},
			wantPartitions: 4,
		},
		{
			name:           "uneven split spreads remainder over first partitions",
			job:            AgentSimJob{JobID: "j1", NumAgents: 10, Partitions: 3},
			wantPartitions: 3,
		},
		{
			name:           "single partition covers all agents",
			job:            AgentSimJob{JobID: "j1", NumAgents: 50, Partitions: 1},
			wantPartitions: 1,
		},
		{name: "zero agents is rejected", job: AgentSimJob{JobID: "j1", NumAgents: 0, Partitions: 3}, wantErr: true},
		{name: "zero partitions is rejected", job: AgentSimJob{JobID: "j1", NumAgents: 10, Partitions: 0}, wantErr: true},
		{name: "negative partitions is rejected", job: AgentSimJob{JobID: "j1", NumAgents: 10, Partitions: -2}, wantErr: true},
		{name: "more partitions than agents is rejected", job: AgentSimJob{JobID: "j1", NumAgents: 2, Partitions: 5}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AgentSimDecompose(tt.job)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("AgentSimDecompose() error = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("AgentSimDecompose() unexpected error: %v", err)
			}
			if len(got) != tt.wantPartitions {
				t.Fatalf("len(AgentSimDecompose()) = %d, want %d", len(got), tt.wantPartitions)
			}
			assertAgentSimTasks(t, tt.job, got)
		})
	}
}

func assertAgentSimTasks(t *testing.T, job AgentSimJob, tasks []model.Task) {
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
	if cur != job.NumAgents {
		t.Fatalf("partitions cover up to %d agents, want %d", cur, job.NumAgents)
	}
}

func TestAgentSimDecomposeIsDeterministic(t *testing.T) {
	job := AgentSimJob{JobID: "j1", NumAgents: 251, Partitions: 7}
	first, err := AgentSimDecompose(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 100; i++ {
		got, err := AgentSimDecompose(job)
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// AgentSimCombine
// -----------------------------------------------------------------------

func TestAgentSimCombine(t *testing.T) {
	state := func(vs ...float64) []byte { return encodeFloat64Vector(vs) }

	tests := []struct {
		name    string
		states  [][]byte
		want    []float64
		wantNil bool
	}{
		{name: "no states", states: nil, wantNil: true},
		{
			name:   "single partition's state passes through",
			states: [][]byte{state(3, 4)},
			want:   []float64{3, 4},
		},
		{
			name:   "four partitions' states sum elementwise",
			states: [][]byte{state(1, 0), state(0, 1), state(2, 2), state(-1, -1)},
			want:   []float64{2, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AgentSimCombine(tt.states)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("AgentSimCombine() = %v, want nil", got)
				}
				return
			}
			gotVec, ok := decodeFloat64Vector(got)
			if !ok {
				t.Fatalf("AgentSimCombine() output is not a valid vector")
			}
			if !reflect.DeepEqual(gotVec, tt.want) {
				t.Fatalf("AgentSimCombine() = %v, want %v", gotVec, tt.want)
			}
		})
	}
}

// TestAgentSimCombineCommutativeAndAssociative checks the law issue #63
// names for the all-reduce combines: order of arrival and grouping do not
// change AgentSimCombine's result.
func TestAgentSimCombineCommutativeAndAssociative(t *testing.T) {
	states := [][]byte{
		encodeFloat64Vector([]float64{1, 1}),
		encodeFloat64Vector([]float64{2, -2}),
		encodeFloat64Vector([]float64{-3, 3}),
		encodeFloat64Vector([]float64{4, 4}),
	}
	want := AgentSimCombine(states)

	reordered := [][]byte{states[3], states[1], states[0], states[2]}
	if got := AgentSimCombine(reordered); !reflect.DeepEqual(got, want) {
		t.Fatalf("reordered combine = %v, want %v (order-independent)", got, want)
	}

	left := AgentSimCombine(states[:2])
	right := AgentSimCombine(states[2:])
	if got := AgentSimCombine([][]byte{left, right}); !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped combine = %v, want %v (associative)", got, want)
	}
}

func TestAgentSimCombineIsDeterministic(t *testing.T) {
	states := [][]byte{
		encodeFloat64Vector([]float64{1, 2}),
		encodeFloat64Vector([]float64{3, 4}),
	}
	first := AgentSimCombine(states)
	for i := 0; i < 100; i++ {
		if got := AgentSimCombine(states); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d", i)
		}
	}
}
