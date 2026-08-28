package global

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func tasksOf(ids ...string) []model.Task {
	out := make([]model.Task, len(ids))
	for i, id := range ids {
		out[i] = model.Task{ID: model.TaskID(id)}
	}
	return out
}

func countsOf(parts map[model.RegionID][]model.Task) map[model.RegionID]int {
	out := make(map[model.RegionID]int, len(parts))
	for r, ts := range parts {
		out[r] = len(ts)
	}
	return out
}

func unionOf(parts map[model.RegionID][]model.Task) map[model.TaskID]int {
	out := make(map[model.TaskID]int)
	for _, ts := range parts {
		for _, t := range ts {
			out[t.ID]++
		}
	}
	return out
}

func TestPartitionTasksProportional(t *testing.T) {
	tests := []struct {
		name    string
		tasks   []model.Task
		weights []regionWeight
		want    map[model.RegionID]int
	}{
		{
			name:  "evenly divisible: exact proportional split",
			tasks: tasksOf("t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"),
			weights: []regionWeight{
				{Region: "a", Weight: 1},
				{Region: "b", Weight: 2},
				{Region: "c", Weight: 1},
			},
			want: map[model.RegionID]int{"a": 2, "b": 4, "c": 2},
		},
		{
			name:  "largest remainder: extra task to the biggest remainder",
			tasks: tasksOf("t1", "t2", "t3", "t4", "t5"),
			weights: []regionWeight{
				{Region: "r1", Weight: 1},
				{Region: "r2", Weight: 2},
				{Region: "r3", Weight: 1},
			},
			// quotas: 1.25 / 2.5 / 1.25 -> floors 1/2/1 (sum 4), remainder 1
			// goes to r2 (largest fractional remainder): 1/3/1.
			want: map[model.RegionID]int{"r1": 1, "r2": 3, "r3": 1},
		},
		{
			name:  "tie on remainder breaks round-robin by ascending RegionID",
			tasks: tasksOf("t1", "t2", "t3", "t4", "t5"),
			weights: []regionWeight{
				{Region: "z", Weight: 1},
				{Region: "a", Weight: 1},
				{Region: "m", Weight: 1},
			},
			// quotas: 5/3 each -> floors 1/1/1 (sum 3), 2 extra distributed by
			// ascending RegionID: a, m (not z).
			want: map[model.RegionID]int{"a": 2, "m": 2, "z": 1},
		},
		{
			name:    "single region gets everything",
			tasks:   tasksOf("t1", "t2", "t3"),
			weights: []regionWeight{{Region: "only", Weight: 7}},
			want:    map[model.RegionID]int{"only": 3},
		},
		{
			name:  "zero-weight region gets nothing when another has weight",
			tasks: tasksOf("t1", "t2", "t3", "t4"),
			weights: []regionWeight{
				{Region: "empty", Weight: 0},
				{Region: "full", Weight: 5},
			},
			want: map[model.RegionID]int{"full": 4},
		},
		{
			name:  "all zero weight falls back to round robin",
			tasks: tasksOf("t1", "t2", "t3", "t4"),
			weights: []regionWeight{
				{Region: "b", Weight: 0},
				{Region: "a", Weight: 0},
			},
			want: map[model.RegionID]int{"a": 2, "b": 2},
		},
		{
			name:    "no tasks",
			tasks:   nil,
			weights: []regionWeight{{Region: "a", Weight: 1}},
			want:    map[model.RegionID]int{},
		},
		{
			name:    "no regions",
			tasks:   tasksOf("t1"),
			weights: nil,
			want:    map[model.RegionID]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := partitionTasks(tt.tasks, tt.weights)
			gotCounts := countsOf(got)
			// zero-count regions never appear in the map (see want's omission
			// of "empty"/zero-weight-losing entries above); normalize both
			// sides by dropping zero entries before comparing.
			for r, c := range tt.want {
				if c == 0 {
					delete(tt.want, r)
				}
			}
			if !reflect.DeepEqual(gotCounts, tt.want) {
				t.Fatalf("partitionTasks() counts = %+v, want %+v", gotCounts, tt.want)
			}

			if len(tt.weights) == 0 {
				return // no region to assign a task to; nothing further to check
			}

			// Every task assigned exactly once: the union across all regions'
			// partitions equals the input set, no drops or duplicates.
			union := unionOf(got)
			if len(union) != len(tt.tasks) {
				t.Fatalf("union has %d distinct tasks, want %d (input size)", len(union), len(tt.tasks))
			}
			for _, task := range tt.tasks {
				if n := union[task.ID]; n != 1 {
					t.Fatalf("task %s assigned %d times, want exactly 1", task.ID, n)
				}
			}
		})
	}
}

// TestPartitionTasksIsDeterministic guards the core defining property this
// pure function must have: identical inputs always produce identical output,
// regardless of the weights slice's input order.
func TestPartitionTasksIsDeterministic(t *testing.T) {
	tasks := tasksOf("t1", "t2", "t3", "t4", "t5", "t6", "t7")
	weights := []regionWeight{
		{Region: "c", Weight: 3},
		{Region: "a", Weight: 1},
		{Region: "b", Weight: 2},
	}

	first := partitionTasks(tasks, weights)
	for i := 0; i < 50; i++ {
		got := partitionTasks(tasks, weights)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}

	// Reordering the weights slice must not change the result.
	reordered := []regionWeight{weights[1], weights[2], weights[0]}
	got := partitionTasks(tasks, reordered)
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("partitionTasks() depends on weights input order: %+v vs %+v", got, first)
	}
}

// TestPartitionTasksPreservesTaskOrderWithinRegion asserts each region's
// partition keeps the input tasks' relative order (not e.g. reshuffled),
// which matters for deterministic downstream placement/testing.
func TestPartitionTasksPreservesTaskOrderWithinRegion(t *testing.T) {
	tasks := tasksOf("t1", "t2", "t3", "t4")
	weights := []regionWeight{{Region: "only", Weight: 1}}

	got := partitionTasks(tasks, weights)
	want := tasks
	if !reflect.DeepEqual(got["only"], want) {
		t.Fatalf("partitionTasks()[only] = %+v, want %+v (input order preserved)", got["only"], want)
	}
}
