package model

import "testing"

// TestCouplingOrder pins the iota order of the Coupling constants: later
// phases (barriers, leader election, message passing) depend on this exact
// ordering being stable.
func TestCouplingOrder(t *testing.T) {
	tests := []struct {
		name string
		got  Coupling
		want Coupling
	}{
		{"Independent", Independent, 0},
		{"Barrier", Barrier, 1},
		{"Leader", Leader, 2},
		{"MessagePassing", MessagePassing, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}
}

// TestZeroValuesUsable asserts each boundary type's zero value is usable and
// that fields round-trip once populated — these are plain data, so there is
// no behavior to test beyond that they hold what is put into them.
func TestZeroValuesUsable(t *testing.T) {
	t.Run("JobSpec", func(t *testing.T) {
		var zero JobSpec
		if zero.ID != "" || zero.Template != "" || zero.Coupling != Independent || zero.Params != nil {
			t.Fatalf("zero JobSpec = %+v, want all zero", zero)
		}
		js := JobSpec{ID: "job-1", Template: "map-reduce", Coupling: Barrier, Params: map[string]string{"k": "v"}}
		if js.ID != "job-1" || js.Template != "map-reduce" || js.Coupling != Barrier || js.Params["k"] != "v" {
			t.Fatalf("JobSpec did not round-trip: %+v", js)
		}
	})

	t.Run("Task", func(t *testing.T) {
		var zero Task
		if zero.ID != "" || zero.JobID != "" || zero.Input != nil || zero.Attempt != 0 {
			t.Fatalf("zero Task = %+v, want all zero", zero)
		}
		tsk := Task{ID: "task-1", JobID: "job-1", Input: []byte("payload"), Attempt: 2}
		if tsk.ID != "task-1" || tsk.JobID != "job-1" || string(tsk.Input) != "payload" || tsk.Attempt != 2 {
			t.Fatalf("Task did not round-trip: %+v", tsk)
		}
	})

	t.Run("TaskResult", func(t *testing.T) {
		var zero TaskResult
		if zero.TaskID != "" || zero.Output != nil || zero.OK != false {
			t.Fatalf("zero TaskResult = %+v, want all zero", zero)
		}
		tr := TaskResult{TaskID: "task-1", Output: []byte("result"), OK: true}
		if tr.TaskID != "task-1" || string(tr.Output) != "result" || !tr.OK {
			t.Fatalf("TaskResult did not round-trip: %+v", tr)
		}
	})

	t.Run("Aggregate", func(t *testing.T) {
		var zero Aggregate
		if zero.JobID != "" || zero.Value != nil || zero.Done != false {
			t.Fatalf("zero Aggregate = %+v, want all zero", zero)
		}
		agg := Aggregate{JobID: "job-1", Value: []byte("final"), Done: true}
		if agg.JobID != "job-1" || string(agg.Value) != "final" || !agg.Done {
			t.Fatalf("Aggregate did not round-trip: %+v", agg)
		}
	})
}
