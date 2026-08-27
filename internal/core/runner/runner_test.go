package runner

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestStep(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	result := model.TaskResult{TaskID: "t1", OK: true}

	tests := []struct {
		name      string
		state     RunState
		event     RunEvent
		wantState RunState
		wantCmds  []RunCmd
	}{
		// StateIdle
		{
			name:      "idle + Idle pulls",
			state:     StateIdle,
			event:     RunEvent{Kind: Idle},
			wantState: StateIdle,
			wantCmds:  []RunCmd{{Kind: Pull}},
		},
		{
			name:      "idle + Pulled executes the task",
			state:     StateIdle,
			event:     RunEvent{Kind: Pulled, Task: task},
			wantState: StateRunning,
			wantCmds:  []RunCmd{{Kind: Execute, Task: task}},
		},
		{
			name:      "idle + Done is ignored",
			state:     StateIdle,
			event:     RunEvent{Kind: Done, Result: result},
			wantState: StateIdle,
			wantCmds:  nil,
		},
		{
			name:      "idle + Failed is ignored",
			state:     StateIdle,
			event:     RunEvent{Kind: Failed, Task: task},
			wantState: StateIdle,
			wantCmds:  nil,
		},

		// StateRunning
		{
			name:      "running + Idle is ignored",
			state:     StateRunning,
			event:     RunEvent{Kind: Idle},
			wantState: StateRunning,
			wantCmds:  nil,
		},
		{
			name:      "running + Pulled is ignored",
			state:     StateRunning,
			event:     RunEvent{Kind: Pulled, Task: task},
			wantState: StateRunning,
			wantCmds:  nil,
		},
		{
			name:      "running + Done reports then re-pulls",
			state:     StateRunning,
			event:     RunEvent{Kind: Done, Result: result},
			wantState: StateIdle,
			wantCmds: []RunCmd{
				{Kind: Report, Result: result},
				{Kind: Pull},
			},
		},
		{
			name:      "running + Failed re-queues the task",
			state:     StateRunning,
			event:     RunEvent{Kind: Failed, Task: task},
			wantState: StateIdle,
			wantCmds:  []RunCmd{{Kind: ReQueue, Task: task}},
		},

		// StateReporting — unreachable via the current table, but Step must
		// still be total: every event leaves it unchanged with no commands.
		{
			name:      "reporting + Idle is ignored",
			state:     StateReporting,
			event:     RunEvent{Kind: Idle},
			wantState: StateReporting,
			wantCmds:  nil,
		},
		{
			name:      "reporting + Pulled is ignored",
			state:     StateReporting,
			event:     RunEvent{Kind: Pulled, Task: task},
			wantState: StateReporting,
			wantCmds:  nil,
		},
		{
			name:      "reporting + Done is ignored",
			state:     StateReporting,
			event:     RunEvent{Kind: Done, Result: result},
			wantState: StateReporting,
			wantCmds:  nil,
		},
		{
			name:      "reporting + Failed is ignored",
			state:     StateReporting,
			event:     RunEvent{Kind: Failed, Task: task},
			wantState: StateReporting,
			wantCmds:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotState, gotCmds := Step(tt.state, tt.event)
			if gotState != tt.wantState {
				t.Errorf("Step() state = %v, want %v", gotState, tt.wantState)
			}
			if !reflect.DeepEqual(gotCmds, tt.wantCmds) {
				t.Errorf("Step() cmds = %+v, want %+v", gotCmds, tt.wantCmds)
			}
		})
	}
}

// TestStepFullLoop walks the natural pull -> execute -> report -> re-pull
// cycle end to end, and the failure -> re-queue cycle, to document the
// intended sequence of calls a shell makes against this core.
func TestStepFullLoop(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	result := model.TaskResult{TaskID: "t1", OK: true}

	state := StateIdle

	state, cmds := Step(state, RunEvent{Kind: Idle})
	if state != StateIdle || !reflect.DeepEqual(cmds, []RunCmd{{Kind: Pull}}) {
		t.Fatalf("kickoff: got (%v, %+v)", state, cmds)
	}

	state, cmds = Step(state, RunEvent{Kind: Pulled, Task: task})
	if state != StateRunning || !reflect.DeepEqual(cmds, []RunCmd{{Kind: Execute, Task: task}}) {
		t.Fatalf("pulled: got (%v, %+v)", state, cmds)
	}

	state, cmds = Step(state, RunEvent{Kind: Done, Result: result})
	want := []RunCmd{{Kind: Report, Result: result}, {Kind: Pull}}
	if state != StateIdle || !reflect.DeepEqual(cmds, want) {
		t.Fatalf("done: got (%v, %+v), want (%v, %+v)", state, cmds, StateIdle, want)
	}

	// A second task that fails should be re-queued rather than reported.
	state, cmds = Step(state, RunEvent{Kind: Pulled, Task: task})
	if state != StateRunning || !reflect.DeepEqual(cmds, []RunCmd{{Kind: Execute, Task: task}}) {
		t.Fatalf("re-pulled: got (%v, %+v)", state, cmds)
	}

	state, cmds = Step(state, RunEvent{Kind: Failed, Task: task})
	if state != StateIdle || !reflect.DeepEqual(cmds, []RunCmd{{Kind: ReQueue, Task: task}}) {
		t.Fatalf("failed: got (%v, %+v)", state, cmds)
	}
}

// TestStepIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestStepIsDeterministic(t *testing.T) {
	task := model.Task{ID: "t1", JobID: "j1"}
	result := model.TaskResult{TaskID: "t1", OK: true}

	cases := []struct {
		state RunState
		event RunEvent
	}{
		{StateIdle, RunEvent{Kind: Idle}},
		{StateIdle, RunEvent{Kind: Pulled, Task: task}},
		{StateRunning, RunEvent{Kind: Done, Result: result}},
		{StateRunning, RunEvent{Kind: Failed, Task: task}},
	}

	for _, c := range cases {
		wantState, wantCmds := Step(c.state, c.event)
		for i := 0; i < 100; i++ {
			gotState, gotCmds := Step(c.state, c.event)
			if gotState != wantState || !reflect.DeepEqual(gotCmds, wantCmds) {
				t.Fatalf("non-deterministic output on run %d for state=%v event=%+v: (%v,%+v) vs (%v,%+v)",
					i, c.state, c.event, gotState, gotCmds, wantState, wantCmds)
			}
		}
	}
}
