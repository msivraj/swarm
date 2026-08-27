package cli

import "testing"

func TestRender(t *testing.T) {
	tests := []struct {
		name  string
		reply Reply
		want  string
	}{
		{
			name:  "submit reply",
			reply: Reply{Kind: ReplySubmit, JobID: "job-1"},
			want:  "submitted job job-1\n",
		},
		{
			name:  "ps reply",
			reply: Reply{Kind: ReplyPs, Cells: 3, Machines: 42, Jobs: 7},
			want:  "CELLS  MACHINES  JOBS\n3      42        7   \n",
		},
		{
			name:  "job status still running, no aggregate yet",
			reply: Reply{Kind: ReplyJobStatus, JobID: "job-1", Done: false},
			want:  "job job-1: running\n",
		},
		{
			name:  "job status done with an aggregate",
			reply: Reply{Kind: ReplyJobStatus, JobID: "job-1", Done: true, Aggregate: []byte("42")},
			want:  "job job-1: done\naggregate: 42\n",
		},
		{
			name:  "unknown reply kind",
			reply: Reply{},
			want:  "unknown reply\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Render(tt.reply); got != tt.want {
				t.Fatalf("Render(%+v) = %q, want %q", tt.reply, got, tt.want)
			}
		})
	}
}

// TestRenderIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestRenderIsDeterministic(t *testing.T) {
	reply := Reply{Kind: ReplyPs, Cells: 3, Machines: 42, Jobs: 7}
	first := Render(reply)
	for i := 0; i < 100; i++ {
		if got := Render(reply); got != first {
			t.Fatalf("non-deterministic output on run %d: %q vs %q", i, got, first)
		}
	}
}
