package templates

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestGraphComputeDecompose(t *testing.T) {
	tests := []struct {
		name           string
		job            GraphComputeJob
		wantPartitions int
		wantErr        bool
	}{
		{
			name:           "even split",
			job:            GraphComputeJob{JobID: "j1", NumVertices: 12, Partitions: 3},
			wantPartitions: 3,
		},
		{
			name:           "uneven split spreads remainder over first partitions",
			job:            GraphComputeJob{JobID: "j1", NumVertices: 11, Partitions: 3},
			wantPartitions: 3,
		},
		{
			name:           "single partition covers every vertex",
			job:            GraphComputeJob{JobID: "j1", NumVertices: 40, Partitions: 1},
			wantPartitions: 1,
		},
		{name: "zero vertices is rejected", job: GraphComputeJob{JobID: "j1", NumVertices: 0, Partitions: 3}, wantErr: true},
		{name: "zero partitions is rejected", job: GraphComputeJob{JobID: "j1", NumVertices: 10, Partitions: 0}, wantErr: true},
		{name: "negative partitions is rejected", job: GraphComputeJob{JobID: "j1", NumVertices: 10, Partitions: -1}, wantErr: true},
		{name: "more partitions than vertices is rejected", job: GraphComputeJob{JobID: "j1", NumVertices: 4, Partitions: 9}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GraphComputeDecompose(tt.job)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("GraphComputeDecompose() error = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("GraphComputeDecompose() unexpected error: %v", err)
			}
			if len(got) != tt.wantPartitions {
				t.Fatalf("len(GraphComputeDecompose()) = %d, want %d", len(got), tt.wantPartitions)
			}
			assertGraphComputeTasks(t, tt.job, got)
		})
	}
}

func assertGraphComputeTasks(t *testing.T, job GraphComputeJob, tasks []model.Task) {
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
	if cur != job.NumVertices {
		t.Fatalf("partitions cover up to %d vertices, want %d", cur, job.NumVertices)
	}
}

func TestGraphComputeDecomposeIsDeterministic(t *testing.T) {
	job := GraphComputeJob{JobID: "j1", NumVertices: 311, Partitions: 9}
	first, err := GraphComputeDecompose(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 100; i++ {
		got, err := GraphComputeDecompose(job)
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// GraphComputeCombine
// -----------------------------------------------------------------------

func TestGraphComputeCombine(t *testing.T) {
	tests := []struct {
		name         string
		partial      [][]byte
		wantActive   uint64
		wantMessages [][]byte
	}{
		{
			name:         "no partials",
			partial:      nil,
			wantActive:   0,
			wantMessages: [][]byte{},
		},
		{
			name: "single partition, no messages, still active",
			partial: [][]byte{
				EncodeGraphSuperstepPartial(5, nil),
			},
			wantActive:   5,
			wantMessages: [][]byte{{}},
		},
		{
			name: "three partitions sum active counts and pack messages in order",
			partial: [][]byte{
				EncodeGraphSuperstepPartial(2, []byte("to-p2")),
				EncodeGraphSuperstepPartial(0, []byte("to-p0")),
				EncodeGraphSuperstepPartial(3, nil),
			},
			wantActive:   5,
			wantMessages: [][]byte{[]byte("to-p2"), []byte("to-p0"), {}},
		},
		{
			name: "zero active vertices everywhere signals convergence",
			partial: [][]byte{
				EncodeGraphSuperstepPartial(0, nil),
				EncodeGraphSuperstepPartial(0, nil),
			},
			wantActive:   0,
			wantMessages: [][]byte{{}, {}},
		},
		{
			name: "malformed entry (too short) is skipped",
			partial: [][]byte{
				EncodeGraphSuperstepPartial(4, []byte("msg")),
				[]byte("bad"),
			},
			wantActive:   4,
			wantMessages: [][]byte{[]byte("msg")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GraphComputeCombine(tt.partial)
			active, messages, ok := DecodeGraphSuperstepCombined(got)
			if !ok {
				t.Fatalf("DecodeGraphSuperstepCombined rejected GraphComputeCombine's own output")
			}
			if active != tt.wantActive {
				t.Fatalf("active vertices = %d, want %d", active, tt.wantActive)
			}
			if len(messages) != len(tt.wantMessages) {
				t.Fatalf("got %d message chunks, want %d", len(messages), len(tt.wantMessages))
			}
			for i := range tt.wantMessages {
				if string(messages[i]) != string(tt.wantMessages[i]) {
					t.Fatalf("message %d = %q, want %q", i, messages[i], tt.wantMessages[i])
				}
			}
		})
	}
}

// TestGraphComputeCombineActiveCountIsCommutative checks the part of the
// combine that is a true reduction (the active-vertex count is a plain sum,
// so it doesn't depend on input order), even though the packed message
// stream itself is order-sensitive like SciSimCombine's boundary exchange —
// graph-compute's combine is not one of the two combines issue #63 requires
// full order-independence for, but the count sub-reduction still shouldn't
// depend on order.
func TestGraphComputeCombineActiveCountIsCommutative(t *testing.T) {
	partial := [][]byte{
		EncodeGraphSuperstepPartial(3, []byte("a")),
		EncodeGraphSuperstepPartial(7, []byte("b")),
		EncodeGraphSuperstepPartial(1, []byte("c")),
	}
	wantActive, _, _ := DecodeGraphSuperstepCombined(GraphComputeCombine(partial))

	reordered := [][]byte{partial[2], partial[0], partial[1]}
	gotActive, _, _ := DecodeGraphSuperstepCombined(GraphComputeCombine(reordered))

	if gotActive != wantActive {
		t.Fatalf("reordered active count = %d, want %d", gotActive, wantActive)
	}
}

func TestGraphComputeCombineIsDeterministic(t *testing.T) {
	partial := [][]byte{
		EncodeGraphSuperstepPartial(2, []byte("x")),
		EncodeGraphSuperstepPartial(3, []byte("y")),
	}
	first := GraphComputeCombine(partial)
	for i := 0; i < 100; i++ {
		if got := GraphComputeCombine(partial); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d", i)
		}
	}
}

func TestDecodeGraphSuperstepCombinedRejectsMalformed(t *testing.T) {
	if _, _, ok := DecodeGraphSuperstepCombined([]byte{1, 2, 3}); ok {
		t.Fatal("DecodeGraphSuperstepCombined accepted a too-short input (missing active-count header)")
	}

	// A well-formed active-count header followed by a truncated message
	// chunk stream (a length prefix claiming more bytes than remain).
	malformed := append(EncodeGraphSuperstepPartial(1, nil)[:graphActiveCountLen], 0, 0, 0, 99, 'x')
	if _, _, ok := DecodeGraphSuperstepCombined(malformed); ok {
		t.Fatal("DecodeGraphSuperstepCombined accepted a truncated message chunk stream")
	}
}
