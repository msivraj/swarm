package templates

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestSciSimDecompose(t *testing.T) {
	tests := []struct {
		name        string
		job         SciSimJob
		wantDomains int
		wantErr     bool
	}{
		{
			name:        "even split",
			job:         SciSimJob{JobID: "j1", GridCells: 8, Domains: 4},
			wantDomains: 4,
		},
		{
			name:        "uneven split spreads remainder over first domains",
			job:         SciSimJob{JobID: "j1", GridCells: 10, Domains: 3},
			wantDomains: 3,
		},
		{
			name:        "single domain covers the whole grid",
			job:         SciSimJob{JobID: "j1", GridCells: 100, Domains: 1},
			wantDomains: 1,
		},
		{name: "zero grid cells is rejected", job: SciSimJob{JobID: "j1", GridCells: 0, Domains: 3}, wantErr: true},
		{name: "zero domains is rejected", job: SciSimJob{JobID: "j1", GridCells: 10, Domains: 0}, wantErr: true},
		{name: "negative domains is rejected", job: SciSimJob{JobID: "j1", GridCells: 10, Domains: -3}, wantErr: true},
		{name: "more domains than cells is rejected", job: SciSimJob{JobID: "j1", GridCells: 2, Domains: 9}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SciSimDecompose(tt.job)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("SciSimDecompose() error = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("SciSimDecompose() unexpected error: %v", err)
			}
			if len(got) != tt.wantDomains {
				t.Fatalf("len(SciSimDecompose()) = %d, want %d", len(got), tt.wantDomains)
			}
			assertSciSimTasks(t, tt.job, got)
		})
	}
}

func assertSciSimTasks(t *testing.T, job SciSimJob, tasks []model.Task) {
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
	if cur != job.GridCells {
		t.Fatalf("domains cover up to %d cells, want %d", cur, job.GridCells)
	}
}

func TestSciSimDecomposeIsDeterministic(t *testing.T) {
	job := SciSimJob{JobID: "j1", GridCells: 97, Domains: 5}
	first, err := SciSimDecompose(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 100; i++ {
		got, err := SciSimDecompose(job)
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// SciSimCombine
// -----------------------------------------------------------------------

func TestSciSimCombine(t *testing.T) {
	tests := []struct {
		name       string
		boundaries [][]byte
	}{
		{"no boundaries", nil},
		{"one domain's boundary", [][]byte{[]byte("left-edge")}},
		{"three domains' boundaries, one empty", [][]byte{[]byte("north"), {}, []byte("south")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SciSimCombine(tt.boundaries)
			decoded, ok := DecodeSciSimExchange(got)
			if !ok {
				t.Fatalf("DecodeSciSimExchange rejected SciSimCombine's own output")
			}
			want := tt.boundaries
			if want == nil {
				want = [][]byte{}
			}
			if len(decoded) != len(want) {
				t.Fatalf("decoded %d boundaries, want %d", len(decoded), len(want))
			}
			for i := range want {
				if string(decoded[i]) != string(want[i]) {
					t.Fatalf("boundary %d = %q, want %q", i, decoded[i], want[i])
				}
			}
		})
	}
}

// TestSciSimCombineKnownExample fixes a small, hand-checked example so the
// combined packet's exact byte layout is pinned, not just its round-trip.
func TestSciSimCombineKnownExample(t *testing.T) {
	got := SciSimCombine([][]byte{[]byte("ab"), []byte("c")})
	want := []byte{
		0, 0, 0, 2, 'a', 'b', // "ab", length-prefixed
		0, 0, 0, 1, 'c', // "c", length-prefixed
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SciSimCombine() = %v, want %v", got, want)
	}
}

// TestSciSimCombinePreservesOrder checks boundary exchange is
// order-sensitive (unlike the all-reduce combines): reordering the input
// reorders the combined packet.
func TestSciSimCombinePreservesOrder(t *testing.T) {
	a, b := SciSimCombine([][]byte{[]byte("x"), []byte("y")}), SciSimCombine([][]byte{[]byte("y"), []byte("x")})
	if reflect.DeepEqual(a, b) {
		t.Fatalf("SciSimCombine() did not preserve input order: %v == %v", a, b)
	}
}

func TestSciSimCombineIsDeterministic(t *testing.T) {
	boundaries := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
	first := SciSimCombine(boundaries)
	for i := 0; i < 100; i++ {
		if got := SciSimCombine(boundaries); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d", i)
		}
	}
}
