package model

import "testing"

// TestTierOrder pins the iota order of the Tier constants: adaptive-by-tier
// detection (O4) depends on Core being the zero value and on this exact
// ordering being stable.
func TestTierOrder(t *testing.T) {
	tests := []struct {
		name string
		got  Tier
		want Tier
	}{
		{"Core", Core, 0},
		{"Open", Open, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero Tier
	if zero != Core {
		t.Fatalf("zero Tier = %d, want Core (%d)", zero, Core)
	}
}

// TestDurationZeroAndRoundTrip asserts Duration's zero value is usable and
// that it round-trips once populated — a plain int64 alias, no behavior
// beyond holding what is put into it.
func TestDurationZeroAndRoundTrip(t *testing.T) {
	var zero Duration
	if zero != 0 {
		t.Fatalf("zero Duration = %d, want 0", zero)
	}

	d := Duration(5_000_000_000) // 5s in nanoseconds
	if d != 5_000_000_000 {
		t.Fatalf("Duration did not round-trip: %d", d)
	}
}

// TestCapSetOrderingContract documents and pins the CapSet contract: it is a
// bare slice, and callers — not this package — are responsible for passing
// it sorted and de-duplicated so pure cores can compare CapSets
// deterministically without a normalization helper. This test exercises
// that a CapSet holds exactly the sorted, deduped tags a caller puts into
// it; it does not sort or dedup on the type's behalf.
func TestCapSetOrderingContract(t *testing.T) {
	tests := []struct {
		name string
		caps CapSet
		want []string
	}{
		{"nil", nil, nil},
		{"empty", CapSet{}, []string{}},
		{"sorted deduped", CapSet{"gpu", "nvlink"}, []string{"gpu", "nvlink"}},
		{"single", CapSet{"gpu"}, []string{"gpu"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.caps) != len(tt.want) {
				t.Fatalf("len(CapSet) = %d, want %d", len(tt.caps), len(tt.want))
			}
			for i, tag := range tt.want {
				if tt.caps[i] != tag {
					t.Fatalf("CapSet[%d] = %q, want %q", i, tt.caps[i], tag)
				}
			}
		})
	}
}

// TestCellCapacityZeroAndRoundTrip asserts CellCapacity's zero value is
// usable and that its fields round-trip once populated.
func TestCellCapacityZeroAndRoundTrip(t *testing.T) {
	var zero CellCapacity
	if zero.ID != "" || zero.Free != 0 || zero.Caps != nil {
		t.Fatalf("zero CellCapacity = %+v, want all zero", zero)
	}

	cc := CellCapacity{ID: "cell-1", Free: 4, Caps: CapSet{"gpu", "nvlink"}}
	if cc.ID != "cell-1" || cc.Free != 4 || len(cc.Caps) != 2 || cc.Caps[0] != "gpu" || cc.Caps[1] != "nvlink" {
		t.Fatalf("CellCapacity did not round-trip: %+v", cc)
	}
}

// TestP2FieldAdditionsRoundTrip asserts the new P2 fields on existing
// boundary types (CellView.Caps, Task.Requires, JobSpec.MinMembers)
// round-trip and default to their zero value, which is what keeps existing
// P0/P1 callers unaffected.
func TestP2FieldAdditionsRoundTrip(t *testing.T) {
	t.Run("CellView.Caps", func(t *testing.T) {
		var zero CellView
		if zero.Caps != nil {
			t.Fatalf("zero CellView.Caps = %v, want nil", zero.Caps)
		}
		cv := CellView{ID: "cell-1", Size: 3, Free: 1, Caps: CapSet{"gpu"}}
		if len(cv.Caps) != 1 || cv.Caps[0] != "gpu" {
			t.Fatalf("CellView.Caps did not round-trip: %+v", cv)
		}
	})

	t.Run("Task.Requires", func(t *testing.T) {
		var zero Task
		if zero.Requires != nil {
			t.Fatalf("zero Task.Requires = %v, want nil", zero.Requires)
		}
		tsk := Task{ID: "task-1", JobID: "job-1", Requires: CapSet{"gpu", "nvlink"}}
		if len(tsk.Requires) != 2 || tsk.Requires[0] != "gpu" || tsk.Requires[1] != "nvlink" {
			t.Fatalf("Task.Requires did not round-trip: %+v", tsk)
		}
	})

	t.Run("JobSpec.MinMembers", func(t *testing.T) {
		var zero JobSpec
		if zero.MinMembers != 0 {
			t.Fatalf("zero JobSpec.MinMembers = %d, want 0", zero.MinMembers)
		}
		js := JobSpec{ID: "job-1", MinMembers: 4}
		if js.MinMembers != 4 {
			t.Fatalf("JobSpec.MinMembers did not round-trip: %d", js.MinMembers)
		}
	})
}
