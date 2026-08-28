package admission

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func capOf(id string, free int) model.CellCapacity {
	return model.CellCapacity{ID: model.CellID(id), Free: free}
}

func gangJob(minMembers int) model.JobSpec {
	return model.JobSpec{ID: "gang-job", MinMembers: minMembers}
}

// -----------------------------------------------------------------------
// AdmitGang
// -----------------------------------------------------------------------

func TestAdmitGang(t *testing.T) {
	tests := []struct {
		name string
		job  model.JobSpec
		free []model.CellCapacity
		want Gang
	}{
		{
			name: "headline: 128 needed, 100 free -> Wait",
			job:  gangJob(128),
			free: []model.CellCapacity{capOf("a", 60), capOf("b", 40)}, // 100 total
			want: Gang{Kind: Wait},
		},
		{
			name: "exactly enough, spread over cells -> Place summing to MinMembers",
			job:  gangJob(128),
			free: []model.CellCapacity{capOf("a", 64), capOf("b", 64)},
			want: Gang{Kind: Place, Assignments: []Assignment{
				{Cell: "a", Members: 64},
				{Cell: "b", Members: 64},
			}},
		},
		{
			name: "comfortably fits in one cell -> Place, first-fit, no over-allocation",
			job:  gangJob(10),
			free: []model.CellCapacity{capOf("a", 50), capOf("b", 50)},
			want: Gang{Kind: Place, Assignments: []Assignment{
				{Cell: "a", Members: 10},
			}},
		},
		{
			name: "more than enough spread across cells: first-fit, deterministic order",
			job:  gangJob(150),
			free: []model.CellCapacity{capOf("b", 100), capOf("a", 100)}, // input order, not CellID order
			want: Gang{Kind: Place, Assignments: []Assignment{
				{Cell: "b", Members: 100},
				{Cell: "a", Members: 50},
			}},
		},
		{
			name: "one short -> Wait",
			job:  gangJob(101),
			free: []model.CellCapacity{capOf("a", 60), capOf("b", 40)}, // 100 total
			want: Gang{Kind: Wait},
		},
		{
			name: "zero free -> Wait",
			job:  gangJob(1),
			free: nil,
			want: Gang{Kind: Wait},
		},
		{
			name: "cells with zero or negative free are skipped, not assigned",
			job:  gangJob(5),
			free: []model.CellCapacity{capOf("a", 0), capOf("b", -3), capOf("c", 5)},
			want: Gang{Kind: Place, Assignments: []Assignment{
				{Cell: "c", Members: 5},
			}},
		},
		{
			name: "MinMembers == 0 is not a gang -> Place with empty Assignments",
			job:  gangJob(0),
			free: []model.CellCapacity{capOf("a", 5)},
			want: Gang{Kind: Place},
		},
		{
			name: "MinMembers == 0 with no free capacity at all -> still Place empty",
			job:  gangJob(0),
			free: nil,
			want: Gang{Kind: Place},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AdmitGang(tt.job, tt.free)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("AdmitGang() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestAdmitGangIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestAdmitGangIsDeterministic(t *testing.T) {
	job := gangJob(90)
	free := []model.CellCapacity{capOf("c", 30), capOf("a", 40), capOf("b", 50)}

	first := AdmitGang(job, free)
	for i := 0; i < 100; i++ {
		if got := AdmitGang(job, free); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

// -----------------------------------------------------------------------
// AdmitGang — B4 property: a gang never starts half-scheduled
// -----------------------------------------------------------------------

// TestAdmitGangNeverPartial is the property test for B4: across a broad
// sweep of MinMembers floors and free-capacity layouts, AdmitGang never
// returns a Place whose Assignments sum to less than MinMembers, and never
// assigns more than a cell's own Free to it.
func TestAdmitGangNeverPartial(t *testing.T) {
	layouts := [][]model.CellCapacity{
		nil,
		{capOf("a", 0)},
		{capOf("a", 1)},
		{capOf("a", 3), capOf("b", 4)},
		{capOf("a", 10), capOf("b", -5), capOf("c", 0), capOf("d", 25)},
		{capOf("z", 1), capOf("y", 1), capOf("x", 1), capOf("w", 1), capOf("v", 1)},
		{capOf("a", 128), capOf("b", 128), capOf("c", 128)},
	}

	freeIn := func(layout []model.CellCapacity) map[model.CellID]int {
		m := make(map[model.CellID]int, len(layout))
		for _, c := range layout {
			m[c.ID] = c.Free
		}
		return m
	}

	for _, layout := range layouts {
		total := 0
		for _, c := range layout {
			if c.Free > 0 {
				total += c.Free
			}
		}
		byID := freeIn(layout)

		// Sweep MinMembers from 0 through comfortably past the total free
		// capacity, so both Wait and Place branches, and their boundary, are
		// exercised for every layout.
		for min := 0; min <= total+5; min++ {
			job := gangJob(min)
			got := AdmitGang(job, layout)

			if got.Kind == Wait {
				if len(got.Assignments) != 0 {
					t.Fatalf("Wait carries non-empty Assignments: job=%+v layout=%+v got=%+v", job, layout, got)
				}
				continue
			}

			// Place: sum must be >= MinMembers (never partial), and no cell
			// may be over-allocated beyond its own Free.
			sum := 0
			seen := make(map[model.CellID]bool)
			for _, a := range got.Assignments {
				if seen[a.Cell] {
					t.Fatalf("cell %q assigned more than once: job=%+v layout=%+v got=%+v", a.Cell, job, layout, got)
				}
				seen[a.Cell] = true
				if a.Members > byID[a.Cell] {
					t.Fatalf("cell %q over-allocated: assigned %d, Free %d: job=%+v layout=%+v", a.Cell, a.Members, byID[a.Cell], job, layout)
				}
				if a.Members <= 0 {
					t.Fatalf("cell %q assigned non-positive Members %d: job=%+v layout=%+v", a.Cell, a.Members, job, layout)
				}
				sum += a.Members
			}
			if sum < job.MinMembers {
				t.Fatalf("Place under MinMembers: sum=%d MinMembers=%d job=%+v layout=%+v got=%+v", sum, job.MinMembers, job, layout, got)
			}
		}
	}
}

// TestAdmitGangHeadline pins the phase doc's own headline example: "128
// needed, 100 free -> Wait", with the shortfall split unevenly.
func TestAdmitGangHeadline(t *testing.T) {
	job := gangJob(128)
	free := []model.CellCapacity{capOf("a", 37), capOf("b", 63)} // 100 total, still short

	got := AdmitGang(job, free)
	if got.Kind != Wait {
		t.Fatalf("AdmitGang(128 needed, 100 free) = %+v, want Wait", got)
	}
	if len(got.Assignments) != 0 {
		t.Fatalf("Wait carries assignments: %+v", got.Assignments)
	}
}
