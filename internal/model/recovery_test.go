package model

import "testing"

// TestLossKindOrder pins the iota order of the LossKind constants:
// recoveryPlan (P5) branches on this exact ordering.
func TestLossKindOrder(t *testing.T) {
	tests := []struct {
		name string
		got  LossKind
		want LossKind
	}{
		{"RegionLoss", RegionLoss, 0},
		{"StoreLoss", StoreLoss, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero LossKind
	if zero != RegionLoss {
		t.Fatalf("zero LossKind = %d, want RegionLoss (%d)", zero, RegionLoss)
	}
}

// TestStepKindOrder pins the iota order of the StepKind constants and the
// zero-value contract: NoStep must be zero so an uninitialized/empty Step
// takes no action rather than re-homing or rerouting anything by accident.
func TestStepKindOrder(t *testing.T) {
	tests := []struct {
		name string
		got  StepKind
		want StepKind
	}{
		{"NoStep", NoStep, 0},
		{"ReHome", ReHome, 1},
		{"RestoreRegistry", RestoreRegistry, 2},
		{"Reroute", Reroute, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero Step
	if zero.Kind != NoStep {
		t.Fatalf("zero Step.Kind = %d, want NoStep (%d)", zero.Kind, NoStep)
	}
	if zero.Region != "" || zero.Backup != 0 || zero.Traffic != "" {
		t.Fatalf("zero Step = %+v, want all zero", zero)
	}
}

// TestLossAndStepRoundTrip asserts Loss and each Step variant round-trip.
func TestLossAndStepRoundTrip(t *testing.T) {
	t.Run("Loss", func(t *testing.T) {
		l := Loss{Kind: RegionLoss, Region: "us-east"}
		if l.Kind != RegionLoss || l.Region != "us-east" {
			t.Fatalf("Loss did not round-trip: %+v", l)
		}
	})

	t.Run("Step ReHome", func(t *testing.T) {
		s := Step{Kind: ReHome, Region: "us-west"}
		if s.Kind != ReHome || s.Region != "us-west" {
			t.Fatalf("Step ReHome did not round-trip: %+v", s)
		}
	})

	t.Run("Step RestoreRegistry", func(t *testing.T) {
		s := Step{Kind: RestoreRegistry, Backup: Instant(1000)}
		if s.Kind != RestoreRegistry || s.Backup != 1000 {
			t.Fatalf("Step RestoreRegistry did not round-trip: %+v", s)
		}
	})

	t.Run("Step Reroute", func(t *testing.T) {
		s := Step{Kind: Reroute, Traffic: "us-east"}
		if s.Kind != Reroute || s.Traffic != "us-east" {
			t.Fatalf("Step Reroute did not round-trip: %+v", s)
		}
	})
}

// TestFleetStateRecoveryFieldsAdditive asserts the new region-topology and
// backup fields on FleetState default to nil (unchanged from the P4 zero
// value) and round-trip once populated, alongside the existing P4 fields.
func TestFleetStateRecoveryFieldsAdditive(t *testing.T) {
	var zero FleetState
	if zero.Cells != nil || zero.Regions != nil || zero.Backups != nil {
		t.Fatalf("zero FleetState recovery fields = %+v, want all nil", zero)
	}
	// existing P4 fields still nil, confirming the addition is additive
	if zero.Versions != nil || zero.Jobs != nil || zero.Cordoned != nil {
		t.Fatalf("zero FleetState pre-P5 fields changed: %+v", zero)
	}

	fs := FleetState{
		Cells:   map[CellID]RegionID{"cell-1": "us-east"},
		Regions: []RegionID{"us-east", "us-west"},
		Backups: map[RegionID]Instant{"us-east": 500},
	}
	if fs.Cells["cell-1"] != "us-east" {
		t.Fatalf("FleetState.Cells did not round-trip: %+v", fs.Cells)
	}
	if len(fs.Regions) != 2 || fs.Regions[0] != "us-east" || fs.Regions[1] != "us-west" {
		t.Fatalf("FleetState.Regions did not round-trip: %+v", fs.Regions)
	}
	if fs.Backups["us-east"] != 500 {
		t.Fatalf("FleetState.Backups did not round-trip: %+v", fs.Backups)
	}
}
