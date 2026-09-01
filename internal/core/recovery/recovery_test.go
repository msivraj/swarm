package recovery

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// usEastFleet is the fixed multi-region fleet the exactSteps property test
// drills against: three regions, us-east lost, a backup available for each
// surviving region plus one for us-east itself (recovery should still pick
// the newest across the whole map, not just the lost region's).
func usEastFleet() model.FleetState {
	return model.FleetState{
		Regions: []model.RegionID{"us-east", "us-west", "eu-central"},
		Cells: map[model.CellID]model.RegionID{
			"cell-1": "us-east",
			"cell-2": "us-west",
			"cell-3": "eu-central",
		},
		Backups: map[model.RegionID]model.Instant{
			"us-east":    900,
			"us-west":    1000,
			"eu-central": 700,
		},
	}
}

// TestRecoveryPlanExactSteps is the exactSteps property: "us-east lost =>
// these exact steps in this order". It also proves map-iteration
// independence by rebuilding the fleet's maps in a different insertion
// order and asserting the plan is unchanged.
func TestRecoveryPlanExactSteps(t *testing.T) {
	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}
	want := []model.Step{
		{Kind: model.ReHome, Region: "eu-central"},  // lowest-sorted survivor: "eu-central" < "us-west"
		{Kind: model.RestoreRegistry, Backup: 1000}, // newest across all regions, not just us-east
		{Kind: model.Reroute, Traffic: "us-east"},
	}

	fleet := usEastFleet()
	got := RecoveryPlan(loss, fleet)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RecoveryPlan(us-east lost) = %+v, want %+v", got, want)
	}

	// Rebuild the same fleet with maps constructed in a different order and
	// a differently-ordered Regions slice's underlying values reinserted —
	// the plan must be byte-for-byte identical.
	reordered := model.FleetState{
		Regions: []model.RegionID{"eu-central", "us-west", "us-east"},
		Cells: map[model.CellID]model.RegionID{
			"cell-3": "eu-central",
			"cell-1": "us-east",
			"cell-2": "us-west",
		},
		Backups: map[model.RegionID]model.Instant{
			"eu-central": 700,
			"us-west":    1000,
			"us-east":    900,
		},
	}
	got2 := RecoveryPlan(loss, reordered)
	if !reflect.DeepEqual(got2, want) {
		t.Fatalf("RecoveryPlan(reordered fleet) = %+v, want %+v (map-iteration dependence)", got2, want)
	}
}

// TestRecoveryPlanRegionLossNoBackup covers a RegionLoss where no backup is
// available: the RestoreRegistry step must be skipped entirely, never
// emitted with a zero/garbage Backup.
func TestRecoveryPlanRegionLossNoBackup(t *testing.T) {
	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}
	fleet := model.FleetState{
		Regions: []model.RegionID{"us-east", "us-west"},
	}
	want := []model.Step{
		{Kind: model.ReHome, Region: "us-west"},
		{Kind: model.Reroute, Traffic: "us-east"},
	}

	got := RecoveryPlan(loss, fleet)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RecoveryPlan(no backup) = %+v, want %+v", got, want)
	}
}

// TestRecoveryPlanRegionLossNoSurvivor covers a RegionLoss where the lost
// region is the only region the fleet knows about: ReHome has nowhere to
// send agents, so it is skipped, but RestoreRegistry and Reroute still
// apply.
func TestRecoveryPlanRegionLossNoSurvivor(t *testing.T) {
	loss := model.Loss{Kind: model.RegionLoss, Region: "us-east"}
	fleet := model.FleetState{
		Regions: []model.RegionID{"us-east"},
		Backups: map[model.RegionID]model.Instant{"us-east": 500},
	}
	want := []model.Step{
		{Kind: model.RestoreRegistry, Backup: 500},
		{Kind: model.Reroute, Traffic: "us-east"},
	}

	got := RecoveryPlan(loss, fleet)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RecoveryPlan(no survivor) = %+v, want %+v", got, want)
	}
}

// TestRecoveryPlanStoreLossAndNoop is the StoreLoss + no-op table test.
func TestRecoveryPlanStoreLossAndNoop(t *testing.T) {
	tests := []struct {
		name  string
		loss  model.Loss
		fleet model.FleetState
		want  []model.Step
	}{
		{
			name: "store loss with a backup restores from the latest",
			loss: model.Loss{Kind: model.StoreLoss},
			fleet: model.FleetState{
				Backups: map[model.RegionID]model.Instant{
					"us-east": 200,
					"us-west": 800,
				},
			},
			want: []model.Step{{Kind: model.RestoreRegistry, Backup: 800}},
		},
		{
			name:  "store loss with no backup is a well-defined empty plan",
			loss:  model.Loss{Kind: model.StoreLoss},
			fleet: model.FleetState{},
			want:  nil,
		},
		{
			name:  "region loss for an unknown region is a no-op",
			loss:  model.Loss{Kind: model.RegionLoss, Region: "mars"},
			fleet: usEastFleet(),
			want:  nil,
		},
		{
			name:  "region loss over an empty fleet is a no-op",
			loss:  model.Loss{Kind: model.RegionLoss, Region: "us-east"},
			fleet: model.FleetState{},
			want:  nil,
		},
		{
			name:  "zero-value loss (RegionLoss, empty region) over an empty fleet is a no-op",
			loss:  model.Loss{},
			fleet: model.FleetState{},
			want:  nil,
		},
		{
			name:  "unrecognized loss kind yields an empty plan",
			loss:  model.Loss{Kind: model.LossKind(99), Region: "us-east"},
			fleet: usEastFleet(),
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RecoveryPlan(tt.loss, tt.fleet)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("RecoveryPlan() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestRecoveryPlanIsDeterministic guards the core's defining property:
// identical (loss, fleet) inputs always produce an identical plan, run after
// run, over both the RegionLoss and StoreLoss paths.
func TestRecoveryPlanIsDeterministic(t *testing.T) {
	cases := []struct {
		name  string
		loss  model.Loss
		fleet model.FleetState
	}{
		{"region loss", model.Loss{Kind: model.RegionLoss, Region: "us-east"}, usEastFleet()},
		{"store loss", model.Loss{Kind: model.StoreLoss}, usEastFleet()},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			first := RecoveryPlan(c.loss, c.fleet)
			for i := 0; i < 100; i++ {
				if got := RecoveryPlan(c.loss, c.fleet); !reflect.DeepEqual(got, first) {
					t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
				}
			}
		})
	}
}

// TestRpoBoundary is the rpoBoundary table test: strictly within the RPO,
// exactly at the boundary (inclusive), and past it, plus a future backup and
// a zero RPO.
func TestRpoBoundary(t *testing.T) {
	tests := []struct {
		name       string
		lastBackup model.Instant
		now        model.Instant
		rpo        model.Duration
		want       bool
	}{
		{"strictly within rpo", 0, 500, 1000, true},
		{"exactly at rpo boundary is met", 0, 1000, 1000, true},
		{"one tick past rpo is not met", 0, 1001, 1000, false},
		{"backup in the future is met", 1000, 500, 1000, true},
		{"zero rpo, backup now, met", 1000, 1000, 0, true},
		{"zero rpo, backup one tick ago, not met", 999, 1000, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RpoMet(tt.lastBackup, tt.now, tt.rpo); got != tt.want {
				t.Fatalf("RpoMet(%d, %d, %d) = %v, want %v", tt.lastBackup, tt.now, tt.rpo, got, tt.want)
			}
		})
	}
}
