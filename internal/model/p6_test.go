package model

import "testing"

// TestRepTierOrder pins the iota order of the RepTier constants: reputation
// maturity (P6) depends on RepUntrusted being the zero value — a
// never-classified identity must read untrusted, not trusted — and on this
// exact ordering being stable.
func TestRepTierOrder(t *testing.T) {
	tests := []struct {
		name string
		got  RepTier
		want RepTier
	}{
		{"RepUntrusted", RepUntrusted, 0},
		{"RepProvisional", RepProvisional, 1},
		{"RepTrusted", RepTrusted, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero RepTier
	if zero != RepUntrusted {
		t.Fatalf("zero RepTier = %d, want RepUntrusted (%d)", zero, RepUntrusted)
	}
}

// TestCellSignalZeroAndRoundTrip asserts a zero CellSignal (P99 == 0) is a
// valid "no measured signal" input — signal-based mitosis falls back to the
// count proxy — and that populated fields round-trip.
func TestCellSignalZeroAndRoundTrip(t *testing.T) {
	var zero CellSignal
	if zero.Cell != "" || zero.Coupling != Independent || zero.Size != 0 || zero.P99 != 0 || zero.Tput != 0 {
		t.Fatalf("zero CellSignal = %+v, want all zero", zero)
	}

	cs := CellSignal{Cell: "cell-1", Coupling: Barrier, Size: 12, P99: 5_000_000, Tput: 42.5}
	if cs.Cell != "cell-1" || cs.Coupling != Barrier || cs.Size != 12 || cs.P99 != 5_000_000 || cs.Tput != 42.5 {
		t.Fatalf("CellSignal did not round-trip: %+v", cs)
	}
}

// TestSignalThresholdsZeroAndRoundTrip asserts SignalThresholds' zero value
// is usable (Target/CooldownNS both 0, SLO zero) and that populated fields
// round-trip.
func TestSignalThresholdsZeroAndRoundTrip(t *testing.T) {
	var zero SignalThresholds
	if zero.Target != 0 || zero.CooldownNS != 0 || zero.SLO != (SLO{}) {
		t.Fatalf("zero SignalThresholds = %+v, want all zero", zero)
	}

	cfg := SignalThresholds{Target: 4, CooldownNS: 30_000_000_000, SLO: SLO{Objective: 0.999, AtRisk: 0.1}}
	if cfg.Target != 4 || cfg.CooldownNS != 30_000_000_000 || cfg.SLO.Objective != 0.999 || cfg.SLO.AtRisk != 0.1 {
		t.Fatalf("SignalThresholds did not round-trip: %+v", cfg)
	}
}

// TestThresholdZeroAndRoundTrip asserts Threshold's zero value and that
// populated fields round-trip.
func TestThresholdZeroAndRoundTrip(t *testing.T) {
	var zero Threshold
	if zero.SplitP99 != 0 || zero.MergeP99 != 0 {
		t.Fatalf("zero Threshold = %+v, want all zero", zero)
	}

	th := Threshold{SplitP99: 200_000_000, MergeP99: 50_000_000}
	if th.SplitP99 != 200_000_000 || th.MergeP99 != 50_000_000 {
		t.Fatalf("Threshold did not round-trip: %+v", th)
	}
}

// TestTopologyZeroAndRoundTrip asserts Topology's zero value and that
// populated fields round-trip.
func TestTopologyZeroAndRoundTrip(t *testing.T) {
	var zero Topology
	if zero.Region != "" || zero.AZ != "" || zero.Rack != "" {
		t.Fatalf("zero Topology = %+v, want all zero", zero)
	}

	top := Topology{Region: "us-east", AZ: "us-east-1a", Rack: "rack-7"}
	if top.Region != "us-east" || top.AZ != "us-east-1a" || top.Rack != "rack-7" {
		t.Fatalf("Topology did not round-trip: %+v", top)
	}
}

// TestLocalityGraphZeroAndRoundTrip asserts a nil Zone (the zero value) is
// treated as "no locality info" and that a populated graph round-trips.
func TestLocalityGraphZeroAndRoundTrip(t *testing.T) {
	var zero LocalityGraph
	if zero.Origin != (Topology{}) || zero.Zone != nil {
		t.Fatalf("zero LocalityGraph = %+v, want Origin zero and Zone nil", zero)
	}

	lg := LocalityGraph{
		Origin: Topology{Region: "us-east", AZ: "us-east-1a", Rack: "rack-1"},
		Zone: map[CellID]Topology{
			"cell-1": {Region: "us-east", AZ: "us-east-1a", Rack: "rack-1"},
			"cell-2": {Region: "us-west", AZ: "us-west-1a", Rack: "rack-3"},
		},
	}
	if lg.Origin.Rack != "rack-1" {
		t.Fatalf("LocalityGraph.Origin did not round-trip: %+v", lg.Origin)
	}
	if len(lg.Zone) != 2 || lg.Zone["cell-2"].Region != "us-west" {
		t.Fatalf("LocalityGraph.Zone did not round-trip: %+v", lg.Zone)
	}
}

// TestRankedZeroAndRoundTrip asserts Ranked's zero value and that populated
// fields round-trip — the exact fields rank() sorts on.
func TestRankedZeroAndRoundTrip(t *testing.T) {
	var zero Ranked
	if zero.Cell != "" || zero.CapMatch != false || zero.Distance != 0 || zero.Free != 0 {
		t.Fatalf("zero Ranked = %+v, want all zero", zero)
	}

	r := Ranked{Cell: "cell-1", CapMatch: true, Distance: 2, Free: 8}
	if r.Cell != "cell-1" || !r.CapMatch || r.Distance != 2 || r.Free != 8 {
		t.Fatalf("Ranked did not round-trip: %+v", r)
	}
}
