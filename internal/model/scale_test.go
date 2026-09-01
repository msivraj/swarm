package model

import "testing"

// TestLevelOrder pins the iota order of the Level constants: the rollup
// tree walks LevelCell -> LevelRegion -> LevelGlobal in this exact order,
// and LevelCell being zero means an uninitialized Level reads as the
// finest-grained, most conservative tier.
func TestLevelOrder(t *testing.T) {
	tests := []struct {
		name string
		got  Level
		want Level
	}{
		{"LevelCell", LevelCell, 0},
		{"LevelRegion", LevelRegion, 1},
		{"LevelGlobal", LevelGlobal, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero Level
	if zero != LevelCell {
		t.Fatalf("zero Level = %d, want LevelCell (%d)", zero, LevelCell)
	}
}

// TestLoadDecisionKindOrder pins the iota order of the LoadDecisionKind
// constants and the zero-value contract: Shed must be zero so an
// uninitialized LoadDecision never silently admits under load.
func TestLoadDecisionKindOrder(t *testing.T) {
	tests := []struct {
		name string
		got  LoadDecisionKind
		want LoadDecisionKind
	}{
		{"Shed", Shed, 0},
		{"AdmitLoad", AdmitLoad, 1},
		{"Throttle", Throttle, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero LoadDecision
	if zero.Kind != Shed {
		t.Fatalf("zero LoadDecision.Kind = %d, want Shed (%d)", zero.Kind, Shed)
	}
	if zero.Delay != 0 {
		t.Fatalf("zero LoadDecision.Delay = %d, want 0", zero.Delay)
	}
}

// TestDrainStepKindOrder pins the iota order of the DrainStepKind
// constants and the zero-value contract: Done must be zero so a
// zero-value/nil plan drains nothing rather than cordoning a cell by
// accident.
func TestDrainStepKindOrder(t *testing.T) {
	tests := []struct {
		name string
		got  DrainStepKind
		want DrainStepKind
	}{
		{"Done", Done, 0},
		{"Cordon", Cordon, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero DrainStep
	if zero.Kind != Done {
		t.Fatalf("zero DrainStep.Kind = %d, want Done (%d)", zero.Kind, Done)
	}
	if zero.Cell != "" {
		t.Fatalf("zero DrainStep.Cell = %q, want empty", zero.Cell)
	}
}

// TestShardIDAndKeyZeroAndRoundTrip documents the zero-value semantics of
// Key and ShardID (both plain, comparable data) and that they round-trip.
func TestShardIDAndKeyZeroAndRoundTrip(t *testing.T) {
	var zeroKey Key
	if zeroKey != "" {
		t.Fatalf("zero Key = %q, want empty", zeroKey)
	}
	var zeroShard ShardID
	if zeroShard != 0 {
		t.Fatalf("zero ShardID = %d, want 0", zeroShard)
	}

	k := Key("cell/1")
	s := ShardID(42)
	if k != "cell/1" || s != 42 {
		t.Fatalf("Key/ShardID did not round-trip: %q %d", k, s)
	}
}

// TestCardinalityZeroAndRoundTrip asserts Cardinality's zero value means no
// budget, and that it round-trips once populated.
func TestCardinalityZeroAndRoundTrip(t *testing.T) {
	var zero Cardinality
	if zero != 0 {
		t.Fatalf("zero Cardinality = %d, want 0", zero)
	}

	c := Cardinality(1000)
	if c != 1000 {
		t.Fatalf("Cardinality did not round-trip: %d", c)
	}
}

// TestMetricsZeroAndRoundTrip asserts the zero value of each metrics tier
// is usable (an empty rollup) and that fields round-trip once populated,
// documenting the combine rules each field carries.
func TestMetricsZeroAndRoundTrip(t *testing.T) {
	t.Run("CellMetrics", func(t *testing.T) {
		var zero CellMetrics
		if zero.Cell != "" || zero.Count != 0 || zero.Gauge != 0 || zero.Samples != 0 {
			t.Fatalf("zero CellMetrics = %+v, want all zero", zero)
		}
		cm := CellMetrics{Cell: "cell-1", Count: 10, Gauge: 2.5, Samples: 4}
		if cm.Cell != "cell-1" || cm.Count != 10 || cm.Gauge != 2.5 || cm.Samples != 4 {
			t.Fatalf("CellMetrics did not round-trip: %+v", cm)
		}
	})

	t.Run("RegionMetrics", func(t *testing.T) {
		var zero RegionMetrics
		if zero.Region != "" || zero.Count != 0 || zero.Gauge != 0 || zero.Samples != 0 {
			t.Fatalf("zero RegionMetrics = %+v, want all zero", zero)
		}
		rm := RegionMetrics{Region: "region-1", Count: 20, Gauge: 3.5, Samples: 8}
		if rm.Region != "region-1" || rm.Count != 20 || rm.Gauge != 3.5 || rm.Samples != 8 {
			t.Fatalf("RegionMetrics did not round-trip: %+v", rm)
		}
	})

	t.Run("GlobalMetrics", func(t *testing.T) {
		var zero GlobalMetrics
		if zero.Count != 0 || zero.Gauge != 0 || zero.Samples != 0 {
			t.Fatalf("zero GlobalMetrics = %+v, want all zero", zero)
		}
		gm := GlobalMetrics{Count: 30, Gauge: 4.5, Samples: 12}
		if gm.Count != 30 || gm.Gauge != 4.5 || gm.Samples != 12 {
			t.Fatalf("GlobalMetrics did not round-trip: %+v", gm)
		}
	})
}

// TestReqZeroPriorityIsLowest asserts a zero-value Req claims no special
// priority — it is the lowest, not an accidental high priority.
func TestReqZeroPriorityIsLowest(t *testing.T) {
	var zero Req
	if zero.Priority != 0 {
		t.Fatalf("zero Req.Priority = %d, want 0", zero.Priority)
	}

	r := Req{Priority: 5}
	if r.Priority != 5 {
		t.Fatalf("Req did not round-trip: %+v", r)
	}
}

// TestLoadStateAndLimitsAndLoadEventRoundTrip asserts the backpressure data
// types round-trip and their zero values are the empty/unconfigured case.
func TestLoadStateAndLimitsAndLoadEventRoundTrip(t *testing.T) {
	t.Run("LoadState", func(t *testing.T) {
		var zero LoadState
		if zero.InFlight != 0 || zero.QueueDepth != 0 {
			t.Fatalf("zero LoadState = %+v, want all zero", zero)
		}
		ls := LoadState{InFlight: 90, QueueDepth: 10}
		if ls.InFlight != 90 || ls.QueueDepth != 10 {
			t.Fatalf("LoadState did not round-trip: %+v", ls)
		}
	})

	t.Run("Limits", func(t *testing.T) {
		var zero Limits
		if zero.Capacity != 0 || zero.ShedThreshold != 0 {
			t.Fatalf("zero Limits = %+v, want all zero", zero)
		}
		lim := Limits{Capacity: 100, ShedThreshold: 0.95}
		if lim.Capacity != 100 || lim.ShedThreshold != 0.95 {
			t.Fatalf("Limits did not round-trip: %+v", lim)
		}
	})

	t.Run("LoadEvent", func(t *testing.T) {
		var zero LoadEvent
		if zero.InFlightDelta != 0 || zero.QueueDelta != 0 {
			t.Fatalf("zero LoadEvent = %+v, want all zero", zero)
		}
		ev := LoadEvent{InFlightDelta: 1, QueueDelta: -1}
		if ev.InFlightDelta != 1 || ev.QueueDelta != -1 {
			t.Fatalf("LoadEvent did not round-trip: %+v", ev)
		}
	})
}

// TestVersionZeroAndRoundTrip asserts Version's zero value is usable and
// that it round-trips.
func TestVersionZeroAndRoundTrip(t *testing.T) {
	var zero Version
	if zero.Major != 0 || zero.Minor != 0 {
		t.Fatalf("zero Version = %+v, want all zero", zero)
	}

	v := Version{Major: 2, Minor: 3}
	if v.Major != 2 || v.Minor != 3 {
		t.Fatalf("Version did not round-trip: %+v", v)
	}
}

// TestFleetStateAndUpgradePlanZeroAndRoundTrip asserts the rolling-upgrade
// data types compose existing CellID/JobID and round-trip.
func TestFleetStateAndUpgradePlanZeroAndRoundTrip(t *testing.T) {
	t.Run("FleetState", func(t *testing.T) {
		var zero FleetState
		if zero.Versions != nil || zero.Jobs != nil || zero.Cordoned != nil {
			t.Fatalf("zero FleetState = %+v, want all nil", zero)
		}
		fs := FleetState{
			Versions: map[CellID]Version{"cell-1": {Major: 1, Minor: 0}},
			Jobs:     map[CellID][]JobID{"cell-1": {"job-1", "job-2"}},
			Cordoned: map[CellID]bool{"cell-2": true},
		}
		if fs.Versions["cell-1"] != (Version{Major: 1, Minor: 0}) {
			t.Fatalf("FleetState.Versions did not round-trip: %+v", fs.Versions)
		}
		if len(fs.Jobs["cell-1"]) != 2 || fs.Jobs["cell-1"][0] != "job-1" {
			t.Fatalf("FleetState.Jobs did not round-trip: %+v", fs.Jobs)
		}
		if !fs.Cordoned["cell-2"] {
			t.Fatalf("FleetState.Cordoned did not round-trip: %+v", fs.Cordoned)
		}
	})

	t.Run("UpgradePlan", func(t *testing.T) {
		var zero UpgradePlan
		if zero.Target != (Version{}) || zero.Order != nil {
			t.Fatalf("zero UpgradePlan = %+v, want all zero", zero)
		}
		up := UpgradePlan{Target: Version{Major: 2}, Order: []CellID{"cell-1", "cell-2"}}
		if up.Target.Major != 2 || len(up.Order) != 2 || up.Order[0] != "cell-1" {
			t.Fatalf("UpgradePlan did not round-trip: %+v", up)
		}
	})
}
