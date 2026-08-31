package model

import "testing"

// TestVerdictKindOrder pins the iota order of the VerdictKind constants and
// the zero-value contract: Insufficient (undecided) must be zero so a
// zero-value Verdict never reads as an accepted result.
func TestVerdictKindOrder(t *testing.T) {
	tests := []struct {
		name string
		got  VerdictKind
		want VerdictKind
	}{
		{"Insufficient", Insufficient, 0},
		{"Disputed", Disputed, 1},
		{"Agreed", Agreed, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero Verdict
	if zero.Kind != Insufficient {
		t.Fatalf("zero Verdict.Kind = %d, want Insufficient (%d)", zero.Kind, Insufficient)
	}
	if zero.Value != nil {
		t.Fatalf("zero Verdict.Value = %v, want nil", zero.Value)
	}
}

// TestProbeOrder pins the iota order of the Probe constants: Match must be
// zero per the ticket's documented ordering.
func TestProbeOrder(t *testing.T) {
	tests := []struct {
		name string
		got  Probe
		want Probe
	}{
		{"Match", Match, 0},
		{"Lie", Lie, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero Probe
	if zero != Match {
		t.Fatalf("zero Probe = %d, want Match (%d)", zero, Match)
	}
}

// TestActionZeroIsNoAction asserts the zero value of Action is inert: an
// uninitialized Action must never blacklist an identity.
func TestActionZeroIsNoAction(t *testing.T) {
	tests := []struct {
		name string
		got  ActionKind
		want ActionKind
	}{
		{"NoAction", NoAction, 0},
		{"Blacklist", Blacklist, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero Action
	if zero.Kind != NoAction {
		t.Fatalf("zero Action.Kind = %d, want NoAction (%d)", zero.Kind, NoAction)
	}
	if zero.ID != "" {
		t.Fatalf("zero Action.ID = %q, want empty", zero.ID)
	}
}

// TestAdmitZeroIsReject asserts the zero value of Admit is Reject: an
// uninitialized Admit must never grant an identity.
func TestAdmitZeroIsReject(t *testing.T) {
	tests := []struct {
		name string
		got  AdmitKind
		want AdmitKind
	}{
		{"Reject", Reject, 0},
		{"Accept", Accept, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero Admit
	if zero.Kind != Reject {
		t.Fatalf("zero Admit.Kind = %d, want Reject (%d)", zero.Kind, Reject)
	}
	if zero.ID != "" {
		t.Fatalf("zero Admit.ID = %q, want empty", zero.ID)
	}
}

// TestReputationZeroIsUntrusted documents the zero-start property: a fresh
// Reputation must be the lowest-trust value (no score, no observations) so
// faking many identities buys no shortcut.
func TestReputationZeroIsUntrusted(t *testing.T) {
	var zero Reputation
	if zero.Score != 0 {
		t.Fatalf("zero Reputation.Score = %d, want 0 (brand-new/untrusted)", zero.Score)
	}
	if zero.Observations != 0 {
		t.Fatalf("zero Reputation.Observations = %d, want 0 (brand-new/untrusted)", zero.Observations)
	}

	honest := Reputation{Score: 5, Observations: 3}
	if honest.Score <= zero.Score {
		t.Fatalf("honest.Score = %d, want > zero.Score (%d)", honest.Score, zero.Score)
	}

	lied := Reputation{Score: -5, Observations: 3}
	if lied.Score >= zero.Score {
		t.Fatalf("lied.Score = %d, want < zero.Score (%d)", lied.Score, zero.Score)
	}
}

// TestWasiCapsZeroGrantsNothing asserts the zero value of WasiCaps is the
// least-privilege default: no paths, no env, no stdio, no clock.
func TestWasiCapsZeroGrantsNothing(t *testing.T) {
	var zero WasiCaps
	if zero.ReadPaths != nil || zero.WritePaths != nil || zero.Env != nil {
		t.Fatalf("zero WasiCaps paths/env = %+v, want all nil", zero)
	}
	if zero.Stdio || zero.Clock {
		t.Fatalf("zero WasiCaps = %+v, want Stdio=false, Clock=false", zero)
	}
}

// TestZeroValuesRoundTrip asserts each new boundary type's zero value is
// usable and that fields round-trip once populated.
func TestZeroValuesRoundTrip(t *testing.T) {
	t.Run("Result", func(t *testing.T) {
		var zero Result
		if zero.ID != "" || zero.Value != nil || zero.OK != false {
			t.Fatalf("zero Result = %+v, want all zero", zero)
		}
		r := Result{ID: "spiffe://open/1", Value: []byte("out"), OK: true}
		if r.ID != "spiffe://open/1" || string(r.Value) != "out" || !r.OK {
			t.Fatalf("Result did not round-trip: %+v", r)
		}
	})

	t.Run("WasiCaps", func(t *testing.T) {
		wc := WasiCaps{
			ReadPaths:  []string{"/data"},
			WritePaths: []string{"/tmp"},
			Env:        []string{"PATH"},
			Stdio:      true,
			Clock:      true,
		}
		if len(wc.ReadPaths) != 1 || wc.ReadPaths[0] != "/data" {
			t.Fatalf("WasiCaps.ReadPaths did not round-trip: %+v", wc)
		}
		if len(wc.WritePaths) != 1 || wc.WritePaths[0] != "/tmp" {
			t.Fatalf("WasiCaps.WritePaths did not round-trip: %+v", wc)
		}
		if len(wc.Env) != 1 || wc.Env[0] != "PATH" {
			t.Fatalf("WasiCaps.Env did not round-trip: %+v", wc)
		}
		if !wc.Stdio || !wc.Clock {
			t.Fatalf("WasiCaps.Stdio/Clock did not round-trip: %+v", wc)
		}
	})

	t.Run("JoinReq/PowProof/PowCfg", func(t *testing.T) {
		jr := JoinReq{PubKey: PubKey("key"), Nonce: []byte("n1")}
		if string(jr.PubKey) != "key" || string(jr.Nonce) != "n1" {
			t.Fatalf("JoinReq did not round-trip: %+v", jr)
		}

		pp := PowProof{Nonce: []byte("n1"), Solution: []byte("sol")}
		if string(pp.Nonce) != "n1" || string(pp.Solution) != "sol" {
			t.Fatalf("PowProof did not round-trip: %+v", pp)
		}

		var zeroCfg PowCfg
		if zeroCfg.DifficultyBits != 0 {
			t.Fatalf("zero PowCfg.DifficultyBits = %d, want 0", zeroCfg.DifficultyBits)
		}
		cfg := PowCfg{DifficultyBits: 20}
		if cfg.DifficultyBits != 20 {
			t.Fatalf("PowCfg did not round-trip: %+v", cfg)
		}
	})

	t.Run("Sig/PubKey/MachineID/SpiffeID", func(t *testing.T) {
		var id MachineID = "machine-1"
		var sp SpiffeID = "spiffe://open/1"
		sig := Sig([]byte{0x01, 0x02})
		key := PubKey([]byte{0x03, 0x04})
		if id != "machine-1" || sp != "spiffe://open/1" {
			t.Fatalf("MachineID/SpiffeID did not round-trip: %q %q", id, sp)
		}
		if len(sig) != 2 || len(key) != 2 {
			t.Fatalf("Sig/PubKey did not round-trip: %v %v", sig, key)
		}
	})
}

// TestJobSpecTierRoundTrip asserts the new JobSpec.Tier field defaults to
// Core (existing P0-P2 behavior) and round-trips once populated.
func TestJobSpecTierRoundTrip(t *testing.T) {
	var zero JobSpec
	if zero.Tier != Core {
		t.Fatalf("zero JobSpec.Tier = %d, want Core (%d)", zero.Tier, Core)
	}

	js := JobSpec{ID: "job-1", Tier: Open}
	if js.Tier != Open {
		t.Fatalf("JobSpec.Tier did not round-trip: %+v", js)
	}
}
