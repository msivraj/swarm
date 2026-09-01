package model

import "testing"

// TestTenancyZeroAndRoundTrip asserts the P5 tenancy types' zero values are
// the safe/untagged case and that fields round-trip once populated.
func TestTenancyZeroAndRoundTrip(t *testing.T) {
	t.Run("TenantID", func(t *testing.T) {
		var zero TenantID
		if zero != "" {
			t.Fatalf("zero TenantID = %q, want empty (the default/untagged tenant)", zero)
		}
	})

	t.Run("ResourceVec", func(t *testing.T) {
		var zero ResourceVec
		if zero != nil {
			t.Fatalf("zero ResourceVec = %v, want nil", zero)
		}
		rv := ResourceVec{"cpu": 0.25, "mem": 0.5}
		if rv["cpu"] != 0.25 || rv["mem"] != 0.5 {
			t.Fatalf("ResourceVec did not round-trip: %+v", rv)
		}
	})

	t.Run("Tenant", func(t *testing.T) {
		var zero Tenant
		if zero.ID != "" || zero.Weight != 0 {
			t.Fatalf("zero Tenant = %+v, want all zero", zero)
		}
		tn := Tenant{ID: "acme", Weight: 2}
		if tn.ID != "acme" || tn.Weight != 2 {
			t.Fatalf("Tenant did not round-trip: %+v", tn)
		}
	})

	t.Run("Usage", func(t *testing.T) {
		var zero Usage
		if zero.Consumed != nil {
			t.Fatalf("zero Usage = %+v, want nil Consumed", zero)
		}
		u := Usage{Consumed: ResourceVec{"cpu": 0.1}}
		if u.Consumed["cpu"] != 0.1 {
			t.Fatalf("Usage did not round-trip: %+v", u)
		}
	})

	t.Run("Quota", func(t *testing.T) {
		var zero Quota
		if zero.Limit != nil {
			t.Fatalf("zero Quota = %+v, want nil Limit", zero)
		}
		q := Quota{Limit: ResourceVec{"cpu": 0.5}}
		if q.Limit["cpu"] != 0.5 {
			t.Fatalf("Quota did not round-trip: %+v", q)
		}
	})

	t.Run("TenantScope", func(t *testing.T) {
		var zero TenantScope
		if zero.Tenant != "" {
			t.Fatalf("zero TenantScope = %+v, want empty Tenant", zero)
		}
		ts := TenantScope{Tenant: "acme"}
		if ts.Tenant != "acme" {
			t.Fatalf("TenantScope did not round-trip: %+v", ts)
		}
	})
}

// TestJobSpecTenantFieldsUnchangedZeroValue asserts JobSpec's new Tenant and
// Demand fields default to the untenanted/no-demand case, so a zero-value or
// pre-P5 JobSpec (which never sets them) behaves identically to before.
func TestJobSpecTenantFieldsUnchangedZeroValue(t *testing.T) {
	var zero JobSpec
	if zero.Tenant != "" {
		t.Fatalf("zero JobSpec.Tenant = %q, want empty (default tenant)", zero.Tenant)
	}
	if zero.Demand != nil {
		t.Fatalf("zero JobSpec.Demand = %v, want nil (no demand)", zero.Demand)
	}
	// existing P0-P4 fields still zero, confirming the addition is additive
	if zero.ID != "" || zero.Template != "" || zero.Coupling != Independent || zero.MinMembers != 0 || zero.Tier != Core {
		t.Fatalf("zero JobSpec pre-P5 fields changed: %+v", zero)
	}

	js := JobSpec{ID: "job-1", Tenant: "acme", Demand: ResourceVec{"gpu": 0.2}}
	if js.Tenant != "acme" || js.Demand["gpu"] != 0.2 {
		t.Fatalf("JobSpec.Tenant/Demand did not round-trip: %+v", js)
	}
}
