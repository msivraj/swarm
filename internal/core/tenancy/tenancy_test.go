package tenancy

import (
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestWithinQuota(t *testing.T) {
	tests := []struct {
		name string
		t    model.Tenant
		u    model.Usage
		q    model.Quota
		want bool
	}{
		{
			name: "all resources under limit",
			t:    model.Tenant{ID: "a"},
			u:    model.Usage{Consumed: model.ResourceVec{"cpu": 0.1, "mem": 0.2}},
			q:    model.Quota{Limit: model.ResourceVec{"cpu": 0.5, "mem": 0.5}},
			want: true,
		},
		{
			name: "one resource over limit fails",
			t:    model.Tenant{ID: "a"},
			u:    model.Usage{Consumed: model.ResourceVec{"cpu": 0.6}},
			q:    model.Quota{Limit: model.ResourceVec{"cpu": 0.5}},
			want: false,
		},
		{
			name: "boundary: exactly at limit is within quota",
			t:    model.Tenant{ID: "a"},
			u:    model.Usage{Consumed: model.ResourceVec{"cpu": 0.5}},
			q:    model.Quota{Limit: model.ResourceVec{"cpu": 0.5}},
			want: true,
		},
		{
			name: "resource absent from Limit is a zero ceiling: any nonzero use fails",
			t:    model.Tenant{ID: "a"},
			u:    model.Usage{Consumed: model.ResourceVec{"gpu": 0.01}},
			q:    model.Quota{Limit: model.ResourceVec{"cpu": 0.5}},
			want: false,
		},
		{
			name: "resource absent from Limit at exactly zero use is within quota",
			t:    model.Tenant{ID: "a"},
			u:    model.Usage{Consumed: model.ResourceVec{"gpu": 0}},
			q:    model.Quota{Limit: model.ResourceVec{"cpu": 0.5}},
			want: true,
		},
		{
			name: "resource absent from Consumed can never fail, regardless of limit",
			t:    model.Tenant{ID: "a"},
			u:    model.Usage{},
			q:    model.Quota{Limit: model.ResourceVec{"cpu": 0.0, "mem": 0.5}},
			want: true,
		},
		{
			name: "empty usage and empty quota is within quota",
			t:    model.Tenant{ID: "a"},
			u:    model.Usage{},
			q:    model.Quota{},
			want: true,
		},
		{
			name: "multi-resource: one over, one under -> fails",
			t:    model.Tenant{ID: "a"},
			u:    model.Usage{Consumed: model.ResourceVec{"cpu": 0.1, "mem": 0.9}},
			q:    model.Quota{Limit: model.ResourceVec{"cpu": 0.5, "mem": 0.5}},
			want: false,
		},
		{
			name: "tenant weight does not affect the quota check",
			t:    model.Tenant{ID: "a", Weight: 10},
			u:    model.Usage{Consumed: model.ResourceVec{"cpu": 0.6}},
			q:    model.Quota{Limit: model.ResourceVec{"cpu": 0.5}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t2 *testing.T) {
			if got := WithinQuota(tt.t, tt.u, tt.q); got != tt.want {
				t2.Fatalf("WithinQuota(%+v, %+v, %+v) = %v, want %v", tt.t, tt.u, tt.q, got, tt.want)
			}
		})
	}
}

// TestOverQuotaRejected is the named property test: for any tenant whose
// projected Usage exceeds Quota on ANY single resource dimension,
// WithinQuota must return false — an over-quota tenant is never admitted.
// It also confirms the strictly-under case admits, at the boundary.
func TestOverQuotaRejected(t *testing.T) {
	limit := model.ResourceVec{"cpu": 0.4, "mem": 0.4, "gpu": 0.4}
	q := model.Quota{Limit: limit}
	tenant := model.Tenant{ID: "t"}

	// Exceed on exactly one resource at a time; every other resource stays
	// comfortably under its own limit. Each case must be rejected.
	over := []model.Usage{
		{Consumed: model.ResourceVec{"cpu": 0.41, "mem": 0.1, "gpu": 0.1}},
		{Consumed: model.ResourceVec{"cpu": 0.1, "mem": 0.41, "gpu": 0.1}},
		{Consumed: model.ResourceVec{"cpu": 0.1, "mem": 0.1, "gpu": 0.41}},
		{Consumed: model.ResourceVec{"cpu": 1.0, "mem": 1.0, "gpu": 1.0}},
	}
	for _, u := range over {
		if WithinQuota(tenant, u, q) {
			t.Fatalf("WithinQuota(_, %+v, %+v) = true, want false (over quota)", u, q)
		}
	}

	under := model.Usage{Consumed: model.ResourceVec{"cpu": 0.39, "mem": 0.39, "gpu": 0.39}}
	if !WithinQuota(tenant, under, q) {
		t.Fatalf("WithinQuota(_, %+v, %+v) = false, want true (under quota)", under, q)
	}

	atLimit := model.Usage{Consumed: model.ResourceVec{"cpu": 0.4, "mem": 0.4, "gpu": 0.4}}
	if !WithinQuota(tenant, atLimit, q) {
		t.Fatalf("WithinQuota(_, %+v, %+v) = false, want true (at limit is within quota)", atLimit, q)
	}
}

func job(id, tenant string) model.JobSpec {
	return model.JobSpec{ID: model.JobID(id), Tenant: model.TenantID(tenant)}
}

func usage(kv ...interface{}) model.Usage {
	rv := model.ResourceVec{}
	for i := 0; i+1 < len(kv); i += 2 {
		rv[model.ResourceKind(kv[i].(string))] = kv[i+1].(float64)
	}
	return model.Usage{Consumed: rv}
}

func TestNextFair(t *testing.T) {
	tests := []struct {
		name    string
		pending []model.JobSpec
		usage   map[model.TenantID]model.Usage
		want    model.JobID
	}{
		{
			name:    "empty pending returns zero JobID",
			pending: nil,
			usage:   map[model.TenantID]model.Usage{},
			want:    "",
		},
		{
			name:    "single tenant returns its earliest pending job",
			pending: []model.JobSpec{job("j1", "a"), job("j2", "a")},
			usage:   map[model.TenantID]model.Usage{},
			want:    "j1",
		},
		{
			name:    "lower dominant share wins",
			pending: []model.JobSpec{job("j1", "a"), job("j2", "b")},
			usage: map[model.TenantID]model.Usage{
				"a": usage("cpu", 0.5),
				"b": usage("cpu", 0.1),
			},
			want: "j2",
		},
		{
			name:    "tenant absent from usage has zero share and is prioritized",
			pending: []model.JobSpec{job("j1", "a"), job("j2", "b")},
			usage: map[model.TenantID]model.Usage{
				"a": usage("cpu", 0.5),
			},
			want: "j2",
		},
		{
			name:    "tie breaks on lowest TenantID",
			pending: []model.JobSpec{job("j1", "z"), job("j2", "a")},
			usage: map[model.TenantID]model.Usage{
				"z": usage("cpu", 0.2),
				"a": usage("cpu", 0.2),
			},
			want: "j2",
		},
		{
			name:    "tie breaks on lowest TenantID regardless of pending order",
			pending: []model.JobSpec{job("j1", "a"), job("j2", "z")},
			usage: map[model.TenantID]model.Usage{
				"z": usage("cpu", 0.2),
				"a": usage("cpu", 0.2),
			},
			want: "j1",
		},
		{
			name:    "untenanted jobs (empty TenantID) are a valid tenant bucket",
			pending: []model.JobSpec{job("j1", "")},
			usage:   map[model.TenantID]model.Usage{},
			want:    "j1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t2 *testing.T) {
			if got := NextFair(tt.pending, tt.usage); got != tt.want {
				t2.Fatalf("NextFair(%+v, %+v) = %v, want %v", tt.pending, tt.usage, got, tt.want)
			}
		})
	}
}

// TestFairAlternates is the headline DRF property test: two equal-weight
// tenants (A and B), each continuously flooding pending, alternate strictly
// A, B, A, B, ... across successive NextFair picks — feeding the picked
// job's Demand back into the winner's Usage after each round. Neither
// tenant starves, and one flooding does not monopolize.
func TestFairAlternates(t *testing.T) {
	const rounds = 20
	demand := model.ResourceVec{"cpu": 0.1}
	usage := map[model.TenantID]model.Usage{
		"a": {Consumed: model.ResourceVec{}},
		"b": {Consumed: model.ResourceVec{}},
	}

	var picks []model.TenantID
	for i := 0; i < rounds; i++ {
		// Both tenants flood: many pending jobs each, every round.
		pending := []model.JobSpec{
			{ID: model.JobID("a-1"), Tenant: "a", Demand: demand},
			{ID: model.JobID("a-2"), Tenant: "a", Demand: demand},
			{ID: model.JobID("a-3"), Tenant: "a", Demand: demand},
			{ID: model.JobID("b-1"), Tenant: "b", Demand: demand},
			{ID: model.JobID("b-2"), Tenant: "b", Demand: demand},
			{ID: model.JobID("b-3"), Tenant: "b", Demand: demand},
		}

		picked := NextFair(pending, usage)
		var winner model.TenantID
		for _, j := range pending {
			if j.ID == picked {
				winner = j.Tenant
				break
			}
		}
		if winner == "" {
			t.Fatalf("round %d: NextFair picked %q, no matching pending job", i, picked)
		}
		picks = append(picks, winner)

		u := usage[winner]
		next := model.ResourceVec{}
		for k, v := range u.Consumed {
			next[k] = v
		}
		for k, v := range demand {
			next[k] += v
		}
		usage[winner] = model.Usage{Consumed: next}
	}

	for i, p := range picks {
		want := model.TenantID("a")
		if i%2 == 1 {
			want = "b"
		}
		if p != want {
			t.Fatalf("pick sequence %v: round %d = %q, want %q (strict alternation)", picks, i, p, want)
		}
	}
}

// TestDominantShare is the DRF-correctness table test: it verifies the
// choice is driven by each tenant's DOMINANT (max) resource share, not by a
// raw sum, and that weighting the usage fed in (the caller pre-dividing raw
// consumption by Tenant.Weight, per NextFair's doc comment) shifts the pick.
func TestDominantShare(t *testing.T) {
	// cpu-heavy tenant's dominant resource is cpu (0.6); mem-heavy tenant's
	// dominant resource is mem (0.5). cpu-heavy's dominant share (0.6) is
	// larger, so mem-heavy is picked, even though summing raw usage
	// (0.6+0.05=0.65 vs 0.1+0.5=0.6) would pick the other way if this were
	// sum-based instead of max-based.
	pending := []model.JobSpec{
		job("cpu-job", "cpu-heavy"),
		job("mem-job", "mem-heavy"),
	}
	rawUsage := map[model.TenantID]model.Usage{
		"cpu-heavy": usage("cpu", 0.6, "mem", 0.05),
		"mem-heavy": usage("cpu", 0.1, "mem", 0.5),
	}
	if got, want := NextFair(pending, rawUsage), model.JobID("mem-job"); got != want {
		t.Fatalf("NextFair (unweighted) = %v, want %v (dominant share must be max, not sum)", got, want)
	}

	// Weighting: give cpu-heavy a much larger Tenant.Weight. Per NextFair's
	// documented convention, the caller pre-divides each tenant's raw
	// Consumed by its Weight before building the usage map (the same
	// shell-side-normalization pattern used for cluster-capacity shares).
	// A high enough weight for cpu-heavy drives its weighted dominant share
	// below mem-heavy's, flipping the pick.
	cpuHeavyTenant := model.Tenant{ID: "cpu-heavy", Weight: 10}
	memHeavyTenant := model.Tenant{ID: "mem-heavy", Weight: 1}
	weightedUsage := map[model.TenantID]model.Usage{
		cpuHeavyTenant.ID: weightAdjust(rawUsage["cpu-heavy"], cpuHeavyTenant.Weight),
		memHeavyTenant.ID: weightAdjust(rawUsage["mem-heavy"], memHeavyTenant.Weight),
	}
	if got, want := NextFair(pending, weightedUsage), model.JobID("cpu-job"); got != want {
		t.Fatalf("NextFair (weighted) = %v, want %v (higher Tenant.Weight should shift the pick)", got, want)
	}
}

// weightAdjust divides a Usage vector's components by weight, mirroring the
// shell-side normalization NextFair's doc comment describes for weighted DRF.
func weightAdjust(u model.Usage, weight float64) model.Usage {
	out := model.ResourceVec{}
	for k, v := range u.Consumed {
		out[k] = v / weight
	}
	return model.Usage{Consumed: out}
}

// TestNextFairDeterministic proves identical (pending, usage) always yields
// an identical pick, including when the usage map is built via different
// insertion orders — Go randomizes map iteration order per-process, so this
// specifically guards against any hidden dependence on that order.
func TestNextFairDeterministic(t *testing.T) {
	pending := []model.JobSpec{
		job("j1", "a"), job("j2", "b"), job("j3", "c"), job("j4", "d"),
	}

	buildA := func() map[model.TenantID]model.Usage {
		m := map[model.TenantID]model.Usage{}
		m["a"] = usage("cpu", 0.3)
		m["b"] = usage("cpu", 0.1)
		m["c"] = usage("cpu", 0.1)
		m["d"] = usage("cpu", 0.2)
		return m
	}
	buildB := func() map[model.TenantID]model.Usage {
		m := map[model.TenantID]model.Usage{}
		m["d"] = usage("cpu", 0.2)
		m["c"] = usage("cpu", 0.1)
		m["b"] = usage("cpu", 0.1)
		m["a"] = usage("cpu", 0.3)
		return m
	}

	first := NextFair(pending, buildA())
	for i := 0; i < 50; i++ {
		if got := NextFair(pending, buildA()); got != first {
			t.Fatalf("non-deterministic output on run %d (buildA): %v vs %v", i, got, first)
		}
		if got := NextFair(pending, buildB()); got != first {
			t.Fatalf("non-deterministic output on run %d (buildB, different insertion order): %v vs %v", i, got, first)
		}
	}
}

func TestScope(t *testing.T) {
	tests := []struct {
		name string
		job  model.JobSpec
		want model.TenantScope
	}{
		{
			name: "tenanted job scopes to its tenant",
			job:  model.JobSpec{ID: "j1", Tenant: "acme"},
			want: model.TenantScope{Tenant: "acme"},
		},
		{
			name: "untenanted job (zero value Tenant) scopes to the zero TenantScope",
			job:  model.JobSpec{ID: "j2"},
			want: model.TenantScope{Tenant: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t2 *testing.T) {
			if got := Scope(tt.job); got != tt.want {
				t2.Fatalf("Scope(%+v) = %+v, want %+v", tt.job, got, tt.want)
			}
		})
	}
}
