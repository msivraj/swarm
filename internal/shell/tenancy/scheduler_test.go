package tenancy

import (
	"context"
	"sync"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func job(id model.JobID, tenant model.TenantID, demand model.ResourceVec) model.JobSpec {
	return model.JobSpec{ID: id, Tenant: tenant, Demand: demand}
}

// TestFairAlternationEndToEnd is the headline fairness property, driven
// through the real NextFair core with usage fed back after every pick: two
// equal-weight tenants both flood pending submissions, and the resulting
// dispatch sequence must ALTERNATE — the flooder cannot monopolize the
// scheduler.
func TestFairAlternationEndToEnd(t *testing.T) {
	placer := NewFakePlacer()
	sched := New(Config{
		ClusterCapacity: model.ResourceVec{"cpu": 100},
		Tenants: map[model.TenantID]model.Tenant{
			"a": {ID: "a", Weight: 1},
			"b": {ID: "b", Weight: 1},
		},
		Quotas: map[model.TenantID]model.Quota{
			"a": {Limit: model.ResourceVec{"cpu": 1}},
			"b": {Limit: model.ResourceVec{"cpu": 1}},
		},
		Placer: placer,
	})

	// A floods 6 jobs up front; B submits none yet.
	for i := 0; i < 6; i++ {
		id := model.JobID("a" + string(rune('0'+i)))
		if got := sched.Submit(job(id, "a", model.ResourceVec{"cpu": 1})); got != Admitted {
			t.Fatalf("submit %s: got %v, want Admitted", id, got)
		}
	}
	// B submits one job per tick, alongside A's flood, to prove B is never
	// starved out even though A always has more pending work available.
	bJobs := []model.JobID{"b0", "b1", "b2", "b3", "b4", "b5"}

	var got []model.TenantID
	for i := 0; i < 6; i++ {
		if got2 := sched.Submit(job(bJobs[i], "b", model.ResourceVec{"cpu": 1})); got2 != Admitted {
			t.Fatalf("submit %s: got %v, want Admitted", bJobs[i], got2)
		}
		id, err := sched.DispatchNext(context.Background())
		if err != nil {
			t.Fatalf("DispatchNext: %v", err)
		}
		if id == "" {
			t.Fatalf("DispatchNext returned no job on tick %d", i)
		}
		got = append(got, tenantOf(id))
	}

	for i, tenant := range got {
		want := model.TenantID("a")
		if i%2 == 1 {
			want = "b"
		}
		if tenant != want {
			t.Fatalf("dispatch sequence %v: pick %d = %q, want %q (must alternate)", got, i, tenant, want)
		}
	}
}

// tenantOf recovers a job's tenant from its ID, since test job IDs are
// prefixed with their tenant ("a0", "b3", ...).
func tenantOf(id model.JobID) model.TenantID {
	return model.TenantID(string(id)[:1])
}

// TestOverQuotaRejected asserts a tenant whose demand would exceed its
// quota is rejected/queued and NEVER dispatched — through the real
// WithinQuota on shell-normalized shares.
func TestOverQuotaRejected(t *testing.T) {
	placer := NewFakePlacer()
	sched := New(Config{
		ClusterCapacity: model.ResourceVec{"cpu": 100, "mem": 100},
		Tenants:         map[model.TenantID]model.Tenant{"a": {ID: "a", Weight: 1}},
		Quotas:          map[model.TenantID]model.Quota{"a": {Limit: model.ResourceVec{"cpu": 0.4}}},
		Placer:          placer,
	})

	// 60 of 100 cpu -> share 0.6, over the 0.4 limit.
	if got := sched.Submit(job("j1", "a", model.ResourceVec{"cpu": 60})); got != Rejected {
		t.Fatalf("over-quota submit: got %v, want Rejected", got)
	}
	if sched.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0 (rejected job must never be queued)", sched.Pending())
	}

	id, err := sched.DispatchNext(context.Background())
	if err != nil {
		t.Fatalf("DispatchNext: %v", err)
	}
	if id != "" {
		t.Fatalf("DispatchNext dispatched %q, want nothing (rejected job never runs)", id)
	}
	if calls := placer.Calls(); len(calls) != 0 {
		t.Fatalf("Placer.Calls() = %v, want none (rejected job never reaches placement)", calls)
	}

	// An undeclared resource (not in ClusterCapacity or the tenant's Quota
	// at all) at any positive demand must also be rejected.
	if got := sched.Submit(job("j2", "a", model.ResourceVec{"gpu": 1})); got != Rejected {
		t.Fatalf("undeclared-resource submit: got %v, want Rejected", got)
	}
	if sched.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0 after undeclared-resource rejection", sched.Pending())
	}

	// A within-quota job on a properly declared/capacitated resource is
	// still admitted, confirming the rejections above are real, not a
	// blanket failure.
	if got := sched.Submit(job("j3", "a", model.ResourceVec{"cpu": 10})); got != Admitted {
		t.Fatalf("within-quota submit: got %v, want Admitted", got)
	}
}

// TestScopedDispatchIsolation asserts a dispatched job lands under its
// tenant's Scope — tenant A's jobs are placed only under A's scope, isolated
// from tenant B's, whose jobs are placed only under B's scope.
func TestScopedDispatchIsolation(t *testing.T) {
	placer := NewFakePlacer()
	sched := New(Config{
		ClusterCapacity: model.ResourceVec{"cpu": 100},
		Tenants: map[model.TenantID]model.Tenant{
			"a": {ID: "a", Weight: 1},
			"b": {ID: "b", Weight: 1},
		},
		Quotas: map[model.TenantID]model.Quota{
			"a": {Limit: model.ResourceVec{"cpu": 1}},
			"b": {Limit: model.ResourceVec{"cpu": 1}},
		},
		Placer: placer,
	})

	for _, id := range []model.JobID{"a0", "a1", "a2"} {
		if got := sched.Submit(job(id, "a", model.ResourceVec{"cpu": 1})); got != Admitted {
			t.Fatalf("submit %s: got %v, want Admitted", id, got)
		}
	}
	for _, id := range []model.JobID{"b0", "b1"} {
		if got := sched.Submit(job(id, "b", model.ResourceVec{"cpu": 1})); got != Admitted {
			t.Fatalf("submit %s: got %v, want Admitted", id, got)
		}
	}

	for sched.Pending() > 0 {
		if _, err := sched.DispatchNext(context.Background()); err != nil {
			t.Fatalf("DispatchNext: %v", err)
		}
	}

	for _, placed := range placer.Calls() {
		want := model.TenantScope{Tenant: placed.Job.Tenant}
		if placed.Scope != want {
			t.Fatalf("job %s scope = %+v, want %+v", placed.Job.ID, placed.Scope, want)
		}
		if placed.Job.Tenant == "a" && placed.Scope.Tenant == "b" {
			t.Fatalf("tenant a's job %s landed under tenant b's scope", placed.Job.ID)
		}
		if placed.Job.Tenant == "b" && placed.Scope.Tenant == "a" {
			t.Fatalf("tenant b's job %s landed under tenant a's scope", placed.Job.ID)
		}
	}
}

// TestWeightingShiftsFairShare asserts raising one tenant's Weight shifts
// the fair share toward it: with A at weight 4 and B at weight 1, both with
// a deep backlog of pending work (so neither ever starves for lack of
// supply), A's dispatch share converges to its 4:1 weight ratio over B —
// not the 1:1 an equal-weight tenant would get (TestFairAlternationEndToEnd).
// The scheduler is entirely deterministic, so this asserts the EXACT
// dispatch split DRF produces for this fixed scenario, not just a direction.
func TestWeightingShiftsFairShare(t *testing.T) {
	placer := NewFakePlacer()
	sched := New(Config{
		ClusterCapacity: model.ResourceVec{"cpu": 1000},
		Tenants: map[model.TenantID]model.Tenant{
			"a": {ID: "a", Weight: 4},
			"b": {ID: "b", Weight: 1},
		},
		Quotas: map[model.TenantID]model.Quota{
			"a": {Limit: model.ResourceVec{"cpu": 1}},
			"b": {Limit: model.ResourceVec{"cpu": 1}},
		},
		Placer: placer,
	})

	const backlog = 100
	for i := 0; i < backlog; i++ {
		aID := model.JobID("a" + itoa(i))
		bID := model.JobID("b" + itoa(i))
		if got := sched.Submit(job(aID, "a", model.ResourceVec{"cpu": 1})); got != Admitted {
			t.Fatalf("submit %s: got %v, want Admitted", aID, got)
		}
		if got := sched.Submit(job(bID, "b", model.ResourceVec{"cpu": 1})); got != Admitted {
			t.Fatalf("submit %s: got %v, want Admitted", bID, got)
		}
	}

	const dispatches = 50
	aWins, bWins := 0, 0
	for i := 0; i < dispatches; i++ {
		id, err := sched.DispatchNext(context.Background())
		if err != nil {
			t.Fatalf("DispatchNext: %v", err)
		}
		if tenantOf(id) == "a" {
			aWins++
		} else {
			bWins++
		}
	}

	if aWins != 40 || bWins != 10 {
		t.Fatalf("weight 4:1 split over %d dispatches = a:%d b:%d, want a:40 b:10 (the 4:1 weight ratio)", dispatches, aWins, bWins)
	}
}

// itoa is a tiny decimal formatter, avoiding a strconv import for a helper
// this small.
func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return itoa(n/10) + string(rune('0'+n%10))
}

// TestDispatchNextRequeuesOnPlacerFailure asserts a Placer error does not
// lose the job: it is requeued for a later DispatchNext rather than
// silently dropped, and usage is not charged for a dispatch that failed.
func TestDispatchNextRequeuesOnPlacerFailure(t *testing.T) {
	placer := NewFakePlacer()
	sched := New(Config{
		ClusterCapacity: model.ResourceVec{"cpu": 100},
		Tenants:         map[model.TenantID]model.Tenant{"a": {ID: "a", Weight: 1}},
		Quotas:          map[model.TenantID]model.Quota{"a": {Limit: model.ResourceVec{"cpu": 1}}},
		Placer:          placer,
	})

	if got := sched.Submit(job("j1", "a", model.ResourceVec{"cpu": 1})); got != Admitted {
		t.Fatalf("submit: got %v, want Admitted", got)
	}

	placer.FailNext(model.TenantScope{Tenant: "a"}, errBoom)
	if _, err := sched.DispatchNext(context.Background()); err == nil {
		t.Fatal("DispatchNext: want error from failing placer")
	}
	if sched.Pending() != 1 {
		t.Fatalf("Pending() = %d after failed dispatch, want 1 (job must be requeued)", sched.Pending())
	}
	if got := sched.UsageOf("a").Consumed["cpu"]; got != 0 {
		t.Fatalf("usage after failed dispatch = %v, want 0 (not charged for a failed placement)", got)
	}

	id, err := sched.DispatchNext(context.Background())
	if err != nil {
		t.Fatalf("retry DispatchNext: %v", err)
	}
	if id != "j1" {
		t.Fatalf("retry DispatchNext = %q, want j1", id)
	}
	if got := sched.UsageOf("a").Consumed["cpu"]; got != 0.01 {
		t.Fatalf("usage after successful dispatch = %v, want 0.01", got)
	}
}

var errBoom = &testError{"tenancy: boom"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

// TestRelease asserts Release gives back usage a completed job no longer
// holds, floored at 0.
func TestRelease(t *testing.T) {
	sched := New(Config{Placer: NewFakePlacer()})
	sched.usage["a"] = model.Usage{Consumed: model.ResourceVec{"cpu": 0.5}}

	sched.Release("a", model.ResourceVec{"cpu": 0.2})
	if got := sched.UsageOf("a").Consumed["cpu"]; got != 0.3 {
		t.Fatalf("usage after Release = %v, want 0.3", got)
	}

	sched.Release("a", model.ResourceVec{"cpu": 100})
	if got := sched.UsageOf("a").Consumed["cpu"]; got != 0 {
		t.Fatalf("usage after over-releasing = %v, want 0 (floored)", got)
	}
}

// TestUnregisteredTenantDefaults asserts a tenant with no Config.Tenants
// entry still works end to end: it defaults to fair-share weight 1 and is
// admitted/dispatched normally under its own scope, given a quota.
func TestUnregisteredTenantDefaults(t *testing.T) {
	placer := NewFakePlacer()
	sched := New(Config{
		ClusterCapacity: model.ResourceVec{"cpu": 100},
		Quotas:          map[model.TenantID]model.Quota{"z": {Limit: model.ResourceVec{"cpu": 1}}},
		Placer:          placer,
	})

	if got := sched.Submit(job("j1", "z", model.ResourceVec{"cpu": 1})); got != Admitted {
		t.Fatalf("submit for unregistered tenant: got %v, want Admitted", got)
	}
	id, err := sched.DispatchNext(context.Background())
	if err != nil {
		t.Fatalf("DispatchNext: %v", err)
	}
	if id != "j1" {
		t.Fatalf("DispatchNext = %q, want j1", id)
	}
	calls := placer.Calls()
	if len(calls) != 1 || calls[0].Scope != (model.TenantScope{Tenant: "z"}) {
		t.Fatalf("Calls() = %+v, want one call scoped to tenant z", calls)
	}
}

// TestFakePlacerRefusesEmptyID asserts FakePlacer.Place rejects a job with
// an empty JobID outright, independent of any configured failure.
func TestFakePlacerRefusesEmptyID(t *testing.T) {
	placer := NewFakePlacer()
	if err := placer.Place(context.Background(), model.TenantScope{Tenant: "a"}, model.JobSpec{}); err == nil {
		t.Fatal("Place with empty JobID: want error")
	}
	if calls := placer.Calls(); len(calls) != 0 {
		t.Fatalf("Calls() = %v, want none", calls)
	}
}

// TestConcurrentSubmitAndDispatch exercises Submit/DispatchNext/Release
// concurrently from many goroutines — meant to be run with -race.
func TestConcurrentSubmitAndDispatch(t *testing.T) {
	placer := NewFakePlacer()
	sched := New(Config{
		ClusterCapacity: model.ResourceVec{"cpu": 10000},
		Tenants: map[model.TenantID]model.Tenant{
			"a": {ID: "a", Weight: 1},
			"b": {ID: "b", Weight: 2},
		},
		Quotas: map[model.TenantID]model.Quota{
			"a": {Limit: model.ResourceVec{"cpu": 1}},
			"b": {Limit: model.ResourceVec{"cpu": 1}},
		},
		Placer: placer,
	})

	var wg sync.WaitGroup
	const perTenant = 50
	for _, tenant := range []model.TenantID{"a", "b"} {
		for i := 0; i < perTenant; i++ {
			wg.Add(1)
			go func(tenant model.TenantID, i int) {
				defer wg.Done()
				id := model.JobID(string(tenant) + string(rune('0'+i%10)) + string(rune('A'+i/10)))
				sched.Submit(job(id, tenant, model.ResourceVec{"cpu": 1}))
			}(tenant, i)
		}
	}
	wg.Wait()

	for i := 0; i < perTenant*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = sched.DispatchNext(context.Background())
		}()
	}
	wg.Wait()

	sched.Release("a", model.ResourceVec{"cpu": 0.01})
}
