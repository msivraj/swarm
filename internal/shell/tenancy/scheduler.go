package tenancy

import (
	"context"

	coretenancy "github.com/msivraj/swarm/internal/core/tenancy"
	"github.com/msivraj/swarm/internal/model"
)

// Submit admits or rejects job against its tenant's quota. job.Demand must
// hold RAW, absolute-unit amounts (see the package doc); Submit normalizes
// it to a capacity share, adds it to the tenant's current usage, and calls
// the pure core's WithinQuota on the projection. Rejected means the job is
// never queued — it will never reach DispatchNext. Admitted queues a copy
// of job whose Demand has been replaced with its normalized share, so every
// later step (DispatchNext, NextFair, Scope, usage accounting) works
// entirely in shares.
func (s *Scheduler) Submit(job model.JobSpec) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	share := normalizeToCapacity(job.Demand, s.cfg.ClusterCapacity)
	projected := model.Usage{Consumed: addVec(s.usage[job.Tenant].Consumed, share)}
	quota := s.cfg.Quotas[job.Tenant]
	if !coretenancy.WithinQuota(s.tenant(job.Tenant), projected, quota) {
		return Rejected
	}

	job.Demand = share
	s.pending = append(s.pending, job)
	return Admitted
}

// DispatchNext dispatches the next job under dominant-resource fair share:
// it asks the pure core's NextFair for the pick (usage weighted by each
// tenant's Weight, per the package doc), removes it from pending, dispatches
// it via Config.Placer under its tenant's Scope, and — on success — adds its
// Demand to the tenant's usage. It returns the zero JobID ("") with a nil
// error when pending is empty.
//
// A Placer error re-queues job at the tail of pending (it is not lost, and
// not counted as dispatched — usage is left untouched) and returns the
// error; the caller decides whether/when to retry the tick.
func (s *Scheduler) DispatchNext(ctx context.Context) (model.JobID, error) {
	job, ok := s.popFair()
	if !ok {
		return "", nil
	}

	scope := coretenancy.Scope(job)
	if err := s.cfg.Placer.Place(ctx, scope, job); err != nil {
		s.requeue(job)
		return "", err
	}

	s.mu.Lock()
	s.usage[job.Tenant] = model.Usage{Consumed: addVec(s.usage[job.Tenant].Consumed, job.Demand)}
	s.mu.Unlock()

	return job.ID, nil
}

// Release gives back demand (in the same normalized shares Submit queued —
// i.e. what a prior DispatchNext added to usage) from tenant's recorded
// usage, once a dispatched job completes or is otherwise given up. Each
// resource component is floored at 0, so Release can never push a tenant's
// recorded usage negative.
func (s *Scheduler) Release(tenant model.TenantID, demand model.ResourceVec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage[tenant] = model.Usage{Consumed: subVecFloor(s.usage[tenant].Consumed, demand)}
}

// UsageOf returns a copy of tenant's currently recorded usage.
func (s *Scheduler) UsageOf(tenant model.TenantID) model.Usage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return model.Usage{Consumed: copyVec(s.usage[tenant].Consumed)}
}

// Pending returns the number of jobs currently queued for dispatch.
func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// popFair picks the DRF-fair job (per NextFair, over weight-adjusted usage)
// and removes it from pending, returning ok=false if pending is empty or
// NextFair otherwise found nothing to pick.
func (s *Scheduler) popFair() (model.JobSpec, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) == 0 {
		return model.JobSpec{}, false
	}

	pick := coretenancy.NextFair(s.pending, s.weightedUsageLocked())
	if pick == "" {
		return model.JobSpec{}, false
	}

	for i, j := range s.pending {
		if j.ID != pick {
			continue
		}
		s.pending = append(s.pending[:i], s.pending[i+1:]...)
		return j, true
	}
	return model.JobSpec{}, false
}

// requeue appends job back onto pending (tail), for a dispatch a Placer
// failed to carry out.
func (s *Scheduler) requeue(job model.JobSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, job)
}

// weightedUsageLocked builds the usage map NextFair requires: each pending
// tenant's stored Usage, divided componentwise by its fair-share Weight (see
// the package doc). Callers must hold s.mu.
func (s *Scheduler) weightedUsageLocked() map[model.TenantID]model.Usage {
	out := make(map[model.TenantID]model.Usage, len(s.pending))
	for _, job := range s.pending {
		if _, done := out[job.Tenant]; done {
			continue
		}
		out[job.Tenant] = model.Usage{Consumed: divVec(s.usage[job.Tenant].Consumed, s.weightOf(job.Tenant))}
	}
	return out
}

// tenant returns id's registered model.Tenant, defaulting to {ID: id,
// Weight: 0} (WithinQuota does not read Weight, so this default is only
// ever observed by weightOf).
func (s *Scheduler) tenant(id model.TenantID) model.Tenant {
	if t, ok := s.cfg.Tenants[id]; ok {
		return t
	}
	return model.Tenant{ID: id}
}

// weightOf returns id's fair-share weight: its registered Tenant.Weight, or
// 1 if id is unregistered or its Weight is <= 0.
func (s *Scheduler) weightOf(id model.TenantID) float64 {
	if t, ok := s.cfg.Tenants[id]; ok && t.Weight > 0 {
		return t.Weight
	}
	return 1
}
