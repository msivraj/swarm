// Package tenancy is a pure core: it decides whether a tenant is within its
// quota, whose job runs next under dominant-resource fair share (DRF), and
// the isolation scope a job dispatches under. It performs no I/O and reads
// no clock or randomness — Usage/Quota/Demand arrive as normalized shares of
// cluster capacity, already computed by the shell (design fork (a), #183),
// so every decision here is a pure function of its inputs. See
// docs/phases/swarm-p5-components.txt §02 (QUOTAS & FAIRNESS) and §03.
package tenancy

import (
	"sort"

	"github.com/msivraj/swarm/internal/model"
)

// WithinQuota reports whether tenant t's usage u is within quota q.
//
// Rule: true iff, for every ResourceKind present in u.Consumed,
// u.Consumed[kind] <= q.Limit[kind]. A ResourceKind absent from q.Limit is
// treated as a ZERO CEILING (any nonzero use of it is over quota); a
// ResourceKind absent from u.Consumed is treated as ZERO USE (it can never
// cause a failure, regardless of its limit). The comparison is <=, so usage
// exactly at the limit is within quota (the boundary is inclusive). t is
// carried for API symmetry with the rest of the tenancy core (Scope,
// callers keyed by Tenant) — the quota check itself only compares u and q,
// per the ticket's exact rule.
//
// The shell is expected to add a prospective job's Demand into u BEFORE
// calling WithinQuota, so "would admitting this job exceed quota?" is this
// same pure check.
func WithinQuota(t model.Tenant, u model.Usage, q model.Quota) bool {
	for kind, consumed := range u.Consumed {
		if consumed > q.Limit[kind] {
			return false
		}
	}
	return true
}

// NextFair returns the JobID of the next job to run under dominant-resource
// fair share (DRF): among the tenants with at least one pending job, it
// picks the tenant with the LOWEST dominant share and returns that tenant's
// EARLIEST job in pending (stable input order, never map order).
//
// Dominant share of a tenant is the max component of its current Usage
// vector (usage[tenant].Consumed) — the resource on which it is proportionally
// most consumed. NextFair's signature (fixed by the resolved design fork,
// #183 fork (a)) keys usage by TenantID only, so it has no direct access to
// Tenant.Weight; DRF here is therefore computed over Usage as given. A
// caller that wants WEIGHTED DRF pre-divides each tenant's raw Consumed
// values by that tenant's Tenant.Weight before building the usage map it
// passes in — the same shell-side-normalization pattern fork (a) already
// uses for cluster-capacity shares, so a higher-weight tenant's usage
// appears proportionally smaller and it is picked more often, without
// widening this function's signature. A tenant with no entry in usage (a
// brand-new tenant) has a dominant share of 0 and is prioritized first.
//
// Ties (equal dominant share) break DETERMINISTICALLY: the tenant with the
// lowest TenantID (ordinary string ordering) wins. This, combined with
// updating the winner's usage after every pick, is what makes two
// equal-weight tenants continuously flooding pending alternate strictly
// (fairAlternates) rather than one starving the other.
//
// Empty pending returns the zero JobID ("").
func NextFair(pending []model.JobSpec, usage map[model.TenantID]model.Usage) model.JobID {
	if len(pending) == 0 {
		return ""
	}

	firstJob := make(map[model.TenantID]model.JobID, len(pending))
	var tenants []model.TenantID
	for _, job := range pending {
		if _, seen := firstJob[job.Tenant]; seen {
			continue
		}
		firstJob[job.Tenant] = job.ID
		tenants = append(tenants, job.Tenant)
	}

	sort.Slice(tenants, func(i, j int) bool { return tenants[i] < tenants[j] })

	var (
		best      model.TenantID
		bestShare float64
		haveBest  bool
	)
	for _, tid := range tenants {
		share := dominantShare(usage[tid])
		if !haveBest || share < bestShare {
			best, bestShare, haveBest = tid, share, true
		}
	}

	return firstJob[best]
}

// dominantShare is the max component of a Usage vector — the resource on
// which the tenant is proportionally most consumed. A nil or empty Consumed
// vector (no recorded usage) has a dominant share of 0.
func dominantShare(u model.Usage) float64 {
	var max float64
	for _, v := range u.Consumed {
		if v > max {
			max = v
		}
	}
	return max
}

// Scope returns the isolation binding job dispatches under: its owning
// tenant. An untenanted job (job.Tenant == "", the zero value — the
// default/untagged tenant, matching pre-P5 JobSpecs) scopes to the zero
// TenantScope, {Tenant: ""}. Scope only NAMES the binding; the shell REUSES
// R4 dedicated cells + the P3 sandbox to actually enforce isolation.
func Scope(job model.JobSpec) model.TenantScope {
	return model.TenantScope{Tenant: job.Tenant}
}
