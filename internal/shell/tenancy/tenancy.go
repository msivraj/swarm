// Package tenancy is the P5 multi-tenancy enforcement shell: it tracks each
// tenant's usage, admits or rejects jobs against quota, and dispatches the
// fair-share pick into its tenant's isolated scope. Per the #183 design
// ruling (fork a) and the ticket's scope boundary, every DECISION — is this
// tenant within quota, whose job runs next, which scope a job dispatches
// under — comes from the pure internal/core/tenancy core. This package only
// measures inputs (usage) and carries the decision out (admission, dispatch):
// it adds no quota or fairness logic of its own. See
// docs/phases/swarm-p5-components.txt §02 (QUOTAS & FAIRNESS).
//
// # Normalizing at the boundary (design fork a, #183)
//
// internal/core/tenancy.WithinQuota/NextFair operate on model.Usage/model.Quota
// expressed as normalized SHARES of cluster capacity — a fraction in [0,1]
// per resource — and DRF's dominant share is weighted by dividing a tenant's
// usage by its model.Tenant.Weight, per NextFair's doc. Neither conversion
// belongs in the core (it would need a capacity argument, changing the
// doc's exact signature), so both happen here, at the shell boundary:
//
//   - Submit accepts a JobSpec whose Demand holds RAW, absolute-unit amounts
//     (e.g. cores, GB) — normalizeToCapacity divides each component by
//     Config.ClusterCapacity's matching component to get a share before the
//     JobSpec is queued or WithinQuota is consulted. A resource the cluster
//     has none of (capacity 0) but a tenant demands a positive amount of
//     divides to +Inf, which always compares over any finite quota Limit —
//     the same "zero ceiling" WithinQuota already applies to an undeclared
//     resource, achieved here without a branch (IEEE754: x/0 = +Inf for
//     x>0, 0/0 = NaN, and NaN's comparisons are always false, so a genuinely
//     zero demand on an undeclared/uncapacitated resource never spuriously
//     fails).
//   - DispatchNext divides each pending tenant's stored Usage by that
//     tenant's Config.Tenants Weight (a missing tenant or Weight <= 0 default
//     to weight 1) before calling NextFair, so a higher-weight tenant's
//     usage looks proportionally smaller and DRF favors it more often —
//     exactly the shell-side weighting NextFair's doc calls for.
//
// # Isolation reuses R4 dedicated cells + the P3 sandbox
//
// Scope only NAMES the isolation binding (model.TenantScope{Tenant}); this
// shell does not reinvent isolation. The Placer interface is the one I/O
// seam a dispatch goes through, and its production implementation is meant
// to compose the EXISTING control-plane placement path: map a TenantScope to
// a per-tenant dedicated-cell gang reservation (R4 — see
// internal/shell/controlplane/gang.go's admission.AdmitGang / Server.submitGang,
// which already reserves a cell exclusively for one job's lifetime) tagged
// to the tenant, and run the job inside that cell's
// internal/shell/sandbox.Runner (P3). Per the ticket's scope boundary ("do
// NOT do deep control-plane surgery"), that wiring is a later capstone
// (mirroring how internal/shell/verification's Coordinator documented its
// control-plane hook in #140/#143); this package defines the seam plus an
// in-memory FakePlacer, and keeps this scheduler standalone and testable —
// exactly how internal/shell/verification's coordinator shipped.
package tenancy

import (
	"context"
	"sync"

	"github.com/msivraj/swarm/internal/model"
)

// Placer dispatches job into the isolated placement its scope names — the
// existing control-plane path into a tenant's dedicated cell (R4) + the P3
// sandbox. This is the scheduler's one I/O seam; see the package doc's
// "Isolation reuses R4 dedicated cells + the P3 sandbox" section for how a
// production implementation is meant to compose it.
type Placer interface {
	Place(ctx context.Context, scope model.TenantScope, job model.JobSpec) error
}

// Decision is Submit's admit/reject outcome.
type Decision int

const (
	// Admitted means the job was queued for dispatch.
	Admitted Decision = iota
	// Rejected means the job's projected usage would exceed its tenant's
	// quota (on any resource, including one undeclared in its Quota.Limit)
	// — it was never queued and will never be dispatched.
	Rejected
)

// Config configures a Scheduler.
type Config struct {
	// ClusterCapacity is the total fleet capacity per resource, in the same
	// absolute units Submit's JobSpec.Demand is expressed in. normalizeToCapacity
	// divides a job's raw Demand by this to get the share WithinQuota/NextFair
	// require. A resource absent here (or present at 0) means the cluster
	// offers none of it — see the package doc's zero-ceiling note.
	ClusterCapacity model.ResourceVec
	// Tenants carries each known tenant's fair-share Weight. A TenantID with
	// no entry (or a nonpositive Weight) defaults to weight 1.
	Tenants map[model.TenantID]model.Tenant
	// Quotas carries each known tenant's per-resource ceiling, in normalized
	// shares. A TenantID with no entry defaults to the zero Quota (every
	// resource ceilinged at 0 — nothing is admitted until a quota is set).
	Quotas map[model.TenantID]model.Quota
	// Placer dispatches the fair pick into its tenant's isolated scope.
	// Required.
	Placer Placer
}

// Scheduler is the tenancy enforcement shell: Submit admits or rejects a job
// against its tenant's quota, DispatchNext dispatches the DRF-fair pick
// under its tenant's scope, and Release gives back usage a completed job no
// longer holds. Safe for concurrent use.
type Scheduler struct {
	cfg Config

	mu      sync.Mutex
	pending []model.JobSpec
	usage   map[model.TenantID]model.Usage
}

// New builds a Scheduler from cfg.
func New(cfg Config) *Scheduler {
	return &Scheduler{
		cfg:   cfg,
		usage: make(map[model.TenantID]model.Usage),
	}
}
