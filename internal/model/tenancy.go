package model

// P5 boundary types: QUOTAS & FAIRNESS. Multi-tenancy splits into a pure
// policy decision — is this tenant over quota? whose turn is it? — and a
// hardened shell that tracks usage and dispatches the fair pick into the
// tenant's isolated cell (reusing R4 dedicated cells + the P3 sandbox). See
// docs/phases/swarm-p5-components.txt §02.

// TenantID identifies a tenant whose jobs clear their own quota and fair
// share. The zero value "" is the default/untagged tenant, so a pre-P5
// JobSpec (which never sets Tenant) is unaffected.
type TenantID string

// ResourceKind names a fungible resource DRF shares (cpu, mem, gpu, ...).
type ResourceKind string

// ResourceVec is a per-resource nonnegative amount, EXPRESSED AS A FRACTION
// OF TOTAL CLUSTER CAPACITY (a normalized share in [0,1] per resource). The
// shell normalizes at the boundary so withinQuota/nextFair stay pure with
// the doc's exact signatures — no separate capacity argument. The zero value
// (nil map) means no demand/usage on any resource.
type ResourceVec map[ResourceKind]float64

// Tenant carries the identity plus fair-share weight; equal weight means
// equal shares under nextFair.
type Tenant struct {
	ID     TenantID
	Weight float64
}

// Usage is a tenant's CURRENT per-resource consumption, normalized shares.
type Usage struct {
	Consumed ResourceVec
}

// Quota is a tenant's per-resource ceiling, normalized shares. withinQuota
// compares Usage to Quota componentwise.
type Quota struct {
	Limit ResourceVec
}

// TenantScope is the isolation binding scope() returns — which tenant, and
// (via the shell) the dedicated cell it dispatches under. This type only
// names the binding; it REUSES R4 dedicated cells + the P3 sandbox rather
// than reinventing isolation.
type TenantScope struct {
	Tenant TenantID
}
