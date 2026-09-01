package model

// P5 boundary types: DISASTER RECOVERY. recoveryPlan turns a Loss event plus
// fleet topology into a deterministic, ordered list of Steps — pure, so a
// drill asserts "us-east lost -> these exact steps" before anything
// executes. See docs/phases/swarm-p5-components.txt §02.

// LossKind classifies what was lost. Cell/agent-level loss is already
// covered by P1 failover and is out of scope here.
type LossKind int

const (
	// RegionLoss means a whole region is gone.
	RegionLoss LossKind = iota
	// StoreLoss means the registry store is gone/corrupt.
	StoreLoss
)

// Loss is the failure event recoveryPlan reasons over.
type Loss struct {
	Kind   LossKind
	Region RegionID
}

// StepKind is the recovery action kind. The zero value is NoStep so an
// empty/uninitialized Step is inert rather than re-homing or rerouting
// anything by accident.
type StepKind int

const (
	// NoStep takes no action — the safe, zero-value default.
	NoStep StepKind = iota
	// ReHome re-homes agents from the lost region onto Region.
	ReHome
	// RestoreRegistry restores the FDB registry from the backup at Backup.
	RestoreRegistry
	// Reroute redirects global traffic away from Traffic.
	Reroute
)

// Step is ReHome{Region} | RestoreRegistry{Backup} | Reroute{Traffic} — the
// sum type recoveryPlan returns, one entry of the ordered plan.
type Step struct {
	Kind StepKind
	// Region is the ReHome target region.
	Region RegionID
	// Backup is the backup timestamp RestoreRegistry restores from.
	Backup Instant
	// Traffic is the region Reroute steers traffic away from.
	Traffic RegionID
}
