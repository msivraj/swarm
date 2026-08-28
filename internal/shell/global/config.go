package global

import (
	"time"

	"github.com/msivraj/swarm/internal/model"
)

// Config holds the global routing layer's tunables (issue #45). None of
// these are pinned by the phase doc; the values in DefaultConfig are sane P1
// defaults, documented here so the auditor can review the choice.
type Config struct {
	// RegionTargets maps a RegionID to its ControlPlane dial address, so a
	// routing.To/Spread decision becomes a concrete DispatchTasks (and, for
	// a To route, a proxied JobStatus) dial. Wired from cmd/swarmd.
	RegionTargets map[model.RegionID]string

	// SelfAddress is the dial address regions should use to reach this
	// global layer — carried as DispatchTasksRequest.result_sink for a
	// Spread partition, so the receiving region's own cfg.GlobalRouter
	// comparison (see controlplane's isSpillForward) recognizes this job as
	// global-sink and reports its rolled-up partial back via ReportPartial
	// rather than owning the job's completion itself.
	SelfAddress string

	// DivergeSweep is how often the background loop recomputes
	// routing.Diverged over the held GlobalView, using the injected clock —
	// health downgrade and observability between GetGlobalView polls.
	DivergeSweep time.Duration

	// RegionDialer opens a connection to a region's ControlPlane service
	// (DispatchTasks, and JobStatus for a To route's proxy). Defaults to a
	// plaintext gRPC dial (GRPCRegionDialer); tests supply one backed by an
	// in-process (bufconn) fake ControlPlane.
	RegionDialer RegionDialer
}

// DefaultConfig returns the P1 defaults: a 30s diverged-region recompute
// cadence, matching routing.StalenessWindow's own order of magnitude.
// cmd/swarmd's global-layer mode uses these unless overridden.
func DefaultConfig() Config {
	return Config{
		DivergeSweep: 30 * time.Second,
	}
}
