package controlplane

import (
	"time"

	"github.com/msivraj/swarm/internal/core/mitosis"
	"github.com/msivraj/swarm/internal/model"
)

// Config holds the control plane's tunables. None of these are pinned by the
// phase doc (see issue #19 "Ambiguities"); the values in DefaultConfig are
// sane P0 defaults, documented here so the auditor can review the choice.
type Config struct {
	// DefaultCellCapacity is the capacity a brand-new cell gets when
	// rendezvous.AdmitAgent returns NewCell (no existing cell has room). It
	// is raised to the joining agent's requested Caps if that is larger, so
	// the agent that triggered the new cell always fits.
	DefaultCellCapacity int

	// HeartbeatTimeout is how long an agent may go without a Heartbeat (or a
	// JoinAgent, which also refreshes last-seen) before the membership
	// reaper evicts it with an AgentLeft event.
	HeartbeatTimeout time.Duration

	// HeartbeatSweep is how often the reaper loop checks for expired agents.
	HeartbeatSweep time.Duration

	// MitosisInterval is how often the mitosis loop reads the registry
	// snapshot and calls mitosis.Decide.
	MitosisInterval time.Duration

	// MitosisThresholds configures mitosis.Decide's split/merge band and
	// cooldown window (see internal/core/mitosis).
	MitosisThresholds mitosis.Thresholds

	// RegionID stamps this region's identity on every RegionalSummary it
	// publishes and PartialAggregate it reports upward (issue #44, S2).
	RegionID model.RegionID

	// GlobalRouter is the dial address of the P1 global routing layer (S3).
	// Empty puts the control plane in standalone P0 mode: the publish loop,
	// spill, and upward roll-up are all disabled, and the server behaves
	// exactly as P0/S1 did — this is the switch that makes S2 additive
	// rather than a breaking change for a single-region deployment.
	GlobalRouter string

	// SummaryInterval is how often the publish loop computes
	// routing.Summarize over the region's registry and calls
	// GlobalRouter.PublishSummary (also refreshing the cached peer view via
	// GlobalRouter.GetGlobalView, which placement's spill decision reads).
	// Meaningless (and unused) when GlobalRouter is empty.
	SummaryInterval time.Duration

	// PeerTargets maps a peer RegionID to its control-plane dial address, so
	// a placement.Spill{RegionID} decision becomes a concrete
	// ControlPlane.DispatchTasks dial. Wired from cmd/swarmd.
	PeerTargets map[model.RegionID]string

	// SelfAddress is the dial address other regions should use to reach this
	// region's ControlPlane — carried as DispatchTasksRequest.result_sink
	// when this region spills a task to a peer, so the peer's agent can
	// forward that task's raw result back here via the ordinary
	// ControlPlane.ReportResult RPC. Meaningless when GlobalRouter is empty.
	SelfAddress string

	// GlobalRouterDialer opens a connection to GlobalRouter (PublishSummary,
	// GetGlobalView, ReportPartial). Defaults to a plaintext gRPC dial
	// (GRPCGlobalRouterDialer); tests supply one backed by an in-process
	// (bufconn) fake GlobalRouter.
	GlobalRouterDialer GlobalRouterDialer

	// PeerDialer opens a connection to a peer (or spill-origin) region's
	// ControlPlane (DispatchTasks, ReportResult). Defaults to a plaintext
	// gRPC dial (GRPCPeerDialer); tests supply one backed by an in-process
	// (bufconn) fake ControlPlane.
	PeerDialer PeerDialer
}

// DefaultConfig returns the P0 defaults: an 8-slot starting cell, a 30s
// heartbeat timeout swept every 5s, a mitosis tick every 10s targeting
// 4-agent cells with a 30s resize cooldown, and a 5s regional-summary publish
// cadence (meaningless until Config.GlobalRouter is set). cmd/swarmd's
// control-plane mode uses these unless overridden.
func DefaultConfig() Config {
	return Config{
		DefaultCellCapacity: 8,
		HeartbeatTimeout:    30 * time.Second,
		HeartbeatSweep:      5 * time.Second,
		MitosisInterval:     10 * time.Second,
		MitosisThresholds: mitosis.Thresholds{
			Target:     4,
			CooldownNS: int64(30 * time.Second),
		},
		SummaryInterval: 5 * time.Second,
	}
}
