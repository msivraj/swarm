package controlplane

import (
	"time"

	"github.com/msivraj/swarm/internal/core/mitosis"
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
}

// DefaultConfig returns the P0 defaults: an 8-slot starting cell, a 30s
// heartbeat timeout swept every 5s, and a mitosis tick every 10s targeting
// 4-agent cells with a 30s resize cooldown. cmd/swarmd's control-plane mode
// uses these unless overridden.
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
	}
}
