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
	// snapshot and calls mitosis.DecideSignal.
	MitosisInterval time.Duration

	// MitosisThresholds carries the count-based split/merge band and
	// cooldown window (Target/CooldownNS) that fed mitosis.Decide directly
	// in P0. mitosisOnce now calls mitosis.DecideSignal instead, but it
	// still reads Target/CooldownNS straight off this field (composed with
	// SLO into a model.SignalThresholds by signalThresholds()) — so every
	// existing deployment/test that only ever set MitosisThresholds keeps
	// getting exactly the same count-based band, and an unmeasured cell
	// falls back to it exactly as Decide decided (P6's subsumption
	// guarantee) — this field's own name/type/zero-value stay untouched
	// (see #222).
	MitosisThresholds mitosis.Thresholds

	// SLO is the P6 latency objective mitosis.DecideSignal judges a cell's
	// MEASURED p99 against (via the mitosis core's per-coupling
	// signalThreshold). The zero value (AtRisk 0) is clamped by
	// signalThreshold to its own minimum tightening fraction, so an unset
	// SLO still yields a usable (if maximally tight) band rather than a
	// degenerate one — DefaultConfig sets a real one.
	SLO model.SLO

	// CellSignals is the seam mitosisOnce reads each cell's MEASURED p99/
	// throughput signal from — P4's observability rollups, at per-cell
	// granularity (see CellSignalSource's doc for why this is not simply a
	// read through observability.Reporter's own Region()/Global(), which
	// are already folded across many cells). nil, the zero value, means no
	// measured signal is available anywhere: mitosisOnce builds every
	// model.CellSignal with P99==0, so mitosis.DecideSignal falls back to
	// exactly the P0 count-based decision for every cell — the subsumption
	// guarantee, end to end, and exactly why a deployment that never wires
	// observability behaves identically to the prior count-based loop.
	CellSignals CellSignalSource

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

	// Logger receives one formatted line for shell-observability events this
	// server discovers off the RPC path it could otherwise report the error
	// on directly — e.g. a coupled-cell activation failure discovered inside
	// admitGangLocked, whose caller (SubmitJob, or a background capacity
	// retry with no RPC caller at all) must still succeed (see
	// activateCoupledCellLocked's doc). Defaults to log.Printf; tests inject
	// a sink so they can assert on the exact line rather than scraping
	// stdout.
	Logger func(format string, args ...interface{})

	// Limits configures backpressure.AdmitUnderLoad's admission thresholds
	// (Capacity, ShedThreshold) — see internal/core/backpressure and
	// docs/phases/swarm-p4-components.txt §02. The zero value (Capacity 0)
	// makes AdmitUnderLoad shed every request unconditionally (its own
	// documented conservative default for an unconfigured control plane),
	// so a deployment that wants the admission middleware to do anything
	// other than reject everything must set a real Capacity — DefaultConfig
	// does.
	Limits model.Limits

	// JoinPriority is the model.Req.Priority every JoinAgent request maps
	// to for the backpressure admission check. JoinAgentRequest carries no
	// native Priority field on the wire (unlike SubmitJobRequest, which
	// carries one via Params — see requestPriority's doc), so this
	// per-deployment default is JoinAgent's only priority signal. The zero
	// value (0) is the lowest priority, matching model.Req's own
	// zero-value convention.
	JoinPriority int

	// Sleep waits out a backpressure.Throttle decision's delay. Defaults to
	// a real time.Sleep; tests inject a fake that returns immediately (and
	// records the requested duration), so a delayed RPC resolves
	// deterministically with no real wall-clock wait.
	Sleep func(model.Duration)
}

// DefaultConfig returns the P0 defaults: an 8-slot starting cell, a 30s
// heartbeat timeout swept every 5s, a mitosis tick every 10s targeting
// 4-agent cells with a 30s resize cooldown, and a 5s regional-summary publish
// cadence (meaningless until Config.GlobalRouter is set). cmd/swarmd's
// control-plane mode uses these unless overridden.
//
// Limits.Capacity (1000) is sized generously above any realistic
// concurrent-RPC count a single control-plane process serves in P0/P1/P4's
// tests and typical deployments, at a P4 doc-example 95% shed threshold — so
// the backpressure middleware stays transparent (see admitIngress's doc)
// until load genuinely spikes, never regressing existing P0-P3 behavior.
//
// SLO's Objective/AtRisk (0.999/0.5) is a plausible mid-tightening default —
// half the coupling's base latency band remains before a measured p99 is
// judged AtRisk — matching the mitosis core's own doc-example usage
// (mitosis_test.go). CellSignals is left nil: DefaultConfig's mitosis loop
// starts in the pure count-based fallback until a caller wires a real
// per-cell signal source.
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
		SLO:             model.SLO{Objective: 0.999, AtRisk: 0.5},
		SummaryInterval: 5 * time.Second,
		Limits:          model.Limits{Capacity: 1000, ShedThreshold: 0.95},
	}
}

// signalThresholds composes the model.SignalThresholds mitosis.DecideSignal
// reasons over from cfg.MitosisThresholds (Target/CooldownNS — the P0 count
// band and cooldown window, read verbatim) plus cfg.SLO (the P6 latency
// objective). This is the "wraps" side of #222: every existing caller that
// only ever set MitosisThresholds still gets exactly the same Target/
// CooldownNS band, now additionally judged against SLO's derived latency
// threshold whenever a cell has a measured P99.
func (cfg Config) signalThresholds() model.SignalThresholds {
	return model.SignalThresholds{
		Target:     cfg.MitosisThresholds.Target,
		CooldownNS: cfg.MitosisThresholds.CooldownNS,
		SLO:        cfg.SLO,
	}
}

// CellSignalSource is the seam mitosisOnce reads a cell's MEASURED p99/
// throughput signal from. It is deliberately per-cell rather than a wrapper
// over observability.Reporter's own Region()/Global() reads: a Reporter's
// stored series are already folded across every cell in a region (that
// folding is the whole point of P4's O1 cardinality bound, §03) — reading
// back through it would blend away exactly the one hot cell mitosis needs to
// single out. A real implementation sits alongside the shell that feeds
// observability.Reporter.Collect and keeps the per-cell CellMetrics that
// call was given (or a windowed p99 derived from them); tests supply a fake.
//
// A nil CellSignalSource (Config.CellSignals' zero value) means no measured
// signal exists anywhere: mitosisOnce leaves every cell's P99 at 0, and
// mitosis.DecideSignal falls back to exactly the count-based decision — see
// Config.CellSignals' doc.
type CellSignalSource interface {
	// CellSignal returns cell's last measured p99 latency and throughput,
	// and whether a measurement exists for it at all. ok == false leaves
	// that cell's model.CellSignal.P99 at its zero value in mitosisOnce.
	CellSignal(cell model.CellID) (p99 model.Duration, tput float64, ok bool)
}
