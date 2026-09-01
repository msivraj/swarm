// Package backpressure is a pure core: it decides whether an inbound
// control-plane request is admitted, throttled, or shed under the current
// load. It performs no I/O and reads no clock — any delay it returns is data
// (model.Duration, a relative span) the shell later waits out, never a
// time.Sleep called here. Measuring queue depth / in-flight RPCs and
// enforcing the decision (delaying, rejecting, token-bucketing) is the
// shell's job; this package only reduces (request, load, limits) to a
// decision. See docs/phases/swarm-p4-components.txt §02.
package backpressure

import "github.com/msivraj/swarm/internal/model"

// Load bands, expressed as a fraction of Limits.Capacity:
//
//	ratio < lowWaterMark                        -> AdmitLoad (any priority)
//	lowWaterMark <= ratio < effectiveShed(req)   -> Throttle{Delay}
//	ratio >= effectiveShed(req)                  -> Shed
//
// ratio is (InFlight+QueueDepth)/Capacity: InFlight alone under-counts load
// once requests start backing up in the queue, so both measured quantities
// feed the same denominator (Capacity, the control plane's sizing target).
//
// effectiveShed(req) raises Limits.ShedThreshold by priorityShedBonus per
// priority point, so a higher-priority request needs more load than a
// lower-priority one before it is shed — it is never shed at a load a lower
// priority request would merely be throttled at. It is clamped to never fall
// below lowWaterMark, so a pathological (negative) priority still leaves a
// well-formed Throttle band instead of an inverted one.
const (
	// lowWaterMark is the ratio below which every request, regardless of
	// priority, is admitted immediately.
	lowWaterMark = 0.50

	// priorityShedBonus is how much one Req.Priority point raises the
	// effective shed threshold above Limits.ShedThreshold.
	priorityShedBonus = 0.01

	// maxThrottleDelay is the backoff a request is given when the load ratio
	// sits right at a request's effective shed threshold — the far edge of
	// the throttle band, immediately before Shed takes over. Delay scales
	// linearly down from this to zero at lowWaterMark.
	maxThrottleDelay = model.Duration(1_000_000_000) // 1s, in ns
)

// AdmitUnderLoad decides how to handle req given the current load snapshot
// and the configured limits: AdmitLoad | Throttle{Delay} | Shed. Pure and
// total — see the package doc for the exact bands.
func AdmitUnderLoad(req model.Req, load model.LoadState, lim model.Limits) model.LoadDecision {
	if lim.Capacity <= 0 {
		// An unconfigured or zero-capacity control plane cannot serve
		// anything: shed rather than divide by zero or admit blindly.
		return model.LoadDecision{Kind: model.Shed}
	}

	ratio := loadRatio(load, lim)
	shedAt := effectiveShedThreshold(req, lim)

	switch {
	case ratio >= shedAt:
		return model.LoadDecision{Kind: model.Shed}
	case ratio < lowWaterMark:
		return model.LoadDecision{Kind: model.AdmitLoad}
	default:
		return model.LoadDecision{Kind: model.Throttle, Delay: throttleDelay(ratio, shedAt)}
	}
}

// loadRatio is the fraction of Capacity currently claimed by in-flight and
// queued requests together.
func loadRatio(load model.LoadState, lim model.Limits) float64 {
	claimed := load.InFlight + load.QueueDepth
	if claimed <= 0 {
		return 0
	}
	return float64(claimed) / float64(lim.Capacity)
}

// effectiveShedThreshold is Limits.ShedThreshold raised by priorityShedBonus
// per priority point, clamped so it never drops below lowWaterMark.
func effectiveShedThreshold(req model.Req, lim model.Limits) float64 {
	shedAt := lim.ShedThreshold + float64(req.Priority)*priorityShedBonus
	if shedAt < lowWaterMark {
		return lowWaterMark
	}
	return shedAt
}

// throttleDelay linearly scales from 0 at lowWaterMark to maxThrottleDelay at
// shedAt, so the closer load sits to the point a request would be shed, the
// longer the shell is told to make it wait.
func throttleDelay(ratio, shedAt float64) model.Duration {
	band := shedAt - lowWaterMark
	if band <= 0 {
		return 0
	}
	frac := (ratio - lowWaterMark) / band
	switch {
	case frac < 0:
		frac = 0
	case frac > 1:
		frac = 1
	}
	return model.Duration(frac * float64(maxThrottleDelay))
}

// UpdateLoad folds a measured load event (an arrival, a completion, a
// queue-depth sample) into the load snapshot. Pure — the shell measures,
// this reduces. InFlight and QueueDepth are each clamped at zero: load can
// never go negative, no matter what sequence of deltas arrives.
func UpdateLoad(load model.LoadState, ev model.LoadEvent) model.LoadState {
	return model.LoadState{
		InFlight:   clampNonNegative(load.InFlight + ev.InFlightDelta),
		QueueDepth: clampNonNegative(load.QueueDepth + ev.QueueDelta),
	}
}

func clampNonNegative(v int) int {
	if v < 0 {
		return 0
	}
	return v
}
