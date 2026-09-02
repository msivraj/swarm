// Package mitosis is a pure core: it decides when cells split or merge. It
// performs no I/O and reads no clock — the shell injects `now` and any cooldown
// timestamps as data. This package is the reference shape every core follows:
// take data, return commands, never execute an effect.
//
// Gate is a P2 delta: it enforces B3 (a coupled cell may only split/merge at
// a checkpoint boundary) as a pure post-filter over Decide's output. It does
// not change Decide's own behavior.
package mitosis

import "github.com/msivraj/swarm/internal/model"

// Op is the kind of a mitosis Command.
type Op int

const (
	// None is the zero value; a real decision is Split or Merge.
	None Op = iota
	Split
	Merge
)

// Command is a mitosis decision the shell will execute. Cores return Commands;
// they never carry them out.
//
// Deferred marks a coupled cell's Split/Merge as postponed until the next
// checkpoint boundary (B3, enforced by Gate) rather than dropped: the shell
// re-offers the command once the cell's driver reaches its next checkpoint.
// The zero value is false, so P0's Decide — which never sets it — is
// unchanged and every existing caller/test stays green.
//
// Resolved ambiguity: the ticket offered either a `Deferred bool` field or a
// separate `Defer` Op. A bool field was chosen: it lets a deferred command
// keep its original Op/Cell/Other untouched — the command "survives"
// deferral verbatim and is re-emitted unchanged at the checkpoint boundary,
// as the acceptance criteria require — without wrapping or duplicating
// Command, and it leaves P0's Decide byte-for-byte unchanged.
type Command struct {
	Op       Op
	Cell     model.CellID // the cell to split, or the first cell to merge
	Other    model.CellID // the second cell, for Merge
	Deferred bool         // true: postponed until the next checkpoint (B3)
}

// Thresholds configures the split/merge band and the cooldown window.
type Thresholds struct {
	Target     int   // target cell size T: split above 2T, merge two neighbors below T combined
	CooldownNS int64 // suppress a cell's resize for this long after its last one
}

// Decide returns the split/merge commands for the given cells. It is pure: the
// same inputs always yield the same output, with no I/O and no wall-clock read.
// `now` and the cooldown timestamps are supplied by the caller (the shell).
func Decide(cells []model.CellView, cfg Thresholds, cooldowns map[model.CellID]model.Instant, now model.Instant) []Command {
	var cmds []Command
	var mergeable []model.CellView

	for _, c := range cells {
		if inCooldown(cooldowns, c.ID, now, cfg.CooldownNS) {
			continue
		}
		switch {
		case c.Size > 2*cfg.Target:
			cmds = append(cmds, Command{Op: Split, Cell: c.ID})
		case c.Size < cfg.Target:
			mergeable = append(mergeable, c)
		}
	}

	// Merge consecutive under-full cells whose combined size stays under Target,
	// so a freshly merged cell is still healthy and cannot immediately re-split.
	for i := 0; i+1 < len(mergeable); i += 2 {
		a, b := mergeable[i], mergeable[i+1]
		if a.Size+b.Size < cfg.Target {
			cmds = append(cmds, Command{Op: Merge, Cell: a.ID, Other: b.ID})
		}
	}
	return cmds
}

// inCooldown reports whether a cell resized within the cooldown window.
func inCooldown(cooldowns map[model.CellID]model.Instant, id model.CellID, now model.Instant, windowNS int64) bool {
	last, ok := cooldowns[id]
	if !ok {
		return false
	}
	return int64(now-last) < windowNS
}

// ResizeSafe reports whether a resize (Split or Merge) for a cell governed by
// coupling c may execute right now. An Independent cell may always resize —
// no driver depends on its membership staying fixed mid-step. A coupled cell
// (Barrier, Leader, or MessagePassing) may resize only at a checkpoint
// boundary (atCkpt == true): B3, resizing mid-step would race the driver's
// lockstep and corrupt its coordination state.
func ResizeSafe(c model.Coupling, atCkpt bool) bool {
	if c == model.Independent {
		return true
	}
	return atCkpt
}

// Gate enforces B3 over P0's Decide output: it is a pure post-filter, applied
// per cell, that never changes Decide's own behavior. When ResizeSafe(c,
// atCkpt) is true — an Independent cell at any time, or a coupled cell at a
// checkpoint boundary — every command passes through unchanged. Otherwise
// every command is marked Deferred (see Command) rather than dropped: the
// shell holds it and re-offers it once the coupled cell's driver reaches its
// next checkpoint, at which point Gate lets it through unchanged.
func Gate(cmds []Command, c model.Coupling, atCkpt bool) []Command {
	if cmds == nil {
		return nil
	}
	safe := ResizeSafe(c, atCkpt)
	out := make([]Command, len(cmds))
	for i, cmd := range cmds {
		cmd.Deferred = !safe
		out[i] = cmd
	}
	return out
}

// --- P6 §03 upgrade: signal-based adaptive mitosis ---------------------
//
// DecideSignal is ADDITIVE alongside Decide: Decide's signature and behavior
// are unchanged (it has a live shell caller and P0 tests), and the shell
// rewires to DecideSignal in a follow-up ticket. See
// docs/phases/swarm-p6-components.txt §01-§02.

// Base per-coupling P99 split band (nanoseconds), before an SLO's
// remaining-budget fraction tightens it. Coupled cells (Barrier, Leader,
// MessagePassing) have members that block on each other's progress, so a
// growing p99 there costs the whole cell's throughput — their base bands are
// the tightest. Independent cells share no state and tolerate the most
// latency before a split is worth the disruption, so it is loosest.
const (
	baseSplitBarrier        model.Duration = 20 * 1_000_000  // 20ms
	baseSplitLeader         model.Duration = 30 * 1_000_000  // 30ms
	baseSplitMessagePassing model.Duration = 50 * 1_000_000  // 50ms
	baseSplitIndependent    model.Duration = 200 * 1_000_000 // 200ms

	// minBudgetFraction floors the SLO tightening factor so a zero-value or
	// fully-exhausted SLO budget still yields a positive, usable threshold
	// instead of collapsing SplitP99 to zero (which would split every cell
	// with any measured signal, every tick).
	minBudgetFraction = 0.1

	// mergeBand is the split/merge hysteresis gap: MergeP99 sits at this
	// fraction of SplitP99, so a cell must fall well under the split line —
	// not merely under it — before it becomes merge-eligible. Without this
	// gap a cell hovering at the line could split and re-merge every tick.
	mergeBand = 0.5
)

// signalThreshold derives the per-coupling latency band DecideSignal judges a
// cell's measured P99 against. model.SLO is a ratio (Objective/AtRisk), not a
// latency, so this maps it onto the coupling's base band via AtRisk — the
// budget-fraction remaining at/below which SLO state goes AtRisk (see
// model.SLO). A small AtRisk (little error-budget headroom tolerated before
// the SLO is at risk) tightens the latency band toward the coupling's most
// latency-sensitive setting; an AtRisk near 1 leaves the base band intact.
// The result is always a positive band clamped to at most the base one — the
// SLO can only tighten a threshold, never loosen it past the coupling's
// baseline.
func signalThreshold(c model.Coupling, slo model.SLO) model.Threshold {
	base := baseSplitP99(c)
	frac := remainingBudgetFraction(slo)
	split := model.Duration(float64(base) * frac)
	merge := model.Duration(float64(split) * mergeBand)
	return model.Threshold{SplitP99: split, MergeP99: merge}
}

// baseSplitP99 returns the per-coupling base latency band, tightest to
// loosest: Barrier < Leader < MessagePassing < Independent.
func baseSplitP99(c model.Coupling) model.Duration {
	switch c {
	case model.Barrier:
		return baseSplitBarrier
	case model.Leader:
		return baseSplitLeader
	case model.MessagePassing:
		return baseSplitMessagePassing
	default: // model.Independent, and any future/unknown coupling
		return baseSplitIndependent
	}
}

// remainingBudgetFraction clamps SLO.AtRisk to [minBudgetFraction, 1] so the
// derived threshold is always positive and never loosens past the coupling's
// base band.
func remainingBudgetFraction(slo model.SLO) float64 {
	switch {
	case slo.AtRisk <= minBudgetFraction:
		return minBudgetFraction
	case slo.AtRisk > 1:
		return 1
	default:
		return slo.AtRisk
	}
}

// DecideSignal is the §03 upgrade of Decide: it splits/merges on a MEASURED
// signal (barrier-completion / queue-latency p99) crossing a per-coupling,
// SLO-derived band, rather than only a size count. It is pure and returns
// the same []Command shape Decide does.
//
// Signal-based subsumes count-based: when a cell's P99 is absent (the zero
// value, meaning "not measured"), DecideSignal falls back to exactly the
// count rule Decide applies — split above 2*Target, merge two consecutive
// under-full neighbors whose combined size stays under Target — so
// signal-based mitosis given only size input reproduces P0's decisions
// exactly (see TestDecideSignalSubsumesCount).
func DecideSignal(cells []model.CellSignal, cfg model.SignalThresholds, cooldowns map[model.CellID]model.Instant, now model.Instant) []Command {
	var cmds []Command
	var mergeable []model.CellSignal

	for _, c := range cells {
		if inCooldown(cooldowns, c.Cell, now, cfg.CooldownNS) {
			continue
		}

		th := signalThreshold(c.Coupling, cfg.SLO)
		switch {
		case c.P99 > th.SplitP99:
			cmds = append(cmds, Command{Op: Split, Cell: c.Cell})
		case c.P99 == 0:
			if split, eligible := countFallback(c.Size, cfg.Target); split {
				cmds = append(cmds, Command{Op: Split, Cell: c.Cell})
			} else if eligible {
				mergeable = append(mergeable, c)
			}
		case c.P99 <= th.MergeP99 && c.Size < cfg.Target:
			mergeable = append(mergeable, c)
		}
	}

	// Merge consecutive merge-eligible cells whose combined size stays under
	// Target, exactly as Decide does.
	for i := 0; i+1 < len(mergeable); i += 2 {
		a, b := mergeable[i], mergeable[i+1]
		if a.Size+b.Size < cfg.Target {
			cmds = append(cmds, Command{Op: Merge, Cell: a.Cell, Other: b.Cell})
		}
	}
	return cmds
}

// countFallback reproduces Decide's per-cell count rule for a cell with no
// measured P99 signal: split above 2*target, or mark merge-eligible below
// target, matching neither in between. It is factored out so DecideSignal's
// fallback path is provably identical to Decide's own logic rather than a
// parallel reimplementation that could drift from it.
func countFallback(size, target int) (split, mergeEligible bool) {
	switch {
	case size > 2*target:
		return true, false
	case size < target:
		return false, true
	default:
		return false, false
	}
}
