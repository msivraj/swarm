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
