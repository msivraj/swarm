// Package mitosis is a pure core: it decides when cells split or merge. It
// performs no I/O and reads no clock — the shell injects `now` and any cooldown
// timestamps as data. This package is the reference shape every core follows:
// take data, return commands, never execute an effect.
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
type Command struct {
	Op    Op
	Cell  model.CellID // the cell to split, or the first cell to merge
	Other model.CellID // the second cell, for Merge
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
