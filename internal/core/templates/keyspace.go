// Package templates holds Swarm's job templates: pure decompose/merge pairs
// the planner and job tracker call. A template turns a typed job description
// into independent Tasks, and turns the TaskResults back into one Aggregate.
// Like every core, it performs no I/O, reads no clock, and draws no
// randomness — any seed a template needs is supplied by the caller as data.
package templates

import (
	"encoding/binary"
	"fmt"

	"github.com/msivraj/swarm/internal/model"
)

// KeyspaceJob describes a keyspace-search job: search the numeric key range
// [Start, End) for a match, split across Shards independent tasks.
//
// The phase doc pins decompose(j KeyspaceJob) []Task / merge(rs) Aggregate
// but not the concrete fields of KeyspaceJob or the byte layout of
// Task.Input (see issue #2 "Notes / ambiguities"). This is the minimal,
// documented shape chosen here: a numeric range (e.g. a nonce or hash
// space) is the common case for a keyspace search, and a plain [start,end)
// pair splits deterministically without gaps or overlaps. A caller parses
// this out of JobSpec.Params (e.g. "start", "end", "shards"); that parsing
// is a shell/planner concern, not this package's.
type KeyspaceJob struct {
	JobID  model.JobID
	Start  uint64 // inclusive lower bound of the whole keyspace
	End    uint64 // exclusive upper bound of the whole keyspace
	Shards int    // number of sub-ranges to split into
}

// keyspaceInput is the wire layout of Task.Input for a keyspace-search task:
// the sub-range [Start, End) assigned to that task, as two big-endian
// uint64s. Fixed-width and unambiguous to decode in the agent's shell.
type keyspaceInput struct {
	Start uint64
	End   uint64
}

func (r keyspaceInput) bytes() []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], r.Start)
	binary.BigEndian.PutUint64(b[8:16], r.End)
	return b
}

func decodeKeyspaceInput(b []byte) (keyspaceInput, bool) {
	if len(b) != 16 {
		return keyspaceInput{}, false
	}
	return keyspaceInput{
		Start: binary.BigEndian.Uint64(b[0:8]),
		End:   binary.BigEndian.Uint64(b[8:16]),
	}, true
}

// KeyspaceDecompose splits j's [Start, End) range into j.Shards contiguous,
// non-overlapping tasks that together cover the whole range with no gaps.
// Sizes differ by at most one key across tasks (the remainder of an uneven
// division is spread over the first ranges).
//
// Shards <= 0 is treated as 1 (a single task covering the whole range). An
// empty or inverted range (End <= Start) yields no tasks. If Shards exceeds
// the number of keys in the range, it is clamped down to the range size so
// every task still covers at least one key — asking for more shards than
// there are keys cannot otherwise tile without an empty task.
func KeyspaceDecompose(j KeyspaceJob) []model.Task {
	if j.End <= j.Start {
		return nil
	}
	shards := j.Shards
	if shards <= 0 {
		shards = 1
	}
	total := j.End - j.Start
	if uint64(shards) > total {
		shards = int(total)
	}

	base := total / uint64(shards)
	rem := total % uint64(shards)

	tasks := make([]model.Task, 0, shards)
	cur := j.Start
	for i := 0; i < shards; i++ {
		size := base
		if uint64(i) < rem {
			size++ // spread the remainder over the first `rem` shards
		}
		next := cur + size
		tasks = append(tasks, model.Task{
			ID:    model.TaskID(fmt.Sprintf("%s-ks-%d", j.JobID, i)),
			JobID: j.JobID,
			Input: keyspaceInput{Start: cur, End: next}.bytes(),
		})
		cur = next
	}
	return tasks
}

// KeyspaceMerge implements first-hit merge: it returns the first result in
// rs with OK == true as a Done Aggregate (Value is that result's Output).
// "First" means first in the given slice — the caller is responsible for
// passing results in whatever order it wants treated as priority (e.g.
// arrival order). If no result is a hit, it returns Done == false with a
// nil Value.
//
// merge's signature (issue #2 / phase doc) takes only []TaskResult, which
// carries no JobID, so the returned Aggregate's JobID is left as the zero
// value; the caller (the job tracker, which already knows the JobID) fills
// it in.
func KeyspaceMerge(rs []model.TaskResult) model.Aggregate {
	for _, r := range rs {
		if r.OK {
			return model.Aggregate{Value: r.Output, Done: true}
		}
	}
	return model.Aggregate{Done: false}
}

// decodeKeyspaceHit decodes a keyspace-search hit's Value: the matching key
// as a big-endian uint64. This is the layout KeyspaceMerge's callers use for
// a winning TaskResult.Output (mirrored by hand for test workers in
// internal/e2e/wire.go's Encode/DecodeKeyspaceHit). An empty or malformed
// Value (no hit) decodes with ok == false.
func decodeKeyspaceHit(b []byte) (key uint64, ok bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

// KeyspaceCombine merges two keyspace-search partial Aggregates' Value: the
// deterministic winning hit. If only one side has a hit, that hit wins. If
// both have a hit, the smaller matching key wins — a fixed,
// order-independent tiebreak (ties can only occur if two shards somehow
// report the same key, in which case both sides' Value is identical anyway).
// If neither has a hit, the result carries no Value: the identity for this
// combine, matching the zero Aggregate.
//
// KeyspaceCombine only combines Value: it is aggregate.Merge's job (the
// caller) to combine JobID and Done, which follow the same rule at every
// template, not just this one. The returned Aggregate's JobID and Done are
// left at their zero values.
func KeyspaceCombine(a, b model.Aggregate) model.Aggregate {
	aKey, aHit := decodeKeyspaceHit(a.Value)
	bKey, bHit := decodeKeyspaceHit(b.Value)

	switch {
	case aHit && bHit:
		if bKey < aKey {
			return model.Aggregate{Value: b.Value}
		}
		return model.Aggregate{Value: a.Value}
	case aHit:
		return model.Aggregate{Value: a.Value}
	case bHit:
		return model.Aggregate{Value: b.Value}
	default:
		return model.Aggregate{}
	}
}
