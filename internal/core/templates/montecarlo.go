package templates

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/msivraj/swarm/internal/model"
)

// MCJob describes a monte-carlo job: run Trials total simulation trials,
// split into independent blocks of BlockSize trials each, seeded from
// BaseSeed so the whole decomposition is reproducible.
//
// As with KeyspaceJob, the phase doc does not pin MCJob's concrete fields
// (issue #2 "Notes / ambiguities"); this is the minimal, documented shape:
// a total trial count, a block size to chop it into, and a base seed each
// block's seed is deterministically derived from. A caller parses this out
// of JobSpec.Params (e.g. "trials", "blockSize", "seed").
type MCJob struct {
	JobID     model.JobID
	Trials    int64 // total trials to run across all tasks
	BlockSize int64 // trials per task
	BaseSeed  int64 // seed the per-block seeds are derived from
}

// mcTaskInput is the wire layout of Task.Input for a monte-carlo task: the
// seed to run with and the number of trials to run, as big-endian int64s.
type mcTaskInput struct {
	Seed   int64
	Trials int64
}

func (b mcTaskInput) bytes() []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[0:8], uint64(b.Seed))
	binary.BigEndian.PutUint64(out[8:16], uint64(b.Trials))
	return out
}

func decodeMCTaskInput(b []byte) (mcTaskInput, bool) {
	if len(b) != 16 {
		return mcTaskInput{}, false
	}
	return mcTaskInput{
		Seed:   int64(binary.BigEndian.Uint64(b[0:8])),
		Trials: int64(binary.BigEndian.Uint64(b[8:16])),
	}, true
}

// mcResult is the wire layout of TaskResult.Output for a monte-carlo task:
// the count of trials actually run and their sum and sum-of-squares, as
// big-endian values — enough to merge blocks into a combined mean and
// variance without re-reading individual trial outcomes.
type mcResult struct {
	Count int64
	Sum   float64
	SumSq float64
}

func encodeMCResult(r mcResult) []byte {
	out := make([]byte, 24)
	binary.BigEndian.PutUint64(out[0:8], uint64(r.Count))
	binary.BigEndian.PutUint64(out[8:16], math.Float64bits(r.Sum))
	binary.BigEndian.PutUint64(out[16:24], math.Float64bits(r.SumSq))
	return out
}

func decodeMCResult(b []byte) (mcResult, bool) {
	if len(b) != 24 {
		return mcResult{}, false
	}
	return mcResult{
		Count: int64(binary.BigEndian.Uint64(b[0:8])),
		Sum:   math.Float64frombits(binary.BigEndian.Uint64(b[8:16])),
		SumSq: math.Float64frombits(binary.BigEndian.Uint64(b[16:24])),
	}, true
}

// mcAggregate is the wire layout of Aggregate.Value for a monte-carlo job:
// the combined trial count, sum, mean, and population variance across every
// merged block.
type mcAggregate struct {
	Count    int64
	Sum      float64
	Mean     float64
	Variance float64
}

func (a mcAggregate) bytes() []byte {
	out := make([]byte, 32)
	binary.BigEndian.PutUint64(out[0:8], uint64(a.Count))
	binary.BigEndian.PutUint64(out[8:16], math.Float64bits(a.Sum))
	binary.BigEndian.PutUint64(out[16:24], math.Float64bits(a.Mean))
	binary.BigEndian.PutUint64(out[24:32], math.Float64bits(a.Variance))
	return out
}

func decodeMCAggregate(b []byte) (mcAggregate, bool) {
	if len(b) != 32 {
		return mcAggregate{}, false
	}
	return mcAggregate{
		Count:    int64(binary.BigEndian.Uint64(b[0:8])),
		Sum:      math.Float64frombits(binary.BigEndian.Uint64(b[8:16])),
		Mean:     math.Float64frombits(binary.BigEndian.Uint64(b[16:24])),
		Variance: math.Float64frombits(binary.BigEndian.Uint64(b[24:32])),
	}, true
}

// MonteCarloDecompose splits j.Trials into blocks of j.BlockSize trials
// each (the last block holding the remainder), one Task per block. Each
// block's seed is BaseSeed + its block index, so every block gets a
// distinct, reproducible seed with no randomness drawn here.
//
// Trials <= 0 or BlockSize <= 0 yields no tasks — there is nothing to run.
func MonteCarloDecompose(j MCJob) []model.Task {
	if j.Trials <= 0 || j.BlockSize <= 0 {
		return nil
	}

	numBlocks := (j.Trials + j.BlockSize - 1) / j.BlockSize // ceil
	tasks := make([]model.Task, 0, numBlocks)
	remaining := j.Trials
	for i := int64(0); i < numBlocks; i++ {
		n := j.BlockSize
		if n > remaining {
			n = remaining
		}
		tasks = append(tasks, model.Task{
			ID:    model.TaskID(fmt.Sprintf("%s-mc-%d", j.JobID, i)),
			JobID: j.JobID,
			Input: mcTaskInput{Seed: j.BaseSeed + i, Trials: n}.bytes(),
		})
		remaining -= n
	}
	return tasks
}

// MonteCarloMerge sums each block's TaskResult into a combined count, sum,
// mean, and population variance. Results with OK == false are skipped —
// a failed block contributes no trials to the total, since its Output
// cannot be trusted. A result whose Output is not a validly-shaped mcResult
// is likewise skipped (also treated as "not usable"), rather than merge
// erroring out for one bad block.
//
// Merge is a straight reduction over whatever results it is given, so it
// always returns Done == true — unlike KeyspaceMerge, whose Done tracks
// whether a hit was found, MonteCarloMerge's Done simply reports that the
// merge over the given inputs completed (the job tracker calls it once it
// has collected the results it considers final).
//
// The empty-input case (rs == nil or all skipped) returns a zeroed
// Aggregate with Count == 0 and Done == true.
func MonteCarloMerge(rs []model.TaskResult) model.Aggregate {
	var count int64
	var sum, sumSq float64

	for _, r := range rs {
		if !r.OK {
			continue
		}
		block, ok := decodeMCResult(r.Output)
		if !ok {
			continue
		}
		count += block.Count
		sum += block.Sum
		sumSq += block.SumSq
	}

	agg := mcAggregate{Count: count, Sum: sum}
	if count > 0 {
		mean := sum / float64(count)
		agg.Mean = mean
		agg.Variance = sumSq/float64(count) - mean*mean
	}
	return model.Aggregate{Value: agg.bytes(), Done: true}
}

// sumSqOf recovers a's sufficient-statistic sum-of-squares from its stored
// mean and variance: since Variance == SumSq/Count - Mean^2, SumSq ==
// Count*(Variance + Mean^2). A zero-count aggregate (the identity) has no
// sum-of-squares to recover.
func sumSqOf(a mcAggregate) float64 {
	if a.Count == 0 {
		return 0
	}
	return float64(a.Count) * (a.Variance + a.Mean*a.Mean)
}

// MonteCarloCombine merges two monte-carlo partial Aggregates' Value on
// sufficient statistics: it recovers each side's sum-of-squares (sumSqOf),
// adds Count, Sum, and the recovered SumSq elementwise, then re-derives Mean
// and Variance from the combined totals — the same reduction MonteCarloMerge
// performs over raw blocks, so re-merging an already-merged partial is
// lossless, and associative up to floating-point rounding. A malformed or
// empty Value (e.g. the zero Aggregate) decodes as the zero mcAggregate
// (Count 0), the identity for this combine.
//
// MonteCarloCombine only combines Value: it is aggregate.Merge's job (the
// caller) to combine JobID and Done, which follow the same rule at every
// template, not just this one. The returned Aggregate's JobID and Done are
// left at their zero values.
func MonteCarloCombine(a, b model.Aggregate) model.Aggregate {
	aAgg, ok := decodeMCAggregate(a.Value)
	if !ok {
		aAgg = mcAggregate{}
	}
	bAgg, ok := decodeMCAggregate(b.Value)
	if !ok {
		bAgg = mcAggregate{}
	}

	count := aAgg.Count + bAgg.Count
	sum := aAgg.Sum + bAgg.Sum
	sumSq := sumSqOf(aAgg) + sumSqOf(bAgg)

	out := mcAggregate{Count: count, Sum: sum}
	if count > 0 {
		mean := sum / float64(count)
		out.Mean = mean
		out.Variance = sumSq/float64(count) - mean*mean
	}
	return model.Aggregate{Value: out.bytes()}
}
