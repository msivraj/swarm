package templates

import (
	"fmt"

	"github.com/msivraj/swarm/internal/model"
)

// DistTrainingJob describes a distributed-training job: train over a
// dataset of Samples examples, split into Shards contiguous, disjoint
// shards, one per worker, coordinated by a barrier driver (issue #63; phase
// doc §06).
//
// As with KeyspaceJob and MCJob, the phase doc pins the decompose/combine
// signatures but not DistTrainingJob's concrete fields; this is the minimal,
// documented shape: a sample count and a shard count, the same "how many
// items, how many workers" pair every partition-based template here needs.
// A caller parses this out of JobSpec.Params (e.g. "samples", "shards").
type DistTrainingJob struct {
	JobID   model.JobID
	Samples uint64 // total training samples in the dataset
	Shards  int    // number of workers to split the dataset across
}

// DistTrainingDecompose splits j's dataset into j.Shards contiguous,
// non-overlapping sample-index shards, one Task per shard, using the same
// even-split-with-remainder-tiling rule as KeyspaceDecompose (see
// partitionRange). Task.Input is the shard's [Start, End) sample-index
// range, encoded as two big-endian uint64s (idRange.bytes).
//
// Unlike KeyspaceDecompose, invalid input is rejected with an error rather
// than silently clamped: a barrier driver needs every worker it starts to
// actually train on a live shard, so Samples==0, Shards<=0, or
// Shards>Samples are all errors (see partitionRange's doc).
func DistTrainingDecompose(job DistTrainingJob) ([]model.Task, error) {
	ranges, err := partitionRange(job.Samples, job.Shards, "dist-training")
	if err != nil {
		return nil, err
	}

	tasks := make([]model.Task, 0, len(ranges))
	for i, r := range ranges {
		tasks = append(tasks, model.Task{
			ID:    model.TaskID(fmt.Sprintf("%s-dt-%d", job.JobID, i)),
			JobID: job.JobID,
			Input: r.bytes(),
		})
	}
	return tasks, nil
}

// DistTrainingCombine all-reduces one barrier step's per-worker gradients:
// each entry of gradients is one worker's gradient vector, encoded as
// consecutive big-endian float64s (one per model parameter); the combined
// result is their elementwise sum (see sumFloat64Vectors) — the standard
// all-reduce reducer for synchronous data-parallel training, where every
// worker's locally-computed gradient over its shard is summed before the
// barrier releases the next step with the combined gradient.
//
// This is a documented choice (issue #63 asks for "e.g. elementwise sum"):
// callers that want a mean instead of a sum can divide by len(gradients)
// themselves, since that scalar rescale does not change which reduction is
// order-independent.
func DistTrainingCombine(gradients [][]byte) []byte {
	return sumFloat64Vectors(gradients)
}
