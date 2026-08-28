package templates

import (
	"encoding/binary"
	"fmt"

	"github.com/msivraj/swarm/internal/model"
)

// GraphComputeJob describes a graph-compute job (Pregel-style, bulk
// synchronous supersteps): compute over a graph of NumVertices vertices,
// split into Partitions contiguous, disjoint vertex-ID partitions, one per
// worker, coordinated by a leader driver (issue #63; phase doc §06): the
// leader combines every partition's superstep result and decides whether
// another superstep runs.
//
// As with the other templates in this package, the phase doc pins the
// decompose/combine signatures but not GraphComputeJob's concrete fields;
// this mirrors DistTrainingJob's shape: an item count and a worker count. A
// caller parses this out of JobSpec.Params (e.g. "vertices", "partitions").
type GraphComputeJob struct {
	JobID       model.JobID
	NumVertices uint64 // total number of vertices in the graph
	Partitions  int    // number of workers to split the vertices across
}

// GraphComputeDecompose splits j's vertices into j.Partitions contiguous,
// non-overlapping vertex-ID partitions, one Task per partition (see
// partitionRange). Task.Input is the partition's [Start, End) vertex-ID
// range, encoded as two big-endian uint64s (idRange.bytes).
//
// NumVertices==0, Partitions<=0, or Partitions>NumVertices are all
// rejected: a leader driver needs every worker it starts to own a live
// partition to compute a superstep over.
func GraphComputeDecompose(job GraphComputeJob) ([]model.Task, error) {
	ranges, err := partitionRange(job.NumVertices, job.Partitions, "graph-compute")
	if err != nil {
		return nil, err
	}

	tasks := make([]model.Task, 0, len(ranges))
	for i, r := range ranges {
		tasks = append(tasks, model.Task{
			ID:    model.TaskID(fmt.Sprintf("%s-gc-%d", job.JobID, i)),
			JobID: job.JobID,
			Input: r.bytes(),
		})
	}
	return tasks, nil
}

// graphSuperstepPartial is the wire layout of one partition's per-superstep
// result in GraphComputeCombine's input: an 8-byte big-endian count of
// vertices in that partition still active for the next superstep (the
// leader's termination signal — a superstep with zero active vertices
// anywhere means the computation has converged), followed by that
// partition's pending cross-partition messages, verbatim (this template
// treats a partition's message payload as opaque bytes; the graph
// algorithm's own message encoding is a shell/agent concern).
const graphActiveCountLen = 8

// GraphComputeCombine combines one superstep's per-partition results for
// the leader: each entry of partial is one partition's graphSuperstepPartial
// (an 8-byte active-vertex count, then its opaque outgoing-message bytes).
// The combined output is the same layout: the total active-vertex count
// summed across every partition (so the leader can check "count == 0" to
// decide the computation has converged and no further superstep is needed),
// followed by every partition's message bytes packed with packChunks (in
// the given partition order) — the combined message stream released as the
// input to the next superstep's tasks, from which each partition reads the
// messages addressed to its own vertices.
//
// An entry shorter than graphActiveCountLen is malformed and is skipped
// entirely — mirroring MonteCarloMerge's precedent of a bad block
// contributing nothing rather than aborting the whole combine. A nil or
// empty partial combines to a zero active count and an empty message
// stream.
func GraphComputeCombine(partial [][]byte) []byte {
	var totalActive uint64
	messages := make([][]byte, 0, len(partial))

	for _, p := range partial {
		if len(p) < graphActiveCountLen {
			continue
		}
		totalActive += binary.BigEndian.Uint64(p[:graphActiveCountLen])
		messages = append(messages, p[graphActiveCountLen:])
	}

	out := make([]byte, graphActiveCountLen)
	binary.BigEndian.PutUint64(out, totalActive)
	return append(out, packChunks(messages)...)
}

// EncodeGraphSuperstepPartial builds one partition's Task-result-side
// input to GraphComputeCombine: activeVertices is that partition's count of
// vertices still active after the superstep, and messages is its pending
// outgoing-message bytes. Exported so a worker's shell has a matching
// encoder for the layout GraphComputeCombine decodes.
func EncodeGraphSuperstepPartial(activeVertices uint64, messages []byte) []byte {
	out := make([]byte, graphActiveCountLen, graphActiveCountLen+len(messages))
	binary.BigEndian.PutUint64(out, activeVertices)
	return append(out, messages...)
}

// DecodeGraphSuperstepCombined decodes GraphComputeCombine's output back
// into the total active-vertex count and the per-partition message chunks
// packed into it (in the order they were combined) — the leader-side
// inverse, and the round-trip check the tests in graphcompute_test.go use
// to assert combine's output.
func DecodeGraphSuperstepCombined(combined []byte) (activeVertices uint64, messages [][]byte, ok bool) {
	if len(combined) < graphActiveCountLen {
		return 0, nil, false
	}
	activeVertices = binary.BigEndian.Uint64(combined[:graphActiveCountLen])
	messages, ok = unpackChunks(combined[graphActiveCountLen:])
	if !ok {
		return 0, nil, false
	}
	return activeVertices, messages, true
}
