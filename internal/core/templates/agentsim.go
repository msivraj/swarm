package templates

import (
	"fmt"

	"github.com/msivraj/swarm/internal/model"
)

// AgentSimJob describes an agent-based simulation job: simulate NumAgents
// agents, split into Partitions contiguous, disjoint agent-ID partitions,
// one per worker, coordinated by a message-passing driver (issue #63; phase
// doc §06) as agents in different partitions exchange messages mid-step.
//
// As with the other templates in this package, the phase doc pins the
// decompose/combine signatures but not AgentSimJob's concrete fields; this
// mirrors DistTrainingJob and GraphComputeJob's shape: an item count and a
// worker count. A caller parses this out of JobSpec.Params (e.g. "agents",
// "partitions").
type AgentSimJob struct {
	JobID      model.JobID
	NumAgents  uint64 // total number of agents in the simulation
	Partitions int    // number of workers to split the agents across
}

// AgentSimDecompose splits j's agents into j.Partitions contiguous,
// non-overlapping agent-ID partitions, one Task per partition (see
// partitionRange). Task.Input is the partition's [Start, End) agent-ID
// range, encoded as two big-endian uint64s (idRange.bytes).
//
// NumAgents==0, Partitions<=0, or Partitions>NumAgents are all rejected: a
// message-passing driver needs every worker it starts to own a live
// partition of agents to exchange messages with.
func AgentSimDecompose(job AgentSimJob) ([]model.Task, error) {
	ranges, err := partitionRange(job.NumAgents, job.Partitions, "agent-sim")
	if err != nil {
		return nil, err
	}

	tasks := make([]model.Task, 0, len(ranges))
	for i, r := range ranges {
		tasks = append(tasks, model.Task{
			ID:    model.TaskID(fmt.Sprintf("%s-as-%d", job.JobID, i)),
			JobID: job.JobID,
			Input: r.bytes(),
		})
	}
	return tasks, nil
}

// AgentSimCombine aggregates one message-passing step's per-partition
// simulation state: each entry of states is one worker's local state
// vector for its agent partition (e.g. per-bucket counts, summed resource
// levels — whatever fixed-shape numeric statistics the simulation tracks),
// encoded as consecutive big-endian float64s; the combined result is their
// elementwise sum (see sumFloat64Vectors), the same "aggregate state across
// workers" reduction dist-training's gradient all-reduce uses, applied here
// to simulation state instead of gradients.
func AgentSimCombine(states [][]byte) []byte {
	return sumFloat64Vectors(states)
}
