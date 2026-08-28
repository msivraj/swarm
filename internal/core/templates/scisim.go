package templates

import (
	"fmt"

	"github.com/msivraj/swarm/internal/model"
)

// SciSimJob describes a scientific-simulation job: simulate a spatial grid
// of GridCells cells (flattened to a single zero-based axis — a 2D/3D
// mesh's own tiling into that axis is a planner/shell concern, not this
// package's), split into Domains contiguous, disjoint spatial subdomains,
// one per worker, coordinated by a barrier driver (issue #63; phase doc
// §06): every domain computes its interior for a step, then all domains
// exchange the values along their shared boundaries before the next step.
//
// As with the other templates in this package, the phase doc pins the
// decompose/combine signatures but not SciSimJob's concrete fields; this
// mirrors DistTrainingJob's shape: a cell count and a domain count. A
// caller parses this out of JobSpec.Params (e.g. "cells", "domains").
type SciSimJob struct {
	JobID     model.JobID
	GridCells uint64 // total number of cells in the (flattened) grid
	Domains   int    // number of workers to split the grid across
}

// SciSimDecompose splits j's grid into j.Domains contiguous, non-overlapping
// spatial subdomains, one Task per domain (see partitionRange). Task.Input
// is the domain's [Start, End) cell-index range, encoded as two big-endian
// uint64s (idRange.bytes) — a worker computes its interior cells and owns
// the two boundary cells at Start and End-1 for the exchange SciSimCombine
// performs each step.
//
// GridCells==0, Domains<=0, or Domains>GridCells are all rejected: a
// barrier driver needs every worker it starts to own a live subdomain to
// step and exchange boundaries with its neighbors.
func SciSimDecompose(job SciSimJob) ([]model.Task, error) {
	ranges, err := partitionRange(job.GridCells, job.Domains, "sci-sim")
	if err != nil {
		return nil, err
	}

	tasks := make([]model.Task, 0, len(ranges))
	for i, r := range ranges {
		tasks = append(tasks, model.Task{
			ID:    model.TaskID(fmt.Sprintf("%s-ss-%d", job.JobID, i)),
			JobID: job.JobID,
			Input: r.bytes(),
		})
	}
	return tasks, nil
}

// SciSimCombine performs one barrier step's boundary exchange: each entry of
// boundaries is one domain's boundary payload for the step just completed
// (the values along the edges its neighbors need to compute their next
// interior), in the same domain order SciSimDecompose assigned Tasks in.
// The combined result packs every domain's payload (packChunks) into one
// exchange packet the barrier releases to every domain for its next step —
// each domain reads the whole packet and picks out its neighbors' entries by
// position.
//
// Unlike the all-reduce combines (DistTrainingCombine, AgentSimCombine),
// boundary exchange is a per-domain concatenation, not a fold to a single
// numeric value, so its result is order-sensitive: it is the caller's job
// (the barrier driver, which already tracks step membership by domain) to
// pass boundaries in a stable domain order, not this function's to sort
// them. A nil or empty boundaries yields an empty (non-nil) packet.
func SciSimCombine(boundaries [][]byte) []byte {
	return packChunks(boundaries)
}

// DecodeSciSimExchange decodes a packet SciSimCombine produced back into
// its per-domain boundary payloads, in the same order they were combined —
// the shell/agent-side inverse a worker uses to read its neighbors'
// boundaries out of the released packet, and the round-trip check the tests
// in scisim_test.go use to assert combine's output.
func DecodeSciSimExchange(packet []byte) ([][]byte, bool) {
	return unpackChunks(packet)
}
