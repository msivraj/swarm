package controlplane

import (
	"fmt"
	"strconv"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// The Params keys a dist-training Barrier gang's SubmitJob sets so
// activateCoupledCellLocked can decompose it: samples/shards feed
// templates.DistTrainingDecompose exactly as gangMinMembersParam feeds
// admission.AdmitGang (see gang.go); steps/k are D6/#96's completion and
// checkpoint-cadence parameters, threaded straight through to the
// CellAssignment (#101) an agent polls for.
const (
	distTrainingSamplesParam = "samples"
	distTrainingShardsParam  = "shards"
	distTrainingStepsParam   = "steps"
	distTrainingKParam       = "k"
)

// agentAddr is the P2 raft/cell-leader listener pair an agent advertises at
// JoinAgent (#101's JoinAgentRequest.raft_addr/cell_leader_addr): the TCP
// address its raft NetworkTransport listens on and its CellLeader gRPC
// address. Both empty for a P0/P1 agent that never joins a coupled cell.
type agentAddr struct {
	raftAddr       string
	cellLeaderAddr string
}

// activateCoupledCellLocked turns an admitted Barrier gang (g.Kind ==
// admission.Place) into a runnable coupled cell: it decomposes spec's
// dataset into one shard per member agent of the cell g was placed on
// (templates.DistTrainingDecompose) and stores one CellAssignment (#101,
// see transport.CellAssignmentResponse) per agent, served the next time
// that agent polls the CellAssignment RPC — the only channel that tells an
// agent it is now in a coupled cell (activating #96/#102).
//
// It is a no-op — not an error — for anything that is not a well-formed
// dist-training gang request: a Barrier gang submitted without "samples"/
// "shards" Params (P0/P1's existing gang tests, see gang_test.go, never set
// them) leaves nothing to activate and must not fail admission, matching
// this ticket's "min_members==0/Independent jobs are untouched" guard,
// extended here to every Coupling other than Barrier and every Barrier gang
// that never asked for cell activation in the first place.
//
// The coupled cell this activates is exactly the cell g's single Assignment
// names (the ticket's "the assigned cell"): a raft cluster is one physical
// cell's agents, so a gang whose reservation spans more than one cell — a
// case admission.AdmitGang's general first-fit CAN produce, see
// TestGangQueuedThenAdmittedOnCapacityChange in gang_test.go — is out of
// this ticket's scope and reported as an error rather than silently
// activating a fraction of it. Callers must hold s.mu.
func (s *Server) activateCoupledCellLocked(spec model.JobSpec, g admission.Gang) error {
	if spec.Coupling != model.Barrier {
		return nil
	}

	samples, hasSamples := parseUintParam(spec.Params, distTrainingSamplesParam)
	shards, hasShards := parseIntParam(spec.Params, distTrainingShardsParam)
	if !hasSamples || !hasShards {
		return nil
	}
	steps, _ := parseIntParam(spec.Params, distTrainingStepsParam)
	k, _ := parseIntParam(spec.Params, distTrainingKParam)

	if len(g.Assignments) != 1 {
		return fmt.Errorf("activate coupled cell for job %s: gang spans %d cells, want exactly 1", spec.ID, len(g.Assignments))
	}
	cell := g.Assignments[0].Cell

	tasks, err := templates.DistTrainingDecompose(templates.DistTrainingJob{
		JobID:   spec.ID,
		Samples: samples,
		Shards:  shards,
	})
	if err != nil {
		return err
	}

	peers := sortedAgents(s.cellAgents[cell])
	if len(peers) == 0 {
		return fmt.Errorf("activate coupled cell for job %s: cell %s has no member agents", spec.ID, cell)
	}
	if len(tasks) != len(peers) {
		return fmt.Errorf("activate coupled cell for job %s: %d shard(s) for %d agent(s) in cell %s", spec.ID, len(tasks), len(peers), cell)
	}

	// sortedAgents returns peers in lexicographic order, so peers[0] is
	// deterministically the lowest agent id — the ticket's "e.g. lowest
	// agent id" bootstrap tie-break.
	bootstrap := peers[0]

	wirePeers := make([]*transport.CellPeer, len(peers))
	for i, agent := range peers {
		addr := s.agentAddrs[agent]
		wirePeers[i] = &transport.CellPeer{
			AgentId:        agent,
			RaftAddr:       addr.raftAddr,
			CellLeaderAddr: addr.cellLeaderAddr,
		}
	}

	for i, agent := range peers {
		s.cellAssignments[agent] = &transport.CellAssignmentResponse{
			HasAssignment: true,
			JobId:         string(spec.ID),
			WorkerId:      string(tasks[i].ID),
			ShardInput:    tasks[i].Input,
			K:             int32(k),
			MinMembers:    int32(spec.MinMembers),
			Steps:         int32(steps),
			Bootstrap:     agent == bootstrap,
			Peers:         wirePeers,
		}
	}
	return nil
}

// surfaceActivationFailureLocked makes an activateCoupledCellLocked error
// visible without touching this gang's already-committed admission
// decision (gang.go's admitGangLocked doc explains why that decision cannot
// change here): it logs a structured line via cfg.Logger — spec.ID plus the
// activation error — and records the same reason as spec's Aggregate,
// reusing onCoupledComplete's existing store surface (store.PutAggregate)
// rather than a new proto field, so JobStatus/Ps read it back through
// JobStatusResponse.Aggregate exactly as they would a completed job's
// result. Done stays false: activation, not the job itself, failed, and
// nothing will ever run to complete it, so it must not read as finished.
// Callers must hold s.mu.
func (s *Server) surfaceActivationFailureLocked(spec model.JobSpec, activateErr error) {
	s.cfg.Logger("controlplane: activate coupled cell for job %s: %v", spec.ID, activateErr)

	reason := fmt.Sprintf("activation failed: %v", activateErr)
	agg := model.Aggregate{JobID: spec.ID, Value: []byte(reason), Done: false}
	if err := s.store.PutAggregate(agg); err != nil {
		s.cfg.Logger("controlplane: record activation failure for job %s: %v", spec.ID, err)
	}
}

// onCoupledComplete stores combined — the coupled cell's elected leader's
// final, all-reduced gradient (D6) — as jobID's Aggregate and marks it
// Done, so JobStatus flips exactly as it does for a P0/P1 job's normal
// roll-up (maybeRollup's self-sink branch). ReportResult calls this in
// place of its usual per-task path once it recognizes the reported TaskId
// as a gang job id rather than any TaskID this server ever enqueued (see
// ReportResult's doc).
func (s *Server) onCoupledComplete(jobID model.JobID, combined []byte) error {
	return s.store.PutAggregate(model.Aggregate{JobID: jobID, Value: combined, Done: true})
}

// parseUintParam and parseIntParam extract a positive numeric Params value,
// mirroring parseMinMembers's "absent, empty, non-numeric, non-positive all
// mean not set" convention (see gang.go): distTrainingSamplesParam widens to
// uint64 (a sample count plausibly exceeds a 32-bit range; matches
// templates.DistTrainingJob.Samples), the rest stay int (matching every
// other typed Params field this shell already parses this way).
func parseUintParam(params map[string]string, key string) (uint64, bool) {
	v, ok := params[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || n == 0 {
		return 0, false
	}
	return n, true
}

func parseIntParam(params map[string]string, key string) (int, bool) {
	v, ok := params[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
