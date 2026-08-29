// barrier_disttraining_test.go is the P2 exit criterion (issue #99): a
// runnable Barrier dist-training job through real swarmd-style agents and a
// real controlplane.Server over gRPC, coordinated by a real agent-hosted
// per-cell leader elected over multi-node raft (issue #94/#95/#98/#102).
//
// Unlike TestKeyspaceSearch/TestMonteCarlo (this package's P0/P1 exit
// criteria, e2e_test.go), a coupled cell's agents run TWO extra loops beyond
// the plain registration/runner pair: the barrier follower (#96, execing the
// dist-training worker via buildWorker) and the agent-hosted cell leader
// (#102, joining the cell's real raft cluster and, whenever elected,
// hosting the production driver Loop). Both need CONCRETE (non-":0") raft
// and cell-leader listen addresses reserved up front — issue #109 fixed the
// production bug this ticket's own prior attempt surfaced: execJoinCell now
// advertises those addresses at the agent's very first JoinAgent, before any
// CellAssignment exists to gate advertising them post-hoc, so
// advertised == actual from the start (see registration.go's doc).
package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/agent"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// freeTCPAddr reserves a free loopback TCP port by binding to ":0" and
// immediately closing the listener, returning the resolved address — the
// same "grab a concrete address up front" trick internal/shell/cell's own
// production-raft tests use (raftnode_test.go's freeTCPAddr), needed here
// because CellLeaderConfig.RaftListen (and, per this ticket, FollowerConfig.
// Listen) must be a concrete host:port that the agent advertises BEFORE it
// ever binds it, not an ephemeral ":0" resolved only after listening.
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free TCP port: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}
	return addr
}

// fastRaftConfig tightens raft's election/heartbeat timers so a test
// cluster elects reasonably quickly without being so aggressive that
// ordinary scheduling jitter (a busy CI box, -race's overhead, three real
// subprocess execs per barrier step sharing the machine with everything
// else this suite runs) trips a spurious election and never lets the
// cluster settle — internal/shell/cell/raftnode_test.go's tcpRaftConfig
// uses tighter timers (300ms) than this because that suite's nodes do
// nothing but replicate; this ticket's nodes also run a real driver Loop
// and exec real worker processes on the elected leader's critical path, so
// this config trades a bit of election latency for headroom, well within
// the 15-30s settle budget the ticket calls for.
func fastRaftConfig() *raft.Config {
	c := raft.DefaultConfig()
	c.HeartbeatTimeout = 1 * time.Second
	c.ElectionTimeout = 1 * time.Second
	c.LeaderLeaseTimeout = 500 * time.Millisecond
	c.CommitTimeout = 50 * time.Millisecond
	return c
}

// startCoupledAgent constructs and runs an Agent configured for P2 coupled-
// cell duty (#96 follower + #102 leader-host, on top of the ordinary P0
// registration/runner loops): it execs worker for every barrier step
// (Process.Argv, the same field the follower's default Worker execs) and
// joins the cell's raft cluster on raftAddr, bootstrapping iff the control
// plane's CellAssignment says so.
func startCoupledAgent(t *testing.T, id string, dial agent.Dialer, worker, raftAddr, followerAddr string, raftCfg *raft.Config) {
	t.Helper()

	a := agent.New(agent.Config{
		AgentID: id,
		Region:  "us",
		Caps:    1,
		Targets: []string{"bufnet"},
		Dialer:  dial,
		Jitter:  func() float64 { return 0 },
		// Deliberately looser than e2e_test.go's plain-agent 20ms/10ms: a
		// coupled agent runs three extra chatty loops (CellAssignment
		// polling, the raft cluster's own election/heartbeat timers, real
		// worker-process AssignWork/StepReport round trips) sharing this
		// same test process's goroutine scheduler, and raft's election
		// timing is comparatively fragile — tight P0-style polling here
		// starves it under load and produces spurious elections that never
		// let the cluster settle (see fastRaftConfig's doc). Still far
		// faster than production defaults.
		HeartbeatInterval: 200 * time.Millisecond,
		PullInterval:      100 * time.Millisecond,
		Process:           agent.ProcessSpec{Argv: []string{worker}},
		Follower: agent.FollowerConfig{
			Listen: followerAddr,
		},
		CellLeader: agent.CellLeaderConfig{
			RaftListen:  raftAddr,
			RaftDataDir: t.TempDir(),
			RaftConfig:  raftCfg,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			requireCleanAgentShutdown(t, id, err)
		case <-time.After(10 * time.Second):
			t.Errorf("agent %s Run did not return within 10s of cancellation", id)
		}
	})
}

// requireCleanAgentShutdown accepts nil, a plain context.Canceled, or a gRPC
// status wrapping Canceled: a coupled agent's follower/leader loops can have
// a StepReport/AssignWork RPC still unwinding through the leader's dial
// cache at the exact moment the test cancels ctx, which can surface either
// shape depending on timing — the same tolerance
// internal/shell/agent/leader_test.go's requireCleanShutdown documents.
func requireCleanAgentShutdown(t *testing.T, who string, err error) {
	t.Helper()
	if err == nil || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return
	}
	t.Errorf("agent %s Run: %v", who, err)
}

// waitForCoupledJobDone polls JobStatus until Done, like waitForJobDone, but
// with a longer, configurable timeout: a coupled cell's job must first form
// a real multi-node raft cluster and elect a leader (real TCP election,
// documented as ~1s+ under -race/CI load) before it can even start step 0,
// on top of running every step's real worker-process exec end to end.
func waitForCoupledJobDone(t *testing.T, client transport.ControlPlaneClient, jobID string, timeout time.Duration) *transport.JobStatusResponse {
	t.Helper()

	var status *transport.JobStatusResponse
	waitFor(t, timeout, func() bool {
		resp, err := client.JobStatus(context.Background(), &transport.JobStatusRequest{JobId: jobID})
		if err != nil {
			t.Fatalf("JobStatus: %v", err)
		}
		status = resp
		return status.GetDone()
	})
	return status
}

// expectedDistTrainingAggregate independently folds ExpectedAllReducedGradient
// over steps sequential barrier rounds, feeding each step's combined result
// back in as the next step's incoming gradient — the same recurrence
// LeaderHost's production run drives via real AssignWork/StepReport RPCs
// (onBecomeLeader kicks step 0 with no incoming gradient; wrapExec threads
// each step's Combined bytes into the next AssignWork's payload) — and
// returns the LAST step's combined result, exactly what the terminal
// OpRelease (Step == steps) reports as the job's final Aggregate (see
// LeaderHost.wrapExec's doc). This never calls the leader's own code path
// (LeaderHost, cell.Loop, raft) or even templates.DistTrainingCombine
// directly: it re-derives the whole reduction from DTPartial alone, so a bug
// in the live all-reduce, the raft-replicated command log, or step
// sequencing fails the comparison against it.
func expectedDistTrainingAggregate(shards [][2]uint64, steps int) []float64 {
	var incoming []float64
	for step := 0; step < steps; step++ {
		incoming = ExpectedAllReducedGradient(shards, uint64(step), incoming)
	}
	return incoming
}

// TestBarrierDistTraining is the P2 daemon-assembly exit criterion (issue
// #99): a control plane and a cell of coupled agents form a real multi-node
// raft cluster, elect one leader among themselves, and run a Barrier
// dist-training job to completion — N steps, each all-reducing every
// follower's real worker-process output — with the final Aggregate equal to
// an independently computed expected result.
func TestBarrierDistTraining(t *testing.T) {
	worker := buildWorker(t, "./workers/disttraining", "disttraining")

	clock := &testClock{}
	client, dial, teardown := newControlPlane(t, fastConfig(), clock)
	defer teardown()

	const (
		numAgents  = 3
		samples    = uint64(300)
		shards     = numAgents
		steps      = 3
		checkpoint = 1
		minMembers = numAgents
	)

	for i := 0; i < numAgents; i++ {
		id := fmt.Sprintf("dt-agent-%d", i)
		// A fresh *raft.Config PER AGENT, not one shared across the loop:
		// cell.NewNode mutates its RaftConfig argument in place
		// (rc.LocalID = raft.ServerID(cfg.ID)) before handing it to
		// raft.NewRaft, and all three agents' joinCellRaft calls run
		// concurrently once their CellAssignments land — sharing one
		// *raft.Config across them would race that mutation across
		// goroutines, letting one node's raft.Raft momentarily observe (or
		// even permanently copy, via raft's own internal Config snapshot)
		// a DIFFERENT node's LocalID.
		raftCfg := fastRaftConfig()
		raftAddr := freeTCPAddr(t)
		followerAddr := freeTCPAddr(t)
		startCoupledAgent(t, id, dial, worker, raftAddr, followerAddr, raftCfg)
	}

	ctx := context.Background()

	// Every agent must have joined (and so advertised its concrete raft/
	// cell-leader addresses, #109) before SubmitJob, so
	// activateCoupledCellLocked's raft peer set is fully populated the
	// moment the gang is placed.
	waitFor(t, 10*time.Second, func() bool {
		resp, err := client.Ps(ctx, &transport.PsRequest{})
		if err != nil {
			t.Fatalf("Ps: %v", err)
		}
		return resp.GetMachines() == numAgents && resp.GetCells() == 1
	})

	submitResp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "dist-training",
		Coupling: transport.Coupling_COUPLING_BARRIER,
		Params: map[string]string{
			"min_members": strconv.Itoa(minMembers),
			"samples":     strconv.FormatUint(samples, 10),
			"shards":      strconv.Itoa(shards),
			"steps":       strconv.Itoa(steps),
			"k":           strconv.Itoa(checkpoint),
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := submitResp.GetJobId()
	if jobID == "" {
		t.Fatalf("SubmitJob returned empty job id")
	}

	// Bounded but generous settle window: real TCP raft election plus
	// `steps` full rounds of real worker-process AssignWork/StepReport.
	status := waitForCoupledJobDone(t, client, jobID, 30*time.Second)
	if !status.GetDone() {
		t.Fatalf("JobStatus.Done = false, want true")
	}

	got, ok := DecodeGradient(status.GetAggregate())
	if !ok {
		t.Fatalf("Aggregate = %x, not a valid gradient", status.GetAggregate())
	}

	// Recover the exact shard [start,end) ranges activateCoupledCellLocked
	// handed out, via the same real templates.DistTrainingDecompose call it
	// made (JobID only affects generated TaskIDs, never the shard math) —
	// idRange's wire layout is byte-identical to keyspace's, see
	// DecodeKeyspaceRange's doc.
	tasks, err := templates.DistTrainingDecompose(templates.DistTrainingJob{
		JobID: model.JobID(jobID), Samples: samples, Shards: shards,
	})
	if err != nil {
		t.Fatalf("DistTrainingDecompose: %v", err)
	}
	shardRanges := make([][2]uint64, len(tasks))
	for i, task := range tasks {
		start, end, ok := DecodeKeyspaceRange(task.Input)
		if !ok {
			t.Fatalf("shard %d Input = %x, not a valid 16-byte range", i, task.Input)
		}
		shardRanges[i] = [2]uint64{start, end}
	}

	// activateCoupledCellLocked assigns tasks[i] to sortedAgents(peers)[i]
	// (lexicographic), and gatheredPayloads (internal/shell/cell/combine.go)
	// combines each step's partials sorted by worker id — so with agent ids
	// "dt-agent-0".."dt-agent-{n-1}" (lexicographic order == shard index
	// order), shardRanges is already in the exact order production sums in,
	// making the float64 accumulation bit-for-bit identical, not just
	// numerically close.
	want := expectedDistTrainingAggregate(shardRanges, steps)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Aggregate = %v, want %v (expectedDistTrainingAggregate)", got, want)
	}
}
