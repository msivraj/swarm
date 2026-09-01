// Package controlplane is the P0 control-plane shell: a gRPC server
// (transport.ControlPlaneServer) whose handlers gather requests, call the
// pure cores (admission, placement, registry, rendezvous, mitosis,
// templates), and execute the resulting decisions against a store.Store. It
// also owns two background loops — a membership reaper and a mitosis ticker
// — that read the clock and drive the same cores on a timer.
//
// Membership is CENTRAL in P0: a dialing agent is admitted by JoinAgent
// applying a registry.AgentJoined event, and a missed heartbeat is evicted
// by the reaper applying registry.AgentLeft. There is no gossip/SWIM here —
// that is deferred to P1 (see the phase doc's "gossip" wording, which does
// not apply yet).
package controlplane

import (
	"fmt"
	"log"
	"net"
	"sort"
	"sync"

	"google.golang.org/grpc"

	"github.com/msivraj/swarm/internal/core/placement"
	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/store"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// Server implements transport.ControlPlaneServer. It holds the store and the
// mutable bookkeeping the shell needs that the store does not: per-agent
// last-contact times, the agent<->cell membership this shell learned from
// JoinAgent (the registry core folds events but does not expose per-cell
// membership back out), mitosis cooldown stamps, the task->job index
// SubmitJob populates so ReportResult can tell when a job is complete, and
// the ingress pending buffer for tasks placement.Place could not yet assign
// to any cell.
//
// mu guards every mutable field below, including the registry mutations
// (store.Registry/store.SetRegistry) driven by JoinAgent, the reaper, and
// the mitosis loop, so those two loops and concurrent RPCs never race folding
// events into the registry.
type Server struct {
	transport.UnimplementedControlPlaneServer

	store store.Store
	cfg   Config
	now   func() model.Instant

	grpcServer *grpc.Server
	stop       chan struct{}
	wg         sync.WaitGroup

	mu            sync.Mutex
	lastSeen      map[string]model.Instant                  // agent -> last Heartbeat/JoinAgent instant
	agentCell     map[string]model.CellID                   // agent -> the cell it currently belongs to
	cellAgents    map[model.CellID]map[string]struct{}      // cell -> its member agents
	cooldowns     map[model.CellID]model.Instant            // cell -> last mitosis resize instant
	taskJob       map[model.TaskID]model.JobID              // task -> owning job, learned at SubmitJob/DispatchTasks
	taskTotal     map[model.JobID]int                       // job -> total tasks admitted/dispatched for it at this region
	taskCell      map[model.TaskID]model.CellID             // task -> the cell (or the spillCellID sentinel) its by-cell roll-up group is keyed on
	resultSink    map[model.JobID]string                    // job -> result_sink ("" means self: this region owns the final aggregate)
	reportedTasks map[model.JobID]map[model.TaskID]struct{} // job -> distinct task IDs that have reported at least one result
	finalized     map[model.JobID]struct{}                  // job -> present once maybeRollup has run its completion action (PutAggregate / reportPartialUp) for it, exactly once
	pending       []model.Task                              // tasks placement.Place (and, in regional mode, placement.PlaceAcross) could not assign anywhere yet; drained as capacity or a peer appears
	peerView      []model.RegionView                        // cached GlobalRouter.GetGlobalView peers (excluding this region), refreshed by the publish loop; nil until the first successful poll
	nextCellID    int
	nextJobID     int

	gangReserved map[model.CellID]int            // cell -> slots currently held by an admitted gang's reservation (see gang.go)
	gangJobs     map[model.JobID]gangReservation // job -> its committed Place reservation, once admitted
	gangPending  []model.JobSpec                 // gang JobSpecs admitGangLocked returned Wait for, FIFO, retried by retryPendingGangsLocked on capacity change

	agentAddrs      map[string]agentAddr                         // agent -> its advertised raft_addr/cell_leader_addr (#101 JoinAgent fields), learned at JoinAgent
	cellAssignments map[string]*transport.CellAssignmentResponse // agent -> its coupled-cell CellAssignment (#101), once activateCoupledCellLocked has built one for it

	// loadMu guards load, kept separate from mu: an ingress admission check
	// (admitIngress/admitThrottleOnly) must be able to read/update the load
	// snapshot without contending on — or, worse, holding through a
	// Throttle delay — the same mutex the registry/placement bookkeeping
	// above serializes on.
	loadMu sync.Mutex
	load   model.LoadState // live in-flight + queued snapshot; see beginRPC/waitQueued
}

// New returns a Server ready to Serve. now supplies the clock (and the
// mitosis/reaper loops' notion of "current time") as data — the server never
// reads time.Now itself outside this injected function, keeping every
// decision it drives through the pure cores reproducible from (state, event,
// now) triples.
//
// cfg's GlobalRouterDialer/PeerDialer default to their production gRPC
// implementations when left nil, matching internal/shell/agent's Config
// defaulting convention; tests override them directly on the returned
// *Server before calling Serve (same package, same as tests already reach
// into e.g. srv.lastSeen).
func New(st store.Store, cfg Config, now func() model.Instant) *Server {
	if cfg.GlobalRouterDialer == nil {
		cfg.GlobalRouterDialer = GRPCGlobalRouterDialer()
	}
	if cfg.PeerDialer == nil {
		cfg.PeerDialer = GRPCPeerDialer()
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Printf
	}
	if cfg.Sleep == nil {
		cfg.Sleep = realSleep
	}
	s := &Server{
		store:         st,
		cfg:           cfg,
		now:           now,
		stop:          make(chan struct{}),
		lastSeen:      make(map[string]model.Instant),
		agentCell:     make(map[string]model.CellID),
		cellAgents:    make(map[model.CellID]map[string]struct{}),
		cooldowns:     make(map[model.CellID]model.Instant),
		taskJob:       make(map[model.TaskID]model.JobID),
		taskTotal:     make(map[model.JobID]int),
		taskCell:      make(map[model.TaskID]model.CellID),
		resultSink:    make(map[model.JobID]string),
		reportedTasks: make(map[model.JobID]map[model.TaskID]struct{}),
		finalized:     make(map[model.JobID]struct{}),
		gangReserved:  make(map[model.CellID]int),
		gangJobs:      make(map[model.JobID]gangReservation),

		agentAddrs:      make(map[string]agentAddr),
		cellAssignments: make(map[string]*transport.CellAssignmentResponse),
	}
	// Create + register the gRPC server here, once, before any goroutine or
	// Stop can run — so s.grpcServer is immutable after construction. This
	// removes the Serve-vs-Stop data race (and the Stop-before-Serve hang)
	// on s.grpcServer that flaked -race CI (#129); Serve/Stop only ever read it.
	s.grpcServer = grpc.NewServer()
	transport.RegisterControlPlaneServer(s.grpcServer, s)
	return s
}

// Serve registers the ControlPlane service on lis, starts the reaper,
// mitosis, and publish background loops, and blocks serving RPCs until the
// gRPC server stops (via Stop or lis closing). It returns the error
// grpc.Server.Serve returns.
func (s *Server) Serve(lis net.Listener) error {
	// s.grpcServer was created + registered in New (immutable since), so Serve
	// and Stop only read it — no race.
	s.wg.Add(3)
	go s.reapLoop()
	go s.mitosisLoop()
	go s.publishLoop()

	err := s.grpcServer.Serve(lis)
	close(s.stop)
	s.wg.Wait()
	return err
}

// Stop gracefully stops the gRPC server, which in turn causes Serve to
// return and the background loops to exit.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// applyRegistryEventLocked folds ev into the store's registry and persists
// the result. Callers must hold s.mu.
func (s *Server) applyRegistryEventLocked(ev registry.RegistryEvent) []registry.Change {
	reg := s.store.Registry()
	newReg, changes := registry.Apply(reg, ev)
	// memStore.SetRegistry only ever errors on a malformed key, which
	// RegistryEvent (built entirely from server-generated or validated IDs)
	// never carries; there is nothing more useful to do with the error here
	// than the shell equivalent of a bug report, so it is intentionally not
	// surfaced to the RPC caller, whose request already succeeded logically.
	_ = s.store.SetRegistry(newReg)
	return changes
}

// drainPendingLocked re-runs placement.Place over the pending buffer,
// enqueuing (via store.EnqueueTask) every task that now fits a cell and
// leaving the rest pending. It scans s.pending in its stable, FIFO arrival
// order, so the result depends only on (pending order, registry state) —
// never on map iteration order or wall-clock timing. Callers must hold s.mu.
//
// placement.Place's Free is agent capacity (registry.Snapshot derives it
// from cell membership), not a task-slot count, and store.EnqueueTask never
// touches the registry — so a single registry.Snapshot call reused
// unmodified across every task in the buffer would let one cell with any
// Free>0 silently absorb the entire buffer, defeating the point of one
// queue per cell. To spread a batch across cells the way the ticket's
// "per-cell task queues" replaces P0's single shared queue, this function
// takes one registry.Snapshot and decrements its own working copy's Free by
// one for the cell each successful Assign lands on, before placing the next
// task — placement.Place itself is called unmodified and stays a pure,
// P0-unchanged function; only the CellView data fed to it evolves within
// this one drain pass, exactly as a shell is meant to orchestrate repeated
// core calls. The real registry (and thus agent admission) is untouched.
//
// A locally Assign'd task records taskCell[t.ID] = the cell it landed on, the
// grouping key the by-cell roll-up (rollupByCell) reads. A task
// placement.Place cannot fit locally falls through to trySpillLocked (S2,
// issue #44): region-full is no longer an automatic hold-pending — in
// regional mode (cfg.GlobalRouter set) it first tries placement.PlaceAcross
// against the cached peer view, and only lands back in s.pending when no
// peer qualifies either (or spill is disabled/inapplicable), preserving S1's
// "hold, never lose" behavior for that case.
func (s *Server) drainPendingLocked() error {
	if len(s.pending) == 0 {
		return nil
	}
	working := registry.Snapshot(s.store.Registry())

	remaining := make([]model.Task, 0, len(s.pending))
	for _, t := range s.pending {
		p := placement.Place(t, working)
		if p.Kind == placement.Assign {
			if err := s.store.EnqueueTask(p.Cell, t); err != nil {
				return err
			}
			s.taskCell[t.ID] = p.Cell
			decrementFreeLocal(working, p.Cell)
			continue
		}
		if s.trySpillLocked(t, working) {
			continue
		}
		remaining = append(remaining, t)
	}
	s.pending = remaining
	return nil
}

// decrementFreeLocal decrements cells' entry for id's Free count by one, in
// place. It is a no-op if id is not present. See drainPendingLocked's doc
// for why this local, in-memory adjustment exists.
func decrementFreeLocal(cells []model.CellView, id model.CellID) {
	for i := range cells {
		if cells[i].ID == id {
			cells[i].Free--
			return
		}
	}
}

// newCellID returns a fresh, unique CellID. Callers must hold s.mu.
func (s *Server) newCellIDLocked() model.CellID {
	s.nextCellID++
	return model.CellID(fmt.Sprintf("cell-%d", s.nextCellID))
}

// newJobID returns a fresh, unique JobID. Callers must hold s.mu.
func (s *Server) newJobIDLocked() model.JobID {
	s.nextJobID++
	return model.JobID(fmt.Sprintf("job-%d", s.nextJobID))
}

// sortedAgents returns cellAgents[cell]'s members in a stable order, so the
// split below divides deterministically instead of on Go's randomized map
// iteration order. Callers must hold s.mu.
func sortedAgents(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for a := range set {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// cellCapacity returns the capacity (Size+Free) of id in snap, or 0 if id is
// not present.
func cellCapacity(snap []model.CellView, id model.CellID) int {
	for _, c := range snap {
		if c.ID == id {
			return c.Size + c.Free
		}
	}
	return 0
}
