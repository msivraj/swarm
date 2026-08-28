// Package global is the P1 global routing layer's shell (S3, issue #45): a
// gRPC server (transport.GlobalRouterServer) whose handlers gather requests,
// call the pure cores (admission, routing, aggregate), and execute the
// resulting decisions by dialing regions' ControlPlane service. It mirrors
// internal/shell/controlplane's shape (Server/New/Serve/Stop, mutex
// discipline, injected now).
//
// The global layer is thin and eventually-consistent: it holds the merged
// GlobalView (routing.MergeGlobal folds each region's periodic
// RegionalSummary), routes each submitted job (routing.Decide), fans a
// Spread job's tasks out across regions proportional to their reported
// capacity, and combines the region partials a Spread job's participants
// report back into the final Aggregate (aggregate.MergeAll) — never raw
// per-task results.
package global

import (
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/msivraj/swarm/internal/core/routing"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/store"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// spreadJob tracks one Spread job's roll-up state: the template needed to
// call aggregate.Merge, the set of regions a partition was actually
// dispatched to (expected to report back), the latest partial ReportPartial
// received per region (idempotent — a duplicate replaces, never accumulates),
// and the folded final Aggregate once every expected region has reported
// done.
type spreadJob struct {
	template string
	expected map[model.RegionID]struct{}
	arrived  map[model.RegionID]model.Aggregate
	final    model.Aggregate
	done     bool
}

// Server implements transport.GlobalRouterServer. It holds the merged
// GlobalView and per-Spread-job roll-up bookkeeping the shell needs beyond
// what store.Store models — store.Store persists only the finished Aggregate
// for a completed Spread job (JobStatus's read path), mirroring how
// controlplane.Server uses PutAggregate/GetAggregate for a self-sink job.
//
// mu guards every mutable field below, so concurrent Submit/PublishSummary/
// ReportPartial/GetGlobalView RPCs and the diverge-sweep background loop
// never race.
type Server struct {
	transport.UnimplementedGlobalRouterServer

	store store.Store
	cfg   Config
	now   func() model.Instant

	grpcServer *grpc.Server
	stop       chan struct{}
	wg         sync.WaitGroup

	mu           sync.Mutex
	view         routing.GlobalView
	toJobRegion  map[model.JobID]model.RegionID // jobID -> the region a To route sent it to, for JobStatus's proxy
	spreadJobs   map[model.JobID]*spreadJob
	lastDiverged []model.RegionID // most recent divergeSweepLoop result, for observability
	nextJobID    int
}

// New returns a Server ready to Serve. now supplies the clock (and the
// diverge-sweep loop's notion of "current time") as data — the server never
// reads time.Now itself outside this injected function, matching
// controlplane.New's contract.
func New(st store.Store, cfg Config, now func() model.Instant) *Server {
	if cfg.RegionDialer == nil {
		cfg.RegionDialer = GRPCRegionDialer()
	}
	if cfg.DivergeSweep <= 0 {
		cfg.DivergeSweep = DefaultConfig().DivergeSweep
	}
	return &Server{
		store:       st,
		cfg:         cfg,
		now:         now,
		stop:        make(chan struct{}),
		toJobRegion: make(map[model.JobID]model.RegionID),
		spreadJobs:  make(map[model.JobID]*spreadJob),
	}
}

// Serve registers the GlobalRouter service on lis, starts the diverge-sweep
// background loop, and blocks serving RPCs until the gRPC server stops (via
// Stop or lis closing). It returns the error grpc.Server.Serve returns.
func (s *Server) Serve(lis net.Listener) error {
	s.grpcServer = grpc.NewServer()
	transport.RegisterGlobalRouterServer(s.grpcServer, s)

	s.wg.Add(1)
	go s.divergeSweepLoop()

	err := s.grpcServer.Serve(lis)
	close(s.stop)
	s.wg.Wait()
	return err
}

// Stop gracefully stops the gRPC server, which in turn causes Serve to
// return and the background loop to exit.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// newJobIDLocked returns a fresh, unique JobID. Callers must hold s.mu.
func (s *Server) newJobIDLocked() model.JobID {
	s.nextJobID++
	return model.JobID(fmt.Sprintf("global-job-%d", s.nextJobID))
}
