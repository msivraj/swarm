package agent

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// GlobalViewDialer opens a connection to the global routing layer's
// GlobalRouter service, used to poll GetGlobalView for the health map
// region.SelectRegion needs to pick a failover target. Tests supply a
// GlobalViewDialer backed by an in-process (bufconn) fake GlobalRouter;
// production uses GRPCGlobalViewDialer.
type GlobalViewDialer func(ctx context.Context, target string) (transport.GlobalRouterClient, io.Closer, error)

// GRPCGlobalViewDialer is the GlobalViewDialer the shell uses in production:
// a plaintext gRPC dial, matching GRPCDialer's trust assumptions (P0 assumes
// trusted machines).
func GRPCGlobalViewDialer() GlobalViewDialer {
	return func(_ context.Context, target string) (transport.GlobalRouterClient, io.Closer, error) {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return transport.NewGlobalRouterClient(conn), conn, nil
	}
}

// healthCache holds the most recently polled region health map: refreshed by
// refreshGlobalView and read by execFailover to build region.SelectRegion's
// health argument. A region absent from the cache is treated Unreachable by
// SelectRegion (fail closed) — exactly right both before the first
// successful poll and for a region GetGlobalView simply didn't report.
type healthCache struct {
	mu     sync.Mutex
	health map[model.RegionID]model.Health
}

func newHealthCache() *healthCache {
	return &healthCache{}
}

// set replaces the cached health map wholesale with the result of the most
// recent successful poll.
func (h *healthCache) set(health map[model.RegionID]model.Health) {
	h.mu.Lock()
	h.health = health
	h.mu.Unlock()
}

// get returns a copy of the cached health map, safe for the caller to read
// without further synchronization.
func (h *healthCache) get() map[model.RegionID]model.Health {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[model.RegionID]model.Health, len(h.health))
	for id, hh := range h.health {
		out[id] = hh
	}
	return out
}

// runGlobalView polls GetGlobalView on a timer and caches the resulting
// health map for execFailover to consume. In single-region mode
// (GlobalRouter unset) it is a no-op that just waits on ctx — the P0 path
// never needs a health view, and never dials GlobalRouter at all. It runs
// until ctx is done.
func (a *Agent) runGlobalView(ctx context.Context) error {
	if a.cfg.GlobalRouter == "" {
		<-ctx.Done()
		return ctx.Err()
	}

	// Prime the cache immediately so a Failover that fires shortly after
	// startup has a view to work from, rather than treating every region as
	// unreachable until the first ticker fires.
	a.refreshGlobalView(ctx)

	ticker := time.NewTicker(a.cfg.GlobalViewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			a.refreshGlobalView(ctx)
		}
	}
}

// refreshGlobalView dials GlobalRouter, calls GetGlobalView, and caches the
// resulting health map, converting each RegionView's Health and forcing
// every diverged region to Unreachable — the "stale summary" case the
// global layer's diverged set exists to flag. A dial or RPC failure leaves
// the cache exactly as it was: the next poll tries again, and until then
// SelectRegion just keeps treating unpolled regions as unreachable.
func (a *Agent) refreshGlobalView(ctx context.Context) {
	client, closer, err := a.cfg.GlobalViewDialer(ctx, a.cfg.GlobalRouter)
	if err != nil {
		return
	}
	defer func() { _ = closer.Close() }()

	resp, err := client.GetGlobalView(ctx, &transport.GlobalViewRequest{})
	if err != nil {
		return
	}

	health := make(map[model.RegionID]model.Health, len(resp.GetRegions()))
	for _, rv := range resp.GetRegions() {
		health[model.RegionID(rv.GetId())] = model.Health(rv.GetHealth())
	}
	for _, id := range resp.GetDiverged() {
		health[model.RegionID(id)] = model.Unreachable
	}
	a.health.set(health)
}
