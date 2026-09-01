// p4_capstone_test.go is the P4 exit-criterion capstone (issue #168, design
// ruling #157 fork a): a HERMETIC synthetic scaled-simulation over thousands
// of SIMULATED cells/regions, proving each of the four §03 SLO mechanisms
// (docs/phases/swarm-p4-components.txt §03) holds:
//
//  1. registry write-rate / sharding coverage,
//  2. bounded metric cardinality,
//  3. control-plane stability under a load spike, and
//  4. zero-loss rolling upgrade.
//
// This composes the REAL, already-merged P4 shells and cores over synthetic
// data — no reimplementation of any decision, and no edits to any shipped
// component:
//
//   - internal/shell/store.NewShardedMemStore + internal/core/registry
//     (ShardOf/Apply/Snapshot) — mechanism 1.
//   - internal/shell/observability.Reporter + internal/core/observability
//     (RollupRegion/RollupGlobal/Budget) — mechanism 2.
//   - internal/shell/controlplane.Server (the real gRPC handlers, dialed
//     in-process over bufconn — the same hermetic pattern e2e_test.go and
//     internal/shell/controlplane's own tests already use) driving
//     internal/core/backpressure (AdmitUnderLoad/UpdateLoad) — mechanism 3.
//   - internal/shell/upgrade.Run + internal/core/upgrade
//     (NextDrain/SkewSafe) over a synthetic, e2e-local Fleet — mechanism 4.
//
// The literal 1M-node cloud run and the real FoundationDB backend are OUT of
// scope here (owner-infra, #167 and the deferred 1M run): this suite proves
// the MECHANISMS at a synthetic scale (thousands of cells/regions, not
// millions) fast and deterministically enough to run in the normal
// make gate-full gate. No FDB, no real network (bufconn is in-process, per
// this repo's existing hermetic e2e convention), no real sleep (fake
// Clock/Config.Sleep), no math/rand (every synthetic id/value is a
// deterministic function of a loop index).
package e2e

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	coreobservability "github.com/msivraj/swarm/internal/core/observability"
	"github.com/msivraj/swarm/internal/core/registry"
	coreupgrade "github.com/msivraj/swarm/internal/core/upgrade"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/controlplane"
	"github.com/msivraj/swarm/internal/shell/observability"
	"github.com/msivraj/swarm/internal/shell/store"
	"github.com/msivraj/swarm/internal/shell/transport"
	"github.com/msivraj/swarm/internal/shell/upgrade"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// =============================================================================
// Mechanism 1 — registry write-rate / sharding coverage
// =============================================================================

// p4SpreadCellID deterministically builds the i'th of a spread of synthetic
// CellIDs whose two LEADING bytes cover the full byte range — mirroring
// internal/shell/store's own spreadCellID test helper (see
// TestShardCoverageRoutes): registry.ShardOf is a range partition, so a
// realistic key scheme meant to avoid hotspotting a range-sharded store (the
// point of this mechanism) varies its LEADING bytes, not just a trailing
// suffix.
func p4SpreadCellID(i int) model.CellID {
	lead := []byte{byte(i % 256), byte((i / 256) % 256)}
	return model.CellID(append(lead, []byte(fmt.Sprintf("cell-%d", i))...))
}

// p4RegistryScript builds a deterministic membership event sequence for
// numCells synthetic cells: each cell comes up with two agents, and every
// 7th cell additionally churns (an agent leaves, the cell goes down, comes
// back up under a new capacity, and a fresh agent joins) — exercising
// membership churn across the sharded key space, not just a static
// snapshot, the way a real fleet's cells continuously join/leave/resize.
func p4RegistryScript(numCells int) []registry.RegistryEvent {
	var evs []registry.RegistryEvent
	for i := 0; i < numCells; i++ {
		cell := p4SpreadCellID(i)
		agentA := registry.AgentID(fmt.Sprintf("agent-%d-a", i))
		agentB := registry.AgentID(fmt.Sprintf("agent-%d-b", i))

		evs = append(evs,
			registry.RegistryEvent{Kind: registry.CellUp, Cell: cell, Capacity: 4},
			registry.RegistryEvent{Kind: registry.AgentJoined, Cell: cell, Agent: agentA},
			registry.RegistryEvent{Kind: registry.AgentJoined, Cell: cell, Agent: agentB},
		)

		if i%7 == 0 {
			agentC := registry.AgentID(fmt.Sprintf("agent-%d-c", i))
			evs = append(evs,
				registry.RegistryEvent{Kind: registry.AgentLeft, Cell: cell, Agent: agentA},
				registry.RegistryEvent{Kind: registry.CellDown, Cell: cell},
				registry.RegistryEvent{Kind: registry.CellUp, Cell: cell, Capacity: 6},
				registry.RegistryEvent{Kind: registry.AgentJoined, Cell: cell, Agent: agentC},
			)
		}
	}
	return evs
}

// p4ApplyAndStore folds ev into s's current registry and persists the
// result — mirroring exactly how internal/shell/controlplane's
// applyRegistryEventLocked (and internal/shell/store's own
// TestShardedStoreDecisionsMatchMemStore) drives a store.Store, so this
// exercises the sharded store the same way the real control plane does.
func p4ApplyAndStore(t *testing.T, s store.Store, ev registry.RegistryEvent) {
	t.Helper()
	reg := s.Registry()
	newReg, _ := registry.Apply(reg, ev)
	if err := s.SetRegistry(newReg); err != nil {
		t.Fatalf("SetRegistry() = %v, want nil", err)
	}
}

// p4ShardCoverer is the structural interface store.NewShardedMemStore's
// concrete type satisfies via its exported ShardCoverage method (documented
// as "additional to the Store interface... for tests and observability
// only") — asserting against it through this LOCAL interface reaches only a
// public method, never an unexported field or type.
type p4ShardCoverer interface {
	ShardCoverage() [][]model.CellID
}

// TestP4Capstone_ShardedRegistryCoverageAndDecisions is the registry
// write-rate mechanism (§03 "FoundationDB sustains membership churn across
// the sharded key space"): thousands of synthetic cells, with churn, are
// folded through the REAL sharded store (#162) and asserted to (a) route
// every key to exactly one shard with no single shard holding the whole
// fleet, matching registry.ShardOf, and (b) reach the IDENTICAL registry
// decision (registry.Snapshot) a single, unsharded store reaches on the same
// event script — sharding is transparent to the decision, at scale.
func TestP4Capstone_ShardedRegistryCoverageAndDecisions(t *testing.T) {
	const numShards = 64
	const numCells = 800 // i%256 cycles ~3.1 times: comfortably more than the
	// >=2 cycles TestShardCoverageRoutes documents as enough for every shard
	// bucket (256/numShards keys wide in the dominant leading byte) to see
	// coverage. registry.Apply is copy-on-write (a full map clone per event,
	// by design — see internal/core/registry's doc), so folding a script is
	// inherently O(numCells^2); 800 keeps this hermetic and -race-fast while
	// still folding thousands of registry events (~2900, below) across the
	// sharded key space.

	script := p4RegistryScript(numCells)

	mem := store.NewMemStore()
	for _, ev := range script {
		p4ApplyAndStore(t, mem, ev)
	}
	wantView := registry.Snapshot(mem.Registry())
	if len(wantView) == 0 {
		t.Fatal("baseline memStore registry is empty — fixture is broken")
	}

	sharded := store.NewShardedMemStore(numShards)
	for _, ev := range script {
		p4ApplyAndStore(t, sharded, ev)
	}

	// Decisions match: the sharded store's registry.Apply/Snapshot path is
	// byte-for-byte the same core as the unsharded baseline (P4's FCIS
	// proof), at thousands-of-cells scale, including the churn subset.
	gotView := registry.Snapshot(sharded.Registry())
	if !reflect.DeepEqual(gotView, wantView) {
		t.Fatalf("registry.Snapshot(sharded store) diverged from the unsharded baseline at %d cells", numCells)
	}

	coverer, ok := sharded.(p4ShardCoverer)
	if !ok {
		t.Fatalf("NewShardedMemStore's Store does not expose ShardCoverage — cannot assert coverage")
	}
	coverage := coverer.ShardCoverage()
	if len(coverage) != numShards {
		t.Fatalf("ShardCoverage() has %d shards, want %d", len(coverage), numShards)
	}

	seen := make(map[model.CellID]int, len(wantView))
	nonEmpty := 0
	for shardIdx, ids := range coverage {
		if len(ids) > 0 {
			nonEmpty++
		}
		if len(ids) == len(wantView) {
			t.Fatalf("shard %d holds all %d cells — no single shard should hold the whole fleet (the write-rate mechanism this ticket proves)", shardIdx, len(wantView))
		}
		for _, id := range ids {
			wantShard := registry.ShardOf(model.Key(id), numShards)
			if int(wantShard) != shardIdx {
				t.Fatalf("cell %q found in shard %d, but registry.ShardOf routes it to %d", id, shardIdx, wantShard)
			}
			seen[id]++
		}
	}
	for _, v := range wantView {
		if seen[v.ID] != 1 {
			t.Fatalf("cell %q appeared in %d shards (via ShardCoverage), want exactly 1 — the coverage index must partition the keyspace", v.ID, seen[v.ID])
		}
	}
	if len(seen) != len(wantView) {
		t.Fatalf("coverage names %d distinct cells, want %d", len(seen), len(wantView))
	}
	if nonEmpty < numShards {
		t.Fatalf("coverage spread across only %d of %d shards for %d cells — the sustained-write-rate mechanism needs every shard to share the load", nonEmpty, numShards, len(wantView))
	}
}

// =============================================================================
// Mechanism 2 — bounded metric cardinality
// =============================================================================

// p4SyntheticCells deterministically builds numRegions regions of
// cellsPerRegion synthetic CellMetrics each, using a running index (never
// math/rand) so Count/Gauge/Samples vary the same way
// internal/core/observability's own makeCells test helper does.
func p4SyntheticCells(numRegions, cellsPerRegion int) map[model.RegionID][]model.CellMetrics {
	out := make(map[model.RegionID][]model.CellMetrics, numRegions)
	idx := 0
	for r := 0; r < numRegions; r++ {
		region := model.RegionID(fmt.Sprintf("region-%d", r))
		cells := make([]model.CellMetrics, cellsPerRegion)
		for c := 0; c < cellsPerRegion; c++ {
			cells[c] = model.CellMetrics{
				Cell:    model.CellID(fmt.Sprintf("cell-%d-%d", r, c)),
				Count:   int64(idx % 11),
				Gauge:   float64(idx%7) + 0.5,
				Samples: int64(idx%5) + 1, // always > 0: no zero-weight terms
			}
			idx++
		}
		out[region] = cells
	}
	return out
}

// p4FlattenCells concatenates every region's cells into one flat slice, in
// deterministic RegionID order.
func p4FlattenCells(cellsByRegion map[model.RegionID][]model.CellMetrics) []model.CellMetrics {
	regions := make([]model.RegionID, 0, len(cellsByRegion))
	for r := range cellsByRegion {
		regions = append(regions, r)
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i] < regions[j] })

	var flat []model.CellMetrics
	for _, r := range regions {
		flat = append(flat, cellsByRegion[r]...)
	}
	return flat
}

// p4CountCells returns the total number of CellMetrics across every region.
func p4CountCells(cellsByRegion map[model.RegionID][]model.CellMetrics) int {
	n := 0
	for _, cells := range cellsByRegion {
		n += len(cells)
	}
	return n
}

const p4GaugeEps = 1e-6 // weighted-average arithmetic is not bit-exact across groupings

// TestP4Capstone_ObservabilityBoundedCardinality is the metric-cardinality
// mechanism (§03 "rollups keep the observability series bounded as machine
// count climbs"): thousands of synthetic CellMetrics, growing 4x between two
// collection cycles, are fed through the REAL observability.Reporter (#163).
// It asserts the stored/emitted region-series count never exceeds
// core.Budget(LevelRegion) regardless of fleet size, AND that the two-step
// cell->region->global rollup the Reporter computes equals a flat reduce
// over every cell directly.
func TestP4Capstone_ObservabilityBoundedCardinality(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter(observability.MeterName)
	reporter, err := observability.NewReporter(meter)
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}

	ctx := context.Background()
	budget := int(coreobservability.Budget(model.LevelRegion))

	// Two fleet sizes, the second ~4x the first in both region and cell
	// count — "growing fleet" per the ticket — each with MORE regions than
	// the budget allows, so the cap is genuinely exercised both times.
	sizes := []struct {
		name           string
		regions, cells int
	}{
		{"6000 cells across 200 regions", 200, 30},
		{"24000 cells across 400 regions", 400, 60},
	}

	for _, sz := range sizes {
		t.Run(sz.name, func(t *testing.T) {
			cellsByRegion := p4SyntheticCells(sz.regions, sz.cells)
			totalCells := p4CountCells(cellsByRegion)
			if sz.regions <= budget {
				t.Fatalf("fixture bug: %d regions does not exceed budget %d — the cap would never engage", sz.regions, budget)
			}

			reporter.Collect(ctx, cellsByRegion)

			// Boundedness: however many regions/cells fed this Collect, the
			// stored region-series count never exceeds the budget.
			if got := reporter.RegionCount(); got > budget {
				t.Fatalf("RegionCount() = %d after %d cells across %d regions, want <= budget %d", got, totalCells, sz.regions, budget)
			}
			if got := reporter.RegionCount(); got != budget {
				t.Fatalf("RegionCount() = %d, want exactly the budget %d (this fixture always has more regions than budget)", got, budget)
			}

			// Hierarchical == flat: the Reporter's two-step rollup
			// (cell -> region -> global) must equal one flat fold over every
			// cell directly, regardless of how many regions/cells fed it.
			flat := coreobservability.RollupRegion(p4FlattenCells(cellsByRegion))
			global := reporter.Global()
			if global.Count != flat.Count || global.Samples != flat.Samples {
				t.Fatalf("Global() = %+v, want Count/Samples matching the flat reduce %+v", global, flat)
			}
			if diff := global.Gauge - flat.Gauge; diff > p4GaugeEps || diff < -p4GaugeEps {
				t.Fatalf("Global().Gauge = %v, want the flat reduce's Gauge %v (within %v)", global.Gauge, flat.Gauge, p4GaugeEps)
			}
		})
	}
}

// TestP4Capstone_ObservabilitySeriesCountDoesNotGrowWithFleetSize is a
// second angle on the same mechanism: collecting from a much larger fleet
// does not grow the number of series stored — bounded by the region budget,
// never by cell (or even region) count.
func TestP4Capstone_ObservabilitySeriesCountDoesNotGrowWithFleetSize(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter(observability.MeterName)
	reporter, err := observability.NewReporter(meter)
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}
	ctx := context.Background()

	small := p4SyntheticCells(20, 10) // 200 cells, 20 regions (under budget)
	reporter.Collect(ctx, small)
	smallSeries := reporter.RegionCount() + 1 // +1 for the single global series

	large := p4SyntheticCells(2000, 25) // 50000 cells, 2000 regions (way over budget)
	reporter.Collect(ctx, large)
	largeSeries := reporter.RegionCount() + 1

	budget := int(coreobservability.Budget(model.LevelRegion))
	if largeSeries > budget+1 {
		t.Fatalf("series count = %d after a 250x-larger fleet, want <= budget+1 (%d)", largeSeries, budget+1)
	}
	if largeSeries < smallSeries {
		t.Fatalf("series count shrank (%d -> %d) growing the fleet — suspicious, expected it to hold at the budget once regions exceed it", smallSeries, largeSeries)
	}
	t.Logf("series count: %d cells/%d regions -> %d series; %d cells/%d regions -> %d series (bounded by budget %d)",
		p4CountCells(small), len(small), smallSeries, p4CountCells(large), len(large), largeSeries, budget)
}

// =============================================================================
// Mechanism 3 — control-plane stability under a load spike
// =============================================================================

// p4GatingStore wraps a real store.Store and blocks a PutJob call whose
// JobSpec carries Params["p4gate"] == "1" until release is closed, signaling
// on entered exactly once per blocked call. This is the deterministic
// rendezvous this capstone uses to hold a chosen number of SubmitJob RPCs
// GENUINELY in-flight against the REAL controlplane.Server — no unexported
// field access, no timing race — so the spike this test drives is a real,
// measured model.LoadState the real admission middleware computed, not one
// injected directly.
type p4GatingStore struct {
	store.Store
	entered chan struct{}
	release chan struct{}
}

func (g *p4GatingStore) PutJob(spec model.JobSpec) error {
	if spec.Params["p4gate"] == "1" {
		g.entered <- struct{}{}
		<-g.release
	}
	return g.Store.PutJob(spec)
}

// p4SleepRecorder is a controlplane.Config.Sleep fake: it records every
// requested delay instead of actually waiting, so a request driven into the
// Throttle band (or PullTask's degraded Shed->wait) resolves instantly and
// deterministically — no real sleep anywhere in this suite.
type p4SleepRecorder struct {
	mu    sync.Mutex
	calls int
}

func (r *p4SleepRecorder) sleep(model.Duration) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
}

func (r *p4SleepRecorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// newP4ControlPlane starts a REAL controlplane.Server (st is the caller's
// choice of store.Store, so this capstone can inject p4GatingStore) over an
// in-process bufconn listener — the same hermetic, no-real-network pattern
// e2e_test.go's newControlPlane and internal/shell/controlplane's own
// newTestServer already use.
func newP4ControlPlane(t *testing.T, cfg controlplane.Config, clock *testClock, st store.Store) (transport.ControlPlaneClient, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := controlplane.New(st, cfg, clock.now)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial control plane: %v", err)
	}

	teardown := func() {
		_ = conn.Close()
		srv.Stop()
		if err := <-serveErr; err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Serve: %v", err)
		}
	}
	return transport.NewControlPlaneClient(conn), teardown
}

// p4AssertResourceExhausted fails t unless err is a gRPC status with
// codes.ResourceExhausted — the wire signal backpressure.Shed maps to.
func p4AssertResourceExhausted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("got no error, want codes.ResourceExhausted")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("err = %v, want codes.ResourceExhausted", err)
	}
}

// p4SubmitFiller issues one gated, high-priority SubmitJob (never itself
// shed, regardless of the current spike) that blocks genuinely in-flight
// until gate.release is closed.
func p4SubmitFiller(ctx context.Context, client transport.ControlPlaneClient) (*transport.SubmitJobResponse, error) {
	return client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search",
		Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{
			"start": "0", "end": "1", "shards": "1",
			"priority": "100000", // never sheds itself, whatever the load
			"p4gate":   "1",
		},
	})
}

// TestP4Capstone_BackpressureHoldsAdmissionSteadyUnderSpike is the
// control-plane-stability mechanism (§03 "backpressure holds admission
// steady under a load spike"): a REAL controlplane.Server (#164) is driven
// through a genuine, deterministically-manufactured load spike (real
// in-flight SubmitJob RPCs held open via p4GatingStore, never an injected
// LoadState), and this asserts: low-priority ingress sheds while
// higher-priority ingress is admitted/throttled; PullTask is throttled
// across thousands of calls but never hard-rejected; ReportResult is always
// accepted and never even delayed; and admission returns to normal once the
// spike passes — the control plane is never permanently collapsed by it.
func TestP4Capstone_BackpressureHoldsAdmissionSteadyUnderSpike(t *testing.T) {
	clock := &testClock{}
	sleeps := &p4SleepRecorder{}
	cfg := fastConfig()
	cfg.Limits = model.Limits{Capacity: 20, ShedThreshold: 0.55}
	cfg.JoinPriority = 100000
	cfg.Sleep = sleeps.sleep

	gate := &p4GatingStore{Store: store.NewMemStore(), entered: make(chan struct{}), release: make(chan struct{})}
	client, teardown := newP4ControlPlane(t, cfg, clock, gate)
	defer teardown()
	ctx := context.Background()

	if _, err := client.JoinAgent(ctx, &transport.JoinAgentRequest{Agent: "agent-1", Region: "us", Caps: 2000}); err != nil {
		t.Fatalf("JoinAgent (baseline, no spike): %v", err)
	}

	// --- Phase A: baseline, no spike -> transparent (regression guard) ---
	if _, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search", Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{"start": "0", "end": "1", "shards": "1"},
	}); err != nil {
		t.Fatalf("SubmitJob at baseline load: %v", err)
	}
	if got := sleeps.callCount(); got != 0 {
		t.Fatalf("Sleep called %d times at baseline load, want 0", got)
	}

	// --- Phase B: drive a genuine synthetic load spike ---
	// fillers are launched ONE AT A TIME, each awaited on gate.entered before
	// the next is launched, so InFlight after the loop is EXACTLY
	// fillers/Capacity — deterministic, never a race between concurrent
	// admission checks.
	const fillers = 12 // ratio 12/20 = 0.60
	var wg sync.WaitGroup
	fillerErr := make(chan error, fillers)
	for i := 0; i < fillers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p4SubmitFiller(ctx, client)
			fillerErr <- err
		}()
		<-gate.entered
	}

	// ratio == 0.60: >= effectiveShed(priority 0) == max(0.55, 0.50) == 0.55
	// -> Shed; < effectiveShed(priority 1000) == 0.55 + 10 == 10.55, and
	// >= the core's lowWaterMark (0.50) -> Throttle, never Shed, for the
	// higher-priority request. This is the exact §03 mechanism, driven by
	// REAL measured load.
	p4AssertResourceExhausted(t, p4SubmitLowPriority(ctx, client))

	if _, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search", Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{"start": "0", "end": "1", "shards": "1", "priority": "1000"},
	}); err != nil {
		t.Fatalf("SubmitJob(priority 1000) under the spike: %v, want admitted or throttled, never shed", err)
	}

	// PullTask, at scale, across the whole spike: throttled (the fake
	// Sleep is engaged), never a hard rejection.
	const pulls = 3000
	pullSleepsBefore := sleeps.callCount()
	for i := 0; i < pulls; i++ {
		if _, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: "agent-1"}); err != nil {
			t.Fatalf("PullTask #%d under the spike returned an error (should only ever throttle): %v", i, err)
		}
	}
	if got := sleeps.callCount(); got <= pullSleepsBefore {
		t.Fatalf("PullTask never engaged the throttle path across %d calls under a genuine spike (Sleep call count %d -> %d)", pulls, pullSleepsBefore, got)
	}

	// ReportResult, at scale, across the whole spike: never gated at all —
	// no ResourceExhausted, and no delay recorded.
	const reports = 3000
	reportSleepsBefore := sleeps.callCount()
	for i := 0; i < reports; i++ {
		_, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: fmt.Sprintf("unknown-task-%d", i), Output: []byte("x"), Ok: true,
		})
		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("ReportResult #%d: non-gRPC error %v", i, err)
		}
		if st.Code() == codes.ResourceExhausted {
			t.Fatalf("ReportResult #%d was shed under load — it must never be gated", i)
		}
		if st.Code() != codes.NotFound {
			t.Fatalf("ReportResult #%d code = %v, want NotFound (an unknown task, not a backpressure rejection)", i, st.Code())
		}
	}
	if got := sleeps.callCount(); got != reportSleepsBefore {
		t.Fatalf("ReportResult waited out a delay across %d calls (Sleep call count %d -> %d), want no backpressure call at all", reports, reportSleepsBefore, got)
	}

	// --- Phase C: the spike passes -> admission is steady again ---
	close(gate.release)
	wg.Wait()
	for i := 0; i < fillers; i++ {
		if err := <-fillerErr; err != nil {
			t.Fatalf("filler SubmitJob failed once released: %v", err)
		}
	}

	if _, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search", Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{"start": "0", "end": "1", "shards": "1", "priority": "0"},
	}); err != nil {
		t.Fatalf("SubmitJob(priority 0) after the spike passed: %v, want success (backpressure recovers, never permanently collapsed)", err)
	}
}

// p4SubmitLowPriority issues a priority-0 SubmitJob — the lowest priority,
// shed first under load.
func p4SubmitLowPriority(ctx context.Context, client transport.ControlPlaneClient) error {
	_, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: "keyspace-search", Coupling: transport.Coupling_COUPLING_INDEPENDENT,
		Params: map[string]string{"start": "0", "end": "1", "shards": "1", "priority": "0"},
	})
	return err
}

// =============================================================================
// Mechanism 4 — zero-loss rolling upgrade
// =============================================================================

// p4UpgradeEvent is one fake-fleet effect call, recorded in call order — the
// observation surface the zero-loss ("never two cells of the same job
// cordoned at once") assertion replays.
type p4UpgradeEvent struct {
	kind string // "cordon" | "drain" | "roll" | "uncordon"
	cell model.CellID
}

// p4FakeFleet is the swarmd-shaped FAKE fleet #157's fork (c) calls for: no
// real node, no real binary — cordon/drain/roll/uncordon just mutate plain
// maps and log an event. It implements the REAL, exported
// internal/shell/upgrade.Fleet interface, composed the same way p3Pool
// composes verification.Dispatcher in this package's P3 capstone.
type p4FakeFleet struct {
	versions map[model.CellID]model.Version
	jobs     map[model.CellID][]model.JobID
	cordoned map[model.CellID]bool
	events   []p4UpgradeEvent
}

func newP4FakeFleet(versions map[model.CellID]model.Version, jobs map[model.CellID][]model.JobID) *p4FakeFleet {
	return &p4FakeFleet{versions: versions, jobs: jobs, cordoned: make(map[model.CellID]bool)}
}

func (f *p4FakeFleet) State() model.FleetState {
	versions := make(map[model.CellID]model.Version, len(f.versions))
	for k, v := range f.versions {
		versions[k] = v
	}
	jobs := make(map[model.CellID][]model.JobID, len(f.jobs))
	for k, v := range f.jobs {
		cp := make([]model.JobID, len(v))
		copy(cp, v)
		jobs[k] = cp
	}
	cordoned := make(map[model.CellID]bool, len(f.cordoned))
	for k, v := range f.cordoned {
		cordoned[k] = v
	}
	return model.FleetState{Versions: versions, Jobs: jobs, Cordoned: cordoned}
}

func (f *p4FakeFleet) Cordon(cell model.CellID) error {
	f.cordoned[cell] = true
	f.events = append(f.events, p4UpgradeEvent{"cordon", cell})
	return nil
}

func (f *p4FakeFleet) Drain(cell model.CellID) error {
	f.events = append(f.events, p4UpgradeEvent{"drain", cell})
	return nil
}

func (f *p4FakeFleet) Roll(cell model.CellID, target model.Version) error {
	f.versions[cell] = target
	f.events = append(f.events, p4UpgradeEvent{"roll", cell})
	return nil
}

func (f *p4FakeFleet) Uncordon(cell model.CellID) error {
	delete(f.cordoned, cell)
	f.events = append(f.events, p4UpgradeEvent{"uncordon", cell})
	return nil
}

var _ upgrade.Fleet = (*p4FakeFleet)(nil)

// p4AllJobIDs flattens every JobID present anywhere in jobs into a sorted,
// deduplicated slice — used to compare "which jobs exist in the fleet"
// before and after a rollout, independent of which cell each is on.
func p4AllJobIDs(jobs map[model.CellID][]model.JobID) []model.JobID {
	set := make(map[model.JobID]struct{})
	for _, ids := range jobs {
		for _, id := range ids {
			set[id] = struct{}{}
		}
	}
	out := make([]model.JobID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// p4AssertNeverConcurrentlyCordoned replays events and fails t if any two
// cells sharing a job (per jobsByCell, the fleet's static job assignment)
// are ever cordoned at the same time — the "never drain two cells of the
// same job at once" zero-loss property, observed through the shell's own
// effect calls at thousands-of-cells scale.
func p4AssertNeverConcurrentlyCordoned(t *testing.T, events []p4UpgradeEvent, jobsByCell map[model.CellID][]model.JobID) {
	t.Helper()
	cordonedNow := make(map[model.CellID]bool)
	busyJobs := make(map[model.JobID]model.CellID)

	for _, e := range events {
		switch e.kind {
		case "cordon":
			for _, j := range jobsByCell[e.cell] {
				if owner, ok := busyJobs[j]; ok && owner != e.cell {
					t.Fatalf("job %q cordoned on both %q and %q simultaneously", j, owner, e.cell)
				}
				busyJobs[j] = e.cell
			}
			cordonedNow[e.cell] = true
		case "uncordon":
			delete(cordonedNow, e.cell)
			for _, j := range jobsByCell[e.cell] {
				delete(busyJobs, j)
			}
		}
	}
}

// TestP4Capstone_ZeroLossRollingUpgrade is the zero-loss-upgrade mechanism
// (§03 "a full rolling upgrade completes with no dropped jobs and tolerated
// version skew"): thousands of synthetic cells, paired so every job spans
// exactly two cells (the pairing the zero-loss invariant is about), are
// rolled from a skew-safe starting version to a target version via the REAL
// internal/shell/upgrade.Run (#165) driving the REAL
// internal/core/upgrade.NextDrain/SkewSafe. It asserts the rollout COMPLETES
// with every cell at target, no job is ever dropped from the fleet, and no
// two cells sharing a job are ever cordoned/draining at the same time.
func TestP4Capstone_ZeroLossRollingUpgrade(t *testing.T) {
	const numCells = 1200 // 600 shared-job pairs. NextDrain rescans candidates
	// in O(numCells) per drained cell (see internal/core/upgrade's doc), so a
	// full rollout is inherently O(numCells^2); 1200 keeps this hermetic and
	// -race-fast while still rolling well over a thousand cells.
	start := model.Version{Major: 1, Minor: 0}
	target := model.Version{Major: 1, Minor: 1} // within upgrade.SkewSafe's window of start

	versions := make(map[model.CellID]model.Version, numCells)
	jobs := make(map[model.CellID][]model.JobID, numCells)
	for i := 0; i < numCells; i += 2 {
		cellA := model.CellID(fmt.Sprintf("cell-%d", i))
		cellB := model.CellID(fmt.Sprintf("cell-%d", i+1))
		job := model.JobID(fmt.Sprintf("job-%d", i/2))

		versions[cellA] = start
		versions[cellB] = start
		jobs[cellA] = []model.JobID{job}
		jobs[cellB] = []model.JobID{job}

		if !coreupgrade.SkewSafe(start, target) {
			t.Fatalf("fixture bug: start %+v is not SkewSafe with target %+v", start, target)
		}
	}

	fleet := newP4FakeFleet(versions, jobs)
	beforeJobs := p4AllJobIDs(fleet.jobs)

	plan := model.UpgradePlan{Target: target}
	result, err := upgrade.Run(fleet, plan)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != upgrade.Complete {
		t.Fatalf("Outcome = %v, want Complete (Unfinished=%v)", result.Outcome, result.Unfinished)
	}
	if len(result.Unfinished) != 0 {
		t.Fatalf("Unfinished = %v, want empty on Complete", result.Unfinished)
	}

	final := fleet.State()
	if len(final.Versions) != numCells {
		t.Fatalf("final fleet has %d cells, want %d", len(final.Versions), numCells)
	}
	for cell, v := range final.Versions {
		if v != target {
			t.Errorf("cell %q ended at %+v, want target %+v", cell, v, target)
		}
	}
	for cell, cordoned := range final.Cordoned {
		if cordoned {
			t.Errorf("cell %q left cordoned after Complete", cell)
		}
	}

	// Zero job loss: the set of jobs present anywhere in the fleet is
	// unchanged by the whole rollout, at 1000-job scale.
	if got := p4AllJobIDs(final.Jobs); !reflect.DeepEqual(got, beforeJobs) {
		t.Fatalf("job set changed across the rollout: before had %d jobs, after has %d", len(beforeJobs), len(got))
	}

	// Zero-loss, observed through the shell: no two cells sharing a job are
	// ever cordoned/draining at the same time, across the whole rollout.
	p4AssertNeverConcurrentlyCordoned(t, fleet.events, jobs)

	// Every cell actually got rolled (Run did real work, not a no-op).
	rolled := make(map[model.CellID]bool, numCells)
	for _, e := range fleet.events {
		if e.kind == "roll" {
			rolled[e.cell] = true
		}
	}
	if len(rolled) != numCells {
		t.Fatalf("%d of %d cells were rolled, want all of them", len(rolled), numCells)
	}
}
