package store

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

// spreadCellID deterministically builds the i'th of a spread of CellIDs
// whose LEADING bytes cover the full byte range — mirroring
// internal/core/registry's own TestShardOfDistribution helper. ShardOf is a
// range partition (see registry.ShardOf's doc): keys sharing a common
// prefix land in the same shard by design, so a realistic key scheme meant
// to avoid hotspotting a range-sharded store varies its leading bytes, not
// just a trailing suffix.
func spreadCellID(i int) model.CellID {
	lead := []byte{byte(i % 256), byte((i / 256) % 256)}
	return model.CellID(append(lead, []byte(fmt.Sprintf("cell-%d", i))...))
}

// var _ Store ensures shardedMemStore satisfies the same Store interface as
// memStore at compile time — the drop-in requirement #166's tagged FDB
// adapter must also meet.
var _ Store = (*shardedMemStore)(nil)

// applyAndStore folds ev into s's current registry and persists the result —
// mirroring exactly how internal/shell/controlplane's
// applyRegistryEventLocked drives a Store, so these tests exercise the
// store the same way the control plane does.
func applyAndStore(t *testing.T, s Store, ev registry.RegistryEvent) []registry.Change {
	t.Helper()
	reg := s.Registry()
	newReg, changes := registry.Apply(reg, ev)
	if err := s.SetRegistry(newReg); err != nil {
		t.Fatalf("SetRegistry() = %v, want nil", err)
	}
	return changes
}

// applyAndStoreLocked is applyAndStore with the read-Apply-write sequence
// serialized under mu — exactly what the control plane's own s.mu does
// around applyRegistryEventLocked in internal/shell/controlplane/server.go.
// Registry()/SetRegistry() is a read-modify-write pair; like memStore, the
// store only guarantees each call is internally race-free, not that
// concurrent, unsynchronized read-modify-write callers never lose an
// update — that safety is the caller's job (the control plane serializes
// it), so tests that grow the registry from many goroutines must too.
func applyAndStoreLocked(t *testing.T, mu *sync.Mutex, s Store, ev registry.RegistryEvent) []registry.Change {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	return applyAndStore(t, s, ev)
}

// registryScript is a fixed sequence of RegistryEvents exercised against
// every store under test, covering CellUp/AgentJoined/AgentLeft/
// CapacityChanged/CellDown/CellUp-again (re-adding a previously removed
// cell must still route to the same shard).
func registryScript() []registry.RegistryEvent {
	return []registry.RegistryEvent{
		{Kind: registry.CellUp, Cell: "cell-a", Capacity: 5},
		{Kind: registry.CellUp, Cell: "cell-b", Capacity: 3},
		{Kind: registry.AgentJoined, Cell: "cell-a", Agent: "agent-1"},
		{Kind: registry.AgentJoined, Cell: "cell-a", Agent: "agent-2"},
		{Kind: registry.AgentJoined, Cell: "cell-b", Agent: "agent-3"},
		{Kind: registry.AgentLeft, Cell: "cell-a", Agent: "agent-1"},
		{Kind: registry.CapacityChanged, Cell: "cell-b", Capacity: 10},
		{Kind: registry.CellDown, Cell: "cell-a"},
		{Kind: registry.CellUp, Cell: "cell-a", Capacity: 8},
		{Kind: registry.AgentJoined, Cell: "cell-a", Agent: "agent-4"},
	}
}

// TestShardedStoreDecisionsMatchMemStore is the FCIS proof of the ticket:
// the same sequence of registry events, folded through registry.Apply the
// exact same way the control plane does, yields an IDENTICAL registry
// (via registry.Snapshot) whether the store behind it is NewMemStore or
// NewShardedMemStore at any shard count — sharding is transparent to the
// decision.
func TestShardedStoreDecisionsMatchMemStore(t *testing.T) {
	shardCounts := []uint32{0, 1, 4, 16, 64}

	mem := NewMemStore()
	var memChanges [][]registry.Change
	for _, ev := range registryScript() {
		memChanges = append(memChanges, applyAndStore(t, mem, ev))
	}
	wantView := registry.Snapshot(mem.Registry())

	for _, n := range shardCounts {
		t.Run(shardLabel(n), func(t *testing.T) {
			sharded := NewShardedMemStore(n)
			var shardedChanges [][]registry.Change
			for _, ev := range registryScript() {
				shardedChanges = append(shardedChanges, applyAndStore(t, sharded, ev))
			}

			if !reflect.DeepEqual(shardedChanges, memChanges) {
				t.Fatalf("Apply() changes through sharded store = %+v, want %+v (memStore)", shardedChanges, memChanges)
			}

			gotView := registry.Snapshot(sharded.Registry())
			if !reflect.DeepEqual(gotView, wantView) {
				t.Fatalf("Snapshot(sharded.Registry()) = %+v, want %+v (memStore)", gotView, wantView)
			}
		})
	}
}

func shardLabel(n uint32) string {
	if n == 0 {
		return "shards=0(treated as 1)"
	}
	return "shards=" + strconv.FormatUint(uint64(n), 10)
}

// TestShardedStoreOperationsMatchMemStore is the table test the ticket
// names: the same non-registry operations (jobs, per-cell queues, results,
// aggregates) run against NewMemStore and NewShardedMemStore observe
// identical externally-visible results — the drop-in requirement.
func TestShardedStoreOperationsMatchMemStore(t *testing.T) {
	run := func(s Store) (job model.JobSpec, jobOK bool, task model.Task, taskOK bool, results []model.TaskResult, agg model.Aggregate, aggOK bool) {
		_ = s.PutJob(model.JobSpec{ID: "j1", Template: "tpl"})
		job, jobOK, _ = s.GetJob("j1")

		_ = s.EnqueueTask("cell-a", model.Task{ID: "t1", JobID: "j1"})
		_ = s.EnqueueTask("cell-a", model.Task{ID: "t2", JobID: "j1"})
		task, taskOK, _ = s.DequeueTask("cell-a")
		_ = s.RequeueTask("cell-a", task)

		_ = s.PutResult(model.TaskResult{TaskID: "t2", Output: []byte("out"), OK: true})
		results, _ = s.ResultsForJob("j1")

		_ = s.PutAggregate(model.Aggregate{JobID: "j1", Value: []byte("v"), Done: true})
		agg, aggOK, _ = s.GetAggregate("j1")
		return
	}

	memJob, memJobOK, memTask, memTaskOK, memResults, memAgg, memAggOK := run(NewMemStore())

	for _, n := range []uint32{1, 4, 16} {
		t.Run(shardLabel(n), func(t *testing.T) {
			job, jobOK, task, taskOK, results, agg, aggOK := run(NewShardedMemStore(n))

			if !reflect.DeepEqual(job, memJob) || jobOK != memJobOK {
				t.Fatalf("GetJob() = %+v, %v, want %+v, %v", job, jobOK, memJob, memJobOK)
			}
			if !reflect.DeepEqual(task, memTask) || taskOK != memTaskOK {
				t.Fatalf("DequeueTask() = %+v, %v, want %+v, %v", task, taskOK, memTask, memTaskOK)
			}
			if !reflect.DeepEqual(results, memResults) {
				t.Fatalf("ResultsForJob() = %+v, want %+v", results, memResults)
			}
			if !reflect.DeepEqual(agg, memAgg) || aggOK != memAggOK {
				t.Fatalf("GetAggregate() = %+v, %v, want %+v, %v", agg, aggOK, memAgg, memAggOK)
			}
		})
	}
}

// TestShardCoverageRoutes drives many synthetic CellIDs through the store
// and asserts they land exactly where registry.ShardOf selects, that the
// coverage index partitions the keyspace (every cell in exactly one
// shard), and that with enough distinct keys no single shard ends up
// holding the whole keyspace.
func TestShardCoverageRoutes(t *testing.T) {
	const numShards = 8
	const numCells = 512 // >= 2 full cycles of spreadCellID's 0..255 leading byte, so every shard gets coverage

	s := NewShardedMemStore(numShards).(*shardedMemStore)

	var cellIDs []model.CellID
	for i := 0; i < numCells; i++ {
		id := spreadCellID(i)
		cellIDs = append(cellIDs, id)
		applyAndStore(t, s, registry.RegistryEvent{Kind: registry.CellUp, Cell: id, Capacity: 1})
	}

	coverage := s.ShardCoverage()
	if len(coverage) != numShards {
		t.Fatalf("ShardCoverage() has %d shards, want %d", len(coverage), numShards)
	}

	seen := make(map[model.CellID]int)
	nonEmpty := 0
	for shardIdx, ids := range coverage {
		if len(ids) > 0 {
			nonEmpty++
		}
		if len(ids) == numCells {
			t.Fatalf("shard %d holds all %d cells — no single shard should hold the whole keyspace", shardIdx, numCells)
		}
		for _, id := range ids {
			wantShard := registry.ShardOf(model.Key(id), numShards)
			if int(wantShard) != shardIdx {
				t.Fatalf("cell %q found in shard %d, but registry.ShardOf routes it to %d", id, shardIdx, wantShard)
			}
			seen[id]++
		}
	}

	for _, id := range cellIDs {
		if seen[id] != 1 {
			t.Fatalf("cell %q appeared in %d shards, want exactly 1", id, seen[id])
		}
	}
	if len(seen) != numCells {
		t.Fatalf("coverage names %d distinct cells, want %d", len(seen), numCells)
	}
	if nonEmpty < numShards {
		t.Fatalf("coverage spread across only %d of %d shard(s) for %d cells, want every shard to get at least one cell — no single shard should hold the whole keyspace", nonEmpty, numShards, numCells)
	}
}

// TestShardCoverageDropsRemovedCells checks that a CellDown removes the
// cell from the coverage index (not just from the registry), so a stale
// entry never lingers in a shard bucket after the cell leaves the fleet.
func TestShardCoverageDropsRemovedCells(t *testing.T) {
	s := NewShardedMemStore(4).(*shardedMemStore)
	applyAndStore(t, s, registry.RegistryEvent{Kind: registry.CellUp, Cell: "gone", Capacity: 1})
	if total := coverageCount(s); total != 1 {
		t.Fatalf("coverage count after CellUp = %d, want 1", total)
	}
	applyAndStore(t, s, registry.RegistryEvent{Kind: registry.CellDown, Cell: "gone"})
	if total := coverageCount(s); total != 0 {
		t.Fatalf("coverage count after CellDown = %d, want 0", total)
	}
}

func coverageCount(s *shardedMemStore) int {
	total := 0
	for _, ids := range s.ShardCoverage() {
		total += len(ids)
	}
	return total
}

// TestConcurrentShardedRegistryAccess drives concurrent registry events for
// distinct cells (spread across shards) plus concurrent job/queue
// operations at once. It is a correctness + -race guard: every cell that
// went up is present exactly once at the end, and the coverage index
// stays a valid partition. The registry's read-modify-write is serialized
// by a local mutex — mirroring the control plane's own s.mu — while
// Registry()/SetRegistry()/ShardCoverage() and the unrelated job/queue
// calls still run genuinely concurrently, so -race still exercises the
// store's own internal locking (atomic registry swap, per-shard mutexes).
func TestConcurrentShardedRegistryAccess(t *testing.T) {
	s := NewShardedMemStore(16).(*shardedMemStore)
	const n = 100
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := spreadCellID(i)
			applyAndStoreLocked(t, &mu, s, registry.RegistryEvent{Kind: registry.CellUp, Cell: id, Capacity: i})
			_ = s.PutJob(model.JobSpec{ID: model.JobID(id), Template: "tpl"})
			_ = s.EnqueueTask(id, model.Task{ID: model.TaskID(id), JobID: model.JobID(id)})
		}(i)
	}
	wg.Wait()

	views := registry.Snapshot(s.Registry())
	if len(views) != n {
		t.Fatalf("Snapshot() has %d cells, want %d", len(views), n)
	}

	names := make([]string, len(views))
	for i, v := range views {
		names[i] = string(v.ID)
	}
	sort.Strings(names)
	for i := 1; i < len(names); i++ {
		if names[i] == names[i-1] {
			t.Fatalf("duplicate cell %q in Snapshot(), registry lost or double-applied an event", names[i])
		}
	}

	total := coverageCount(s)
	if total != n {
		t.Fatalf("shard coverage names %d cells, want %d — coverage index desynced from the registry under concurrency", total, n)
	}
}
