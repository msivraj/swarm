// This file adds NewShardedMemStore beside NewMemStore: an in-memory Store
// that routes the registry/membership key path over N independent shards —
// the in-memory analogue of a range-sharded FoundationDB backend (#166 adds
// the real, build-tagged one behind the same Store interface). See
// docs/phases/swarm-p4-components.txt §01/§02 (SHARDED REGISTRY) and issue
// #157's fork (a)/(d) resolutions.
//
// Composing the registry over N shards. registry.Registry is opaque,
// copy-on-write data (see internal/core/registry): its per-cell membership
// sets are unexported, and the only facts about it observable outside the
// registry package are the CellIDs it names and the registry.Snapshot view
// derived from them (Size/Free per cell). A store that reconstructed a
// Registry from that view — synthesizing agent identities to reach the
// right Size — would silently change how a later AgentJoined/AgentLeft
// event for a REAL agent ID resolves against it (membership is checked by
// exact AgentID), breaking the "every registry decision test passes
// unchanged" requirement. So this store never decomposes or rebuilds a
// registry.Registry value: it holds the single current one exactly as
// SetRegistry received it — the same pass-through memStore uses, and what
// "calls registry.Apply/Snapshot byte-for-byte" requires — behind a
// lock-free atomic swap, so Registry()/SetRegistry() are byte-for-byte
// identical in behavior to memStore's (see TestShardedStoreMatchesMemStore).
//
// What IS sharded is the routing/coverage surface the ticket calls for:
// every CellID named by the current registry is routed to shard
// registry.ShardOf(key, shards) selects, and that shard owns an independent
// mutex plus an index of the CellIDs currently routed to it (kept in sync
// on every SetRegistry by diffing the previous and new registry.Snapshot).
// Two SetRegistry calls for cells in different shards update disjoint
// index buckets under disjoint locks — the scalability point: unrelated
// shards never contend on each other's lock, mirroring how a range-sharded
// FDB backend would only ever touch the transaction range a key falls in.
//
// The non-registry Store methods (jobs, per-cell queues, results,
// aggregates) are not part of the sharded registry key path the ticket
// calls out, so this store reuses memStore for them unchanged via
// embedding — same fields, same locking, same behavior.
package store

import (
	"sync"
	"sync/atomic"

	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

// registryShard is one partition of the registry's CellID keyspace: an
// independent lock guarding an independent index of the CellIDs currently
// routed to it. It never holds registry membership data itself (see the
// package doc for why) — only the coverage index the routing test asserts
// against.
type registryShard struct {
	mu    sync.Mutex
	cells map[model.CellID]struct{}
}

// shardedMemStore is an in-memory Store, safe for concurrent use, that
// partitions the registry key path into `shards` independent shards while
// delegating every non-registry method to an embedded memStore. See the
// package doc.
type shardedMemStore struct {
	*memStore // jobs, queues, results, aggregates: unchanged memStore behavior

	shards    []*registryShard
	numShards uint32

	// reg holds the single current registry.Registry value, exactly as
	// SetRegistry received it, so Registry() is byte-for-byte identical to
	// memStore's. An atomic.Pointer keeps the swap lock-free, so it never
	// becomes the point where unrelated shards contend.
	reg atomic.Pointer[registry.Registry]
}

// NewShardedMemStore returns an empty, ready-to-use in-memory Store whose
// registry/membership key path is partitioned into `shards` independent
// shards via registry.ShardOf; every other method behaves exactly as
// NewMemStore's. shards == 0 is treated as a single shard (mirroring
// registry.ShardOf's own convention), so the store is always usable.
func NewShardedMemStore(shards uint32) Store {
	if shards == 0 {
		shards = 1
	}
	shardList := make([]*registryShard, shards)
	for i := range shardList {
		shardList[i] = &registryShard{cells: make(map[model.CellID]struct{})}
	}
	return &shardedMemStore{
		memStore:  NewMemStore().(*memStore),
		shards:    shardList,
		numShards: shards,
	}
}

// Registry returns the current authoritative registry.Registry value,
// exactly as the most recent SetRegistry call stored it.
func (s *shardedMemStore) Registry() registry.Registry {
	if reg := s.reg.Load(); reg != nil {
		return *reg
	}
	return registry.Registry{}
}

// SetRegistry swaps the stored registry.Registry for reg, then reindexes
// the shard coverage: every CellID reg.Snapshot no longer names is dropped
// from the shard it used to route to, and every CellID it does name is
// (re)recorded under the shard registry.ShardOf currently selects for it.
func (s *shardedMemStore) SetRegistry(reg registry.Registry) error {
	old := s.reg.Swap(&reg)

	var oldViews []model.CellView
	if old != nil {
		oldViews = registry.Snapshot(*old)
	}
	newViews := registry.Snapshot(reg)

	present := make(map[model.CellID]bool, len(newViews))
	for _, v := range newViews {
		present[v.ID] = true
	}
	for _, v := range oldViews {
		if !present[v.ID] {
			s.shardFor(v.ID).remove(v.ID)
		}
	}
	for _, v := range newViews {
		s.shardFor(v.ID).add(v.ID)
	}
	return nil
}

// shardFor returns the registryShard registry.ShardOf routes id to.
func (s *shardedMemStore) shardFor(id model.CellID) *registryShard {
	return s.shards[registry.ShardOf(model.Key(id), s.numShards)]
}

// ShardCoverage returns, for each shard index, the CellIDs currently
// routed to it — the routing/coverage surface the sharding test asserts
// against. It is additional to the Store interface (Store stays the
// unchanged, swappable contract), for tests and observability only.
func (s *shardedMemStore) ShardCoverage() [][]model.CellID {
	out := make([][]model.CellID, len(s.shards))
	for i, shard := range s.shards {
		shard.mu.Lock()
		ids := make([]model.CellID, 0, len(shard.cells))
		for id := range shard.cells {
			ids = append(ids, id)
		}
		shard.mu.Unlock()
		out[i] = ids
	}
	return out
}

func (rs *registryShard) add(id model.CellID) {
	rs.mu.Lock()
	rs.cells[id] = struct{}{}
	rs.mu.Unlock()
}

func (rs *registryShard) remove(id model.CellID) {
	rs.mu.Lock()
	delete(rs.cells, id)
	rs.mu.Unlock()
}
