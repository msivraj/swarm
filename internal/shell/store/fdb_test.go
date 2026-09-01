//go:build fdb

// FDB integration tests: NOT part of the hermetic gate (they compile and
// run only with -tags fdb — see fdb.go's package doc). Run them with:
//
//	go test -tags fdb ./internal/shell/store/ -run FDB -v
//
// against the local single-node fdbserver the environment already has
// running (fdbcli --exec "status minimal" -> "The database is available").
// Every test gets its own key prefix, derived from the test's name (no
// clock, no math/rand — a deterministic, collision-free scheme since Go
// test names are already unique within a run), and clears that prefix's
// keyspace both before and after so runs are isolated and repeatable
// against the shared live cluster.
package store

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"

	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

// testPrefix derives a deterministic, collision-free key prefix from the
// running test's name (t.Name() is unique per test/subtest within a run).
func testPrefix(t *testing.T) string {
	t.Helper()
	return "swarm-fdb-test/" + strings.ReplaceAll(t.Name(), "/", "-")
}

// clearPrefix wipes every key under prefix, so a test starts and ends with
// a clean slice of the shared live cluster's keyspace.
func clearPrefix(t *testing.T, prefix string) {
	t.Helper()
	apiVersionOnce.Do(func() { fdb.MustAPIVersion(fdbAPIVersion) })
	db, err := fdb.OpenDatabase("")
	if err != nil {
		t.Fatalf("fdb.OpenDatabase: %v", err)
	}
	sub := subspace.Sub(prefix)
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(sub)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("clearPrefix ClearRange: %v", err)
	}
}

// newTestFDBStore opens an fdbStore scoped to a prefix unique to the
// running test, clearing that prefix's keyspace before the test runs and
// registering a Cleanup to clear it again after.
func newTestFDBStore(t *testing.T) Store {
	t.Helper()
	prefix := testPrefix(t)
	clearPrefix(t, prefix)
	t.Cleanup(func() { clearPrefix(t, prefix) })

	s, err := NewFDBStoreWithPrefix("", prefix)
	if err != nil {
		t.Fatalf("NewFDBStoreWithPrefix: %v", err)
	}
	return s
}

func TestFDBJobRoundTrip(t *testing.T) {
	s := newTestFDBStore(t)

	if _, ok, err := s.GetJob("missing"); err != nil || ok {
		t.Fatalf("GetJob(missing) = (_, %v, %v), want (_, false, nil)", ok, err)
	}

	spec := model.JobSpec{
		ID:         "job-1",
		Template:   "echo",
		Coupling:   model.Independent,
		Params:     map[string]string{"k": "v"},
		MinMembers: 3,
		Tier:       model.Tier(0),
	}
	if err := s.PutJob(spec); err != nil {
		t.Fatalf("PutJob: %v", err)
	}
	got, ok, err := s.GetJob("job-1")
	if err != nil || !ok {
		t.Fatalf("GetJob(job-1) = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	if !reflect.DeepEqual(got, spec) {
		t.Fatalf("GetJob round-trip mismatch: got %+v, want %+v", got, spec)
	}

	if err := s.PutJob(model.JobSpec{}); err != ErrEmptyJobID {
		t.Fatalf("PutJob(empty ID) = %v, want ErrEmptyJobID", err)
	}
	if _, _, err := s.GetJob(""); err != ErrEmptyJobID {
		t.Fatalf("GetJob(empty ID) = %v, want ErrEmptyJobID", err)
	}
}

func TestFDBQueueFIFO(t *testing.T) {
	s := newTestFDBStore(t)
	cell := model.CellID("cell-a")

	if _, ok, err := s.DequeueTask(cell); err != nil || ok {
		t.Fatalf("DequeueTask(empty queue) = (_, %v, %v), want (_, false, nil)", ok, err)
	}

	tasks := []model.Task{
		{ID: "t1", JobID: "job-1", Input: []byte("a")},
		{ID: "t2", JobID: "job-1", Input: []byte("b")},
		{ID: "t3", JobID: "job-1", Input: []byte("c")},
	}
	for _, task := range tasks {
		if err := s.EnqueueTask(cell, task); err != nil {
			t.Fatalf("EnqueueTask(%v): %v", task.ID, err)
		}
	}

	// Pop the front (t1), then requeue it — it must land behind t2/t3, not
	// jump back to the front.
	got, ok, err := s.DequeueTask(cell)
	if err != nil || !ok || got.ID != "t1" {
		t.Fatalf("DequeueTask #1 = (%+v, %v, %v), want (t1, true, nil)", got, ok, err)
	}
	if err := s.RequeueTask(cell, got); err != nil {
		t.Fatalf("RequeueTask: %v", err)
	}

	wantOrder := []model.TaskID{"t2", "t3", "t1"}
	for _, want := range wantOrder {
		got, ok, err := s.DequeueTask(cell)
		if err != nil || !ok {
			t.Fatalf("DequeueTask() = (_, %v, %v), want (_, true, nil)", ok, err)
		}
		if got.ID != want {
			t.Fatalf("DequeueTask() = %v, want %v", got.ID, want)
		}
	}
	if _, ok, err := s.DequeueTask(cell); err != nil || ok {
		t.Fatalf("DequeueTask(drained) = (_, %v, %v), want (_, false, nil)", ok, err)
	}

	// A second cell's queue is independent of the first's.
	other := model.CellID("cell-b")
	if err := s.EnqueueTask(other, model.Task{ID: "o1", JobID: "job-2"}); err != nil {
		t.Fatalf("EnqueueTask(other): %v", err)
	}
	if _, ok, err := s.DequeueTask(cell); err != nil || ok {
		t.Fatalf("DequeueTask(cell) after enqueue on other cell = (_, %v, %v), want (_, false, nil)", ok, err)
	}
	got, ok, err = s.DequeueTask(other)
	if err != nil || !ok || got.ID != "o1" {
		t.Fatalf("DequeueTask(other) = (%+v, %v, %v), want (o1, true, nil)", got, ok, err)
	}
}

func TestFDBResultsAndAggregate(t *testing.T) {
	s := newTestFDBStore(t)

	// PutResult for a task never enqueued is rejected — the store has no
	// TaskID -> JobID mapping to file it under.
	if err := s.PutResult(model.TaskResult{TaskID: "ghost"}); err != ErrUnknownTask {
		t.Fatalf("PutResult(unknown task) = %v, want ErrUnknownTask", err)
	}

	cell := model.CellID("cell-a")
	task := model.Task{ID: "t1", JobID: "job-1"}
	if err := s.EnqueueTask(cell, task); err != nil {
		t.Fatalf("EnqueueTask: %v", err)
	}

	// A retried task legitimately reports twice — PutResult never
	// deduplicates by TaskID (that's the aggregation core's job).
	r1 := model.TaskResult{TaskID: "t1", Output: []byte("first"), OK: false}
	r2 := model.TaskResult{TaskID: "t1", Output: []byte("second"), OK: true}
	if err := s.PutResult(r1); err != nil {
		t.Fatalf("PutResult(r1): %v", err)
	}
	if err := s.PutResult(r2); err != nil {
		t.Fatalf("PutResult(r2): %v", err)
	}

	got, err := s.ResultsForJob("job-1")
	if err != nil {
		t.Fatalf("ResultsForJob: %v", err)
	}
	want := []model.TaskResult{r1, r2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ResultsForJob = %+v, want %+v (put order, no dedup)", got, want)
	}

	empty, err := s.ResultsForJob("no-such-job")
	if err != nil {
		t.Fatalf("ResultsForJob(unknown): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ResultsForJob(unknown) = %+v, want empty", empty)
	}

	if _, ok, err := s.GetAggregate("job-1"); err != nil || ok {
		t.Fatalf("GetAggregate(absent) = (_, %v, %v), want (_, false, nil)", ok, err)
	}
	agg := model.Aggregate{JobID: "job-1", Value: []byte("merged"), Done: true}
	if err := s.PutAggregate(agg); err != nil {
		t.Fatalf("PutAggregate: %v", err)
	}
	gotAgg, ok, err := s.GetAggregate("job-1")
	if err != nil || !ok {
		t.Fatalf("GetAggregate = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	if !reflect.DeepEqual(gotAgg, agg) {
		t.Fatalf("GetAggregate round-trip mismatch: got %+v, want %+v", gotAgg, agg)
	}
}

// buildRegistry folds a fixed sequence of events into a fresh Registry, for
// tests that need a populated value to round-trip.
func buildRegistry() registry.Registry {
	reg := registry.Registry{}
	reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.CellUp, Cell: "a", Capacity: 5})
	reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.CellUp, Cell: "b", Capacity: 3})
	reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.AgentJoined, Cell: "a", Agent: "x"})
	reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.AgentJoined, Cell: "a", Agent: "y"})
	reg, _ = registry.Apply(reg, registry.RegistryEvent{Kind: registry.AgentJoined, Cell: "b", Agent: "z"})
	return reg
}

func sortedViews(views []model.CellView) []model.CellView {
	out := append([]model.CellView(nil), views...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// TestFDBRegistryRoundTrip proves SetRegistry(reg) then Registry() through a
// real FDB round trip returns a value that is Snapshot-equal to reg AND
// behaves identically under a subsequent Apply (agent identity preserved,
// not just derived Size/Free) — the same property serialize_test.go proves
// hermetically for GobEncode/GobDecode, now proven through the wire.
func TestFDBRegistryRoundTrip(t *testing.T) {
	s := newTestFDBStore(t)

	if got := s.Registry(); !reflect.DeepEqual(got, registry.Registry{}) {
		t.Fatalf("Registry() on a fresh store = %+v, want the empty zero value", got)
	}

	reg := buildRegistry()
	if err := s.SetRegistry(reg); err != nil {
		t.Fatalf("SetRegistry: %v", err)
	}

	got := s.Registry()
	if !reflect.DeepEqual(sortedViews(registry.Snapshot(got)), sortedViews(registry.Snapshot(reg))) {
		t.Fatalf("Registry() Snapshot after round trip mismatch: got %+v, want %+v",
			registry.Snapshot(got), registry.Snapshot(reg))
	}

	// Agent-level equality: rejoining an existing member is a no-op on both
	// the original and the value read back from FDB.
	_, origChanges := registry.Apply(reg, registry.RegistryEvent{Kind: registry.AgentJoined, Cell: "a", Agent: "x"})
	_, gotChanges := registry.Apply(got, registry.RegistryEvent{Kind: registry.AgentJoined, Cell: "a", Agent: "x"})
	if !reflect.DeepEqual(origChanges, gotChanges) {
		t.Fatalf("AgentJoined-for-existing-member changes differ after FDB round trip: orig=%+v got=%+v", origChanges, gotChanges)
	}

	// Leaving a real member produces the same Change on both.
	_, origLeave := registry.Apply(reg, registry.RegistryEvent{Kind: registry.AgentLeft, Cell: "b", Agent: "z"})
	_, gotLeave := registry.Apply(got, registry.RegistryEvent{Kind: registry.AgentLeft, Cell: "b", Agent: "z"})
	if !reflect.DeepEqual(origLeave, gotLeave) {
		t.Fatalf("AgentLeft changes differ after FDB round trip: orig=%+v got=%+v", origLeave, gotLeave)
	}
}

// TestFDBMatchesMemStoreRegistryEvents is the headline test: the same
// registry event sequence, folded through registry.Apply exactly as the
// control plane does (see applyAndStore in sharded_test.go), is persisted
// to BOTH a real FDB-backed store and an in-memory memStore baseline. Their
// registry.Snapshot must be byte-for-byte identical after every single
// event — proving the real FDB backend makes identical decisions to the
// hermetic fake it drops in for (the drop-in guarantee #157/#166 require).
func TestFDBMatchesMemStoreRegistryEvents(t *testing.T) {
	fdbS := newTestFDBStore(t)
	memS := NewMemStore()

	events := []registry.RegistryEvent{
		{Kind: registry.CellUp, Cell: "a", Capacity: 5},
		{Kind: registry.CellUp, Cell: "b", Capacity: 2},
		{Kind: registry.AgentJoined, Cell: "a", Agent: "x"},
		{Kind: registry.AgentJoined, Cell: "a", Agent: "y"},
		{Kind: registry.AgentJoined, Cell: "b", Agent: "z"},
		{Kind: registry.CapacityChanged, Cell: "a", Capacity: 10},
		{Kind: registry.AgentLeft, Cell: "a", Agent: "x"},
		{Kind: registry.CellDown, Cell: "b"},
		{Kind: registry.AgentJoined, Cell: "a", Agent: "w"},
	}

	for i, ev := range events {
		fdbChanges := applyAndStore(t, fdbS, ev)
		memChanges := applyAndStore(t, memS, ev)
		if !reflect.DeepEqual(fdbChanges, memChanges) {
			t.Fatalf("event %d (%+v): Changes differ: fdb=%+v mem=%+v", i, ev, fdbChanges, memChanges)
		}

		fdbView := sortedViews(registry.Snapshot(fdbS.Registry()))
		memView := sortedViews(registry.Snapshot(memS.Registry()))
		if !reflect.DeepEqual(fdbView, memView) {
			t.Fatalf("event %d (%+v): Snapshot differs: fdb=%+v mem=%+v", i, ev, fdbView, memView)
		}
	}
}
