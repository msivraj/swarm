// Package store is a shell package: it holds the control plane's mutable P0
// state — submitted jobs, the pending task pull queue, reported
// results/aggregates, and the current registry.Registry value. It performs
// no decisions of its own; cores decide, and the store only persists what
// the shell commits and serves reads back. In P0 the only implementation is
// in-memory, but the Store interface leaves room for a durable backend
// later without the control plane changing.
//
// Ambiguity resolved here (see issue #18): the proposed interface's
// PutResult/ResultsForJob pair needs a TaskID -> JobID mapping to group
// results by job, but model.TaskResult carries only a TaskID. The store
// learns that mapping from EnqueueTasks/RequeueTask (model.Task carries
// both IDs) and PutResult looks it up; a result for a TaskID the store has
// never seen enqueued returns ErrUnknownTask rather than being silently
// dropped or misfiled.
package store

import (
	"errors"
	"sync"

	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

// ErrEmptyJobID is returned when a JobSpec or lookup carries an empty ID —
// the store has nothing to key the record on.
var ErrEmptyJobID = errors.New("store: empty job id")

// ErrEmptyTaskID is returned when a Task carries an empty ID — the store has
// nothing to key the record on.
var ErrEmptyTaskID = errors.New("store: empty task id")

// ErrUnknownTask is returned by PutResult when the result's TaskID was never
// seen in a prior EnqueueTasks/RequeueTask call — the store has no way to
// tell which job the result belongs to. See the package doc for why.
var ErrUnknownTask = errors.New("store: result for unknown task")

// Store is the persistence surface the control plane uses to hold P0 state.
// It performs no I/O beyond persisting/serving the data it is given — all
// placement, decomposition, and aggregation decisions are made by cores
// before their results reach the store.
//
// Methods return an error to leave room for a durable backend (disk, a KV
// store) where a write or read can genuinely fail; the in-memory
// implementation only ever returns an error for a malformed key (see
// ErrEmptyJobID, ErrEmptyTaskID).
type Store interface {
	// PutJob upserts spec, keyed by spec.ID. Re-submitting a job with the
	// same ID overwrites the stored spec — the store does not decide whether
	// that is a legal resubmission, the shell does.
	PutJob(spec model.JobSpec) error
	// GetJob returns the JobSpec stored under id, and whether it was found.
	GetJob(id model.JobID) (model.JobSpec, bool, error)

	// EnqueueTasks appends tasks to the back of the pending-task pull queue,
	// in the order given. P0 tasks are Independent, so a single FIFO queue
	// shared across jobs is sufficient.
	EnqueueTasks(tasks []model.Task) error
	// DequeueTask pops the task at the front of the queue. ok is false when
	// the queue is empty; that is not an error.
	DequeueTask() (model.Task, bool, error)
	// RequeueTask puts t back on the queue for the shell to retry it (e.g.
	// after a dispatch failure). It is appended to the back of the queue —
	// behind whatever is already pending — so one failing task cannot starve
	// the rest of the queue by being redelivered ahead of them.
	RequeueTask(t model.Task) error

	// PutResult records a reported TaskResult, accumulating it under its
	// task's job. The store does not deduplicate by TaskID: a task retried
	// after failure may legitimately report more than once, and it is the
	// aggregation core's job, not the store's, to decide which results
	// count. Returns ErrUnknownTask if r.TaskID was never enqueued (see the
	// package doc).
	PutResult(r model.TaskResult) error
	// ResultsForJob returns every TaskResult recorded for id, in the order
	// they were put. A job with no recorded results returns an empty,
	// non-nil slice.
	ResultsForJob(id model.JobID) ([]model.TaskResult, error)
	// PutAggregate upserts the Aggregate for a.JobID.
	PutAggregate(a model.Aggregate) error
	// GetAggregate returns the Aggregate stored for id, and whether it was
	// found.
	GetAggregate(id model.JobID) (model.Aggregate, bool, error)

	// Registry returns the current authoritative registry.Registry value.
	// registry.Registry is immutable data, so the caller may hold onto the
	// returned value without it changing underneath them.
	Registry() registry.Registry
	// SetRegistry swaps the stored registry.Registry for reg — the shell
	// calls this after folding an event through registry.Apply.
	SetRegistry(reg registry.Registry) error
}

// memStore is an in-memory Store, safe for concurrent use: every field is
// guarded by mu, and callers may invoke its methods from multiple gRPC
// handler goroutines at once.
type memStore struct {
	mu sync.Mutex

	jobs       map[model.JobID]model.JobSpec
	queue      []model.Task
	taskJob    map[model.TaskID]model.JobID // learned from EnqueueTasks/RequeueTask
	results    map[model.JobID][]model.TaskResult
	aggregates map[model.JobID]model.Aggregate
	reg        registry.Registry
}

// NewMemStore returns an empty, ready-to-use in-memory Store.
func NewMemStore() Store {
	return &memStore{
		jobs:       make(map[model.JobID]model.JobSpec),
		taskJob:    make(map[model.TaskID]model.JobID),
		results:    make(map[model.JobID][]model.TaskResult),
		aggregates: make(map[model.JobID]model.Aggregate),
	}
}

func (s *memStore) PutJob(spec model.JobSpec) error {
	if spec.ID == "" {
		return ErrEmptyJobID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[spec.ID] = spec
	return nil
}

func (s *memStore) GetJob(id model.JobID) (model.JobSpec, bool, error) {
	if id == "" {
		return model.JobSpec{}, false, ErrEmptyJobID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := s.jobs[id]
	return spec, ok, nil
}

func (s *memStore) EnqueueTasks(tasks []model.Task) error {
	for _, t := range tasks {
		if t.ID == "" {
			return ErrEmptyTaskID
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, tasks...)
	for _, t := range tasks {
		s.taskJob[t.ID] = t.JobID
	}
	return nil
}

func (s *memStore) DequeueTask() (model.Task, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return model.Task{}, false, nil
	}
	t := s.queue[0]
	s.queue = s.queue[1:]
	return t, true, nil
}

func (s *memStore) RequeueTask(t model.Task) error {
	if t.ID == "" {
		return ErrEmptyTaskID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, t)
	s.taskJob[t.ID] = t.JobID
	return nil
}

func (s *memStore) PutResult(r model.TaskResult) error {
	if r.TaskID == "" {
		return ErrEmptyTaskID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	jobID, ok := s.taskJob[r.TaskID]
	if !ok {
		return ErrUnknownTask
	}
	s.results[jobID] = append(s.results[jobID], r)
	return nil
}

func (s *memStore) ResultsForJob(id model.JobID) ([]model.TaskResult, error) {
	if id == "" {
		return nil, ErrEmptyJobID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.TaskResult, len(s.results[id]))
	copy(out, s.results[id])
	return out, nil
}

func (s *memStore) PutAggregate(a model.Aggregate) error {
	if a.JobID == "" {
		return ErrEmptyJobID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aggregates[a.JobID] = a
	return nil
}

func (s *memStore) GetAggregate(id model.JobID) (model.Aggregate, bool, error) {
	if id == "" {
		return model.Aggregate{}, false, ErrEmptyJobID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.aggregates[id]
	return a, ok, nil
}

func (s *memStore) Registry() registry.Registry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reg
}

func (s *memStore) SetRegistry(reg registry.Registry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reg = reg
	return nil
}
