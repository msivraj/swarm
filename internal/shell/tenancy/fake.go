package tenancy

import (
	"context"
	"fmt"
	"sync"

	"github.com/msivraj/swarm/internal/model"
)

// PlacedJob records one Placer.Place call a FakePlacer observed.
type PlacedJob struct {
	Scope model.TenantScope
	Job   model.JobSpec
}

// FakePlacer is an in-memory Placer for tests: it records every job placed
// under each scope, so a test can assert dispatch order (fairness) and that
// a scope's jobs never mix with another's (isolation) — without any real
// placement/sandbox infrastructure. Safe for concurrent use.
type FakePlacer struct {
	mu    sync.Mutex
	calls []PlacedJob
	fail  map[model.TenantScope]error
}

// NewFakePlacer returns an empty, ready-to-use FakePlacer.
func NewFakePlacer() *FakePlacer {
	return &FakePlacer{fail: make(map[model.TenantScope]error)}
}

// FailNext makes the next Place call for scope return err instead of
// succeeding — useful for exercising DispatchNext's requeue-on-failure path.
func (f *FakePlacer) FailNext(scope model.TenantScope, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[scope] = err
}

// Place implements Placer.
func (f *FakePlacer) Place(_ context.Context, scope model.TenantScope, job model.JobSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err, ok := f.fail[scope]; ok {
		delete(f.fail, scope)
		return err
	}
	if job.ID == "" {
		return fmt.Errorf("tenancy: fake placer refuses to place a job with an empty ID")
	}

	f.calls = append(f.calls, PlacedJob{Scope: scope, Job: job})
	return nil
}

// Calls returns every job placed so far, in dispatch order.
func (f *FakePlacer) Calls() []PlacedJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]PlacedJob, len(f.calls))
	copy(out, f.calls)
	return out
}
