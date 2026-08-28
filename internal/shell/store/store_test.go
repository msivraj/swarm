package store

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

func TestPutGetJob(t *testing.T) {
	tests := []struct {
		name    string
		seed    []model.JobSpec
		getID   model.JobID
		wantOK  bool
		want    model.JobSpec
		wantErr error
	}{
		{
			name:   "get missing job returns ok=false",
			getID:  "missing",
			wantOK: false,
		},
		{
			name:   "put then get round-trips the spec",
			seed:   []model.JobSpec{{ID: "j1", Template: "tpl", Coupling: model.Independent, Params: map[string]string{"k": "v"}}},
			getID:  "j1",
			wantOK: true,
			want:   model.JobSpec{ID: "j1", Template: "tpl", Coupling: model.Independent, Params: map[string]string{"k": "v"}},
		},
		{
			name: "re-putting the same id overwrites the spec",
			seed: []model.JobSpec{
				{ID: "j1", Template: "old"},
				{ID: "j1", Template: "new"},
			},
			getID:  "j1",
			wantOK: true,
			want:   model.JobSpec{ID: "j1", Template: "new"},
		},
		{
			name:    "get with empty id errors",
			getID:   "",
			wantErr: ErrEmptyJobID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemStore()
			for _, spec := range tt.seed {
				if err := s.PutJob(spec); err != nil {
					t.Fatalf("PutJob(%+v) = %v, want nil", spec, err)
				}
			}
			got, ok, err := s.GetJob(tt.getID)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("GetJob() err = %v, want %v", err, tt.wantErr)
			}
			if ok != tt.wantOK {
				t.Fatalf("GetJob() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("GetJob() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestPutJobEmptyID(t *testing.T) {
	s := NewMemStore()
	if err := s.PutJob(model.JobSpec{}); !errors.Is(err, ErrEmptyJobID) {
		t.Fatalf("PutJob(empty) = %v, want %v", err, ErrEmptyJobID)
	}
}

func TestEnqueueDequeueFIFO(t *testing.T) {
	s := NewMemStore()
	tasks := []model.Task{
		{ID: "t1", JobID: "j1"},
		{ID: "t2", JobID: "j1"},
		{ID: "t3", JobID: "j1"},
	}
	for _, task := range tasks {
		if err := s.EnqueueTask("cell-a", task); err != nil {
			t.Fatalf("EnqueueTask(%+v) = %v, want nil", task, err)
		}
	}

	for _, want := range tasks {
		got, ok, err := s.DequeueTask("cell-a")
		if err != nil {
			t.Fatalf("DequeueTask() err = %v, want nil", err)
		}
		if !ok {
			t.Fatalf("DequeueTask() ok = false, want true")
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("DequeueTask() = %+v, want %+v", got, want)
		}
	}
}

func TestDequeueEmptyReturnsNotOK(t *testing.T) {
	s := NewMemStore()
	got, ok, err := s.DequeueTask("cell-a")
	if err != nil {
		t.Fatalf("DequeueTask() err = %v, want nil", err)
	}
	if ok {
		t.Fatalf("DequeueTask() ok = true on empty queue, want false")
	}
	if !reflect.DeepEqual(got, model.Task{}) {
		t.Fatalf("DequeueTask() = %+v, want zero value", got)
	}
}

func TestEnqueueEmptyTaskID(t *testing.T) {
	s := NewMemStore()
	err := s.EnqueueTask("cell-a", model.Task{ID: "", JobID: "j1"})
	if !errors.Is(err, ErrEmptyTaskID) {
		t.Fatalf("EnqueueTask(empty id) = %v, want %v", err, ErrEmptyTaskID)
	}
}

func TestRequeueTask(t *testing.T) {
	s := NewMemStore()
	first := model.Task{ID: "t1", JobID: "j1"}
	second := model.Task{ID: "t2", JobID: "j1"}
	if err := s.EnqueueTask("cell-a", first); err != nil {
		t.Fatalf("EnqueueTask() = %v, want nil", err)
	}
	if err := s.EnqueueTask("cell-a", second); err != nil {
		t.Fatalf("EnqueueTask() = %v, want nil", err)
	}

	// Dequeue t1, then requeue it — it should land behind t2, at the back.
	got, ok, _ := s.DequeueTask("cell-a")
	if !ok || !reflect.DeepEqual(got, first) {
		t.Fatalf("DequeueTask() = %+v, %v, want %+v, true", got, ok, first)
	}
	if err := s.RequeueTask("cell-a", first); err != nil {
		t.Fatalf("RequeueTask() = %v, want nil", err)
	}

	wantOrder := []model.Task{second, first}
	for _, want := range wantOrder {
		got, ok, _ := s.DequeueTask("cell-a")
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("DequeueTask() = %+v, %v, want %+v, true", got, ok, want)
		}
	}
}

func TestRequeueEmptyTaskID(t *testing.T) {
	s := NewMemStore()
	if err := s.RequeueTask("cell-a", model.Task{}); !errors.Is(err, ErrEmptyTaskID) {
		t.Fatalf("RequeueTask(empty) = %v, want %v", err, ErrEmptyTaskID)
	}
}

// TestPerCellQueuesAreIsolated is the core P1 property this ticket adds: a
// task enqueued on one cell is never visible to a DequeueTask call for a
// different cell, and each cell's queue keeps its own independent FIFO
// order.
func TestPerCellQueuesAreIsolated(t *testing.T) {
	s := NewMemStore()

	a1 := model.Task{ID: "a1", JobID: "j1"}
	a2 := model.Task{ID: "a2", JobID: "j1"}
	b1 := model.Task{ID: "b1", JobID: "j1"}

	if err := s.EnqueueTask("cell-a", a1); err != nil {
		t.Fatalf("EnqueueTask(cell-a, a1) = %v, want nil", err)
	}
	if err := s.EnqueueTask("cell-b", b1); err != nil {
		t.Fatalf("EnqueueTask(cell-b, b1) = %v, want nil", err)
	}
	if err := s.EnqueueTask("cell-a", a2); err != nil {
		t.Fatalf("EnqueueTask(cell-a, a2) = %v, want nil", err)
	}

	// cell-b's queue must yield only b1, never a1 or a2.
	got, ok, err := s.DequeueTask("cell-b")
	if err != nil {
		t.Fatalf("DequeueTask(cell-b) err = %v, want nil", err)
	}
	if !ok || !reflect.DeepEqual(got, b1) {
		t.Fatalf("DequeueTask(cell-b) = %+v, %v, want %+v, true", got, ok, b1)
	}
	if _, ok, _ := s.DequeueTask("cell-b"); ok {
		t.Fatalf("DequeueTask(cell-b) ok = true after draining cell-b's only task")
	}

	// cell-a's queue is untouched by cell-b's dequeues, and keeps its own
	// FIFO order.
	for _, want := range []model.Task{a1, a2} {
		got, ok, err := s.DequeueTask("cell-a")
		if err != nil {
			t.Fatalf("DequeueTask(cell-a) err = %v, want nil", err)
		}
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("DequeueTask(cell-a) = %+v, %v, want %+v, true", got, ok, want)
		}
	}
}

func TestPutResultAccumulatesPerJob(t *testing.T) {
	s := NewMemStore()
	tasks := []model.Task{
		{ID: "t1", JobID: "j1"},
		{ID: "t2", JobID: "j1"},
		{ID: "t3", JobID: "j2"},
	}
	for _, task := range tasks {
		if err := s.EnqueueTask("cell-a", task); err != nil {
			t.Fatalf("EnqueueTask(%+v) = %v, want nil", task, err)
		}
	}

	results := []model.TaskResult{
		{TaskID: "t1", Output: []byte("a"), OK: true},
		{TaskID: "t2", Output: []byte("b"), OK: true},
		{TaskID: "t3", Output: []byte("c"), OK: false},
	}
	for _, r := range results {
		if err := s.PutResult(r); err != nil {
			t.Fatalf("PutResult(%+v) = %v, want nil", r, err)
		}
	}

	j1, err := s.ResultsForJob("j1")
	if err != nil {
		t.Fatalf("ResultsForJob(j1) err = %v, want nil", err)
	}
	wantJ1 := []model.TaskResult{results[0], results[1]}
	if !reflect.DeepEqual(j1, wantJ1) {
		t.Fatalf("ResultsForJob(j1) = %+v, want %+v", j1, wantJ1)
	}

	j2, err := s.ResultsForJob("j2")
	if err != nil {
		t.Fatalf("ResultsForJob(j2) err = %v, want nil", err)
	}
	wantJ2 := []model.TaskResult{results[2]}
	if !reflect.DeepEqual(j2, wantJ2) {
		t.Fatalf("ResultsForJob(j2) = %+v, want %+v", j2, wantJ2)
	}
}

func TestResultsForJobEmptyReturnsEmptySlice(t *testing.T) {
	s := NewMemStore()
	got, err := s.ResultsForJob("nothing-here")
	if err != nil {
		t.Fatalf("ResultsForJob() err = %v, want nil", err)
	}
	if got == nil {
		t.Fatalf("ResultsForJob() = nil, want empty non-nil slice")
	}
	if len(got) != 0 {
		t.Fatalf("ResultsForJob() = %+v, want empty", got)
	}
}

func TestResultsForJobEmptyID(t *testing.T) {
	s := NewMemStore()
	if _, err := s.ResultsForJob(""); !errors.Is(err, ErrEmptyJobID) {
		t.Fatalf("ResultsForJob(empty) err = %v, want %v", err, ErrEmptyJobID)
	}
}

func TestPutResultUnknownTask(t *testing.T) {
	s := NewMemStore()
	err := s.PutResult(model.TaskResult{TaskID: "never-enqueued"})
	if !errors.Is(err, ErrUnknownTask) {
		t.Fatalf("PutResult(unknown task) = %v, want %v", err, ErrUnknownTask)
	}
}

func TestPutResultEmptyTaskID(t *testing.T) {
	s := NewMemStore()
	if err := s.PutResult(model.TaskResult{}); !errors.Is(err, ErrEmptyTaskID) {
		t.Fatalf("PutResult(empty task id) = %v, want %v", err, ErrEmptyTaskID)
	}
}

func TestPutGetAggregate(t *testing.T) {
	tests := []struct {
		name    string
		seed    []model.Aggregate
		getID   model.JobID
		wantOK  bool
		want    model.Aggregate
		wantErr error
	}{
		{
			name:   "get missing aggregate returns ok=false",
			getID:  "missing",
			wantOK: false,
		},
		{
			name:   "put then get round-trips the aggregate",
			seed:   []model.Aggregate{{JobID: "j1", Value: []byte("v"), Done: true}},
			getID:  "j1",
			wantOK: true,
			want:   model.Aggregate{JobID: "j1", Value: []byte("v"), Done: true},
		},
		{
			name: "re-putting the same job id overwrites the aggregate",
			seed: []model.Aggregate{
				{JobID: "j1", Value: []byte("old"), Done: false},
				{JobID: "j1", Value: []byte("new"), Done: true},
			},
			getID:  "j1",
			wantOK: true,
			want:   model.Aggregate{JobID: "j1", Value: []byte("new"), Done: true},
		},
		{
			name:    "get with empty id errors",
			getID:   "",
			wantErr: ErrEmptyJobID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemStore()
			for _, a := range tt.seed {
				if err := s.PutAggregate(a); err != nil {
					t.Fatalf("PutAggregate(%+v) = %v, want nil", a, err)
				}
			}
			got, ok, err := s.GetAggregate(tt.getID)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("GetAggregate() err = %v, want %v", err, tt.wantErr)
			}
			if ok != tt.wantOK {
				t.Fatalf("GetAggregate() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("GetAggregate() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestPutAggregateEmptyJobID(t *testing.T) {
	s := NewMemStore()
	if err := s.PutAggregate(model.Aggregate{}); !errors.Is(err, ErrEmptyJobID) {
		t.Fatalf("PutAggregate(empty) = %v, want %v", err, ErrEmptyJobID)
	}
}

func TestRegistrySetGetSwapsValue(t *testing.T) {
	s := NewMemStore()

	empty := s.Registry()
	if got := registry.Snapshot(empty); got != nil {
		t.Fatalf("Registry() initial = %+v, want empty", got)
	}

	reg, changes := registry.Apply(registry.Registry{}, registry.RegistryEvent{
		Kind: registry.CellUp, Cell: "a", Capacity: 5,
	})
	if len(changes) == 0 {
		t.Fatalf("registry.Apply() produced no changes, test setup is broken")
	}

	if err := s.SetRegistry(reg); err != nil {
		t.Fatalf("SetRegistry() = %v, want nil", err)
	}

	got := s.Registry()
	wantView := []model.CellView{{ID: "a", Size: 0, Free: 5}}
	if gotView := registry.Snapshot(got); !reflect.DeepEqual(gotView, wantView) {
		t.Fatalf("Registry() after SetRegistry = %+v, want %+v", gotView, wantView)
	}
}

// TestConcurrentEnqueueDequeue guards the concurrency requirement: multiple
// goroutines enqueueing and dequeueing a single cell's queue at once must
// not race or lose/misplace tasks. Run with -race.
func TestConcurrentEnqueueDequeue(t *testing.T) {
	s := NewMemStore()
	const cell = model.CellID("cell-a")
	const producers = 20
	const perProducer = 50
	const total = producers * perProducer

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				id := model.TaskID(rune('A' + p))
				task := model.Task{ID: id + model.TaskID(rune(i)), JobID: "j1"}
				if err := s.EnqueueTask(cell, task); err != nil {
					t.Errorf("EnqueueTask() = %v, want nil", err)
				}
			}
		}(p)
	}
	wg.Wait()

	var (
		mu      sync.Mutex
		drained int
	)
	var consumers sync.WaitGroup
	consumers.Add(producers)
	for c := 0; c < producers; c++ {
		go func() {
			defer consumers.Done()
			for {
				_, ok, err := s.DequeueTask(cell)
				if err != nil {
					t.Errorf("DequeueTask() = %v, want nil", err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				drained++
				mu.Unlock()
			}
		}()
	}
	consumers.Wait()

	if drained != total {
		t.Fatalf("drained %d tasks, want %d", drained, total)
	}
	if _, ok, _ := s.DequeueTask(cell); ok {
		t.Fatalf("queue not empty after draining all tasks")
	}
}

// TestConcurrentRegistryAccess guards Registry/SetRegistry under concurrent
// access from multiple goroutines. Run with -race.
func TestConcurrentRegistryAccess(t *testing.T) {
	s := NewMemStore()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			reg, _ := registry.Apply(registry.Registry{}, registry.RegistryEvent{
				Kind: registry.CellUp, Cell: model.CellID(rune('a' + i%26)), Capacity: i,
			})
			_ = s.SetRegistry(reg)
		}(i)
		go func() {
			defer wg.Done()
			_ = s.Registry()
		}()
	}
	wg.Wait()
}
