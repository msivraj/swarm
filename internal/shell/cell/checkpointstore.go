package cell

import (
	"sync"

	"github.com/msivraj/swarm/internal/core/checkpoint"
)

// CheckpointStore reads and writes the last checkpoint.State per cell. The
// production implementation belongs to the checkpoint shell (issue #62's
// shell writes checkpoint.Snapshot's bytes to an object store and reads them
// back — see that ticket); this package only needs an interface narrow
// enough for TransportExecutor's OpCheckpoint handling and Resume's "last
// checkpoint" input, so it declares its own rather than reaching into a
// shell that does not exist yet.
type CheckpointStore interface {
	// Put persists ckpt as cell's latest checkpoint.
	Put(cell string, ckpt checkpoint.State) error
	// Last returns cell's latest checkpoint, or ok=false if none has been
	// written yet.
	Last(cell string) (ckpt checkpoint.State, ok bool)
}

// MemCheckpointStore is an in-memory CheckpointStore — the store this
// package's own tests use, and a reasonable default until a real object
// store is wired in behind the same interface.
type MemCheckpointStore struct {
	mu   sync.Mutex
	byID map[string]checkpoint.State
}

// NewMemCheckpointStore returns a ready-to-use MemCheckpointStore.
func NewMemCheckpointStore() *MemCheckpointStore {
	return &MemCheckpointStore{byID: make(map[string]checkpoint.State)}
}

// Put stores ckpt as cell's latest checkpoint, overwriting any previous one.
func (m *MemCheckpointStore) Put(cell string, ckpt checkpoint.State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byID[cell] = ckpt
	return nil
}

// Last returns cell's latest stored checkpoint.
func (m *MemCheckpointStore) Last(cell string) (checkpoint.State, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ckpt, ok := m.byID[cell]
	return ckpt, ok
}
