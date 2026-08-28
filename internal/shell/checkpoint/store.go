// Package checkpoint is a shell package: it moves the bytes
// checkpoint.Snapshot produces to and from an object store, and reads them
// back on Rollback/resume. All decisions about *when* to checkpoint
// (checkpoint.Due) and all (de)serialization (checkpoint.Snapshot/Restore)
// live in the pure core, package internal/core/checkpoint (#62) — this
// package only persists and retrieves the resulting bytes under a key. A
// caller composes the two:
//
//	store.Save(ctx, key, checkpoint.Snapshot(state))
//	blob, err := store.Load(ctx, key)
//	state := checkpoint.Restore(blob)
//
// Store is deliberately standalone from internal/shell/store (the control
// plane's job/task/result/registry persistence): a driver checkpoint blob is
// an unrelated concern with its own lifecycle (per-driver, per-step,
// potentially large), so this package does not extend that store's
// interface.
//
// The default, in-memory Store (NewMemStore) is what make gate-full runs and
// what other shells should use in tests — no network, no filesystem. A real
// deployment's S3/MinIO backend implements the same Store interface behind a
// thin adapter (see adapter.go); make gate-full never needs a live object
// store to pass.
package checkpoint

import (
	"context"
	"errors"
	"sync"
)

// ErrNotFound is returned by Load when no blob has ever been saved under
// key — a normal "no checkpoint yet" condition (e.g. a fresh driver with no
// prior checkpoint to resume from), not a panic-worthy one.
var ErrNotFound = errors.New("checkpoint: key not found")

// ErrEmptyKey is returned by Save/Load when key is empty — a Store has
// nothing to persist/retrieve under.
var ErrEmptyKey = errors.New("checkpoint: empty key")

// Store persists and retrieves opaque checkpoint bytes by key. It performs
// no (de)serialization of its own — see the package doc for how callers
// compose it with checkpoint.Snapshot/Restore.
type Store interface {
	// Save persists blob under key, replacing any blob previously saved
	// under the same key. Key-naming (e.g. one key per step, or a single
	// "latest" key that Save keeps overwriting) — and therefore the
	// retention policy — is the caller's decision, not the Store's.
	Save(ctx context.Context, key string, blob []byte) error
	// Load returns the blob last saved under key. It returns ErrNotFound,
	// not a panic or a nil-blob success, if no blob has ever been saved
	// under key.
	Load(ctx context.Context, key string) ([]byte, error)
}

// memStore is an in-memory Store, safe for concurrent use: every access is
// guarded by mu, and callers may invoke Save/Load from multiple goroutines
// at once (e.g. concurrent driver shells checkpointing independently).
type memStore struct {
	mu    sync.Mutex
	blobs map[string][]byte
}

// NewMemStore returns an empty, ready-to-use in-memory Store. It is the
// default implementation — the object-store abstraction with no external
// dependency — and is what tests and make gate-full use.
func NewMemStore() Store {
	return &memStore{blobs: make(map[string][]byte)}
}

func (s *memStore) Save(ctx context.Context, key string, blob []byte) error {
	if key == "" {
		return ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cp := make([]byte, len(blob))
	copy(cp, blob)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.blobs[key] = cp
	return nil
}

func (s *memStore) Load(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	blob, ok := s.blobs[key]
	if !ok {
		return nil, ErrNotFound
	}
	cp := make([]byte, len(blob))
	copy(cp, blob)
	return cp, nil
}
