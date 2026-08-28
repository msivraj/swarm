package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
)

// -----------------------------------------------------------------------
// Save / Load — table-driven
// -----------------------------------------------------------------------

func TestMemStoreSaveThenLoad(t *testing.T) {
	tests := []struct {
		name    string
		seed    map[string][]byte // pre-saved key -> blob, applied before saveKey/saveVal
		saveKey string
		saveVal []byte
		loadKey string
		want    []byte
		wantErr error
	}{
		{
			name:    "round-trips a populated blob",
			saveKey: "k1",
			saveVal: []byte{1, 2, 3},
			loadKey: "k1",
			want:    []byte{1, 2, 3},
		},
		{
			name:    "round-trips an empty (non-nil) blob",
			saveKey: "k2",
			saveVal: []byte{},
			loadKey: "k2",
			want:    []byte{},
		},
		{
			name:    "round-trips a nil blob as empty",
			saveKey: "k3",
			saveVal: nil,
			loadKey: "k3",
			want:    []byte{},
		},
		{
			name:    "re-saving the same key overwrites the prior blob",
			seed:    map[string][]byte{"k4": {9, 9, 9}},
			saveKey: "k4",
			saveVal: []byte{4, 5, 6},
			loadKey: "k4",
			want:    []byte{4, 5, 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemStore()
			ctx := context.Background()

			for k, v := range tt.seed {
				if err := s.Save(ctx, k, v); err != nil {
					t.Fatalf("seed Save(%q) error: %v", k, err)
				}
			}
			if err := s.Save(ctx, tt.saveKey, tt.saveVal); err != nil {
				t.Fatalf("Save(%q, ...) unexpected error: %v", tt.saveKey, err)
			}

			got, err := s.Load(ctx, tt.loadKey)
			if err != nil {
				t.Fatalf("Load(%q) unexpected error: %v", tt.loadKey, err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("Load(%q) = %v, want %v", tt.loadKey, got, tt.want)
			}
		})
	}
}

// TestMemStoreErrors covers the error paths that never reach a happy-path
// round trip: a missing key and an empty key on either Save or Load.
func TestMemStoreErrors(t *testing.T) {
	tests := []struct {
		name string
		run  func(s Store, ctx context.Context) error
		want error
	}{
		{
			name: "load a never-saved key returns ErrNotFound",
			run: func(s Store, ctx context.Context) error {
				_, err := s.Load(ctx, "missing")
				return err
			},
			want: ErrNotFound,
		},
		{
			name: "save with an empty key returns ErrEmptyKey",
			run: func(s Store, ctx context.Context) error {
				return s.Save(ctx, "", []byte{1})
			},
			want: ErrEmptyKey,
		},
		{
			name: "load with an empty key returns ErrEmptyKey",
			run: func(s Store, ctx context.Context) error {
				_, err := s.Load(ctx, "")
				return err
			},
			want: ErrEmptyKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run(NewMemStore(), context.Background())
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestMemStoreLoadDoesNotAliasStoredBlob guards against a caller mutating
// the slice Load returns and corrupting what a later Load sees — Load must
// hand back a defensive copy.
func TestMemStoreLoadDoesNotAliasStoredBlob(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	if err := s.Save(ctx, "k", []byte{1, 2, 3}); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	got, err := s.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	got[0] = 99 // mutate the caller's copy

	again, err := s.Load(ctx, "k")
	if err != nil {
		t.Fatalf("second Load error: %v", err)
	}
	if !bytes.Equal(again, []byte{1, 2, 3}) {
		t.Fatalf("stored blob was mutated via a Load-returned slice: got %v", again)
	}
}

// TestMemStoreSaveDoesNotAliasCallerBlob guards the same direction: mutating
// the caller's slice after Save must not affect what a later Load sees.
func TestMemStoreSaveDoesNotAliasCallerBlob(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	blob := []byte{1, 2, 3}
	if err := s.Save(ctx, "k", blob); err != nil {
		t.Fatalf("Save error: %v", err)
	}
	blob[0] = 99 // mutate the caller's original after Save

	got, err := s.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("stored blob was mutated via the caller's slice: got %v", got)
	}
}

// TestMemStoreRespectsCanceledContext guards the seam the S3/MinIO adapter
// relies on for cancellation: a canceled context short-circuits Save/Load
// rather than doing the work anyway.
func TestMemStoreRespectsCanceledContext(t *testing.T) {
	s := NewMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Save(ctx, "k", []byte{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save with canceled ctx error = %v, want context.Canceled", err)
	}
	if _, err := s.Load(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load with canceled ctx error = %v, want context.Canceled", err)
	}
}

// TestMemStoreConcurrentAccess exercises Save/Load from many goroutines at
// once; run with -race (make gate-full does) to catch any unguarded access.
func TestMemStoreConcurrentAccess(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	const n = 50
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_ = s.Save(ctx, "shared-key", []byte{byte(i)})
		}(i)
		go func() {
			defer wg.Done()
			_, _ = s.Load(ctx, "shared-key")
		}()
	}
	wg.Wait()

	// The store must still be in a consistent, readable state afterward.
	if _, err := s.Load(ctx, "shared-key"); err != nil {
		t.Fatalf("Load after concurrent access error: %v", err)
	}
}
