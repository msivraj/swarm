package checkpoint_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	core "github.com/msivraj/swarm/internal/core/checkpoint"
	shellckpt "github.com/msivraj/swarm/internal/shell/checkpoint"
)

// storeCtor builds a fresh, empty Store — used to run the same integration
// tests against every Store implementation this package ships (the default
// in-memory one, and the S3/MinIO adapter seam backed by a fake Client).
type storeCtor struct {
	name string
	new  func() shellckpt.Store
}

func storeImpls() []storeCtor {
	return []storeCtor{
		{name: "mem", new: func() shellckpt.Store { return shellckpt.NewMemStore() }},
		{name: "adapter", new: func() shellckpt.Store { return shellckpt.NewAdapter(newFakeS3Client()) }},
	}
}

// fakeS3Client is a self-contained fake of the Client seam, duplicated here
// (rather than reused from the in-package adapter_test.go) because this file
// lives in the external checkpoint_test package and cannot see unexported
// test helpers.
type fakeS3Client struct{ objects map[string][]byte }

func newFakeS3Client() *fakeS3Client { return &fakeS3Client{objects: make(map[string][]byte)} }

func (c *fakeS3Client) PutObject(_ context.Context, key string, blob []byte) error {
	c.objects[key] = blob
	return nil
}

func (c *fakeS3Client) GetObject(_ context.Context, key string) ([]byte, bool, error) {
	blob, ok := c.objects[key]
	return blob, ok, nil
}

// states enumerates varied-shape checkpoint.State values, mirroring the
// core package's own round-trip fixtures (nil vs. empty vs. populated
// fields, zero/negative steps) so this shell-level test is the echo of the
// core's restore(snapshot(s))==s law, not a narrower one.
func states() []core.State {
	return []core.State{
		{},
		{Step: 7, Members: []string{"a", "b"}, DriverBlob: []byte{1, 2, 3}, Meta: map[string]string{"k": "v"}},
		{Step: -3, Members: []string{}, DriverBlob: []byte{}, Meta: map[string]string{}},
		{Step: 1000000, Members: nil, DriverBlob: nil, Meta: nil},
	}
}

// TestRoundTripThroughStore is the shell-level echo of the core's
// restore(snapshot(s))==s law: Snapshot -> Store.Save -> Store.Load ->
// Restore must reproduce the original State, for every Store implementation.
func TestRoundTripThroughStore(t *testing.T) {
	for _, impl := range storeImpls() {
		t.Run(impl.name, func(t *testing.T) {
			for _, s := range states() {
				s := s
				store := impl.new()
				ctx := context.Background()

				if err := store.Save(ctx, "ckpt", core.Snapshot(s)); err != nil {
					t.Fatalf("Save error: %v", err)
				}
				blob, err := store.Load(ctx, "ckpt")
				if err != nil {
					t.Fatalf("Load error: %v", err)
				}
				got := core.Restore(blob)

				if !reflect.DeepEqual(got, s) {
					t.Fatalf("round trip mismatch:\n got  %#v\n want %#v", got, s)
				}
			}
		})
	}
}

// TestLoadMissingKeyIsASaneError guards that resuming from a key nothing has
// ever been checkpointed under returns a clear, checkable error rather than
// panicking or silently returning a zero State.
func TestLoadMissingKeyIsASaneError(t *testing.T) {
	for _, impl := range storeImpls() {
		t.Run(impl.name, func(t *testing.T) {
			store := impl.new()
			_, err := store.Load(context.Background(), "never-saved")
			if !errors.Is(err, shellckpt.ErrNotFound) {
				t.Fatalf("Load(never-saved) error = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestSaveOverwriteSemantics documents and tests the store's overwrite
// behavior: Save-ing a new checkpoint under a key a driver reuses (e.g. a
// single "latest" key) replaces the prior one — Load only ever sees the
// most recent Save, never a stale or merged checkpoint.
func TestSaveOverwriteSemantics(t *testing.T) {
	for _, impl := range storeImpls() {
		t.Run(impl.name, func(t *testing.T) {
			store := impl.new()
			ctx := context.Background()

			first := core.State{Step: 1, Members: []string{"a"}}
			second := core.State{Step: 2, Members: []string{"a", "b"}}

			if err := store.Save(ctx, "latest", core.Snapshot(first)); err != nil {
				t.Fatalf("Save(first) error: %v", err)
			}
			if err := store.Save(ctx, "latest", core.Snapshot(second)); err != nil {
				t.Fatalf("Save(second) error: %v", err)
			}

			blob, err := store.Load(ctx, "latest")
			if err != nil {
				t.Fatalf("Load error: %v", err)
			}
			got := core.Restore(blob)
			if !reflect.DeepEqual(got, second) {
				t.Fatalf("Load after overwrite = %#v, want %#v (the second Save)", got, second)
			}
		})
	}
}

// TestSaveDistinctKeysDoNotCollide guards that per-step keys (as opposed to
// a single reused "latest" key) coexist independently — a Save under one
// key never disturbs what is Load-able under another.
func TestSaveDistinctKeysDoNotCollide(t *testing.T) {
	store := shellckpt.NewMemStore()
	ctx := context.Background()

	stepA := core.State{Step: 1}
	stepB := core.State{Step: 2}

	if err := store.Save(ctx, "step-1", core.Snapshot(stepA)); err != nil {
		t.Fatalf("Save(step-1) error: %v", err)
	}
	if err := store.Save(ctx, "step-2", core.Snapshot(stepB)); err != nil {
		t.Fatalf("Save(step-2) error: %v", err)
	}

	blobA, err := store.Load(ctx, "step-1")
	if err != nil {
		t.Fatalf("Load(step-1) error: %v", err)
	}
	blobB, err := store.Load(ctx, "step-2")
	if err != nil {
		t.Fatalf("Load(step-2) error: %v", err)
	}

	if got := core.Restore(blobA); !reflect.DeepEqual(got, stepA) {
		t.Fatalf("Load(step-1) = %#v, want %#v", got, stepA)
	}
	if got := core.Restore(blobB); !reflect.DeepEqual(got, stepB) {
		t.Fatalf("Load(step-2) = %#v, want %#v", got, stepB)
	}
}
