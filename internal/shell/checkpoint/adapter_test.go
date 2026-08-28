package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
)

// fakeClient is a fake Client — standing in for a real S3/MinIO SDK client
// so adapter's own logic (key validation, not-found mapping, error
// propagation) is exercised with no network access, which is what
// make gate-full requires. A real deployment would wire an aws-sdk-go-v2 or
// minio-go backed Client in its place; the Store contract adapter exposes
// does not change.
type fakeClient struct {
	mu      sync.Mutex
	objects map[string][]byte
	putErr  error // if set, PutObject always fails with this error
	getErr  error // if set, GetObject always fails with this error
}

func newFakeClient() *fakeClient {
	return &fakeClient{objects: make(map[string][]byte)}
}

func (c *fakeClient) PutObject(_ context.Context, key string, blob []byte) error {
	if c.putErr != nil {
		return c.putErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.objects[key] = blob
	return nil
}

func (c *fakeClient) GetObject(_ context.Context, key string) ([]byte, bool, error) {
	if c.getErr != nil {
		return nil, false, c.getErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	blob, ok := c.objects[key]
	return blob, ok, nil
}

func TestAdapterRoundTrip(t *testing.T) {
	client := newFakeClient()
	s := NewAdapter(client)
	ctx := context.Background()

	if err := s.Save(ctx, "k", []byte{1, 2, 3}); err != nil {
		t.Fatalf("Save error: %v", err)
	}
	got, err := s.Load(ctx, "k")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("Load = %v, want [1 2 3]", got)
	}
}

func TestAdapterMissingKeyMapsToErrNotFound(t *testing.T) {
	s := NewAdapter(newFakeClient())
	if _, err := s.Load(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load(missing) error = %v, want ErrNotFound", err)
	}
}

func TestAdapterEmptyKey(t *testing.T) {
	s := NewAdapter(newFakeClient())
	ctx := context.Background()

	if err := s.Save(ctx, "", []byte{1}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Save(\"\") error = %v, want ErrEmptyKey", err)
	}
	if _, err := s.Load(ctx, ""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Load(\"\") error = %v, want ErrEmptyKey", err)
	}
}

// TestAdapterPropagatesClientErrors guards that a real backend's failure
// (e.g. a network error, an access-denied response) surfaces to the caller
// unwrapped, rather than being swallowed or misreported as ErrNotFound.
func TestAdapterPropagatesClientErrors(t *testing.T) {
	wantPutErr := errors.New("put: connection refused")
	wantGetErr := errors.New("get: access denied")

	client := newFakeClient()
	client.putErr = wantPutErr
	client.getErr = wantGetErr
	s := NewAdapter(client)
	ctx := context.Background()

	if err := s.Save(ctx, "k", []byte{1}); !errors.Is(err, wantPutErr) {
		t.Fatalf("Save error = %v, want %v", err, wantPutErr)
	}
	if _, err := s.Load(ctx, "k"); !errors.Is(err, wantGetErr) {
		t.Fatalf("Load error = %v, want %v", err, wantGetErr)
	}
}

func TestAdapterRespectsCanceledContext(t *testing.T) {
	s := NewAdapter(newFakeClient())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Save(ctx, "k", []byte{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save with canceled ctx error = %v, want context.Canceled", err)
	}
	if _, err := s.Load(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load with canceled ctx error = %v, want context.Canceled", err)
	}
}
