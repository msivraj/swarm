package checkpoint

import "context"

// Client is the minimal surface a real object-store SDK client (S3, MinIO,
// or any S3-compatible backend) must provide for Adapter to turn it into a
// Store. This package deliberately does not vendor an S3/MinIO SDK: a
// deployment wires a concrete Client — a thin wrapper around
// aws-sdk-go-v2's s3.Client, minio-go, or similar, with the bucket/region/
// credentials baked in at construction — and hands it to NewAdapter. That
// keeps make gate-full free of any live network dependency while still
// exercising the adapter's own logic (key validation, not-found mapping)
// against a fake Client in this package's tests.
type Client interface {
	// PutObject writes blob under key, overwriting any existing object at
	// that key.
	PutObject(ctx context.Context, key string, blob []byte) error
	// GetObject returns the object's bytes and ok=true if key exists.
	// ok=false with a nil error means "no such key" — Adapter turns that
	// into ErrNotFound so callers see the same not-found error regardless
	// of which Store implementation is behind the interface.
	GetObject(ctx context.Context, key string) (blob []byte, ok bool, err error)
}

// adapter implements Store by delegating to a Client. It is the seam a real
// S3/MinIO backend plugs into — see NewAdapter.
type adapter struct {
	client Client
}

// NewAdapter returns a Store backed by client. Any retry, bucket, or
// credential handling belongs to client's own construction; adapter only
// translates between Store's Save/Load and Client's PutObject/GetObject,
// including mapping a missing key to ErrNotFound so callers do not need to
// know which Store implementation they hold.
func NewAdapter(client Client) Store {
	return &adapter{client: client}
}

func (a *adapter) Save(ctx context.Context, key string, blob []byte) error {
	if key == "" {
		return ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.client.PutObject(ctx, key, blob)
}

func (a *adapter) Load(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	blob, ok, err := a.client.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return blob, nil
}
