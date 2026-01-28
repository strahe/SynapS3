package cache

import (
	"context"
	"io"
)

// ObjectInfo holds metadata about a cached object.
type ObjectInfo struct {
	Path     string
	Size     int64
	ETag     string
	Checksum string // SHA-256 hex digest
}

// Cache defines the interface for local object caching.
// Implementations must be safe for concurrent use.
type Cache interface {
	// Put writes data to the cache under the given bucket/key and returns
	// the resulting metadata (path, size, etag, checksum).
	// The data is fsync'd before returning to guarantee durability.
	Put(ctx context.Context, bucket, key string, r io.Reader) (*ObjectInfo, error)

	// Get opens a cached object for reading. Returns os.ErrNotExist if the
	// object is not in the cache.
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error)

	// Delete removes an object from the cache.
	Delete(ctx context.Context, bucket, key string) error

	// Exists reports whether an object is present in the cache.
	Exists(ctx context.Context, bucket, key string) bool

	// UsedBytes returns the total bytes consumed by cached objects.
	UsedBytes() int64

	// CreateBucketDir ensures the directory for a bucket exists.
	CreateBucketDir(ctx context.Context, bucket string) error

	// DeleteBucketDir removes the cache directory for a bucket.
	DeleteBucketDir(ctx context.Context, bucket string) error
}
