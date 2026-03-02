package cache

import (
	"context"
	"errors"
	"io"
)

// ErrCacheFull is returned when the cache has reached its maximum capacity.
var ErrCacheFull = errors.New("cache: storage capacity exceeded")

// ErrInvalidPath is returned when a bucket or key would escape the cache root.
var ErrInvalidPath = errors.New("cache: invalid path (traversal attempt)")

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
	// Returns ErrCacheFull if the cache has reached its maximum capacity.
	// Returns ErrInvalidPath if bucket/key would escape the cache root.
	Put(ctx context.Context, bucket, key string, r io.Reader) (*ObjectInfo, error)

	// Get opens a cached object for reading. Returns os.ErrNotExist if the
	// object is not in the cache.
	// Returns ErrInvalidPath if bucket/key would escape the cache root.
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error)

	// Delete removes an object from the cache.
	// Returns ErrInvalidPath if bucket/key would escape the cache root.
	Delete(ctx context.Context, bucket, key string) error

	// Exists reports whether an object is present in the cache.
	Exists(ctx context.Context, bucket, key string) bool

	// UsedBytes returns the total bytes consumed by cached objects.
	UsedBytes() int64

	// CreateBucketDir ensures the directory for a bucket exists.
	// Returns ErrInvalidPath if bucket would escape the cache root.
	CreateBucketDir(ctx context.Context, bucket string) error

	// DeleteBucketDir removes the cache directory for a bucket.
	// Returns ErrInvalidPath if bucket would escape the cache root.
	DeleteBucketDir(ctx context.Context, bucket string) error

	// PutPart writes a multipart upload part to the cache.
	// Parts are stored under .multipart/<uploadID>/<partNumber>.
	// Returns ObjectInfo with the part's Size, ETag (MD5), and Checksum (SHA-256).
	PutPart(ctx context.Context, uploadID string, partNumber int, r io.Reader) (*ObjectInfo, error)

	// AssembleParts concatenates the specified parts in order into a final
	// object at bucket/key. Returns ObjectInfo for the assembled object and
	// the ordered list of individual part MD5 hex digests (for S3 ETag computation).
	// The part files are NOT deleted; call DeleteUpload to clean up.
	AssembleParts(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (*ObjectInfo, []string, error)

	// DeleteUpload removes all part files for the given upload ID.
	DeleteUpload(ctx context.Context, uploadID string) error
}
