package cache

import (
	"context"
	"errors"
	"io"
)

// ErrCacheFull is returned when the cache cannot hold a write's size on top of
// committed and in-progress writes.
var ErrCacheFull = errors.New("cache: storage capacity exceeded")

// ErrSizeExceeded is returned when a write supplies more bytes than the size it
// reserved.
var ErrSizeExceeded = errors.New("cache: write exceeds its declared size")

// ErrInvalidPath is returned when a bucket or key would escape the cache root.
var ErrInvalidPath = errors.New("cache: invalid path (traversal attempt)")

// ObjectInfo holds metadata about a cached object.
type ObjectInfo struct {
	Path     string
	Size     int64
	ETag     string
	Checksum string // SHA-256 hex digest
}

// StagedObject represents a cached file that has been written to a temp location
// but not yet committed to the final cache path. This enables atomic "write then
// commit" workflows where a DB transaction runs between write and commit.
type StagedObject struct {
	Info     *ObjectInfo
	commit   func() error
	commitAs func(bucket, key string) error
	rollback func() error
}

// Commit renames the staged file to the final cache path (atomic).
// Must be called exactly once. After Commit, Rollback is a no-op.
func (s *StagedObject) Commit() error { return s.commit() }

// CommitAs commits to a different final path than the one the object was staged
// under. Cache residency is content-addressed, so writers stage the bytes first
// and only learn the destination once the checksum has produced a content row.
func (s *StagedObject) CommitAs(bucket, key string) error { return s.commitAs(bucket, key) }

// Rollback removes the staged temp file without affecting the final cache path.
// Safe to call multiple times and after Commit (no-op if already committed).
func (s *StagedObject) Rollback() error { return s.rollback() }

// Cache defines the interface for local object caching.
// Implementations must be safe for concurrent use.
type Cache interface {
	// Put writes data to the cache under the given bucket/key and returns
	// the resulting metadata (path, size, etag, checksum).
	// The data is fsync'd before returning to guarantee durability.
	// size is the most bytes r may supply; the write holds that much capacity
	// before reading r and returns ErrCacheFull without reading when the cache
	// cannot hold it, or ErrSizeExceeded when r supplies more.
	// Returns ErrInvalidPath if bucket/key would escape the cache root.
	Put(ctx context.Context, bucket, key string, r io.Reader, size int64) (*ObjectInfo, error)

	// PutStaged writes data to a temp file (fsync'd) without replacing the
	// existing cache entry. Returns a StagedObject whose Commit method
	// atomically renames the temp file to the final path. If Commit is not
	// called, Rollback removes the temp file. The capacity held for size, as
	// in Put, lasts until Commit or Rollback.
	// Callers choose whether to commit before or after their DB transaction.
	PutStaged(ctx context.Context, bucket, key string, r io.Reader, size int64) (*StagedObject, error)

	// Get opens a cached object for reading. Returns os.ErrNotExist if the
	// object is not in the cache.
	// Returns ErrInvalidPath if bucket/key would escape the cache root.
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectInfo, error)

	// Delete removes an object from the cache.
	// Returns ErrInvalidPath if bucket/key would escape the cache root.
	Delete(ctx context.Context, bucket, key string) error

	// Exists reports whether an object is present in the cache.
	Exists(ctx context.Context, bucket, key string) bool

	// UsedBytes returns the total bytes consumed by committed cached objects.
	UsedBytes() int64

	// ConsumeWriteRefusal reports whether a write was refused with
	// ErrCacheFull since the previous call, and clears that record.
	ConsumeWriteRefusal() bool

	// CreateBucketDir ensures the directory for a bucket exists.
	// Returns ErrInvalidPath if bucket would escape the cache root.
	CreateBucketDir(ctx context.Context, bucket string) error

	// DeleteBucketDir removes the cache directory for a bucket.
	// Returns ErrInvalidPath if bucket would escape the cache root.
	DeleteBucketDir(ctx context.Context, bucket string) error

	// PutPart writes a multipart upload part to the cache.
	// Parts are stored under .multipart/<uploadID>/<partNumber>.
	// size bounds and reserves the part as in Put.
	// Returns ObjectInfo with the part's Size, ETag (MD5), and Checksum (SHA-256).
	PutPart(ctx context.Context, uploadID string, partNumber int, r io.Reader, size int64) (*ObjectInfo, error)

	// AssemblePartsStaged concatenates the specified parts in order into a
	// staged file next to bucket/key, plus the ordered list of individual part
	// MD5 hex digests (for S3 ETag computation). The caller names the final
	// destination through Commit or CommitAs once the assembled checksum has
	// resolved a content row. The part files are NOT deleted; call DeleteUpload.
	// Capacity for the assembled size is held before assembly starts.
	AssemblePartsStaged(ctx context.Context, bucket, key, uploadID string, partNumbers []int) (*StagedObject, []string, error)

	// DeleteUpload removes all part files for the given upload ID.
	DeleteUpload(ctx context.Context, uploadID string) error
}
