package objectreader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/storage"
)

var (
	ErrInvalidArgument  = errors.New("object reader: invalid argument")
	ErrNoSuchBucket     = errors.New("object reader: bucket not found")
	ErrNoSuchKey        = errors.New("object reader: object not found")
	ErrNoSuchVersion    = errors.New("object reader: object version not found")
	ErrMethodNotAllowed = errors.New("object reader: method not allowed")
	ErrCacheRead        = errors.New("object reader: cache read failed")
	ErrCacheMiss        = errors.New("object reader: cache miss")
	ErrProviderDownload = errors.New("object reader: provider download failed")
)

type Source string

const (
	SourceCache    Source = "cache"
	SourceProvider Source = "provider"
)

type BucketVisibility func(model.BucketStatus) bool

func S3Visibility(status model.BucketStatus) bool {
	return status.IsVisible()
}

func AdminVisibility(status model.BucketStatus) bool {
	return status.IsAdminVisible()
}

type Result struct {
	Body         io.ReadCloser
	Size         int64
	ETag         string
	Checksum     string
	VersionID    string
	ContentType  string
	LastModified time.Time
	Source       Source
	CacheMiss    bool
}

type Reader struct {
	repos   *repository.Repositories
	cache   cache.Cache
	storage synapse.StorageClient
	logger  *slog.Logger
}

func New(repos *repository.Repositories, cache cache.Cache, storage synapse.StorageClient, logger *slog.Logger) *Reader {
	if logger == nil {
		logger = slog.Default()
	}
	return &Reader{
		repos:   repos,
		cache:   cache,
		storage: storage,
		logger:  logger,
	}
}

func (r *Reader) Open(ctx context.Context, bucketName, key string, visible BucketVisibility) (*Result, error) {
	return r.open(ctx, bucketName, key, visible, true, false)
}

func (r *Reader) OpenVersion(ctx context.Context, bucketName, key, versionID string, visible BucketVisibility) (*Result, error) {
	if versionID == "" {
		return nil, ErrInvalidArgument
	}
	if r == nil || r.repos == nil || r.cache == nil || bucketName == "" || visible == nil {
		return nil, ErrInvalidArgument
	}

	bucket, err := r.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("querying bucket: %w", err)
	}
	if bucket == nil || !visible(bucket.Status) {
		return nil, ErrNoSuchBucket
	}

	version, err := r.repos.Objects.GetVersionByBucketKeyAndID(ctx, bucket.ID, key, versionID)
	if err != nil {
		return nil, fmt.Errorf("querying object version: %w", err)
	}
	if version == nil {
		return nil, ErrNoSuchVersion
	}
	if version.IsDeleteMarker {
		return nil, ErrMethodNotAllowed
	}

	body, _, cacheErr := r.cache.Get(ctx, bucketName, version.CacheKey)
	if cacheErr == nil {
		if !version.InCache {
			r.markCachePresence(ctx, version.VersionID, true)
		}
		return resultFromVersion(version, body, SourceCache, false), nil
	}
	if !os.IsNotExist(cacheErr) {
		return nil, fmt.Errorf("%w: %w", ErrCacheRead, cacheErr)
	}
	if version.InCache {
		r.markCachePresence(ctx, version.VersionID, false)
	}

	rc, err := r.downloadVersionFromProvider(ctx, key, version)
	if errors.Is(err, ErrCacheMiss) {
		return nil, cacheMissError(ErrNoSuchVersion)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w: %w", ErrCacheMiss, ErrProviderDownload, err)
	}

	body = r.streamAndRehydrate(ctx, bucketName, version.CacheKey, version.VersionID, rc)
	return resultFromVersion(version, body, SourceProvider, true), nil
}

func (r *Reader) open(ctx context.Context, bucketName, key string, visible BucketVisibility, allowRestart bool, cacheMiss bool) (*Result, error) {
	if r == nil || r.repos == nil || r.cache == nil || bucketName == "" || visible == nil {
		return nil, ErrInvalidArgument
	}

	bucket, err := r.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("querying bucket: %w", err)
	}
	if bucket == nil || !visible(bucket.Status) {
		return nil, ErrNoSuchBucket
	}

	version, err := r.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, key)
	if err != nil {
		return nil, fmt.Errorf("querying object: %w", err)
	}
	if version == nil {
		return nil, ErrNoSuchKey
	}
	if version.IsDeleteMarker {
		return nil, ErrNoSuchKey
	}

	body, _, cacheErr := r.cache.Get(ctx, bucketName, version.CacheKey)
	if cacheErr == nil {
		if !version.InCache {
			r.markCachePresence(ctx, version.VersionID, true)
		}
		return resultFromVersion(version, body, SourceCache, cacheMiss), nil
	}
	if !os.IsNotExist(cacheErr) {
		return nil, fmt.Errorf("%w: %w", ErrCacheRead, cacheErr)
	}
	cacheMiss = true
	if version.InCache {
		r.markCachePresence(ctx, version.VersionID, false)
	}

	rc, err := r.downloadVersionFromProvider(ctx, key, version)
	if errors.Is(err, ErrCacheMiss) {
		return nil, cacheMissError(ErrNoSuchKey)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w: %w", ErrCacheMiss, ErrProviderDownload, err)
	}

	cur, dbErr := r.repos.Objects.GetCurrentVersionByObjectID(ctx, version.ObjectID)
	if dbErr != nil {
		r.logger.Warn("version check failed, skipping cache rehydration", "key", key, "error", dbErr)
	}
	if dbErr == nil && (cur == nil || cur.VersionID != version.VersionID) {
		_ = rc.Close()
		if allowRestart && cur != nil {
			return r.open(ctx, bucketName, key, visible, false, true)
		}
		return nil, cacheMissError(ErrNoSuchKey)
	}

	body = rc
	if dbErr == nil && cur != nil && cur.VersionID == version.VersionID {
		body = r.streamAndRehydrate(ctx, bucketName, version.CacheKey, version.VersionID, rc)
	}
	return resultFromVersion(version, body, SourceProvider, cacheMiss), nil
}

func resultFromVersion(version *model.ObjectVersion, body io.ReadCloser, source Source, cacheMiss bool) *Result {
	return &Result{
		Body:         body,
		Size:         version.Size,
		ETag:         version.ETag,
		Checksum:     version.Checksum,
		VersionID:    version.VersionID,
		ContentType:  version.ContentType,
		LastModified: version.CreatedAt,
		Source:       source,
		CacheMiss:    cacheMiss,
	}
}

func (r *Reader) downloadVersionFromProvider(ctx context.Context, key string, version *model.ObjectVersion) (io.ReadCloser, error) {
	if version.StorageUploadID == nil || r.storage == nil {
		return nil, ErrCacheMiss
	}
	var copies []repository.ReadableStorageCopy
	var err error
	if version.State == model.ObjectStateReplicating {
		copies, err = r.repos.Uploads.ListReadablePrimaryCopy(ctx, *version.StorageUploadID)
	} else {
		copies, err = r.repos.Uploads.ListReadableCopies(ctx, *version.StorageUploadID)
	}
	if err != nil {
		return nil, err
	}
	if len(copies) == 0 {
		return nil, ErrCacheMiss
	}

	var lastErr error
	for _, copy := range copies {
		pieceCID, err := cid.Decode(copy.PieceCID)
		if err != nil {
			r.logger.Warn("invalid PieceCID, skipping provider copy", "key", key, "versionID", version.VersionID, "pieceCID", copy.PieceCID, "copyIndex", copy.CopyIndex)
			lastErr = err
			continue
		}
		rc, err := r.storage.Download(ctx, pieceCID, &storage.DownloadOptions{URL: copy.RetrievalURL})
		if err == nil {
			return rc, nil
		}
		r.logger.Warn("provider download failed", "key", key, "versionID", version.VersionID, "copyIndex", copy.CopyIndex, "role", copy.Role, "err", err)
		lastErr = err
	}
	if lastErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrProviderDownload, lastErr)
	}
	return nil, ErrCacheMiss
}

func cacheMissError(err error) error {
	return fmt.Errorf("%w: %w", ErrCacheMiss, err)
}

func (r *Reader) streamAndRehydrate(ctx context.Context, bucket, cacheKey, versionID string, rc io.ReadCloser) io.ReadCloser {
	pr, pw := io.Pipe()
	done := make(chan struct{})
	body := &teeReadCloser{
		reader:     io.TeeReader(rc, pw),
		source:     rc,
		pipeWriter: pw,
		done:       done,
	}

	go func() {
		defer close(done)
		if _, err := r.cache.Put(ctx, bucket, cacheKey, pr); err != nil {
			r.logger.Warn("cache rehydration failed (best-effort)", "cacheKey", cacheKey, "error", err)
			_, _ = io.Copy(io.Discard, pr)
			_ = pr.Close()
			return
		}
		r.markCachePresence(ctx, versionID, true)
		_ = pr.Close()
	}()

	return body
}

func (r *Reader) markCachePresence(ctx context.Context, versionID string, inCache bool) {
	if r == nil || r.repos == nil || r.repos.Objects == nil || versionID == "" {
		return
	}
	if err := r.repos.Objects.SetVersionCachePresence(ctx, versionID, inCache); err != nil {
		r.logger.Warn("cache location update failed", "versionID", versionID, "inCache", inCache, "error", err)
	}
}

type teeReadCloser struct {
	reader     io.Reader
	source     io.ReadCloser
	pipeWriter *io.PipeWriter
	done       <-chan struct{}
	closeOnce  sync.Once
}

func (t *teeReadCloser) Read(p []byte) (int, error) {
	n, err := t.reader.Read(p)
	if err != nil {
		t.closePipe(err)
	}
	return n, err
}

func (t *teeReadCloser) Close() error {
	t.closePipe(io.ErrClosedPipe)
	<-t.done
	return t.source.Close()
}

func (t *teeReadCloser) closePipe(err error) {
	t.closeOnce.Do(func() {
		switch {
		case err == nil || errors.Is(err, io.EOF):
			_ = t.pipeWriter.Close()
		default:
			_ = t.pipeWriter.CloseWithError(err)
		}
	})
}
