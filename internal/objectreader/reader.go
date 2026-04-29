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

	body, _, cacheErr := r.cache.Get(ctx, bucketName, version.CacheKey)
	if cacheErr == nil {
		return resultFromVersion(version, body, SourceCache, false), nil
	}
	if !os.IsNotExist(cacheErr) {
		return nil, fmt.Errorf("%w: %w", ErrCacheRead, cacheErr)
	}

	if version.PieceCID == nil || *version.PieceCID == "" || version.RetrievalURL == nil || *version.RetrievalURL == "" || r.storage == nil {
		return nil, cacheMissError(ErrNoSuchVersion)
	}
	pieceCID, err := cid.Decode(*version.PieceCID)
	if err != nil {
		r.logger.Warn("invalid PieceCID, cannot download version from provider", "key", key, "versionID", versionID, "pieceCID", *version.PieceCID)
		return nil, cacheMissError(ErrNoSuchVersion)
	}

	rc, err := r.storage.Download(ctx, pieceCID, &storage.DownloadOptions{URL: *version.RetrievalURL})
	if err != nil {
		r.logger.Warn("provider download failed", "key", key, "versionID", versionID, "err", err)
		return nil, fmt.Errorf("%w: %w: %w", ErrCacheMiss, ErrProviderDownload, err)
	}

	body = r.streamAndRehydrate(ctx, bucketName, version.CacheKey, rc)
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

	obj, err := r.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, key)
	if err != nil {
		return nil, fmt.Errorf("querying object: %w", err)
	}
	if obj == nil {
		return nil, ErrNoSuchKey
	}

	body, _, cacheErr := r.cache.Get(ctx, bucketName, obj.CacheKey)
	if cacheErr == nil {
		return resultFromObject(obj, body, SourceCache, cacheMiss), nil
	}
	if !os.IsNotExist(cacheErr) {
		return nil, fmt.Errorf("%w: %w", ErrCacheRead, cacheErr)
	}
	cacheMiss = true

	if obj.PieceCID == nil || *obj.PieceCID == "" || obj.RetrievalURL == nil || *obj.RetrievalURL == "" || r.storage == nil {
		return nil, cacheMissError(ErrNoSuchKey)
	}
	pieceCID, err := cid.Decode(*obj.PieceCID)
	if err != nil {
		r.logger.Warn("invalid PieceCID, cannot download from provider", "key", key, "pieceCID", *obj.PieceCID)
		return nil, cacheMissError(ErrNoSuchKey)
	}

	rc, err := r.storage.Download(ctx, pieceCID, &storage.DownloadOptions{URL: *obj.RetrievalURL})
	if err != nil {
		r.logger.Warn("provider download failed", "key", key, "err", err)
		return nil, fmt.Errorf("%w: %w: %w", ErrCacheMiss, ErrProviderDownload, err)
	}

	cur, dbErr := r.repos.Objects.GetByID(ctx, obj.ID)
	if dbErr != nil {
		r.logger.Warn("version check failed, skipping cache rehydration", "key", key, "error", dbErr)
	}
	if dbErr == nil && (cur == nil || cur.CurrentVersionID != obj.CurrentVersionID) {
		_ = rc.Close()
		if allowRestart && cur != nil {
			return r.open(ctx, bucketName, key, visible, false, true)
		}
		return nil, cacheMissError(ErrNoSuchKey)
	}

	body = rc
	if dbErr == nil && cur != nil && cur.CurrentVersionID == obj.CurrentVersionID {
		body = r.streamAndRehydrate(ctx, bucketName, obj.CacheKey, rc)
	}
	return resultFromObject(obj, body, SourceProvider, cacheMiss), nil
}

func resultFromObject(obj *model.Object, body io.ReadCloser, source Source, cacheMiss bool) *Result {
	return &Result{
		Body:         body,
		Size:         obj.Size,
		ETag:         obj.ETag,
		Checksum:     obj.Checksum,
		VersionID:    obj.CurrentVersionID,
		ContentType:  obj.ContentType,
		LastModified: obj.UpdatedAt,
		Source:       source,
		CacheMiss:    cacheMiss,
	}
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

func cacheMissError(err error) error {
	return fmt.Errorf("%w: %w", ErrCacheMiss, err)
}

func (r *Reader) streamAndRehydrate(ctx context.Context, bucket, cacheKey string, rc io.ReadCloser) io.ReadCloser {
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
		}
		_ = pr.Close()
	}()

	return body
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
