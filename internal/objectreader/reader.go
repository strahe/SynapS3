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
	"github.com/strahe/synaps3/internal/cacheaccess"
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
	repos         *repository.Repositories
	cache         cache.Cache
	storage       synapse.StorageClient
	cacheGate     *cacheaccess.Gate
	accessTracker *cacheaccess.Tracker
	logger        *slog.Logger
}

func New(
	repos *repository.Repositories,
	cache cache.Cache,
	storage synapse.StorageClient,
	cacheGate *cacheaccess.Gate,
	accessTracker *cacheaccess.Tracker,
	logger *slog.Logger,
) *Reader {
	if cacheGate == nil {
		panic("object reader requires a cache access gate")
	}
	if accessTracker == nil {
		panic("object reader requires a cache access tracker")
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &Reader{
		repos:         repos,
		cache:         cache,
		storage:       storage,
		cacheGate:     cacheGate,
		accessTracker: accessTracker,
		logger:        logger,
	}
	return r
}

func (r *Reader) Open(ctx context.Context, bucketName, key string, visible BucketVisibility) (*Result, error) {
	return r.open(ctx, bucketName, key, visible, true, false)
}

func (r *Reader) OpenVersion(ctx context.Context, bucketName, key, versionID string, visible BucketVisibility) (*Result, error) {
	return r.openVersion(ctx, bucketName, key, versionID, visible, true)
}

// OpenVersionForCopy opens an explicit version without rehydrating a missing source cache entry.
func (r *Reader) OpenVersionForCopy(ctx context.Context, bucketName, key, versionID string, visible BucketVisibility) (*Result, error) {
	return r.openVersion(ctx, bucketName, key, versionID, visible, false)
}

func (r *Reader) openVersion(ctx context.Context, bucketName, key, versionID string, visible BucketVisibility, rehydrate bool) (*Result, error) {
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

	body, cacheErr := r.openCached(ctx, bucketName, version)
	if cacheErr == nil {
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

	body = rc
	if rehydrate {
		body = r.streamAndRehydrate(
			ctx,
			bucketName,
			version.CacheKey,
			version.VersionID,
			rc,
		)
	}
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

	body, cacheErr := r.openCached(ctx, bucketName, version)
	if cacheErr == nil {
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
		body = r.streamAndRehydrate(
			ctx,
			bucketName,
			version.CacheKey,
			version.VersionID,
			rc,
		)
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
	copies, err := r.repos.Uploads.ListReadableCommittedCopies(ctx, *version.StorageUploadID)
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
		r.logger.Warn("provider download failed", "key", key, "versionID", version.VersionID, "copyIndex", copy.CopyIndex, "transferMethod", copy.TransferMethod, "err", err)
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

func (r *Reader) streamAndRehydrate(
	ctx context.Context,
	bucket, cacheKey, versionID string,
	rc io.ReadCloser,
) io.ReadCloser {
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
		var (
			persistErr      error
			skipRehydration bool
		)
		err := r.cacheGate.Commit(
			versionID,
			func() error {
				persistedVersion, err := r.repos.Objects.GetVersionByID(ctx, versionID)
				if err != nil {
					return fmt.Errorf("checking object version before cache rehydration: %w", err)
				}
				if persistedVersion == nil ||
					persistedVersion.IsDeleteMarker ||
					persistedVersion.CacheKey != cacheKey {
					skipRehydration = true
					return nil
				}
				_, err = r.cache.Put(ctx, bucket, cacheKey, pr)
				if err != nil {
					return err
				}
				persistErr = r.accessTracker.RecordCommit(
					ctx,
					versionID,
					persistedVersion.CacheAccessedAt,
				)
				return nil
			},
		)
		if err != nil {
			r.logger.Warn("cache rehydration failed (best-effort)", "cacheKey", cacheKey, "error", err)
			_, _ = io.Copy(io.Discard, pr)
			_ = pr.Close()
			return
		}
		if skipRehydration {
			_, _ = io.Copy(io.Discard, pr)
			_ = pr.Close()
			return
		}
		if persistErr != nil {
			r.logger.Warn(
				"cache access update failed",
				"versionID",
				versionID,
				"error",
				persistErr,
			)
		}
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

func (r *Reader) openCached(
	ctx context.Context,
	bucketName string,
	version *model.ObjectVersion,
) (io.ReadCloser, error) {
	opened, err := r.cacheGate.Open(
		version.VersionID,
		func() (io.ReadCloser, *cache.ObjectInfo, error) {
			return r.cache.Get(ctx, bucketName, version.CacheKey)
		},
	)
	if err != nil {
		return nil, err
	}
	var persistErr error
	if version.InCache {
		persistErr = r.accessTracker.RecordAccess(ctx, version.VersionID, version.CacheAccessedAt)
	} else {
		persistErr = r.accessTracker.RecordCommit(ctx, version.VersionID, version.CacheAccessedAt)
	}
	if persistErr != nil {
		r.logger.Warn(
			"cache access update failed",
			"versionID",
			version.VersionID,
			"error",
			persistErr,
		)
	}
	return opened.Body, nil
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
