package objectreader

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
)

func TestOpenUsesProviderFallbackAndRehydratesCache(t *testing.T) {
	var rehydrated []byte
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			data, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("reading rehydrate body: %v", err)
			}
			rehydrated = data
			return &cache.ObjectInfo{Path: "/cache/remote.txt", Size: int64(len(data)), ETag: "etag", Checksum: "checksum"}, nil
		},
	}
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "reader-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	pieceCID := buildTestCID(t)
	version := &model.ObjectVersion{
		VersionID:   "01J0000000000000000000OR01",
		BucketID:    bucket.ID,
		Key:         "remote.txt",
		Size:        6,
		ETag:        "object-etag",
		Checksum:    "object-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/01J0000000000000000000OR01",
		State:       model.ObjectStateUploading,
	}
	_, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}
	acceptReaderVersionUpload(t, repos, version.VersionID, pieceCID, "https://provider.example/piece")

	storageClient := &testutil.MockStorageClient{
		DownloadFunc: func(_ context.Context, _ cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error) {
			if opts == nil || opts.URL != "https://provider.example/piece" {
				t.Fatalf("DownloadOptions = %#v, want retrieval URL", opts)
			}
			return io.NopCloser(bytes.NewReader([]byte("remote"))), nil
		},
	}
	reader := New(repos, mc, storageClient, slog.Default())

	got, err := reader.Open(ctx, "reader-bucket", "remote.txt", S3Visibility)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	body, err := io.ReadAll(got.Body)
	closeErr := got.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("Body.Close: %v", closeErr)
	}

	if string(body) != "remote" {
		t.Fatalf("body = %q, want remote", string(body))
	}
	if string(rehydrated) != "remote" {
		t.Fatalf("rehydrated = %q, want remote", string(rehydrated))
	}
	if got.Source != SourceProvider {
		t.Fatalf("source = %q, want %q", got.Source, SourceProvider)
	}
	if got.ContentType != "text/plain" || got.ETag != "object-etag" || got.Size != 6 {
		t.Fatalf("metadata = %#v", got)
	}
	dbVersion, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || dbVersion == nil {
		t.Fatalf("version after rehydrate: version=%v err=%v", dbVersion, err)
	}
	if !dbVersion.InCache {
		t.Fatal("version in_cache = false, want true after successful rehydrate")
	}
}

func TestOpenReplicatingVersionUsesPrimaryCopyOnly(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			data, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("reading rehydrate body: %v", err)
			}
			return &cache.ObjectInfo{Path: "/cache/replicating.txt", Size: int64(len(data)), ETag: "etag", Checksum: "checksum"}, nil
		},
	}
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "replicating-reader-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	pieceCID := buildTestCID(t)
	version := &model.ObjectVersion{
		VersionID:   "01J0000000000000000000OR05",
		BucketID:    bucket.ID,
		Key:         "replicating.txt",
		Size:        6,
		ETag:        "object-etag",
		Checksum:    "object-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/01J0000000000000000000OR05",
		State:       model.ObjectStateCached,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}
	uploadID := bindReaderPrimaryCommittedUpload(t, repos, version.VersionID, pieceCID, "https://primary.example/piece")
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     uploadID,
		CopyIndex:    1,
		PieceCID:     pieceCID,
		PieceID:      onChainIDPtr(t, "2"),
		RetrievalURL: "https://secondary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted secondary: %v", err)
	}

	storageClient := &testutil.MockStorageClient{
		DownloadFunc: func(_ context.Context, _ cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error) {
			if opts == nil || opts.URL != "https://primary.example/piece" {
				t.Fatalf("DownloadOptions = %#v, want primary retrieval URL", opts)
			}
			return io.NopCloser(bytes.NewReader([]byte("remote"))), nil
		},
	}
	reader := New(repos, mc, storageClient, slog.Default())

	got, err := reader.Open(ctx, bucket.Name, "replicating.txt", S3Visibility)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	body, err := io.ReadAll(got.Body)
	closeErr := got.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("Body.Close: %v", closeErr)
	}
	if string(body) != "remote" {
		t.Fatalf("body = %q, want remote", string(body))
	}
}

func TestOpenCacheHitDoesNotRewritePresentCacheLocation(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(bytes.NewReader([]byte("cached"))), &cache.ObjectInfo{Size: 6}, nil
		},
	}
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "cache-hit-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID:   "01J0000000000000000000OR08",
		BucketID:    bucket.ID,
		Key:         "cached.txt",
		Size:        6,
		ETag:        "object-etag",
		Checksum:    "object-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/01J0000000000000000000OR08",
		State:       model.ObjectStateCached,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}
	objects := &countingObjectRepo{ObjectRepository: repos.Objects}
	repos.Objects = objects

	reader := New(repos, mc, nil, slog.Default())
	got, err := reader.Open(ctx, bucket.Name, version.Key, S3Visibility)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = got.Body.Close()
	if objects.cachePresenceWrites != 0 {
		t.Fatalf("cache presence writes after Open cache hit = %d, want 0", objects.cachePresenceWrites)
	}

	got, err = reader.OpenVersion(ctx, bucket.Name, version.Key, version.VersionID, S3Visibility)
	if err != nil {
		t.Fatalf("OpenVersion: %v", err)
	}
	_ = got.Body.Close()
	if objects.cachePresenceWrites != 0 {
		t.Fatalf("cache presence writes after OpenVersion cache hit = %d, want 0", objects.cachePresenceWrites)
	}
}

type countingObjectRepo struct {
	repository.ObjectRepository

	cachePresenceWrites int
}

func (r *countingObjectRepo) SetVersionCachePresence(ctx context.Context, versionID string, inCache bool) error {
	r.cachePresenceWrites++
	return r.ObjectRepository.SetVersionCachePresence(ctx, versionID, inCache)
}

func TestOpenCacheMissMarksCacheLocationAbsent(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
	}
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "cache-miss-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID:   "01J0000000000000000000OR06",
		BucketID:    bucket.ID,
		Key:         "missing-cache.txt",
		Size:        6,
		ETag:        "object-etag",
		Checksum:    "object-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/01J0000000000000000000OR06",
		State:       model.ObjectStateCached,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}

	reader := New(repos, mc, nil, slog.Default())
	got, err := reader.Open(ctx, bucket.Name, version.Key, S3Visibility)
	if err == nil {
		_ = got.Body.Close()
		t.Fatal("Open succeeded, want missing cache error")
	}
	if !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("Open error = %v, want ErrNoSuchKey", err)
	}

	dbVersion, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || dbVersion == nil {
		t.Fatalf("version after cache miss: version=%v err=%v", dbVersion, err)
	}
	if dbVersion.InCache {
		t.Fatal("version in_cache = true, want false after confirmed cache miss")
	}
}

func TestOpenRehydrateFailureDoesNotMarkCacheLocationPresent(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			_, _ = io.Copy(io.Discard, r)
			return nil, errors.New("cache full")
		},
	}
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "rehydrate-fail-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	pieceCID := buildTestCID(t)
	version := &model.ObjectVersion{
		VersionID:   "01J0000000000000000000OR07",
		BucketID:    bucket.ID,
		Key:         "remote.txt",
		Size:        6,
		ETag:        "object-etag",
		Checksum:    "object-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/01J0000000000000000000OR07",
		State:       model.ObjectStateUploading,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}
	acceptReaderVersionUpload(t, repos, version.VersionID, pieceCID, "https://provider.example/piece")
	if err := repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); err != nil {
		t.Fatalf("SetVersionCachePresence: %v", err)
	}

	storageClient := &testutil.MockStorageClient{
		DownloadFunc: func(_ context.Context, _ cid.Cid, _ *storage.DownloadOptions) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("remote"))), nil
		},
	}
	reader := New(repos, mc, storageClient, slog.Default())
	got, err := reader.Open(ctx, bucket.Name, version.Key, S3Visibility)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, readErr := io.ReadAll(got.Body)
	closeErr := got.Body.Close()
	if readErr != nil {
		t.Fatalf("ReadAll: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("Body.Close: %v", closeErr)
	}

	dbVersion, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || dbVersion == nil {
		t.Fatalf("version after failed rehydrate: version=%v err=%v", dbVersion, err)
	}
	if dbVersion.InCache {
		t.Fatal("version in_cache = true, want false when rehydrate fails")
	}
}

func TestOpenTreatsCurrentVersionChangeAfterProviderDownloadAsMissing(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			data, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("reading rehydrate body: %v", err)
			}
			return &cache.ObjectInfo{Path: "/cache/deleted.txt", Size: int64(len(data)), ETag: "etag", Checksum: "checksum"}, nil
		},
	}
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "deleted-reader-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	pieceCID := buildTestCID(t)
	version := &model.ObjectVersion{
		VersionID:   "01J0000000000000000000OR02",
		BucketID:    bucket.ID,
		Key:         "changed.txt",
		Size:        6,
		ETag:        "object-etag",
		Checksum:    "object-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/01J0000000000000000000OR02",
		State:       model.ObjectStateUploading,
	}
	_, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}
	acceptReaderVersionUpload(t, repos, version.VersionID, pieceCID, "https://provider.example/deleted")

	storageClient := &testutil.MockStorageClient{
		DownloadFunc: func(_ context.Context, _ cid.Cid, _ *storage.DownloadOptions) (io.ReadCloser, error) {
			replacement := &model.ObjectVersion{
				VersionID:   "01J0000000000000000000OR03",
				BucketID:    bucket.ID,
				Key:         "changed.txt",
				Size:        3,
				ETag:        "new-etag",
				Checksum:    "new-checksum",
				ContentType: "text/plain",
				CacheKey:    ".versions/01J0000000000000000000OR03",
				State:       model.ObjectStateCached,
			}
			if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, replacement); err != nil {
				t.Fatalf("Objects.CreateVersionAndSetCurrent replacement: %v", err)
			}
			return io.NopCloser(bytes.NewReader([]byte("remote"))), nil
		},
	}
	reader := New(repos, mc, storageClient, slog.Default())

	got, err := reader.Open(ctx, bucket.Name, "changed.txt", S3Visibility)
	if err == nil {
		_ = got.Body.Close()
		t.Fatal("Open returned deleted object, want not found")
	}
	if !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("Open error = %v, want ErrNoSuchKey", err)
	}
}

func TestOpenVersionDoesNotRestartWhenCurrentVersionChanges(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			data, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("reading rehydrate body: %v", err)
			}
			return &cache.ObjectInfo{Path: "/cache/version.txt", Size: int64(len(data)), ETag: "etag", Checksum: "checksum"}, nil
		},
	}
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "version-reader-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	pieceCID := buildTestCID(t)
	oldVersion := &model.ObjectVersion{
		VersionID:   "01J0000000000000000000OR04",
		BucketID:    bucket.ID,
		Key:         "changed.txt",
		Size:        3,
		ETag:        "old-etag",
		Checksum:    "old-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/01J0000000000000000000OR04",
		State:       model.ObjectStateUploading,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent old: %v", err)
	}
	acceptReaderVersionUpload(t, repos, oldVersion.VersionID, pieceCID, "https://provider.example/old")

	storageClient := &testutil.MockStorageClient{
		DownloadFunc: func(_ context.Context, _ cid.Cid, _ *storage.DownloadOptions) (io.ReadCloser, error) {
			replacement := &model.ObjectVersion{
				VersionID:   "01J0000000000000000000OR05",
				BucketID:    bucket.ID,
				Key:         "changed.txt",
				Size:        3,
				ETag:        "new-etag",
				Checksum:    "new-checksum",
				ContentType: "text/plain",
				CacheKey:    ".versions/01J0000000000000000000OR05",
				State:       model.ObjectStateCached,
			}
			if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, replacement); err != nil {
				t.Fatalf("Objects.CreateVersionAndSetCurrent replacement: %v", err)
			}
			return io.NopCloser(bytes.NewReader([]byte("old"))), nil
		},
	}
	reader := New(repos, mc, storageClient, slog.Default())

	got, err := reader.OpenVersion(ctx, bucket.Name, "changed.txt", oldVersion.VersionID, S3Visibility)
	if err != nil {
		t.Fatalf("OpenVersion: %v", err)
	}
	body, err := io.ReadAll(got.Body)
	closeErr := got.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("Body.Close: %v", closeErr)
	}
	if string(body) != "old" {
		t.Fatalf("body = %q, want explicitly requested old version", string(body))
	}
	if got.VersionID != oldVersion.VersionID {
		t.Fatalf("VersionID = %s, want %s", got.VersionID, oldVersion.VersionID)
	}
}

func acceptReaderVersionUpload(t *testing.T, repos *repository.Repositories, versionID string, pieceCID string, retrievalURL string) {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for upload accept: version=%v err=%v", version, err)
	}
	providerID := onChainIDPtr(t, "101")
	dataSetID := onChainIDPtr(t, "1001")
	pieceID := onChainIDPtr(t, "1")
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	if err := repos.Uploads.RecordUploadResult(ctx, repository.RecordUploadResultInput{
		UploadID:        upload.ID,
		Complete:        true,
		PieceCID:        &pieceCID,
		RequestedCopies: 1,
		Copies: []repository.StorageUploadCopyInput{{
			ProviderID:   providerID,
			DataSetID:    dataSetID,
			PieceID:      pieceID,
			Role:         "primary",
			RetrievalURL: &retrievalURL,
		}},
	}); err != nil {
		t.Fatalf("record upload result: %v", err)
	}
	if _, err := repos.Uploads.AcceptCompleteUploadForContent(ctx, repository.AcceptCompleteUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("accept upload result: %v", err)
	}
}

func bindReaderPrimaryCommittedUpload(t *testing.T, repos *repository.Repositories, versionID string, pieceCID string, retrievalURL string) int64 {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for primary bind: version=%v err=%v", version, err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("mark committing: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: version.BucketID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: version.BucketID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("secondary binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("primary ready: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: secondary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002"), ClientDataSetID: onChainIDPtr(t, "9002")}); err != nil {
		t.Fatalf("secondary ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, Role: "primary", ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, Role: "secondary", ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("create copy rows: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      onChainIDPtr(t, "1"),
		RetrievalURL: retrievalURL,
	}); err != nil {
		t.Fatalf("primary committed: %v", err)
	}
	if _, err := repos.Uploads.BindPrimaryCommittedUploadForContent(ctx, repository.BindPrimaryCommittedUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("bind primary committed: %v", err)
	}
	return upload.ID
}

func TestTeeReadCloserReadReturnsEOFBeforeRehydrationCompletes(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.Close()
	done := make(chan struct{})
	body := &teeReadCloser{
		reader:     bytes.NewReader(nil),
		source:     io.NopCloser(bytes.NewReader(nil)),
		pipeWriter: pw,
		done:       done,
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := body.Read(make([]byte, 1))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		close(done)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read error = %v, want EOF", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(done)
		<-errCh
		t.Fatal("Read blocked waiting for cache rehydration to finish")
	}
}

func buildTestCID(t *testing.T) string {
	t.Helper()
	hash, err := multihash.Sum([]byte("dummy-piece-data"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("building multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, hash).String()
}
