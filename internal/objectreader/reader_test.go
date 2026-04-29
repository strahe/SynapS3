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
		VersionID:    "01J0000000000000000000OR01",
		BucketID:     bucket.ID,
		Key:          "remote.txt",
		Size:         6,
		ETag:         "object-etag",
		Checksum:     "object-checksum",
		ContentType:  "text/plain",
		CacheKey:     ".versions/01J0000000000000000000OR01",
		State:        model.ObjectStateStored,
		PieceCID:     &pieceCID,
		RetrievalURL: stringPtr("https://provider.example/piece"),
	}
	_, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}

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
		VersionID:    "01J0000000000000000000OR02",
		BucketID:     bucket.ID,
		Key:          "changed.txt",
		Size:         6,
		ETag:         "object-etag",
		Checksum:     "object-checksum",
		ContentType:  "text/plain",
		CacheKey:     ".versions/01J0000000000000000000OR02",
		State:        model.ObjectStateStored,
		PieceCID:     &pieceCID,
		RetrievalURL: stringPtr("https://provider.example/deleted"),
	}
	_, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}

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
				State:       model.ObjectStateStored,
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

func stringPtr(v string) *string {
	return &v
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
