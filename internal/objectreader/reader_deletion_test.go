package objectreader

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectdeletion"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
)

func TestOpenVersionDoesNotRehydrateAfterPermanentDeletion(t *testing.T) {
	downloadStarted := make(chan struct{})
	releaseDownload := make(chan struct{})
	var releaseDownloadOnce sync.Once
	t.Cleanup(func() {
		releaseDownloadOnce.Do(func() {
			close(releaseDownload)
		})
	})
	putCalls := 0
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			putCalls++
			data, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			return &cache.ObjectInfo{Size: int64(len(data))}, nil
		},
		DeleteFunc: func(context.Context, string, string) error {
			return nil
		},
	}
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "reader-permanent-delete-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID:   "01J0000000000000000000OR12",
		BucketID:    bucket.ID,
		Key:         "deleted-during-download.txt",
		Size:        6,
		ETag:        "object-etag",
		Checksum:    "object-checksum",
		ContentType: "text/plain",
		CacheKey:    ".versions/01J0000000000000000000OR12",
		State:       model.ObjectStateUploading,
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("Objects.CreateVersionAndSetCurrent: %v", err)
	}
	acceptReaderVersionUpload(
		t,
		repos,
		version.VersionID,
		buildTestCID(t),
		"https://provider.example/deleted-during-download",
	)

	storageClient := &testutil.MockStorageClient{
		DownloadFunc: func(_ context.Context, _ cid.Cid, _ *storage.DownloadOptions) (io.ReadCloser, error) {
			close(downloadStarted)
			<-releaseDownload
			return io.NopCloser(bytes.NewReader([]byte("remote"))), nil
		},
	}
	gate := cacheaccess.NewGate()
	tracker := cacheaccess.NewTracker(cacheaccess.DefaultPersistenceInterval, repos.Objects)
	reader := New(repos, mc, storageClient, gate, tracker, slog.Default())

	type openResult struct {
		result *Result
		err    error
	}
	openDone := make(chan openResult, 1)
	go func() {
		result, err := reader.OpenVersion(
			ctx,
			bucket.Name,
			version.Key,
			version.VersionID,
			S3Visibility,
		)
		openDone <- openResult{result: result, err: err}
	}()
	select {
	case <-downloadStarted:
	case <-time.After(time.Second):
		t.Fatal("provider download did not start")
	}

	deletion, err := repos.Objects.DeleteObjectVersionPermanently(
		ctx,
		repository.DeleteObjectVersionInput{
			BucketID:  bucket.ID,
			Key:       version.Key,
			VersionID: version.VersionID,
		},
	)
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	status := objectdeletion.RecordCacheCleanup(
		ctx,
		mc,
		gate,
		tracker,
		repos.Objects,
		slog.Default(),
		bucket.Name,
		version.VersionID,
		deletion.CacheKey,
	)
	if status != model.CacheCleanupStatusDeleted {
		t.Fatalf("cache cleanup status = %s, want deleted", status)
	}
	releaseDownloadOnce.Do(func() {
		close(releaseDownload)
	})

	var opened openResult
	select {
	case opened = <-openDone:
	case <-time.After(time.Second):
		t.Fatal("OpenVersion did not return after provider download resumed")
	}
	if opened.err != nil {
		t.Fatalf("OpenVersion: %v", opened.err)
	}
	body, readErr := io.ReadAll(opened.result.Body)
	closeErr := opened.result.Body.Close()
	if readErr != nil {
		t.Fatalf("ReadAll: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("Body.Close: %v", closeErr)
	}
	if string(body) != "remote" {
		t.Fatalf("body = %q, want remote", body)
	}
	if putCalls != 0 {
		t.Fatalf("cache Put calls after permanent deletion = %d, want 0", putCalls)
	}
	if got := tracker.Latest(version.VersionID); !got.IsZero() {
		t.Fatalf("tracking entry after permanent deletion = %s, want none", got)
	}
}
