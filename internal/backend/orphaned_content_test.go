package backend_test

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// newTestBackendWithoutUploadPlanning builds a backend whose task service
// cannot enqueue upload planning, so every data write fails inside its version
// transaction after the content row and cache file already exist.
func newTestBackendWithoutUploadPlanning(t *testing.T) *testBackend {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	fsCache := newTestCache(t, 1<<30)
	gate, tracker := newBackendCacheAccess(repos)
	service, err := taskengine.NewService(taskengine.NewRegistry(), repos, time.Hour)
	if err != nil {
		t.Fatalf("creating task service: %v", err)
	}
	b := backend.New(repos, fsCache, &testutil.MockStorageClient{}, gate, tracker, slog.Default(),
		backend.WithTaskService(service))
	return &testBackend{backend: b, repos: repos, cache: fsCache, gate: gate, db: db, tasks: service}
}

func countContents(t *testing.T, tb *testBackend) int {
	t.Helper()
	count, err := tb.db.NewSelect().Model((*model.StorageContent)(nil)).Count(t.Context())
	if err != nil {
		t.Fatalf("counting content: %v", err)
	}
	return count
}

func TestPutObjectFailureDiscardsTheContentItCreated(t *testing.T) {
	tb := newTestBackendWithoutUploadPlanning(t)
	bucket := seedActiveBucket(t, tb, "failed-write-bucket")
	contentType := "text/plain"
	_, err := tb.backend.PutObject(t.Context(), s3response.PutObjectInput{
		Bucket: &bucket.Name, Key: new("file.txt"), ContentType: &contentType,
		Body: strings.NewReader(validTestObjectBody("bytes no version names")),
	})
	if err == nil {
		t.Fatal("PutObject succeeded without upload planning")
	}
	if count := countContents(t, tb); count != 0 {
		t.Fatalf("content rows after failed write = %d, want 0", count)
	}
	if used := tb.cache.UsedBytes(); used != 0 {
		t.Fatalf("cache bytes after failed write = %d, want 0", used)
	}
}

func TestDiscardOrphanedContentsKeepsNamedContent(t *testing.T) {
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "startup-scan-bucket")
	named := putValidTestObjectOutput(t, tb, bucket.Name, "named.txt", "named bytes")
	namedVersion, err := tb.repos.Objects.GetVersionByID(ctx, named.VersionID)
	if err != nil || namedVersion == nil || namedVersion.ContentID == nil {
		t.Fatalf("named version = %#v, err=%v", namedVersion, err)
	}

	// A process that stopped between resolving a write's content and
	// committing its version left this row and its cached file.
	body := validTestObjectBody("left by an interrupted write")
	orphan, err := tb.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: int64(len(body)), Checksum: testSHA256Hex(body), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}
	if _, err := tb.cache.Put(ctx, bucket.Name, model.ContentCacheKey(orphan.ID), strings.NewReader(body)); err != nil {
		t.Fatalf("caching orphan bytes: %v", err)
	}

	discarded, failed, err := tb.backend.DiscardOrphanedContents(ctx, time.Now().Add(time.Minute))
	if err != nil || discarded != 1 || failed != 0 {
		t.Fatalf("DiscardOrphanedContents = %d discarded, %d failed, err=%v", discarded, failed, err)
	}
	if content, err := tb.repos.Contents.GetByID(ctx, orphan.ID); err != nil || content != nil {
		t.Fatalf("orphan after scan = %#v, err=%v, want gone", content, err)
	}
	if tb.cache.Exists(ctx, bucket.Name, model.ContentCacheKey(orphan.ID)) {
		t.Fatal("scan kept the orphan's cached file")
	}
	if !tb.cache.Exists(ctx, bucket.Name, model.ContentCacheKey(*namedVersion.ContentID)) {
		t.Fatal("scan removed the cached file of content a version names")
	}
}

// Discarding a leftover row while a write of the same bytes is in flight never
// leaves a cached file without its row. The write either names content that
// keeps its row and file, or it lost the row after resolving it and asks the
// client to retry.
func TestDiscardRacingASameBytesWriteKeepsRowsAndFilesTogether(t *testing.T) {
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "race-bucket")
	contentType := "text/plain"
	for i := range 20 {
		body := validTestObjectBody(fmt.Sprintf("contended bytes %d", i))
		leftover, err := tb.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
			BucketID: bucket.ID, ContentSize: int64(len(body)), Checksum: testSHA256Hex(body), RequestedCopies: 1,
		})
		if err != nil {
			t.Fatalf("EnsureContent: %v", err)
		}
		key := fmt.Sprintf("race-%d.txt", i)
		var putErr, scanErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			_, putErr = tb.backend.PutObject(ctx, s3response.PutObjectInput{
				Bucket: &bucket.Name, Key: &key, ContentType: &contentType, Body: strings.NewReader(body),
			})
		})
		wg.Go(func() {
			_, _, scanErr = tb.backend.DiscardOrphanedContents(ctx, time.Now().Add(time.Minute))
		})
		wg.Wait()
		if scanErr != nil {
			t.Fatalf("iteration %d scan: %v", i, scanErr)
		}
		leftoverRow, err := tb.repos.Contents.GetByID(ctx, leftover.ID)
		if err != nil {
			t.Fatalf("iteration %d content: %v", i, err)
		}
		if leftoverRow == nil && tb.cache.Exists(ctx, bucket.Name, model.ContentCacheKey(leftover.ID)) {
			t.Fatalf("iteration %d: cached file outlived its content row", i)
		}
		if putErr != nil {
			requireAPIErrorCode(t, putErr, s3err.GetAPIError(s3err.ErrSlowDown))
			continue
		}
		version, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, key)
		if err != nil || version == nil || version.ContentID == nil {
			t.Fatalf("iteration %d version = %#v, err=%v", i, version, err)
		}
		named, err := tb.repos.Contents.GetByID(ctx, *version.ContentID)
		if err != nil || named == nil || !tb.cache.Exists(ctx, bucket.Name, model.ContentCacheKey(named.ID)) {
			t.Fatalf("iteration %d: stored write names content=%#v err=%v without its cached file", i, named, err)
		}
	}
}
