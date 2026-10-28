package task_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func newLocalTestFilesystem(t *testing.T) *cache.Filesystem {
	t.Helper()
	files, err := cache.NewFilesystem(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func seedLocalCacheContent(t *testing.T, runtime handlerTestRuntime, files *cache.Filesystem, name, expected, actual string) *model.ObjectVersion {
	t.Helper()
	ctx := t.Context()
	bucket := testutil.SeedBucket(t, runtime.db, name)
	digest := sha256.Sum256([]byte(expected))
	content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: int64(len(expected)), Checksum: hex.EncodeToString(digest[:]), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "object",
		ContentID: &content.ID, Size: int64(len(expected)), ETag: "etag", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatal(err)
	}
	if _, err := files.Put(ctx, name, model.ContentCacheKey(content.ID), strings.NewReader(actual), int64(len(actual))); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Objects.ClearContentCachePresence(ctx, content.ID); err != nil {
		t.Fatal(err)
	}
	return version
}

func completeLocalCycle(t *testing.T, runtime handlerTestRuntime, cycle int64) {
	t.Helper()
	row, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheLocalReconcile, Input: taskengine.CycleInput{Cycle: cycle},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
}

func assertLocalPresence(t *testing.T, runtime handlerTestRuntime, contentID int64, expected bool) {
	t.Helper()
	entry, err := runtime.repos.CacheEvictions.GetCacheEntry(t.Context(), contentID)
	if err != nil || entry == nil || entry.InCache != expected {
		t.Fatalf("cache presence = %#v, err=%v, want %t", entry, err, expected)
	}
}

func TestCacheLocalRemovesOnlyInactiveMultipartFiles(t *testing.T) {
	files := newLocalTestFilesystem(t)
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: files})
	bucket := testutil.SeedBucket(t, runtime.db, "local-multipart")
	ctx := t.Context()
	for _, upload := range []struct {
		id     string
		status model.MultipartStatus
	}{
		{"missing", ""},
		{"aborted", model.MultipartStatusAborted},
		{"completed", model.MultipartStatusCompleted},
		{"initiated", model.MultipartStatusInitiated},
		{"busy", model.MultipartStatusInitiated},
	} {
		if upload.status != "" {
			if err := runtime.repos.Multiparts.Create(ctx, &model.MultipartUpload{
				UploadID: upload.id, BucketID: bucket.ID, Key: upload.id, Status: upload.status,
			}); err != nil {
				t.Fatal(err)
			}
		}
		staged, err := files.PutPartStaged(ctx, upload.id, 1, strings.NewReader("part"), 4)
		if err != nil {
			t.Fatal(err)
		}
		if err := staged.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	release, err := runtime.gate.HoldUpload(ctx, "busy")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completeLocalCycle(t, runtime, 1)
	if got := files.UsedBytes(); got != 8 {
		t.Fatalf("UsedBytes = %d, want active and busy parts' 8 bytes", got)
	}
	for _, id := range []string{"missing", "aborted", "completed"} {
		if _, err := os.Stat(filepath.Join(files.RootDir(), ".multipart", id)); !os.IsNotExist(err) {
			t.Fatalf("inactive upload %s remains: %v", id, err)
		}
	}
	for _, id := range []string{"initiated", "busy"} {
		if _, err := os.Stat(filepath.Join(files.RootDir(), ".multipart", id, "1")); err != nil {
			t.Fatalf("active upload %s was removed: %v", id, err)
		}
	}
}

func TestCacheLocalRestoresVerifiedFilesAndContinuesPastQuarantineFailure(t *testing.T) {
	files := newLocalTestFilesystem(t)
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: files})
	blocked := seedLocalCacheContent(t, runtime, files, "a-local-blocked", "good", "evil")
	corrupt := seedLocalCacheContent(t, runtime, files, "b-local-corrupt", "good", "evil")
	valid := seedLocalCacheContent(t, runtime, files, "c-local-valid", "good", "good")
	blockedTarget := filepath.Join(files.RootDir(), "a-local-blocked", ".quarantine", strconv.FormatInt(*blocked.ContentID, 10))
	if err := os.MkdirAll(blockedTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completeLocalCycle(t, runtime, 1)
	assertLocalPresence(t, runtime, *blocked.ContentID, false)
	assertLocalPresence(t, runtime, *corrupt.ContentID, false)
	assertLocalPresence(t, runtime, *valid.ContentID, true)
	if files.UsedBytes() != 12 {
		t.Fatalf("quarantine changed UsedBytes to %d, want 12", files.UsedBytes())
	}
	if _, err := os.Stat(filepath.Join(files.RootDir(), "b-local-corrupt", model.ContentCacheKey(*corrupt.ContentID))); !os.IsNotExist(err) {
		t.Fatalf("corrupt content is still readable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(files.RootDir(), "b-local-corrupt", ".quarantine", strconv.FormatInt(*corrupt.ContentID, 10))); err != nil {
		t.Fatalf("undurable quarantine was removed: %v", err)
	}
}

func TestCacheLocalAfterUploadRestorationReleasesCapacity(t *testing.T) {
	files := newLocalTestFilesystem(t)
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: files, policy: cache.EvictionPolicyAfterUpload})
	version := seedStoredCacheObject(t, runtime, 4, time.Now())
	bucket, err := runtime.repos.Buckets.GetByID(t.Context(), version.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("good"))
	if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("checksum = ?", hex.EncodeToString(digest[:])).
		Where("id = ?", *version.ContentID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := files.Put(t.Context(), bucket.Name, model.ContentCacheKey(*version.ContentID), strings.NewReader("good"), 4); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Objects.ClearContentCachePresence(t.Context(), *version.ContentID); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completeLocalCycle(t, runtime, 1)
	eviction, err := runtime.repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeCacheEvict, model.TaskSubjectStorageContent, strconv.FormatInt(*version.ContentID, 10), 1)
	if err != nil || eviction == nil {
		t.Fatalf("restored content has no eviction: %#v, err=%v", eviction, err)
	}
	waitForTask(t, runtime.repos, eviction.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
	assertLocalPresence(t, runtime, *version.ContentID, false)
	if files.UsedBytes() != 0 {
		t.Fatalf("UsedBytes after restored content eviction = %d", files.UsedBytes())
	}
}

type localReadObserver struct {
	*cache.Filesystem
	once  sync.Once
	onGet func()
}

func (f *localReadObserver) Get(ctx context.Context, bucket, key string) (io.ReadCloser, *cache.ObjectInfo, error) {
	body, info, err := f.Filesystem.Get(ctx, bucket, key)
	if err == nil {
		f.once.Do(f.onGet)
	}
	return body, info, err
}

func TestCacheLocalIsolationClearsPresenceWrittenDuringVerification(t *testing.T) {
	files := newLocalTestFilesystem(t)
	observed := &localReadObserver{Filesystem: files}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: observed})
	version := seedLocalCacheContent(t, runtime, files, "local-read-race", "good", "evil")
	observed.onGet = func() {
		if err := runtime.tracker.RecordCommit(t.Context(), *version.ContentID, nil); err != nil {
			t.Error(err)
		}
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completeLocalCycle(t, runtime, 1)
	assertLocalPresence(t, runtime, *version.ContentID, false)
}

func TestCacheLocalCompensationHandlesPendingPresenceAndRecreatedFiles(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resident  bool
		recreated bool
	}{
		{name: "already absent"},
		{name: "recorded resident", resident: true},
		{name: "recreated", resident: true, recreated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := newLocalTestFilesystem(t)
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: files})
			version := seedLocalCacheContent(t, runtime, files, "local-compensation", "good", "evil")
			contentID := *version.ContentID
			if err := files.Quarantine(t.Context(), "local-compensation", contentID); err != nil {
				t.Fatal(err)
			}
			if tc.resident {
				if err := runtime.repos.Objects.RecordContentCacheCommit(t.Context(), contentID, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			if tc.recreated {
				if _, err := files.Put(t.Context(), "local-compensation", model.ContentCacheKey(contentID), strings.NewReader("good"), 4); err != nil {
					t.Fatal(err)
				}
			}
			restore := testutil.FailWrites(t, runtime.db, "object_cache", "INSERT", "NEW.in_cache = true", "injected presence write failure")
			err := runtime.tracker.RecordCommit(t.Context(), contentID, nil)
			restore()
			if err == nil {
				t.Fatal("presence write fault did not leave a pending Tracker commit")
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			completeLocalCycle(t, runtime, 1)
			release, err := runtime.gate.HoldRead(t.Context(), model.ContentCacheKey(contentID))
			if err != nil {
				t.Fatal(err)
			}
			err = runtime.tracker.FlushWhileGuarded(t.Context(), contentID)
			release()
			if err != nil {
				t.Fatal(err)
			}
			assertLocalPresence(t, runtime, contentID, tc.recreated)
		})
	}
}
