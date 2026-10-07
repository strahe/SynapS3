package task_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
)

func TestMinimumDurabilitySchedulesEvictionBeforeAllRequestedCopiesCommit(t *testing.T) {
	fs, err := cache.NewFilesystem(t.TempDir(), 256)
	if err != nil {
		t.Fatal(err)
	}
	f := newRegistrationFixture(t, 1, nil, 0, func(options *handlerRuntimeOptions) {
		options.cache, options.policy = fs, cache.EvictionPolicyAfterUpload
	})
	copyRow := f.copies[0]
	if _, err := f.runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).
		Set("requested_copies = ?", 2).Where("id = ?", copyRow.ContentID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	bucket, err := f.runtime.repos.Buckets.GetByID(t.Context(), copyRow.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Put(t.Context(), bucket.Name, model.ContentCacheKey(copyRow.ContentID), bytes.NewReader(make([]byte, 128)), 128); err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.repos.Objects.RecordContentCacheCommit(t.Context(), copyRow.ContentID, time.Now()); err != nil {
		t.Fatal(err)
	}
	_, taskID := f.collect(t)
	cancel, done := runHandlerEngine(t, f.runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForCommitTask(t, f.runtime, taskID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	content, err := f.runtime.repos.Contents.GetByID(t.Context(), copyRow.ContentID)
	if err != nil || content.AcceptedAt != nil {
		t.Fatalf("content = %#v, %v, want target still incomplete", content, err)
	}
	eviction, err := f.runtime.repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeCacheEvict, cacheeviction.EvictTaskKey(copyRow.ContentID, 1))
	if err != nil || eviction == nil {
		t.Fatalf("minimum durability did not schedule eviction: %#v, %v", eviction, err)
	}
	waitForTask(t, f.runtime.repos, eviction.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if fs.Exists(t.Context(), bucket.Name, model.ContentCacheKey(copyRow.ContentID)) {
		t.Fatal("cache retained despite meeting the durability minimum")
	}
}
