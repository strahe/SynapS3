package task_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
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
	eviction, err := f.runtime.repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeCacheEvict, model.TaskSubjectStorageContent, strconv.FormatInt(copyRow.ContentID, 10), 1)
	if err != nil || eviction == nil {
		t.Fatalf("minimum durability did not schedule eviction: %#v, %v", eviction, err)
	}
	waitForTask(t, f.runtime.repos, eviction.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if fs.Exists(t.Context(), bucket.Name, model.ContentCacheKey(copyRow.ContentID)) {
		t.Fatal("cache retained despite meeting the durability minimum")
	}
}

func TestDurabilityReconciliationBatchesAndRollsBack(t *testing.T) {
	for _, scenario := range []struct {
		count int
		fail  bool
	}{{101, false}, {3, true}} {
		t.Run(fmt.Sprintf("%d_candidates_fault_%t", scenario.count, scenario.fail), func(t *testing.T) {
			f := newRegistrationFixture(t, scenario.count, nil, 0, func(options *handlerRuntimeOptions) { options.policy = cache.EvictionPolicyAfterUpload })
			ctx := t.Context()
			for _, copy := range f.copies {
				testutil.CommitStorageCopy(t, f.runtime.db, f.runtime.repos, testutil.CommitCopyInput{StorageCopyID: copy.ID, ContentID: copy.ContentID, CopyIndex: copy.CopyIndex})
				if err := f.runtime.repos.Objects.RecordContentCacheCommit(ctx, copy.ContentID, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			generation, err := f.runtime.repos.CacheEvictions.NextDurabilityGeneration(ctx, f.dataSet.BucketID)
			if err != nil {
				t.Fatal(err)
			}
			row, _, err := f.runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeCacheReconcileDurability, Input: cacheeviction.DurabilityInput{BucketID: f.dataSet.BucketID, Generation: generation}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
				return repos.CacheEvictions.BindDurabilityTask(ctx, f.dataSet.BucketID, generation, row.ID)
			})
			if err != nil {
				t.Fatal(err)
			}
			if scenario.fail {
				observer := &capacityArchiveObserver{failed: make(chan struct{})}
				testutil.AddQueryHook(f.runtime.db, observer)
				restore := testutil.FailWrites(t, f.runtime.db, "object_cache", "UPDATE", fmt.Sprintf("NEW.content_id = %d AND NEW.cache_active_task_id IS NOT NULL", f.copies[2].ContentID), "injected durability batch failure")
				defer restore()
				repos := *f.runtime.repos
				repos.Tasks = &limitedClaimRepository{TaskRepository: f.runtime.repos.Tasks, maximum: 1}
				engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration}, &repos, handlerExecutionRegistry(t, f.runtime), nil)
				if err != nil {
					t.Fatal(err)
				}
				cancel, done := runEngine(t, engine)
				select {
				case <-observer.failed:
				case <-time.After(3 * time.Second):
					t.Fatal("batch did not reach injected fault")
				}
				stopHandlerEngine(t, cancel, done)
				for _, copy := range f.copies {
					entry, err := f.runtime.repos.CacheEvictions.GetCacheEntry(ctx, copy.ContentID)
					if err != nil || entry.CacheActiveTaskID != nil || entry.CacheOperationGeneration != 0 {
						t.Fatalf("rolled-back cache reservation = %#v, %v", entry, err)
					}
				}
				count, err := f.runtime.db.NewSelect().Model((*model.Task)(nil)).Where("type = ?", model.TaskTypeCacheEvict).Count(ctx)
				if err != nil || count != 0 {
					t.Fatalf("rolled-back eviction tasks = %d, %v", count, err)
				}
				return
			}
			row = runBatchTaskClaim(t, f.runtime, row)
			count, err := f.runtime.db.NewSelect().Model((*model.ObjectCache)(nil)).Where("cache_active_task_id IS NOT NULL").Count(ctx)
			if err != nil || count != 100 {
				t.Fatalf("first durability batch = %d, %v", count, err)
			}
			wakeTask(t, f.runtime, row.ID)
			runBatchTaskClaim(t, f.runtime, row)
			count, err = f.runtime.db.NewSelect().Model((*model.ObjectCache)(nil)).Where("cache_active_task_id IS NOT NULL").Count(ctx)
			if err != nil || count != 101 {
				t.Fatalf("final durability batch = %d, %v", count, err)
			}
		})
	}
}

func TestDurabilityReconciliationFinishesWithoutAfterUploadPolicy(t *testing.T) {
	for _, policy := range []cache.EvictionPolicy{cache.EvictionPolicyLRU, cache.EvictionPolicyNone} {
		t.Run(string(policy), func(t *testing.T) {
			f := newRegistrationFixture(t, 2, nil, 0, func(options *handlerRuntimeOptions) {
				options.policy, options.maxBytes, options.highPercent, options.lowPercent = policy, 1<<30, 90, 50
			})
			ctx := t.Context()
			for _, copy := range f.copies {
				testutil.CommitStorageCopy(t, f.runtime.db, f.runtime.repos, testutil.CommitCopyInput{StorageCopyID: copy.ID, ContentID: copy.ContentID, CopyIndex: copy.CopyIndex})
				if err := f.runtime.repos.Objects.RecordContentCacheCommit(ctx, copy.ContentID, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			generation, err := f.runtime.repos.CacheEvictions.NextDurabilityGeneration(ctx, f.dataSet.BucketID)
			if err != nil {
				t.Fatal(err)
			}
			row, _, err := f.runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeCacheReconcileDurability, Input: cacheeviction.DurabilityInput{BucketID: f.dataSet.BucketID, Generation: generation}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
				return repos.CacheEvictions.BindDurabilityTask(ctx, f.dataSet.BucketID, generation, row.ID)
			})
			if err != nil {
				t.Fatal(err)
			}
			if candidate, err := f.runtime.repos.CacheEvictions.NextBucketDurabilityCandidate(ctx, f.dataSet.BucketID, generation, row.ID); err != nil || candidate == nil {
				t.Fatalf("durability candidate = %v, %v; want cached content that meets durability", candidate, err)
			}

			runOneStorageTask(t, f.runtime, row, model.TaskStatusCompleted)
			bucket, err := f.runtime.repos.Buckets.GetByID(ctx, f.dataSet.BucketID)
			if err != nil || bucket == nil || bucket.DurabilityTaskID != nil {
				t.Fatalf("bucket durability owner = %v, %v; want released", bucket, err)
			}
			count, err := f.runtime.db.NewSelect().Model((*model.Task)(nil)).Where("type = ?", model.TaskTypeCacheEvict).Count(ctx)
			if err != nil || count != 0 {
				t.Fatalf("eviction tasks = %d, %v; want none", count, err)
			}
		})
	}
}
