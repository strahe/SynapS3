package task_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/uptrace/bun"
)

func runBatchTaskClaim(t *testing.T, runtime handlerTestRuntime, row *model.Task) *model.Task {
	t.Helper()
	cfg, err := config.DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	var excluded []int64
	if err := runtime.db.NewSelect().Model((*model.Task)(nil)).Column("id").Where("id <> ?", row.ID).Scan(t.Context(), &excluded); err != nil {
		t.Fatal(err)
	}
	repos := *runtime.repos
	repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1, excludedTaskIDs: excluded}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: cfg.Worker.Tasks.LeaseDuration}, &repos, handlerExecutionRegistry(t, runtime), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)
	return waitForTask(t, runtime.repos, row.ID, func(current *model.Task) bool {
		return current.ClaimGeneration == row.ClaimGeneration+1 && current.Status == model.TaskStatusPending
	}, cfg.Worker.Tasks.LeaseDuration)
}

func replacementBatchFixture(t *testing.T, count int, remoteSource bool, sizes ...int64) (handlerTestRuntime, *storagereplacement.Replacement, *model.Task) {
	t.Helper()
	contentSize := int64(11)
	if len(sizes) > 0 {
		contentSize = sizes[0]
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	ctx := t.Context()
	bucket := &model.Bucket{Name: "replacement-batch", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	source, err := runtime.repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: testOnChainID(t, 7101), CopyIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	if remoteSource {
		clientID := testOnChainID(t, 7102)
		if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{ID: source.ID, DataSetID: testOnChainID(t, 7103), ClientDataSetID: &clientID}); err != nil {
			t.Fatal(err)
		}
	} else {
		ensure, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeStorageDataSetEnsure, Input: map[string]int64{"data_set_id": source.ID}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
			return repos.Contents.BindDataSetEnsureTask(ctx, source.ID, row.ID)
		})
		if err != nil {
			t.Fatal(err)
		}
		settleHandlerTaskFixture(t, runtime, ensure.ID, model.TaskStatusFailed, nil)
	}
	replacement, _, err := runtime.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: testOnChainID(t, 7201), ClientRequestID: "replacement-batch"})
	if err != nil {
		t.Fatal(err)
	}
	clientID := testOnChainID(t, 7202)
	if err := markTestDataSetReady(t, runtime, ctx, repository.MarkDataSetReadyInput{ID: replacement.TargetDataSetID, DataSetID: testOnChainID(t, 7203), ClientDataSetID: &clientID}); err != nil {
		t.Fatal(err)
	}
	coordinator, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeProviderReplacementCoordinate, Input: storagereplacement.CoordinateInput{ReplacementID: replacement.ID, Generation: replacement.TaskGeneration}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
		return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, row.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Replacements.Activate(ctx, replacement.ID, replacement.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		key := fmt.Sprintf("content-%03d", i)
		content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{BucketID: bucket.ID, ContentSize: contentSize, Checksum: testutil.StorageChecksum(key), RequestedCopies: 1})
		if err != nil {
			t.Fatal(err)
		}
		version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: key, ContentID: &content.ID, Size: contentSize, ETag: key, ContentType: "application/octet-stream"}
		if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatal(err)
		}
	}
	return runtime, replacement, coordinator
}

func TestReplacementBatchAdvancesPastPendingChildren(t *testing.T) {
	runtime, replacement, coordinator := replacementBatchFixture(t, 101, false)
	ctx := t.Context()
	coordinator = runBatchTaskClaim(t, runtime, coordinator)
	row, err := runtime.repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || row.SeedingComplete || row.SeedCursorContentID == 0 {
		t.Fatalf("first seed = %#v, %v", row, err)
	}
	count, err := runtime.db.NewSelect().Model((*storagereplacement.Item)(nil)).Where("replacement_id = ?", replacement.ID).Count(ctx)
	if err != nil || count != 100 {
		t.Fatalf("first seed count = %d, %v", count, err)
	}
	wakeTask(t, runtime, coordinator.ID)
	coordinator = runBatchTaskClaim(t, runtime, coordinator)
	row, err = runtime.repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || !row.SeedingComplete {
		t.Fatalf("final seed = %#v, %v", row, err)
	}
	wakeTask(t, runtime, coordinator.ID)
	coordinator = runBatchTaskClaim(t, runtime, coordinator)
	assertStarted := func(want int) {
		t.Helper()
		count, err := runtime.db.NewSelect().Model((*model.StorageCopy)(nil)).Where("storage_data_set_id = ? AND active_task_id IS NOT NULL", replacement.TargetDataSetID).Count(ctx)
		if err != nil || count != int64(want) {
			t.Fatalf("started target copies = %d, want %d, err=%v", count, want, err)
		}
	}
	assertStarted(100)
	wakeTask(t, runtime, coordinator.ID)
	coordinator = runBatchTaskClaim(t, runtime, coordinator)
	assertStarted(101)
	wakeTask(t, runtime, coordinator.ID)
	runBatchTaskClaim(t, runtime, coordinator)
	assertStarted(101)
}

func TestReplacementBatchCancelsDeletedContentAndContinues(t *testing.T) {
	for _, finalized := range []bool{false, true} {
		t.Run(fmt.Sprintf("finalized_%t", finalized), func(t *testing.T) {
			runtime, replacement, coordinator := replacementBatchFixture(t, 2, false)
			ctx := t.Context()
			coordinator = runStorageTaskClaims(t, runtime, coordinator, model.TaskStatusPending, 1)
			items, err := runtime.repos.Replacements.ListPendingReplacementItemsAfter(ctx, replacement.ID, 0, 100)
			if err != nil || len(items) != 2 {
				t.Fatalf("seeded items = %#v, %v", items, err)
			}
			deletedID := items[0].ContentID
			version := new(model.ObjectVersion)
			if err := runtime.db.NewSelect().Model(version).Where("content_id = ?", deletedID).Scan(ctx); err != nil {
				t.Fatal(err)
			}
			if !finalized {
				if _, err := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("status = ?", model.StorageCopyStatusFailed).Where("content_id = ?", deletedID).Exec(ctx); err != nil {
					t.Fatal(err)
				}
			}
			deletion, err := runtime.repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{BucketID: version.BucketID, Key: version.Key, VersionID: version.VersionID})
			if err != nil || deletion.StorageCleanup == nil {
				t.Fatalf("delete version = %#v, %v", deletion, err)
			}
			if finalized {
				cleanup := deletion.StorageCleanup
				row, _, err := runtime.service.EnqueueTx(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeStorageCleanup, Input: map[string]int64{"content_id": deletedID, "generation": cleanup.Generation}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
					return repos.StorageCleanup.BindTask(ctx, deletedID, cleanup.Generation, row.ID)
				})
				if err != nil {
					t.Fatal(err)
				}
				if released, err := runtime.repos.Objects.ReleaseContentCacheIfUnreferenced(ctx, deletedID, func() error { return nil }); err != nil || !released {
					t.Fatalf("release deleted cache = %t, %v", released, err)
				}
				if err := runtime.repos.WithTx(ctx, func(repos *repository.Repositories) error {
					return repos.StorageCleanup.FinalizeContent(ctx, deletedID, cleanup.Generation, row.ID)
				}); err != nil {
					t.Fatal(err)
				}
			}
			wakeTask(t, runtime, coordinator.ID)
			coordinator = runStorageTaskClaims(t, runtime, coordinator, model.TaskStatusPending, 1)
			if coordinator.RetryCount != 0 {
				t.Fatalf("deleted content consumed parent budget: %#v", coordinator)
			}
			var cancelled storagereplacement.Item
			if err := runtime.db.NewSelect().Model(&cancelled).Where("id = ?", items[0].ID).Scan(ctx); err != nil {
				t.Fatal(err)
			}
			if cancelled.Status != storagereplacement.ItemStatusCancelled {
				t.Fatalf("deleted item status = %s", cancelled.Status)
			}
			copy, err := runtime.repos.Contents.GetUploadCopyForDataSet(ctx, items[1].ContentID, replacement.TargetDataSetID)
			if err != nil || copy == nil || copy.ActiveTaskID == nil {
				t.Fatalf("following copy did not progress = %#v, %v", copy, err)
			}
		})
	}
}

type revokeAfterReplacementPlan struct {
	taskengine.Handler
	revoke func()
}

type replacementSettlementRollbackObserver struct {
	armed              atomic.Bool
	liveVersionChecked atomic.Bool
	contentID          int64
	errs               chan error
	done               chan struct{}
	once               sync.Once
}

func (*replacementSettlementRollbackObserver) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (o *replacementSettlementRollbackObserver) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if !o.armed.Load() {
		return
	}
	liveVersionCheck := event.Operation() == "SELECT" && strings.Contains(event.Query, `FROM "object_versions" AS "object_version"`) &&
		strings.Contains(event.Query, fmt.Sprintf("(content_id = %d)", o.contentID)) && strings.Contains(event.Query, "(is_delete_marker = FALSE)")
	if event.Err != nil && (!liveVersionCheck || !errors.Is(event.Err, sql.ErrNoRows)) {
		select {
		case o.errs <- fmt.Errorf("unexpected guarded-settlement query error: %w", event.Err):
		default:
		}
		return
	}
	if liveVersionCheck {
		o.liveVersionChecked.Store(true)
	}
	if event.Query == "ROLLBACK" {
		if !o.liveVersionChecked.Load() {
			select {
			case o.errs <- errors.New("settlement rolled back before checking its live version"):
			default:
			}
			return
		}
		o.armed.Store(false)
		o.once.Do(func() { close(o.done) })
	}
}

func (h revokeAfterReplacementPlan) Execute(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	result := h.Handler.Execute(ctx, execution)
	h.revoke()
	return result
}

func (h revokeAfterReplacementPlan) Recover(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	result := h.Handler.Recover(ctx, execution)
	h.revoke()
	return result
}

func TestReplacementSeedingRequiresCurrentCoordinator(t *testing.T) {
	for _, count := range []int{0, 1} {
		t.Run(fmt.Sprintf("%d_candidates", count), func(t *testing.T) {
			runtime, replacement, coordinator := replacementBatchFixture(t, count, true)
			registry := taskengine.NewRegistry()
			planned := make(chan struct{})
			for _, taskType := range runtime.registry.Types() {
				handler, _ := runtime.registry.Handler(taskType)
				if taskType == coordinator.Type {
					handler = revokeAfterReplacementPlan{Handler: handler, revoke: func() {
						if _, err := runtime.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).Set("task_generation = task_generation + 1").Set("task_id = NULL").Where("id = ?", replacement.ID).Exec(t.Context()); err != nil {
							t.Error(err)
						}
						close(planned)
					}}
				}
				if err := registry.Register(handler); err != nil {
					t.Fatal(err)
				}
			}
			repos := *runtime.repos
			repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration}, &repos, registry, nil)
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := runEngine(t, engine)
			select {
			case <-planned:
			case <-time.After(3 * time.Second):
				t.Fatal("coordinator did not prepare its settlement")
			}
			stopHandlerEngine(t, cancel, done)
			row, err := runtime.repos.Replacements.GetByID(t.Context(), replacement.ID)
			if err != nil || row.SeedingComplete || row.SeedCursorContentID != 0 {
				t.Fatalf("revoked seed = %#v, %v", row, err)
			}
			items, err := runtime.db.NewSelect().Model((*storagereplacement.Item)(nil)).Where("replacement_id = ?", replacement.ID).Count(t.Context())
			if err != nil || items != 0 {
				t.Fatalf("revoked seed items = %d, %v", items, err)
			}
			copies, err := runtime.db.NewSelect().Model((*model.StorageCopy)(nil)).Where("storage_data_set_id = ?", replacement.TargetDataSetID).Count(t.Context())
			if err != nil || copies != 0 {
				t.Fatalf("revoked seed copies = %d, %v", copies, err)
			}
		})
	}
}

func TestReplacementFailedCopyRecoveryRechecksLiveVersion(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(fmt.Sprintf("exhausted_%t", exhausted), func(t *testing.T) {
			cfg, err := config.DefaultConfig()
			if err != nil {
				t.Fatal(err)
			}
			runtime, replacement, coordinator := replacementBatchFixture(t, 1, false)
			coordinator = runStorageTaskClaims(t, runtime, coordinator, model.TaskStatusPending, 1)
			items, err := runtime.repos.Replacements.ListPendingReplacementItemsAfter(t.Context(), replacement.ID, 0, 1)
			if err != nil || len(items) != 1 {
				t.Fatalf("items = %#v, %v", items, err)
			}
			copy, err := runtime.repos.Contents.GetUploadCopyForDataSet(t.Context(), items[0].ContentID, replacement.TargetDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("status = ?", model.StorageCopyStatusFailed).Where("id = ?", copy.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if exhausted {
				var checkpoint map[string]any
				if err := json.Unmarshal(coordinator.Checkpoint, &checkpoint); err != nil {
					t.Fatal(err)
				}
				checkpoint["dependency_retries"] = map[string]int{fmt.Sprintf("storage_copy:%d", copy.ID): 5}
				coordinator.Checkpoint, err = json.Marshal(checkpoint)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?::jsonb", string(coordinator.Checkpoint)).Where("id = ?", coordinator.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			version := new(model.ObjectVersion)
			if err := runtime.db.NewSelect().Model(version).Where("content_id = ?", items[0].ContentID).Scan(t.Context()); err != nil {
				t.Fatal(err)
			}
			registry := taskengine.NewRegistry()
			observer := &replacementSettlementRollbackObserver{done: make(chan struct{}), contentID: items[0].ContentID, errs: make(chan error, 1)}
			testutil.AddQueryHook(runtime.db, observer)
			for _, taskType := range runtime.registry.Types() {
				handler, _ := runtime.registry.Handler(taskType)
				if taskType == coordinator.Type {
					handler = revokeAfterReplacementPlan{Handler: handler, revoke: func() {
						if _, err := runtime.repos.Objects.DeleteObjectVersionPermanently(t.Context(), repository.DeleteObjectVersionInput{BucketID: version.BucketID, Key: version.Key, VersionID: version.VersionID}); err != nil {
							t.Error(err)
						}
						observer.armed.Store(true)
					}}
				}
				if err := registry.Register(handler); err != nil {
					t.Fatal(err)
				}
			}
			repos := *runtime.repos
			repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
			settled := make(chan struct{})
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: cfg.Worker.Tasks.LeaseDuration, OnTaskSettled: func(*model.Task, repository.TaskTransition) { close(settled) }}, &repos, registry, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			wakeTask(t, runtime, coordinator.ID)
			cancel, done := runEngine(t, engine)
			defer stopHandlerEngine(t, cancel, done)
			select {
			case <-observer.done:
			case err := <-observer.errs:
				t.Fatal(err)
			case <-settled:
				t.Fatal("recovery settled after its live version was deleted")
			case <-time.After(cfg.Worker.Tasks.LeaseDuration):
				t.Fatal("recovery did not reach its guarded settlement")
			}
			stopHandlerEngine(t, cancel, done)
			select {
			case err := <-observer.errs:
				t.Fatal(err)
			default:
			}
			stored, err := runtime.repos.Tasks.GetByID(t.Context(), coordinator.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.RetryCount != 0 || !testutil.JSONEqual(stored.Checkpoint, coordinator.Checkpoint) {
				t.Fatalf("retry budget changed after deletion = %#v", stored)
			}
			var item storagereplacement.Item
			if err := runtime.db.NewSelect().Model(&item).Where("id = ?", items[0].ID).Scan(t.Context()); err != nil || item.Status == storagereplacement.ItemStatusAttention {
				t.Fatalf("item changed after deletion = %#v, %v", item, err)
			}
			row, err := runtime.repos.Replacements.GetByID(t.Context(), replacement.ID)
			if err != nil || row.Status != storagereplacement.StatusMigrating {
				t.Fatalf("replacement failed after deletion = %#v, %v", row, err)
			}
			count, err := runtime.db.NewSelect().Model((*model.Task)(nil)).Where("type = ?", model.TaskTypeStorageTransferPlan).Count(t.Context())
			if err != nil || count != 0 {
				t.Fatalf("recovery tasks after deletion = %d, %v", count, err)
			}
		})
	}
}
