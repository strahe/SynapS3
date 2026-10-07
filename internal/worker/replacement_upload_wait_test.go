package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type activateAfterUploadSelection struct {
	repository.StorageReplacementRepository
	once     sync.Once
	activate func() error
}

func (r *activateAfterUploadSelection) SourceEligibility(ctx context.Context, id int64) (*model.StorageDataSet, bool, error) {
	source, local, err := r.StorageReplacementRepository.SourceEligibility(ctx, id)
	if errors.Is(err, storagereplacement.ErrActiveReplacement) {
		var activationErr error
		r.once.Do(func() { activationErr = r.activate() })
		if activationErr != nil {
			return nil, false, activationErr
		}
	}
	return source, local, err
}

func TestUploadPlansResumeWhenLocalReplacementTakesTheSlot(t *testing.T) {
	for _, stage := range []string{"after suspension", "before suspension settlement", "failed replacement"} {
		t.Run(stage, func(t *testing.T) {
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{register: func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
				if err := h.RegisterStorage(registry); err != nil {
					return err
				}
				return h.RegisterReplacement(registry)
			}, storage: &testutil.MockStorageClient{OpenDataSetTargetFunc: func(_ context.Context, id sdktypes.BigInt, _ storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				return &testutil.MockStorageTarget{ProviderIDValue: sdktypes.NewBigInt(202), DataSetIDValue: &id, ClientDataSetIDValue: sdktypes.NewBigInt(909)}, nil
			}}})
			ctx := t.Context()
			row, target, coordinator, observed := seedRefusedReplacement(t, runtime)
			if stage == "failed replacement" {
				reason := storagereplacement.FailureReasonTargetRejected
				if err := runtime.repos.Replacements.MarkFailed(ctx, row.ID, &reason, storagereplacement.ProviderRejectedMessage); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(time.Hour)).Where("id = ?", coordinator.ID).Exec(ctx); err != nil {
				t.Fatal(err)
			}
			content, err := runtime.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{BucketID: row.BucketID, ContentSize: 11, Checksum: testutil.StorageChecksum("replacement-upload-wait"), RequestedCopies: 1})
			if err != nil {
				t.Fatal(err)
			}
			version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: row.BucketID, Key: "object.bin", Size: 11, ETag: "replacement-upload-wait", ContentType: "application/octet-stream", ContentID: &content.ID}
			if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
				t.Fatal(err)
			}
			upload, _, err := runtime.service.Enqueue(ctx, taskengine.EnqueueRequest{Type: model.TaskTypeUploadPlan, IdempotencyKey: storagepipeline.UploadPlanKey(content.ID), Input: storagepipeline.UploadPlanInput{ContentID: content.ID}})
			if err != nil {
				t.Fatal(err)
			}
			bindReady := func() error {
				return runtime.repos.WithTx(ctx, func(repos *repository.Repositories) error {
					if err := repos.Replacements.BindObservedService(ctx, observed); err != nil {
						return err
					}
					ready, err := repos.Contents.GetDataSetBindingByID(ctx, target.ID)
					if err != nil {
						return err
					}
					return runtime.handlers.ContinueReadyDataSet(ctx, repos, ready)
				})
			}
			if stage == "before suspension settlement" {
				runtime.repos.Replacements = &activateAfterUploadSelection{StorageReplacementRepository: runtime.repos.Replacements, activate: func() error {
					if err := bindReady(); err != nil {
						return err
					}
					return activateReplacement(t, runtime, row.ID)
				}}
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer func() { stopHandlerEngine(t, cancel, done) }()
			if stage == "failed replacement" {
				waiting := waitForTask(t, runtime.repos, upload.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusPending && task.WaitReason != nil })
				if *waiting.WaitReason != "replacement" || waiting.StatusMessage == nil || *waiting.StatusMessage != "Storage replacement needs attention. Check it on the bucket page." {
					t.Fatalf("failed replacement still implied setup would continue: %#v", waiting)
				}
				return
			}
			if stage == "after suspension" {
				waiting := waitForTask(t, runtime.repos, upload.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusPending && task.WaitReason != nil })
				if *waiting.WaitReason != "replacement" || waiting.StatusMessage == nil || *waiting.StatusMessage != "Waiting for the replacement provider to finish setup" {
					t.Fatalf("wrong dependency: %#v", waiting)
				}
				if err := bindReady(); err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.repos.Tasks.WakePending(ctx, []int64{coordinator.ID}); err != nil {
					t.Fatal(err)
				}
			}
			completed := waitForTask(t, runtime.repos, upload.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusCompleted || task.Status == model.TaskStatusFailed
			}, 15*time.Second)
			if completed.Status != model.TaskStatusCompleted {
				t.Fatalf("upload did not resume: %#v", completed)
			}
			copies, err := runtime.repos.Contents.ListCopies(ctx, content.ID)
			if err != nil || len(copies) != 1 || copies[0].StorageDataSetID != target.ID || copies[0].ActiveTaskID == nil {
				t.Fatalf("replacement did not receive scheduled upload: %#v %v", copies, err)
			}
		})
	}
}
