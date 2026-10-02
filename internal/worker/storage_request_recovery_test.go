package worker_test

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestCommitTaskRestoresReleasedRequestForSettlement(t *testing.T) {
	var signatures, sends atomic.Int64
	target := &testutil.MockStorageTarget{SubmitCommitFunc: func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
		sends.Add(1)
		return nil, nil
	}}
	nonces := &testutil.MockCommitNonces{}
	runtime, pipeline := commitFixture(t, handlerRuntimeOptions{commitNonces: nonces}, target)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		signatures.Add(1)
		return testutil.CommitExtraData(12), nil
	}
	seedReleasedRequest(t, runtime, pipeline, "released-request", 11)
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("commit_extra_data_hex = NULL").
		Where("id = ?", pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	nonces.Consume(11, pipeline.targetSet.DataSetID.SDK(), sdktypes.NewBigInt(8803), pipeline.pieceCID)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageCommit)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusCommitted || copyRow.CommitExtraDataHex == nil ||
		*copyRow.CommitExtraDataHex != testutil.CommitExtraDataHex(11) || signatures.Load() != 0 || sends.Load() != 0 {
		t.Fatalf("restored task = copy:%#v err:%v signatures:%d sends:%d", copyRow, err, signatures.Load(), sends.Load())
	}
	old := new(storagecommit.Attempt)
	if err := runtime.db.NewSelect().Model(old).Where("attempt_id = ?", "released-request").Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if old.Status != storagecommit.AttemptStatusReleased || copyRow.ConfirmedAttemptID == nil || *copyRow.ConfirmedAttemptID == old.AttemptID {
		t.Fatalf("task changed its terminal history: %#v copy=%#v", old, copyRow)
	}
}

func TestTransfersSelectHistoricalCommitRequest(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypeStorageStore, model.TaskTypeStoragePull} {
		for _, conflict := range []bool{false, true} {
			name := string(taskType) + "/unique"
			if conflict {
				name = string(taskType) + "/conflict"
			}
			t.Run(name, func(t *testing.T) {
				var signatures, transfers atomic.Int64
				target := &testutil.MockStorageTarget{
					ServiceURLValue: "https://target.example",
					PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
						signatures.Add(1)
						return testutil.CommitExtraData(12), nil
					},
					StoreFunc: func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
						transfers.Add(1)
						return nil, nil
					},
					PullFunc: func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
						transfers.Add(1)
						if string(request.ExtraData) != string(testutil.CommitExtraData(11)) {
							t.Error("Pull sent another commit request")
						}
						return &storage.PullResult{}, nil
					},
				}
				var runtime handlerTestRuntime
				var pipeline seededCopyPipeline
				var pieceCID cid.Cid
				if taskType == model.TaskTypeStorageStore {
					parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
						return synapse.ParkedPieceReady, nil
					})
					runtime, pipeline, pieceCID = storeRecoveryFixture(t, 0, strings.Repeat("r", 128), parked, nil, target)
				} else {
					client := &testutil.MockStorageClient{}
					runtime = newHandlerTestRuntime(t, handlerRuntimeOptions{
						storage: client, policy: cache.EvictionPolicyNone,
						register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
							return handlers.RegisterStorage(registry)
						},
					})
					pipeline = seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
					if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), &model.ObjectVersion{
						VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "historical-pull.bin",
						ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize,
						ETag: "historical-pull", ContentType: "application/octet-stream",
					}); err != nil {
						t.Fatal(err)
					}
					target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
					dataSetID := pipeline.targetSet.DataSetID.SDK()
					target.DataSetIDValue, target.ClientDataSetIDValue = &dataSetID, pipeline.targetClient
					client.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
						return target, nil
					}
				}
				seedReleasedRequest(t, runtime, pipeline, "released-request", 11)
				if conflict {
					seedReleasedRequest(t, runtime, pipeline, "conflicting-request", 10)
				}
				taskRow := bindCopyTask(t, runtime, pipeline.target, taskType)
				if taskType == model.TaskTypeStorageStore {
					seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now().Add(-time.Minute))
				}
				cancel, done := runHandlerEngine(t, runtime)
				defer stopHandlerEngine(t, cancel, done)
				want := model.TaskStatusCompleted
				if conflict {
					want = model.TaskStatusFailed
				}
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == want })
				if signatures.Load() != 0 || (conflict && transfers.Load() != 0) {
					t.Fatalf("historical transfer signed=%d transferred=%d conflict=%v", signatures.Load(), transfers.Load(), conflict)
				}
				if !conflict {
					copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
					if err != nil || copyRow.CommitExtraDataHex == nil || *copyRow.CommitExtraDataHex != testutil.CommitExtraDataHex(11) {
						t.Fatalf("transfer lost its historical request: %#v err=%v", copyRow, err)
					}
				}
			})
		}
	}
}

func seedReleasedRequest(t *testing.T, runtime handlerTestRuntime, pipeline seededCopyPipeline, attemptID string, nonce uint64) {
	t.Helper()
	extra := testutil.CommitExtraDataHex(nonce)
	reason := string(storagecommit.ReleaseManualDuplicateAck)
	attemptedAt, resolvedAt := time.Now().Add(-time.Hour), time.Now().Add(-time.Minute)
	if _, err := runtime.db.NewInsert().Model(&storagecommit.Attempt{
		AttemptID: attemptID, ContentID: pipeline.target.ContentID, StorageDataSetID: pipeline.target.StorageDataSetID,
		Status: storagecommit.AttemptStatusReleased, ExtraDataHex: &extra, ReleaseReason: &reason,
		AttemptedAt: &attemptedAt, ResolvedAt: &resolvedAt,
	}).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
}
