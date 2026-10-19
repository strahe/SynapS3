package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func storeRecoveryFixture(t *testing.T, retries int, payload string, parked synapse.ParkedPieceChecker, cacheStore cache.Cache, target *testutil.MockStorageTarget) (handlerTestRuntime, seededCopyPipeline, cid.Cid) {
	t.Helper()
	if target.FinalizeStoreSessionFunc == nil {
		target.FinalizeStoreSessionFunc = func(context.Context, string, string, cid.Cid) error { return synapse.ErrStoreSessionIncomplete }
	}
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, storage: storageClient, parkedPieces: parked, maxAttempts: taskTestMaxAttempts(&retries),
		policy: cache.EvictionPolicyNone,
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID,
		Key: "store-recovery.bin", ContentID: &pipeline.upload.ID,
		Size: 128, ETag: "store-recovery", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	info, err := piece.Calculate(strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).
		Set("transfer_method = ?", model.StorageCopyTransferMethodCacheRestore).
		Where("id = ?", pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).
		Set("piece_cid = ?", info.CIDv2.String()).Set("checksum = ?", testutil.StorageChecksum(payload)).Where("id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	targetDataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &targetDataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	return runtime, pipeline, info.CIDv2
}

func seedStoreCheckpoint(t *testing.T, runtime handlerTestRuntime, taskID, copyID int64, pieceCID cid.Cid, providerURL string, attemptedAt time.Time) {
	t.Helper()
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyID)
	if err != nil || copyRow == nil {
		t.Fatalf("load checkpoint copy: %v", err)
	}
	content, err := runtime.repos.Contents.GetByID(t.Context(), copyRow.ContentID)
	if err != nil || content == nil {
		t.Fatalf("load checkpoint content: %v", err)
	}
	checkpoint, err := json.Marshal(map[string]any{
		"identity": testutil.DefaultContextIdentity, "pdp_verifier": testutil.DefaultPDPVerifierAddress,
		"verified_checksum": content.Checksum,
		"attempted_at":      attemptedAt.UTC(), "intended_piece_cid": pieceCID.String(),
		"provider_service_url": providerURL, "ingress_attempt": 1,
		"session_id": "00000000-0000-4000-8000-000000000001", "stage": "upload",
		"observation_started_at": attemptedAt.UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET checkpoint_json = ? WHERE id = ?`, json.RawMessage(checkpoint), taskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = 'recover' WHERE id = ?`, taskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, copyID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestStorePreparationFailuresRetainCheckpointAndRetry(t *testing.T) {
	for _, failure := range []string{"cache open", "cache calculation", "cache close", "upload reader", "claim check", "effect admission"} {
		for _, persistent := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/persistent=%v", failure, persistent), func(t *testing.T) {
				payload := strings.Repeat("r", 128)
				var ready atomic.Bool
				var cacheOpens, stores, finalizes atomic.Int64
				fault := errors.New("temporary Store preparation failure")
				cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
					open := cacheOpens.Add(1)
					if failure == "cache open" || failure == "upload reader" && open%2 == 0 {
						return nil, nil, fault
					}
					if failure == "cache calculation" {
						return &storeFaultReader{readErr: fault}, &cache.ObjectInfo{Size: 128}, nil
					}
					if failure == "cache close" {
						return &storeFaultReader{Reader: strings.NewReader(payload), closeErr: fault}, &cache.ObjectInfo{Size: 128}, nil
					}
					return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
				}}
				parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
					if ready.Load() {
						return synapse.ParkedPieceReady, nil
					}
					return synapse.ParkedPieceMissing, nil
				})
				target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example", FinalizeStoreSessionFunc: func(context.Context, string, string, cid.Cid) error {
					finalizes.Add(1)
					return synapse.ErrStoreSessionIncomplete
				}, UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
					stores.Add(1)
					return nil, errors.New("unexpected Store call")
				}}
				runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
				row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
				seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now())
				if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = 'execute' WHERE id = ?`, row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				before, err := runtime.repos.Tasks.GetByID(t.Context(), row.ID)
				if err != nil {
					t.Fatal(err)
				}
				baseTasks := runtime.repos.Tasks
				if failure == "claim check" {
					failing := &validateFailureRepository{TaskRepository: baseTasks, err: fault}
					failing.remaining.Store(10)
					runtime.repos.Tasks = failing
				}
				restoreAdmission := func() {}
				if failure == "effect admission" {
					restoreAdmission = testutil.FailWrites(t, runtime.db, "tasks", "UPDATE OF checkpoint_json", fmt.Sprintf("NEW.id = %d", row.ID), fault.Error())
				}
				if !persistent {
					runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
				}
				settled := make(chan repository.TaskTransition, 8)
				runtime.engine, err = taskengine.NewEngine(taskengine.EngineConfig{
					Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
					OnTaskSettled: func(claimed *model.Task, transition repository.TaskTransition) {
						if claimed.ID == row.ID {
							settled <- transition
						}
					},
				}, runtime.repos, handlerExecutionRegistry(t, runtime), slog.Default())
				if err != nil {
					t.Fatal(err)
				}
				cancel, done := runHandlerEngine(t, runtime)
				t.Cleanup(func() { stopHandlerEngine(t, cancel, done) })
				waitCtx, cancelWait := context.WithTimeout(t.Context(), 2*handlerTestLeaseDuration)
				defer cancelWait()
				for observed := false; !observed; {
					select {
					case transition := <-settled:
						if persistent {
							observed = transition.Status == model.TaskStatusFailed
						} else {
							observed = transition.Status == model.TaskStatusPending && transition.IncrementRetry && transition.ResumeMode == model.TaskResumeModeRecover
						}
						if !observed && transition.Status == model.TaskStatusPending {
							wakeTask(t, runtime, row.ID)
						}
					case <-waitCtx.Done():
						t.Fatal("Store preparation failure did not settle its retry opportunity")
					}
				}
				observed, err := runtime.repos.Tasks.GetByID(t.Context(), row.ID)
				if err != nil || observed == nil {
					t.Fatalf("read settled Store preparation failure: %v", err)
				}
				if !persistent && (observed.Status != model.TaskStatusPending || observed.RetryCount != 1 || observed.ResumeMode != model.TaskResumeModeRecover) {
					t.Fatalf("preparation failure did not schedule recovery: %#v", observed)
				}
				var beforeEvidence, afterEvidence map[string]any
				if err := json.Unmarshal(before.Checkpoint, &beforeEvidence); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(observed.Checkpoint, &afterEvidence); err != nil {
					t.Fatal(err)
				}
				var beforeClock, afterClock struct {
					Started time.Time `json:"observation_started_at"`
				}
				if err := json.Unmarshal(before.Checkpoint, &beforeClock); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(observed.Checkpoint, &afterClock); err != nil {
					t.Fatal(err)
				}
				if finalizes.Load() == 0 {
					if !afterClock.Started.Equal(beforeClock.Started) {
						t.Fatalf("pre-request failure refreshed observation: before=%v after=%v", beforeClock.Started, afterClock.Started)
					}
				} else if !afterClock.Started.IsZero() {
					t.Fatalf("request result began observation before provider confirmation: before=%v after=%v requests=%d", beforeClock.Started, afterClock.Started, finalizes.Load())
				}
				delete(beforeEvidence, "stage")
				delete(afterEvidence, "stage")
				delete(beforeEvidence, "observation_started_at")
				delete(afterEvidence, "observation_started_at")
				beforeJSON, _ := json.Marshal(beforeEvidence)
				afterJSON, _ := json.Marshal(afterEvidence)
				if !testutil.JSONEqual(afterJSON, beforeJSON) || observed.LastError == nil || stores.Load() != 0 {
					t.Fatalf("preparation failure lost evidence or sent bytes: task=%#v stores=%d", observed, stores.Load())
				}
				copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
				if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != row.ID || copyRow.IngressStoreAttempt != 1 || copyRow.IngressBytesTransferred != 0 {
					t.Fatalf("preparation failure changed copy ownership or progress: copy=%#v err=%v", copyRow, err)
				}
				if persistent {
					if observed.FailureReason == nil || *observed.FailureReason != "attempts_exhausted" || observed.RetryCount != 2 {
						t.Fatalf("preparation failure bypassed retry budget: %#v", observed)
					}
					restoreAdmission()
					return
				}
				stopHandlerEngine(t, cancel, done)
				restoreAdmission()
				runtime.repos.Tasks = baseTasks
				ready.Store(true)
				opensBeforeRecovery := cacheOpens.Load()
				cancel, done = runHandlerEngine(t, runtime)
				completed := waitForTask(t, runtime.repos, row.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
				if completed.RetryCount != 1 || stores.Load() != 0 || cacheOpens.Load() != opensBeforeRecovery {
					t.Fatalf("recovery did not confirm the existing piece: task=%#v stores=%d cache_opens=%d", completed, stores.Load(), cacheOpens.Load())
				}
			})
		}
	}
}

type storeFaultReader struct {
	io.Reader
	readErr  error
	closeErr error
}

func (r *storeFaultReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return r.Reader.Read(p)
}

func (r *storeFaultReader) Close() error { return r.closeErr }

func TestStoreReadyCheckpointWriteFailureRetriesWithoutSending(t *testing.T) {
	payload := strings.Repeat("q", 128)
	var stores, checks atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		checks.Add(1)
		return synapse.ParkedPieceReady, nil
	})
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example", UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
		stores.Add(1)
		return nil, errors.New("unexpected Store call")
	}}
	runtime, pipeline, _ := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	restore := testutil.FailWrites(t, runtime.db, "tasks", "UPDATE OF checkpoint_json", fmt.Sprintf("NEW.id = %d", row.ID), "temporary checkpoint write failure")
	baseTasks := runtime.repos.Tasks
	runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: baseTasks, maximum: 1}
	cancel, done := runHandlerEngine(t, runtime)
	t.Cleanup(func() { stopHandlerEngine(t, cancel, done) })
	retrying := waitForTask(t, runtime.repos, row.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.RetryCount == 1 && task.ResumeMode == model.TaskResumeModeRecover
	})
	if len(retrying.Checkpoint) != 0 || stores.Load() != 0 || checks.Load() != 1 {
		t.Fatalf("Ready write failure = task:%#v stores:%d checks:%d", retrying, stores.Load(), checks.Load())
	}
	stopHandlerEngine(t, cancel, done)
	restore()
	runtime.repos.Tasks = baseTasks
	cancel, done = runHandlerEngine(t, runtime)
	completed := waitForTask(t, runtime.repos, row.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if completed.RetryCount != 1 || len(completed.Checkpoint) == 0 || stores.Load() != 0 || checks.Load() < 2 {
		t.Fatalf("Ready write recovery = task:%#v stores:%d checks:%d", completed, stores.Load(), checks.Load())
	}
}

func TestStoreAdmissionStopsWithoutRecoveryEvidenceOrWithConflictingOperation(t *testing.T) {
	for _, tt := range []struct {
		name        string
		conflict    bool
		createStage bool
	}{
		{name: "missing recovery evidence"},
		{name: "conflicting operation", conflict: true},
		{name: "create checkpoint retries admission failure", createStage: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := strings.Repeat("u", 128)
			var stores, creates, finalizes atomic.Int64
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
			}}
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				return synapse.ParkedPieceMissing, nil
			})
			target := &testutil.MockStorageTarget{
				ServiceURLValue: "https://store.example",
				CreateStoreSessionFunc: func(context.Context) (string, error) {
					creates.Add(1)
					return "", errors.New("unexpected session creation")
				},
				FinalizeStoreSessionFunc: func(context.Context, string, string, cid.Cid) error {
					finalizes.Add(1)
					return errors.New("unexpected session finalization")
				},
				UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
					stores.Add(1)
					return nil, errors.New("unexpected Store call")
				},
			}
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			originalStart := time.Now().UTC().Add(-5 * time.Minute)
			if tt.conflict {
				seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), originalStart)
				raw, err := json.Marshal(map[string]any{"operation_key": "store:another-piece", "operation_started_at": time.Now()})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = 'execute', runtime_json = ? WHERE id = ?`, json.RawMessage(raw), row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else {
				if tt.createStage {
					seedStoreCheckpoint(t, runtime, row.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), originalStart)
					if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = 'execute', checkpoint_json = (checkpoint_json - 'session_id') || '{"stage":"create"}'::jsonb WHERE id = ?`, row.ID).Exec(t.Context()); err != nil {
						t.Fatal(err)
					}
				} else if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				testutil.FailWrites(t, runtime.db, "tasks", "UPDATE OF checkpoint_json", fmt.Sprintf("NEW.id = %d", row.ID), "temporary checkpoint write failure")
			}
			var observed *model.Task
			if tt.createStage {
				observed = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
				var checkpoint struct {
					SessionID            string    `json:"session_id"`
					Stage                string    `json:"stage"`
					IngressAttempt       int       `json:"ingress_attempt"`
					ObservationStartedAt time.Time `json:"observation_started_at"`
				}
				if err := json.Unmarshal(observed.Checkpoint, &checkpoint); err != nil || observed.RetryCount != 1 || observed.LastError == nil || observed.FailureReason == nil || *observed.FailureReason != "store_checkpoint_write_failed" || checkpoint.SessionID != "" || checkpoint.Stage != "create" || checkpoint.IngressAttempt != 1 || !checkpoint.ObservationStartedAt.Equal(originalStart) {
					t.Fatalf("legal create checkpoint did not preserve retry evidence: task=%#v checkpoint=%+v err=%v", observed, checkpoint, err)
				}
			} else {
				cancel, done := runHandlerEngine(t, runtime)
				defer stopHandlerEngine(t, cancel, done)
				observed = waitForTask(t, runtime.repos, row.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
				wantReason := "store_checkpoint_write_failed"
				if tt.conflict {
					wantReason = "store_checkpoint_conflict"
				}
				if observed.FailureReason == nil || *observed.FailureReason != wantReason || observed.RetryCount != 0 || (len(observed.Checkpoint) != 0) != tt.conflict {
					t.Fatalf("unsafe admission retried: task=%#v", observed)
				}
			}
			if stores.Load() != 0 || creates.Load() != 0 || finalizes.Load() != 0 {
				t.Fatalf("failed admission sent requests: create:%d uploads:%d finalize:%d", creates.Load(), stores.Load(), finalizes.Load())
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != row.ID || copyRow.IngressStoreAttempt != 1 || copyRow.IngressBytesTransferred != 0 {
				t.Fatalf("failed admission lost ownership or progress: copy=%#v err=%v", copyRow, err)
			}
		})
	}
}

func TestStoreWorkStartsAfterHashPreparation(t *testing.T) {
	payload := strings.Repeat("h", 128)
	var runtime handlerTestRuntime
	var taskID int64
	var opens int
	var readyAt time.Time
	cacheStore := &testutil.MockCache{GetFunc: func(ctx context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
		opens++
		stored, err := runtime.repos.Tasks.GetByID(ctx, taskID)
		if err != nil || stored.WorkStartedAt != nil {
			t.Fatalf("upload started while preparing cached bytes: %#v, err=%v", stored, err)
		}
		if opens == 2 {
			readyAt = time.Now()
		}
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: int64(len(payload))}, nil
	}}
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://store.example",
		UploadStoreSessionFunc: func(ctx context.Context, _ string, _ string, _ io.Reader, _ *storage.StoreOptions) (*storage.StoreResult, error) {
			stored, err := runtime.repos.Tasks.GetByID(ctx, taskID)
			if err != nil || stored.WorkStartedAt == nil || readyAt.IsZero() || stored.WorkStartedAt.Before(readyAt) {
				t.Fatalf("actual upload start = %#v, ready=%v, err=%v", stored, readyAt, err)
			}
			return nil, errors.New("upload awaiting recovery")
		},
	}
	var pipeline seededCopyPipeline
	runtime, pipeline, _ = storeRecoveryFixture(t, 1, payload, nil, cacheStore, target)
	row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	taskID = row.ID
	runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskID, func(row *model.Task) bool {
		return row.Status == model.TaskStatusPending && row.ResumeMode == model.TaskResumeModeRecover
	})
}

type storeClaimObserver struct {
	repository.TaskRepository
	lost           chan struct{}
	identityLoaded chan struct{}
}

func (r *storeClaimObserver) GetByID(ctx context.Context, id int64) (*model.Task, error) {
	row, err := r.TaskRepository.GetByID(ctx, id)
	if row != nil && row.Type == model.TaskTypeStorageDataSetEnsure {
		select {
		case r.identityLoaded <- struct{}{}:
		default:
		}
	}
	return row, err
}

func (r *storeClaimObserver) ValidateClaim(ctx context.Context, id, generation int64) error {
	err := r.TaskRepository.ValidateClaim(ctx, id, generation)
	if errors.Is(err, repository.ErrTaskLeaseLost) {
		select {
		case r.lost <- struct{}{}:
		default:
		}
	}
	return err
}

func TestStoreRechecksExecutionAfterWaitingForCacheReadLock(t *testing.T) {
	for _, scenario := range []string{"cancelled", "claim lost"} {
		t.Run(scenario, func(t *testing.T) {
			var reads, uploads atomic.Int64
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				reads.Add(1)
				return io.NopCloser(strings.NewReader(strings.Repeat("h", 128))), &cache.ObjectInfo{Size: 128}, nil
			}}
			target := &testutil.MockStorageTarget{
				ServiceURLValue: "https://store.example",
				UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
					uploads.Add(1)
					return nil, errors.New("unexpected upload")
				},
			}
			runtime, pipeline, _ := storeRecoveryFixture(t, 1, strings.Repeat("h", 128), nil, cacheStore, target)
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			observer := &storeClaimObserver{TaskRepository: runtime.repos.Tasks, lost: make(chan struct{}, 1), identityLoaded: make(chan struct{}, 1)}
			runtime.repos.Tasks = &limitedClaimRepository{TaskRepository: observer, maximum: 1}
			var opened atomic.Int64
			runtime.storage.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				opened.Add(1)
				return target, nil
			}
			locked, release, unlocked := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				_ = runtime.gate.GuardDeletion(t.Context(), model.ContentCacheKey(pipeline.upload.ID), func() {
					close(locked)
					<-release
				})
				close(unlocked)
			}()
			<-locked
			cancel, done := runHandlerEngine(t, runtime)
			t.Cleanup(func() {
				cancel()
				select {
				case <-release:
				default:
					close(release)
				}
				<-unlocked
				<-done
			})
			select {
			case <-observer.identityLoaded:
			case <-time.After(3 * time.Second):
				t.Fatal("store did not load its creation identity")
			}
			if scenario == "cancelled" {
				cancel()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("canceled Store did not stop while deletion retained the cache gate")
				}
				select {
				case <-unlocked:
					t.Fatal("deletion released the gate before cancellation completed")
				default:
				}
			} else {
				if _, err := runtime.db.NewRaw(`UPDATE tasks SET claim_generation = claim_generation + 1 WHERE id = ?`, row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			if scenario == "claim lost" {
				select {
				case <-observer.lost:
				case <-time.After(3 * time.Second):
					t.Fatal("store did not revalidate its lost claim")
				}
				cancel()
			}
			<-done
			if reads.Load() != 0 || uploads.Load() != 0 || opened.Load() != 0 {
				t.Fatalf("stopped store read cache %d times, opened provider %d times and uploaded %d times", reads.Load(), opened.Load(), uploads.Load())
			}
		})
	}
}

func TestStoreProcessingWaitAndQueryFailureRetainRecoveryEvidence(t *testing.T) {
	for _, tt := range []struct {
		name      string
		state     synapse.ParkedPieceState
		err       error
		expired   bool
		zeroStart bool
	}{
		{name: "processing", state: synapse.ParkedPieceProcessing},
		{name: "processing window expired", state: synapse.ParkedPieceProcessing, expired: true},
		{name: "query failure", err: errors.New("provider unavailable")},
		{name: "query failure before observation", err: errors.New("provider unavailable"), zeroStart: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := strings.Repeat("k", 128)
			target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example"}
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				return tt.state, tt.err
			})
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, nil, target)
			taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			started := time.Now()
			if tt.expired {
				started = started.Add(-31 * time.Minute)
			}
			seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), started)
			if tt.zeroStart {
				if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = checkpoint_json - 'observation_started_at' WHERE id = ?", taskRow.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if tt.expired {
				runtimeJSON, err := json.Marshal(map[string]any{"operation_key": "store:" + pieceCID.String(), "operation_started_at": time.Now().Add(-31 * time.Minute)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewRaw(`UPDATE tasks SET runtime_json = ? WHERE id = ?`, json.RawMessage(runtimeJSON), taskRow.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			observed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				if tt.err != nil || tt.expired {
					return task.Status == model.TaskStatusFailed
				}
				return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider_confirmation"
			})
			if len(observed.Checkpoint) == 0 {
				t.Fatal("Store recovery evidence was lost")
			}
			if tt.zeroStart {
				var evidence struct {
					Started time.Time `json:"observation_started_at"`
				}
				if err := json.Unmarshal(observed.Checkpoint, &evidence); err != nil || !evidence.Started.IsZero() {
					t.Fatalf("failed provider queries began observation: checkpoint=%+v err=%v", evidence, err)
				}
			}
			if tt.expired {
				if observed.FailureReason == nil || *observed.FailureReason != "store_processing_timeout" || observed.RetryCount != 0 {
					t.Fatalf("expired observation = %#v", observed)
				}
			} else if tt.err != nil {
				if observed.FailureReason == nil || *observed.FailureReason != "attempts_exhausted" || observed.RetryCount != 2 {
					t.Fatalf("failed query budget = %#v", observed)
				}
			} else if observed.RetryCount != 0 || observed.LastError != nil {
				t.Fatalf("healthy processing consumed retries = %#v", observed)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID {
				t.Fatalf("Store copy after timeout = %#v, err=%v", copyRow, err)
			}
		})
	}
}

func TestStoreManualRetryUsesFreshCurrentPolicyBudget(t *testing.T) {
	payload := strings.Repeat("r", 128)
	var stores atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example", UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
		stores.Add(1)
		return nil, errors.New("connection closed after upload")
	}}
	runtime, pipeline, _ := storeRecoveryFixture(t, 0, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" || !runtime.service.Retryable(failed) || stores.Load() != 1 {
		t.Fatalf("zero-retry Store = task:%#v uploads:%d", failed, stores.Load())
	}
	stopHandlerEngine(t, cancel, done)
	runtime.testMaxAttempts = nil
	settled := make(chan repository.TaskTransition, 32)
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
		OnTaskSettled: func(claimed *model.Task, transition repository.TaskTransition) {
			if claimed.ID == taskRow.ID {
				settled <- transition
			}
		},
	}, runtime.repos, handlerExecutionRegistry(t, runtime), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	runtime.engine = engine
	{
		var retryErr error
		taskRow, retryErr = runtime.service.Retry(t.Context(), taskRow.ID)
		if retryErr != nil {
			t.Fatal(retryErr)
		}
	}
	cancel, done = runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 2*handlerTestLeaseDuration)
	defer cancelWait()
	for finished := false; !finished; {
		select {
		case transition := <-settled:
			finished = transition.Status == model.TaskStatusFailed
			if transition.Status == model.TaskStatusPending {
				wakeTask(t, runtime, taskRow.ID)
			}
		case <-waitCtx.Done():
			t.Fatal("current-policy Store round did not exhaust its retry budget")
		}
	}
	retrying, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
	if err != nil || retrying == nil {
		t.Fatalf("read exhausted Store round: %v", err)
	}
	if handlerTaskMaxAttempts(t, runtime, retrying) != 10 || retrying.RetryCount != 9 || stores.Load() != 11 {
		t.Fatalf("new Store round budget = task:%#v uploads:%d, want current policy's ten attempts", retrying, stores.Load())
	}
}

func TestStoreStopsAfterConfiguredAutomaticRetransmissions(t *testing.T) {
	payload := strings.Repeat("l", 128)
	var stores, creates, finalizes, queriesSinceStore atomic.Int64
	var runtime handlerTestRuntime
	var taskID int64
	type checkpointEvidence struct {
		IngressAttempt       int       `json:"ingress_attempt"`
		SessionID            string    `json:"session_id"`
		ObservationStartedAt time.Time `json:"observation_started_at"`
		RetryCount           int
	}
	admissions := make(chan checkpointEvidence, 10)
	observations := make(chan checkpointEvidence, 18)
	readEvidence := func(ctx context.Context) (checkpointEvidence, error) {
		row, err := runtime.repos.Tasks.GetByID(ctx, taskID)
		if err != nil {
			return checkpointEvidence{}, err
		}
		var evidence checkpointEvidence
		if err := json.Unmarshal(row.Checkpoint, &evidence); err != nil {
			return checkpointEvidence{}, err
		}
		evidence.RetryCount = row.RetryCount
		return evidence, nil
	}
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(ctx context.Context, _ string, _ cid.Cid) (synapse.ParkedPieceState, error) {
		if stores.Load() == 0 || queriesSinceStore.Add(1) > 2 {
			return synapse.ParkedPieceMissing, nil
		}
		evidence, err := readEvidence(ctx)
		if err != nil {
			return "", err
		}
		select {
		case observations <- evidence:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return synapse.ParkedPieceProcessing, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://store.example",
		CreateStoreSessionFunc: func(context.Context) (string, error) {
			creates.Add(1)
			return "00000000-0000-4000-8000-000000000001", nil
		},
		FinalizeStoreSessionFunc: func(context.Context, string, string, cid.Cid) error {
			finalizes.Add(1)
			return synapse.ErrStoreSessionIncomplete
		},
		UploadStoreSessionFunc: func(ctx context.Context, _ string, _ string, _ io.Reader, _ *storage.StoreOptions) (*storage.StoreResult, error) {
			stores.Add(1)
			queriesSinceStore.Store(0)
			evidence, err := readEvidence(ctx)
			if err != nil {
				return nil, err
			}
			select {
			case admissions <- evidence:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, errors.New("connection closed after upload")
		},
	}
	var pipeline seededCopyPipeline
	runtime, pipeline, _ = storeRecoveryFixture(t, 9, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	taskID = taskRow.ID
	observationDeadline := time.Duration(handlerTaskMaxAttempts(t, runtime, taskRow)) * handlerTestLeaseDuration
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		if task.Status == model.TaskStatusPending {
			wakeTask(t, runtime, task.ID)
		}
		return task.Status == model.TaskStatusFailed
	}, observationDeadline)
	if failed.FailureReason == nil || *failed.FailureReason != "attempts_exhausted" || !runtime.service.Retryable(failed) || stores.Load() != 10 || creates.Load() != 1 || finalizes.Load() != 9 || failed.RetryCount != 9 {
		t.Fatalf("Store retry limit = task:%#v create:%d uploads:%d finalize:%d", failed, creates.Load(), stores.Load(), finalizes.Load())
	}
	if len(admissions) != 10 || len(observations) != 18 {
		t.Fatalf("Store boundary checkpoints = admissions:%d observations:%d", len(admissions), len(observations))
	}
	var previousStart time.Time
	for attempt := 1; attempt <= 10; attempt++ {
		admitted := <-admissions
		if admitted.IngressAttempt != attempt || admitted.RetryCount != attempt-1 || admitted.SessionID != "00000000-0000-4000-8000-000000000001" || !admitted.ObservationStartedAt.IsZero() {
			t.Fatalf("Store admission %d = %+v", attempt, admitted)
		}
		if attempt == 10 {
			break
		}
		first, second := <-observations, <-observations
		if first.IngressAttempt != attempt || second.IngressAttempt != attempt || first.RetryCount != attempt || second.RetryCount != attempt || !first.ObservationStartedAt.IsZero() || !second.ObservationStartedAt.After(previousStart) {
			t.Fatalf("Store observation %d changed clock or budget: first=%+v second=%+v previous=%s", attempt, first, second, previousStart)
		}
		previousStart = second.ObservationStartedAt
	}
	var finalEvidence checkpointEvidence
	if err := json.Unmarshal(failed.Checkpoint, &finalEvidence); err != nil || finalEvidence.IngressAttempt != 10 || !finalEvidence.ObservationStartedAt.IsZero() {
		t.Fatalf("exhausted Store checkpoint = %+v, err:%v", finalEvidence, err)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID || copyRow.IngressStoreAttempt != 10 {
		t.Fatalf("Store copy lost its retryable task or budget: %#v, err:%v", copyRow, err)
	}
}

func TestNewStoreTaskAdoptsPreviousCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name             string
		knownStart       bool
		identityConflict bool
	}{
		{name: "known start", knownStart: true},
		{name: "unknown start"},
		{name: "identity conflict", knownStart: true, identityConflict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := strings.Repeat("a", 128)
			var storeCalls atomic.Int64
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				return synapse.ParkedPieceReady, nil
			})
			target := &testutil.MockStorageTarget{
				ServiceURLValue: "https://store.example",
				UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
					storeCalls.Add(1)
					return nil, errors.New("unexpected upload")
				},
				PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return testutil.CommitExtraData(7), nil },
			}
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, nil, target)
			old := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			seedStoreCheckpoint(t, runtime, old.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now())
			startedAt := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Microsecond)
			var historicalStart *time.Time
			if tc.knownStart {
				historicalStart = &startedAt
			}
			if _, err := runtime.db.NewRaw(`UPDATE tasks SET status = 'failed', finished_at = ?, resume_mode = 'recover', work_started_at = ? WHERE id = ?`, time.Now(), historicalStart, old.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET status = 'failed', active_task_id = NULL WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.Contents.ReopenFailedUploadCopy(t.Context(), pipeline.target.ID); err != nil {
				t.Fatal(err)
			}
			newTask := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			if tc.identityConflict {
				if _, err := runtime.db.NewRaw(`UPDATE tasks SET input_json = ? WHERE id = ?`, newTask.Input, old.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			completed := waitForTask(t, runtime.repos, newTask.ID, func(task *model.Task) bool {
				return task.Status == model.TaskStatusCompleted || task.Status == model.TaskStatusFailed
			})
			if tc.identityConflict {
				if completed.Status != model.TaskStatusFailed || completed.LastError == nil || !strings.Contains(*completed.LastError, "identity conflicts") || completed.WorkStartedAt != nil || len(completed.Checkpoint) != 0 || storeCalls.Load() != 0 {
					t.Fatalf("conflicting Store adoption = %#v, uploads:%d", completed, storeCalls.Load())
				}
				return
			}
			if completed.Status != model.TaskStatusCompleted {
				t.Fatalf("adopted Store status = %s, error:%v", completed.Status, completed.LastError)
			}
			if len(completed.Checkpoint) == 0 {
				t.Fatal("new Store task did not retain the previous checkpoint")
			}
			var evidence struct {
				Started time.Time `json:"observation_started_at"`
			}
			if err := json.Unmarshal(completed.Checkpoint, &evidence); err != nil || !evidence.Started.IsZero() {
				t.Fatalf("adopted Store checkpoint retained the previous observation clock: checkpoint=%+v err=%v", evidence, err)
			}
			if completed.WorkStartedAt == nil || completed.WorkStartedAt.Before(newTask.CreatedAt) {
				t.Fatalf("new Store recovery start = %v, before creation %v", completed.WorkStartedAt, newTask.CreatedAt)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow.Status == model.StorageCopyStatusPending || copyRow.Status == model.StorageCopyStatusFailed || storeCalls.Load() != 0 {
				t.Fatalf("adopted Store copy = %#v, uploads:%d, err:%v", copyRow, storeCalls.Load(), err)
			}
		})
	}
}

func TestNewStoreTaskWithoutCheckpointNeedsReadableCache(t *testing.T) {
	payload := strings.Repeat("m", 128)
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return nil, nil, os.ErrNotExist
	}}
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example"}
	runtime, pipeline, _ := storeRecoveryFixture(t, 2, payload, nil, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != "store_cache_missing" || len(failed.Checkpoint) != 0 {
		t.Fatalf("uncached Store task = %#v", failed)
	}
}

func TestNewStoreTaskWithoutCheckpointChecksProviderBeforeUploading(t *testing.T) {
	payload := strings.Repeat("n", 128)
	var stores, checks atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		checks.Add(1)
		if stores.Load() > 0 {
			return synapse.ParkedPieceReady, nil
		}
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://store.example",
		UploadStoreSessionFunc: func(_ context.Context, _ string, _ string, _ io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
			stores.Add(1)
			return &storage.StoreResult{PieceCID: options.PieceCID, Size: 128}, nil
		},
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return testutil.CommitExtraData(7), nil },
	}
	runtime, pipeline, _ := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		if task.Status == model.TaskStatusPending {
			wakeTask(t, runtime, task.ID)
		}
		return task.Status == model.TaskStatusCompleted
	})
	if stores.Load() != 1 || checks.Load() < 2 || completed.RetryCount != 0 || len(completed.Checkpoint) == 0 {
		t.Fatalf("Store without old checkpoint = task:%#v uploads:%d checks:%d", completed, stores.Load(), checks.Load())
	}
}

func TestNewStoreTaskWaitsForUnavailableProviderAfterEarlierUpload(t *testing.T) {
	payload := strings.Repeat("u", 128)
	var providerReady atomic.Bool
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceReady, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue:      "https://store.example",
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return testutil.CommitExtraData(7), nil },
	}
	runtime, pipeline, _ := storeRecoveryFixture(t, 0, payload, parked, cacheStore, target)
	runtime.storage.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		if !providerReady.Load() {
			return nil, storage.ErrDataSetUnavailable
		}
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waiting := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider"
	})
	if waiting.RetryCount != 0 || len(waiting.Checkpoint) != 0 {
		t.Fatalf("unavailable provider consumed retry or checkpointed Store: %#v", waiting)
	}
	providerReady.Store(true)
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
}

func TestStoreDoesNotUseOldProviderPieceForChangedTarget(t *testing.T) {
	payload := strings.Repeat("p", 128)
	var stores, oldChecks, currentChecks atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(_ context.Context, serviceURL string, _ cid.Cid) (synapse.ParkedPieceState, error) {
		if serviceURL == "https://old.example" {
			oldChecks.Add(1)
			return synapse.ParkedPieceReady, nil
		}
		currentChecks.Add(1)
		if stores.Load() > 0 {
			return synapse.ParkedPieceReady, nil
		}
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://current.example",
		UploadStoreSessionFunc: func(_ context.Context, _ string, _ string, _ io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
			stores.Add(1)
			return &storage.StoreResult{PieceCID: options.PieceCID, Size: 128}, nil
		},
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return testutil.CommitExtraData(7), nil },
	}
	runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, "https://old.example", time.Now())
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		if task.Status == model.TaskStatusPending {
			wakeTask(t, runtime, task.ID)
		}
		return task.Status == model.TaskStatusCompleted
	})
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status == model.StorageCopyStatusPending || copyRow.Status == model.StorageCopyStatusFailed || stores.Load() != 1 || oldChecks.Load() == 0 || currentChecks.Load() == 0 || completed.RetryCount != 0 {
		t.Fatalf("changed provider Store = copy:%#v task:%#v uploads:%d old checks:%d current checks:%d err:%v", copyRow, completed, stores.Load(), oldChecks.Load(), currentChecks.Load(), err)
	}
}

func TestStoreRestartBeforeRetryCheckpointRechecksOldPiece(t *testing.T) {
	payload := strings.Repeat("b", 128)
	var stores atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example", UploadStoreSessionFunc: func(context.Context, string, string, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
		stores.Add(1)
		return nil, errors.New("transfer interrupted")
	}}
	runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now())
	limitedRepos := *runtime.repos
	limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	firstEngine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
	}, &limitedRepos, handlerExecutionRegistry(t, runtime), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runEngine(t, firstEngine)
	before := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	stopHandlerEngine(t, cancel, done)
	var checkpoint struct {
		IngressAttempt int `json:"ingress_attempt"`
	}
	if err := json.Unmarshal(before.Checkpoint, &checkpoint); err != nil || checkpoint.IngressAttempt != 1 || stores.Load() != 0 {
		t.Fatalf("before new checkpoint = attempt:%d uploads:%d err:%v", checkpoint.IngressAttempt, stores.Load(), err)
	}
	cancel, done = runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	after := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover && stores.Load() == 1
	})
	if err := json.Unmarshal(after.Checkpoint, &checkpoint); err != nil || checkpoint.IngressAttempt != 2 || after.RetryCount != 1 {
		t.Fatalf("after new checkpoint = attempt:%d retry:%d err:%v", checkpoint.IngressAttempt, after.RetryCount, err)
	}
}

func TestStoreRestartAfterRetryCheckpointOnlyQueriesProvider(t *testing.T) {
	for _, recoveredState := range []synapse.ParkedPieceState{synapse.ParkedPieceReady, synapse.ParkedPieceProcessing} {
		t.Run(string(recoveredState), func(t *testing.T) {
			payload := strings.Repeat("c", 128)
			var stores, creates, finalizes atomic.Int64
			var restarting atomic.Bool
			var parkedState atomic.Value
			parkedState.Store(synapse.ParkedPieceMissing)
			enteredStore := make(chan struct{})
			resumedQueries := make(chan chan struct{})
			cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
			}}
			parked := parkedPieceCheckerFunc(func(ctx context.Context, _ string, _ cid.Cid) (synapse.ParkedPieceState, error) {
				if restarting.Load() {
					release := make(chan struct{})
					select {
					case resumedQueries <- release:
					case <-ctx.Done():
						return "", ctx.Err()
					}
					select {
					case <-release:
					case <-ctx.Done():
						return "", ctx.Err()
					}
				}
				return parkedState.Load().(synapse.ParkedPieceState), nil
			})
			target := &testutil.MockStorageTarget{
				ServiceURLValue: "https://store.example",
				CreateStoreSessionFunc: func(context.Context) (string, error) {
					creates.Add(1)
					return "", errors.New("unexpected session creation")
				},
				FinalizeStoreSessionFunc: func(context.Context, string, string, cid.Cid) error {
					finalizes.Add(1)
					return synapse.ErrStoreSessionIncomplete
				},
				UploadStoreSessionFunc: func(ctx context.Context, _ string, _ string, _ io.Reader, _ *storage.StoreOptions) (*storage.StoreResult, error) {
					if stores.Add(1) == 1 {
						close(enteredStore)
					}
					<-ctx.Done()
					return nil, ctx.Err()
				},
				PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return testutil.CommitExtraData(7), nil },
			}
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
			taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			originalStart := time.Now().UTC().Add(-5 * time.Minute)
			seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), originalStart)
			cancel, done := runHandlerEngine(t, runtime)
			defer func() { stopHandlerEngine(t, cancel, done) }()
			select {
			case <-enteredStore:
			case <-time.After(3 * time.Second):
				t.Fatal("Store did not start after the new checkpoint")
			}
			inFlight, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
			if err != nil {
				t.Fatal(err)
			}
			var checkpoint struct {
				AttemptedAt          time.Time `json:"attempted_at"`
				IngressAttempt       int       `json:"ingress_attempt"`
				ObservationStartedAt time.Time `json:"observation_started_at"`
			}
			if err := json.Unmarshal(inFlight.Checkpoint, &checkpoint); err != nil || checkpoint.IngressAttempt != 2 || inFlight.RetryCount != 0 || !checkpoint.ObservationStartedAt.IsZero() || !checkpoint.AttemptedAt.Equal(originalStart) {
				t.Fatalf("in-flight checkpoint = %+v retry:%d err:%v", checkpoint, inFlight.RetryCount, err)
			}
			stopHandlerEngine(t, cancel, done)
			parkedState.Store(recoveredState)
			restarting.Store(true)
			if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).
				Set("lease_until = ?", time.Now().Add(-time.Second)).Where("id = ?", taskRow.ID).Exec(t.Context()); err != nil {
				t.Fatalf("expire stopped Store claim: %v", err)
			}
			restarted, err := taskengine.NewEngine(taskengine.EngineConfig{
				Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
			}, runtime.repos, handlerExecutionRegistry(t, runtime), slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			beforeRestart := time.Now().UTC()
			cancel, done = runEngine(t, restarted)
			nextQuery := func() chan struct{} {
				t.Helper()
				select {
				case release := <-resumedQueries:
					return release
				case <-time.After(3 * time.Second):
					t.Fatal("restarted Store did not query provider")
					return nil
				}
			}
			release := nextQuery()
			recovered, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(recovered.Checkpoint, &checkpoint); err != nil || !checkpoint.ObservationStartedAt.IsZero() {
				t.Fatalf("interrupted Store began observation before recovery GET: %+v err:%v", checkpoint, err)
			}
			close(release)
			if recoveredState == synapse.ParkedPieceProcessing {
				release = nextQuery()
				observed, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(observed.Checkpoint, &checkpoint); err != nil || checkpoint.ObservationStartedAt.Before(beforeRestart) || checkpoint.ObservationStartedAt.After(time.Now().UTC()) || checkpoint.IngressAttempt != 2 || observed.RetryCount != 0 {
					t.Fatalf("restarted Store observation = %+v retry:%d err:%v", checkpoint, observed.RetryCount, err)
				}
				observedStart := checkpoint.ObservationStartedAt
				close(release)
				release = nextQuery()
				observed, err = runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(observed.Checkpoint, &checkpoint); err != nil || !checkpoint.ObservationStartedAt.Equal(observedStart) || observed.RetryCount != 0 {
					t.Fatalf("Store recovery query refreshed observation: %+v retry:%d err:%v", checkpoint, observed.RetryCount, err)
				}
				parkedState.Store(synapse.ParkedPieceReady)
				close(release)
			}
			waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
			if stores.Load() != 1 || creates.Load() != 0 || finalizes.Load() != 1 {
				t.Fatalf("Store calls after restart = create:%d uploads:%d finalize:%d", creates.Load(), stores.Load(), finalizes.Load())
			}
		})
	}
}

func TestPeerPullWaitsUntilStoreCopyIsReadable(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	bucket := &model.Bucket{Name: "peer-source-wait", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	content, err := runtime.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 128, Checksum: testutil.StorageChecksum("peer-source-wait"), RequestedCopies: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "peer.bin",
		ContentID: &content.ID, Size: 128, ETag: "peer", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	bindings := make([]*model.StorageDataSet, 2)
	for i := range bindings {
		providerID := testOnChainID(t, int64(61001+i))
		binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: providerID, CopyIndex: i, CreatedByContentID: content.ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		dataSetID := testOnChainID(t, int64(62001+i))
		clientID := testOnChainID(t, int64(63001+i))
		if err := markTestDataSetReady(t, runtime, t.Context(), repository.MarkDataSetReadyInput{
			ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
		}); err != nil {
			t.Fatal(err)
		}
		bindings[i] = binding
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: bindings[0].ID, CopyIndex: 0, ProviderID: bindings[0].ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress},
		{StorageDataSetID: bindings[1].ID, CopyIndex: 1, ProviderID: bindings[1].ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull},
	}); err != nil {
		t.Fatal(err)
	}
	copies, err := runtime.repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 2 {
		t.Fatalf("copies = %#v, err:%v", copies, err)
	}
	peerTask := bindCopyTask(t, runtime, &copies[1], model.TaskTypeStorageTransferPlan)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, peerTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "source"
	})
	pieceID := testOnChainID(t, 64001)
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
		StorageCopyID: copies[0].ID, ContentID: content.ID, CopyIndex: 0,
		PieceCID: testPieceCID(t, "peer-source-wait").String(), PieceID: &pieceID, RetrievalURL: "https://source.example/piece",
	})
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), peerTask.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitForTask(t, runtime.repos, peerTask.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
}
