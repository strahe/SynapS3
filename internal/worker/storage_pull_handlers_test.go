package worker_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestPullCopiesJoinOneBatchWithIndependentAuthorizations(t *testing.T) {
	client := &testutil.MockStorageClient{}
	nonces := &testutil.MockCommitNonces{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: client, policy: cache.EvictionPolicyNone, commitNonces: nonces,
		register: func(h *worker.TaskHandlers, registry *taskengine.Registry) error { return h.RegisterStorage(registry) },
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	copies := []*model.StorageCopy{pipeline.target}
	for i := range 2 {
		content := pipeline.upload
		if i == 1 {
			var err error
			content, err = runtime.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
				BucketID: pipeline.upload.BucketID, ContentSize: 128, RequestedCopies: 2,
				Checksum: testutil.StorageChecksum("second-pull-batch-member"),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{
				{StorageDataSetID: pipeline.source.StorageDataSetID, CopyIndex: 0, ProviderID: pipeline.source.ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress},
				{StorageDataSetID: pipeline.target.StorageDataSetID, CopyIndex: 1, ProviderID: pipeline.target.ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull},
			}); err != nil {
				t.Fatal(err)
			}
			rows, err := runtime.repos.Contents.ListCopies(t.Context(), content.ID)
			if err != nil {
				t.Fatal(err)
			}
			pieceCID := testPieceCID(t, "second-pull-batch-member")
			pieceID := testOnChainID(t, 9001)
			testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{
				StorageCopyID: rows[0].ID, ContentID: content.ID, CopyIndex: 0, PieceCID: pieceCID.String(),
				PieceID: &pieceID, RetrievalURL: "https://source.example/piece/" + pieceCID.String(),
			})
			copies = append(copies, &rows[1])
		}
		version := &model.ObjectVersion{
			VersionID: model.NewVersionID(), BucketID: content.BucketID, Key: fmt.Sprintf("pull-batch-%d", i),
			ContentID: &content.ID, Size: content.ContentSize, ETag: fmt.Sprint(i), ContentType: "application/octet-stream",
		}
		if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
			t.Fatal(err)
		}
	}
	provider := newRegistrationProvider(t, pipeline.targetSet.ProviderID.SDK(), pipeline.targetSet.DataSetID.SDK(), pipeline.targetClient, nonces)
	var pulls [][]byte
	provider.target.PullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
		if len(request.Pieces) != 1 {
			t.Errorf("Pull members = %d, want 1", len(request.Pieces))
		}
		copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copies[len(pulls)].ID)
		if err != nil || copyRow.CommitRequestID != nil {
			t.Errorf("copy before Pull = %#v, %v, want no Commit membership", copyRow, err)
		}
		pulls = append(pulls, append([]byte(nil), request.ExtraData...))
		return &storage.PullResult{}, nil
	}
	client.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return provider.target, nil
	}
	var tasks []*model.Task
	for _, copyRow := range copies {
		tasks = append(tasks, bindCopyTask(t, runtime, copyRow, model.TaskTypeStoragePull))
	}
	limitedRepos := *runtime.repos
	limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 2}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, &limitedRepos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runEngine(t, engine)
	for _, row := range tasks {
		waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
	}
	stopHandlerEngine(t, cancel, done)
	var requestID string
	for _, copyRow := range copies {
		ready, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
		if err != nil || ready.CommitRequestID == nil || ready.CommitSealed() || ready.Status != model.StorageCopyStatusPieceReady {
			t.Fatalf("copy after Pull = %#v, %v", ready, err)
		}
		if requestID != "" && requestID != *ready.CommitRequestID {
			t.Fatal("Pull copies did not join the same batch")
		}
		requestID = *ready.CommitRequestID
	}
	if len(pulls) != 2 || hex.EncodeToString(pulls[0]) == hex.EncodeToString(pulls[1]) {
		t.Fatalf("Pull authorizations = %d, want two different signatures", len(pulls))
	}
	cancel, done = runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForCommitted(t, runtime, copies)
	sends, extras := provider.sent()
	if len(sends) != 1 || len(sends[0]) != 2 {
		t.Fatalf("submissions = %v, want one two-piece batch", sends)
	}
	for _, extra := range pulls {
		nonce, err := storagecommit.ExtraDataNonce(extra)
		if err != nil {
			t.Fatal(err)
		}
		state, err := nonces.ClientNonce(t.Context(), nonce)
		if err != nil || state.Consumed {
			t.Fatalf("Pull nonce = %#v, %v, want unconsumed", state, err)
		}
	}
	nonce, err := storagecommit.ExtraDataNonce(extras[0])
	if err != nil {
		t.Fatal(err)
	}
	state, err := nonces.ClientNonce(t.Context(), nonce)
	if err != nil || !state.Consumed {
		t.Fatalf("Commit nonce = %#v, %v, want consumed", state, err)
	}
}

func TestPullInvalidEvidenceNeverReachesProvider(t *testing.T) {
	for _, scenario := range []string{"missing ledger", "foreign content", "foreign data set", "invalid authorization", "invalid CID", "resolved attempt", "reservation rollback"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int64
			runtime, pipeline, row := newPullErrorTask(t, nil, nil, &calls)
			var input storagepipeline.CopyGenerationInput
			if err := json.Unmarshal(row.Input, &input); err != nil {
				t.Fatal(err)
			}
			if scenario == "reservation rollback" {
				if _, err := runtime.db.Exec(`CREATE TRIGGER reject_pull BEFORE INSERT ON storage_pull_attempts BEGIN SELECT RAISE(ABORT, 'injected reservation failure'); END`); err != nil {
					t.Fatal(err)
				}
			} else {
				sources, err := runtime.repos.Contents.ListReadableCommittedCopies(t.Context(), pipeline.upload.ID)
				if err != nil || len(sources) != 1 {
					t.Fatalf("sources = %v, %v", sources, err)
				}
				source := sources[0]
				if err := runtime.repos.Contents.ReservePullRequest(t.Context(), repository.ReservePullRequestInput{
					CopyID: input.CopyID, Generation: input.Generation, TaskID: row.ID, AttemptID: "existing-pull",
					SourceProviderID: &source.ProviderID, SourceDataSetID: &source.DataSetID, SourcePieceID: &source.PieceID,
					SourcePieceCID: source.PieceCID, SourceRetrievalURL: source.RetrievalURL, ExtraDataHex: hex.EncodeToString(testutil.CommitExtraData(7)),
				}); err != nil {
					t.Fatal(err)
				}
				query := runtime.db.NewUpdate().Model((*storagepull.Attempt)(nil)).Where("attempt_id = ?", "existing-pull")
				switch scenario {
				case "missing ledger":
					_, err = runtime.db.NewDelete().Model((*storagepull.Attempt)(nil)).Where("attempt_id = ?", "existing-pull").Exec(t.Context())
				case "foreign content":
					_, err = query.Set("content_id = ?", pipeline.upload.ID+1).Exec(t.Context())
				case "foreign data set":
					_, err = query.Set("storage_data_set_id = ?", pipeline.target.StorageDataSetID+1).Exec(t.Context())
				case "invalid authorization":
					_, err = query.Set("extra_data_hex = ?", "invalid").Exec(t.Context())
				case "invalid CID":
					_, err = query.Set("source_piece_cid = ?", "invalid").Exec(t.Context())
				case "resolved attempt":
					_, err = query.Set("resolved_at = ?", time.Now()).Exec(t.Context())
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.db.NewUpdate().Model((*model.TaskPayload)(nil)).Set("checkpoint_json = ?", json.RawMessage(`{"attempt_id":"existing-pull"}`)).Where("task_id = ?", row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			limitedRepos := *runtime.repos
			limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: time.Second, Retention: time.Hour, ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1}, &limitedRepos, runtime.registry, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := runEngine(t, engine)
			defer stopHandlerEngine(t, cancel, done)
			result := waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusFailed || (row.Status == model.TaskStatusPending && row.RetryCount > 0)
			})
			if calls.Load() != 0 {
				t.Fatalf("provider calls = %d", calls.Load())
			}
			if scenario == "reservation rollback" {
				count, err := runtime.db.NewSelect().Model((*storagepull.Attempt)(nil)).Count(t.Context())
				if err != nil || count != 0 || len(result.Checkpoint) != 0 {
					t.Fatalf("rollback evidence = %d, %s, %v", count, result.Checkpoint, err)
				}
			} else if result.FailureReason == nil || *result.FailureReason != "invalid_pull_attempt" {
				t.Fatalf("invalid evidence result = %#v", result)
			}
		})
	}
}

func seedSealedReplacementBatch(t *testing.T, runtime handlerTestRuntime, members int, phase storagecommit.RequestStatus) (*model.StorageCopy, *testutil.MockStorageTarget, *storagecommit.Request) {
	t.Helper()
	copyRow, target, _ := seedReplacementPullTarget(t, runtime, true)
	contentBytes := bytes.Repeat([]byte{1}, 128)
	identity, err := piece.Calculate(bytes.NewReader(contentBytes))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("piece_cid = ?", identity.CIDv2.String()).Where("id = ?", copyRow.ContentID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	copies := []*model.StorageCopy{copyRow}
	if members == 2 {
		content, err := runtime.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{BucketID: copyRow.BucketID, ContentSize: 128, RequestedCopies: 2, Checksum: testutil.StorageChecksum("other-sealed-member")})
		if err != nil {
			t.Fatal(err)
		}
		version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: copyRow.BucketID, Key: "other-sealed-member", ContentID: &content.ID, Size: 128, ETag: "other-sealed-member", ContentType: "application/octet-stream"}
		if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
			t.Fatal(err)
		}
		if err := runtime.repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{StorageDataSetID: copyRow.StorageDataSetID, CopyIndex: copyRow.CopyIndex, ProviderID: copyRow.ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull}}); err != nil {
			t.Fatal(err)
		}
		other, err := runtime.repos.Contents.GetUploadCopyForDataSet(t.Context(), content.ID, copyRow.StorageDataSetID)
		if err != nil {
			t.Fatal(err)
		}
		copies = append(copies, other)
	}
	sealMembers := make([]repository.SealMember, len(copies))
	for i, row := range copies {
		pieceCID := identity.CIDv2
		if i != 0 {
			pieceCID = testPieceCID(t, "other-sealed-member")
		}
		if err := runtime.repos.Contents.MarkUploadCopyPieceReady(t.Context(), repository.MarkUploadCopyPieceReadyInput{StorageCopyID: row.ID, ContentID: row.ContentID, CopyIndex: row.CopyIndex, PieceCID: pieceCID.String(), RetrievalURL: target.PieceURL(pieceCID)}); err != nil {
			t.Fatal(err)
		}
		sealMembers[i] = repository.SealMember{CopyID: row.ID, ContentID: row.ContentID, PieceCID: pieceCID.String()}
	}
	fixture := registrationFixture{runtime: runtime, dataSet: &model.StorageDataSet{ID: copyRow.StorageDataSetID}, copies: copies}
	requestID, taskID := fixture.collect(t)
	if _, err := runtime.repos.Contents.SealCommitRequest(t.Context(), repository.SealCommitRequestInput{RequestID: requestID, TaskID: taskID, Members: sealMembers, ExtraDataHex: hex.EncodeToString(testutil.CommitExtraData(90))}); err != nil {
		t.Fatal(err)
	}
	if phase == storagecommit.RequestStatusSubmitted {
		if err := runtime.repos.Contents.BeginCommitSubmission(t.Context(), repository.BeginCommitSubmissionInput{RequestID: requestID, TaskID: taskID}); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.repos.Contents.ReturnCommitMembersToTransfer(t.Context(), requestID, taskID, []int64{copyRow.ID}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(time.Hour)).Where("id = ?", taskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	request, err := runtime.repos.Contents.GetCommitRequest(t.Context(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	copyRow, err = runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil {
		t.Fatal(err)
	}
	return copyRow, target, request
}

func runOneStorageTask(t *testing.T, runtime handlerTestRuntime, row *model.Task, status model.TaskStatus) *model.Task {
	t.Helper()
	repos := *runtime.repos
	repos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: time.Second, Retention: time.Hour, ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1}, &repos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)
	return waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool { return row.Status == status })
}

func TestSealedReplacementTransferKeepsOriginalBatch(t *testing.T) {
	for _, members := range []int{1, 2} {
		for _, phase := range []storagecommit.RequestStatus{storagecommit.RequestStatusReady, storagecommit.RequestStatusSubmitted} {
			for _, cacheRestore := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d members/%s/cache=%v", members, phase, cacheRestore), func(t *testing.T) {
					client := &testutil.MockStorageClient{}
					local := bytes.Repeat([]byte{1}, 128)
					cacheStore := &testutil.MockCache{
						ExistsFunc: func(context.Context, string, string) bool { return true },
						GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
							return io.NopCloser(bytes.NewReader(local)), &cache.ObjectInfo{Size: int64(len(local))}, nil
						},
					}
					runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{cache: cacheStore, storage: client, policy: cache.EvictionPolicyNone, register: func(h *worker.TaskHandlers, r *taskengine.Registry) error { return h.RegisterStorage(r) }})
					copyRow, target, original := seedSealedReplacementBatch(t, runtime, members, phase)
					target.PullFunc = func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
						if cacheRestore {
							return nil, &pdp.HTTPError{StatusCode: 400}
						}
						return &storage.PullResult{}, nil
					}
					target.StoreFunc = func(_ context.Context, reader io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
						data, err := io.ReadAll(reader)
						if err != nil {
							return nil, err
						}
						return &storage.StoreResult{PieceCID: options.PieceCID, Size: int64(len(data))}, nil
					}
					client.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
						return target, nil
					}
					pullTask := bindCopyTask(t, runtime, copyRow, model.TaskTypeStoragePull)
					runOneStorageTask(t, runtime, pullTask, model.TaskStatusCompleted)
					restored, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
					if err != nil || restored.CommitRequestID == nil || *restored.CommitRequestID != original.RequestID || restored.CommitPosition == nil || *restored.CommitPosition != 0 {
						t.Fatalf("recovery copy = %#v, %v", restored, err)
					}
					var history []storagepull.Attempt
					if err := runtime.db.NewSelect().Model(&history).Where("content_id = ? AND storage_data_set_id = ?", copyRow.ContentID, copyRow.StorageDataSetID).Scan(t.Context()); err != nil {
						t.Fatal(err)
					}
					if len(history) != 1 || history[0].ResolvedAt == nil || history[0].ExtraDataHex == *original.ExtraDataHex {
						t.Fatalf("independent Pull authorization = %#v", history)
					}
					attempt, err := runtime.repos.Contents.GetUnresolvedPullAttempt(t.Context(), copyRow.ContentID, copyRow.StorageDataSetID)
					if !errors.Is(err, repository.ErrNotFound) {
						t.Fatalf("unresolved attempt = %#v, %v", attempt, err)
					}
					if cacheRestore {
						if restored.TransferMethod != model.StorageCopyTransferMethodCacheRestore || restored.ActiveTaskID == nil || history[0].Status != storagepull.AttemptStatusAbandoned {
							t.Fatalf("cache recovery = %#v", restored)
						}
						storeTask, err := runtime.repos.Tasks.GetByID(t.Context(), *restored.ActiveTaskID)
						if err != nil {
							t.Fatal(err)
						}
						runOneStorageTask(t, runtime, storeTask, model.TaskStatusCompleted)
					} else if restored.TransferMethod != model.StorageCopyTransferMethodPeerPull || history[0].Status != storagepull.AttemptStatusAttempted {
						t.Fatalf("Pull recovery = %#v", restored)
					}
					restored, err = runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
					if err != nil || restored.Status != model.StorageCopyStatusCommitting || restored.ActiveTaskID != nil || restored.WorkTaskID() == nil || *restored.WorkTaskID() != *original.TaskID {
						t.Fatalf("restored member = %#v, %v", restored, err)
					}
					current, err := runtime.repos.Contents.GetCommitRequest(t.Context(), original.RequestID)
					if err != nil || current.Status != original.Status || current.PieceCount != members || current.ExtraDataHex == nil || *current.ExtraDataHex != *original.ExtraDataHex {
						t.Fatalf("Commit after cache restore = %#v, %v", current, err)
					}
					commitTask, err := runtime.repos.Tasks.GetByID(t.Context(), *original.TaskID)
					if err != nil || commitTask.AvailableAt.After(time.Now()) {
						t.Fatalf("Commit wake = %#v, %v", commitTask, err)
					}
				})
			}
		}
	}
}

// gatedCommitNonces holds Commit's decision while other schedulers inspect its
// released member, so the test exercises the ownership race deterministically.
type gatedCommitNonces struct {
	*testutil.MockCommitNonces
	gate <-chan struct{}
}

func (n gatedCommitNonces) ClientNonce(ctx context.Context, nonce sdktypes.BigInt) (synapse.ClientNonceState, error) {
	select {
	case <-ctx.Done():
		return synapse.ClientNonceState{}, ctx.Err()
	case <-n.gate:
		return n.MockCommitNonces.ClientNonce(ctx, nonce)
	}
}

func TestSealedMemberWaitsForCommitAcrossConcurrentSchedulers(t *testing.T) {
	for _, phase := range []storagecommit.RequestStatus{storagecommit.RequestStatusReady, storagecommit.RequestStatusSubmitted} {
		t.Run(string(phase), func(t *testing.T) {
			gate := make(chan struct{})
			client := &testutil.MockStorageClient{}
			nonces := gatedCommitNonces{MockCommitNonces: &testutil.MockCommitNonces{}, gate: gate}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: client, commitNonces: nonces, policy: cache.EvictionPolicyNone,
				cache: &testutil.MockCache{ExistsFunc: func(context.Context, string, string) bool { return false }},
				register: func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
					if err := h.RegisterStorage(registry); err != nil {
						return err
					}
					return h.RegisterReplacement(registry)
				},
			})
			copyRow, target, original := seedSealedReplacementBatch(t, runtime, 2, phase)
			target.PullFunc = func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
				return nil, &pdp.HTTPError{StatusCode: 400}
			}
			client.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				return target, nil
			}
			pullTask := bindCopyTask(t, runtime, copyRow, model.TaskTypeStoragePull)
			runOneStorageTask(t, runtime, pullTask, model.TaskStatusFailed)
			released, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil || released.ActiveTaskID != nil || released.WorkTaskID() == nil || *released.WorkTaskID() != *original.TaskID {
				t.Fatalf("released member owner = %#v, %v", released, err)
			}
			ensure, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(copyRow.StorageDataSetID), Input: storagepipeline.DataSetInput{DataSetID: copyRow.StorageDataSetID}}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
				return repos.Contents.BindDataSetEnsureTask(ctx, copyRow.StorageDataSetID, row.ID)
			})
			if err != nil {
				t.Fatal(err)
			}
			replacement := new(storagereplacement.Replacement)
			if err := runtime.db.NewSelect().Model(replacement).Where("target_data_set_id = ?", copyRow.StorageDataSetID).Scan(t.Context()); err != nil {
				t.Fatal(err)
			}
			coordinator, _, err := runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
				Type: model.TaskTypeProviderReplacementCoordinate, IdempotencyKey: storagereplacement.CoordinateTaskKey(replacement.ID, replacement.TaskGeneration),
				Input: storagereplacement.CoordinateInput{ReplacementID: replacement.ID, Generation: replacement.TaskGeneration}, SubjectType: "storage_replacement", SubjectKey: fmt.Sprint(replacement.ID),
			}, func(ctx context.Context, repos *repository.Repositories, row *model.Task, _ bool) error {
				return repos.Replacements.BindTask(ctx, replacement.ID, replacement.TaskGeneration, row.ID)
			})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 3, PollInterval: 5 * time.Millisecond, LeaseDuration: time.Second, Retention: time.Hour, ProviderMutationConcurrency: 2, DestructiveMutationConcurrency: 1}, runtime.repos, runtime.registry, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := runEngine(t, engine)
			closed := false
			defer func() {
				if !closed {
					close(gate)
				}
				stopHandlerEngine(t, cancel, done)
			}()
			waitForTask(t, runtime.repos, ensure.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
			waitForTask(t, runtime.repos, coordinator.ID, func(row *model.Task) bool {
				return row.Status == model.TaskStatusPending && row.WaitReason != nil && *row.WaitReason == "copy_work"
			})
			released, err = runtime.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil || released.ActiveTaskID != nil || released.Status != model.StorageCopyStatusPending || released.CommitRequestID == nil || *released.CommitRequestID != original.RequestID {
				t.Fatalf("scheduler stole sealed member = %#v, %v", released, err)
			}
			close(gate)
			closed = true
			if phase == storagecommit.RequestStatusReady {
				waitForCommitTask(t, runtime, *original.TaskID, func(*model.Task) bool {
					request, err := runtime.repos.Contents.GetCommitRequest(t.Context(), original.RequestID)
					return err == nil && request.Status == storagecommit.RequestStatusAbandoned
				})
			} else {
				retried := waitForCopy(t, runtime, copyRow.ID, func(row *model.StorageCopy) bool { return row.ActiveTaskID != nil })
				next, err := runtime.repos.Tasks.GetByID(t.Context(), *retried.ActiveTaskID)
				if err != nil || next.Type != model.TaskTypeStorageTransferPlan || !next.AvailableAt.After(time.Now().Add(time.Minute)) || retried.CommitRequestID == nil || *retried.CommitRequestID != original.RequestID {
					t.Fatalf("Commit retry = %#v, %#v, %v", retried, next, err)
				}
			}
		})
	}
}

func TestPullLateCompletionResolvesAttemptAfterCommitConfirmed(t *testing.T) {
	var calls atomic.Int64
	runtime, pipeline, row := newPullErrorTask(t, nil, nil, &calls)
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(row.Input, &input); err != nil {
		t.Fatal(err)
	}
	sources, err := runtime.repos.Contents.ListReadableCommittedCopies(t.Context(), pipeline.upload.ID)
	if err != nil {
		t.Fatal(err)
	}
	source := sources[0]
	if err := runtime.repos.Contents.ReservePullRequest(t.Context(), repository.ReservePullRequestInput{
		CopyID: input.CopyID, Generation: input.Generation, TaskID: row.ID, AttemptID: "late-pull",
		SourceProviderID: &source.ProviderID, SourceDataSetID: &source.DataSetID, SourcePieceID: &source.PieceID,
		SourcePieceCID: source.PieceCID, SourceRetrievalURL: source.RetrievalURL, ExtraDataHex: hex.EncodeToString(testutil.CommitExtraData(7)),
	}); err != nil {
		t.Fatal(err)
	}
	pieceID := testOnChainID(t, 9002)
	if err := runtime.repos.Contents.MarkUploadCopyPieceReady(t.Context(), repository.MarkUploadCopyPieceReadyInput{StorageCopyID: pipeline.target.ID, ContentID: pipeline.upload.ID, CopyIndex: pipeline.target.CopyIndex, PieceCID: pipeline.pieceCID.String()}); err != nil {
		t.Fatal(err)
	}
	fixture := registrationFixture{runtime: runtime, dataSet: pipeline.targetSet, copies: []*model.StorageCopy{pipeline.target}}
	requestID, taskID := fixture.collect(t)
	if _, err := runtime.repos.Contents.SealCommitRequest(t.Context(), repository.SealCommitRequestInput{RequestID: requestID, TaskID: taskID, Members: []repository.SealMember{{CopyID: pipeline.target.ID, ContentID: pipeline.upload.ID, PieceCID: pipeline.pieceCID.String()}}, ExtraDataHex: hex.EncodeToString(testutil.CommitExtraData(90))}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.repos.Contents.ConfirmCommitRequest(t.Context(), repository.ConfirmCommitRequestInput{RequestID: requestID, TaskID: taskID, FirstPieceID: pieceID, RetrievalURLs: []string{"https://target.example/piece/" + pipeline.pieceCID.String()}}); err != nil {
		t.Fatal(err)
	}
	// No checkpoint remains, but the ledger still owns the unfinished request.
	runOneStorageTask(t, runtime, row, model.TaskStatusCompleted)
	settled, err := runtime.repos.Contents.GetPullAttempt(t.Context(), "late-pull", pipeline.upload.ID, pipeline.target.StorageDataSetID)
	if err != nil || settled.ResolvedAt == nil {
		t.Fatalf("late attempt = %#v, %v", settled, err)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusCommitted || copyRow.PieceID == nil || !copyRow.PieceID.Equal(pieceID) || copyRow.ActiveTaskID != nil || calls.Load() != 0 {
		t.Fatalf("late copy = %#v, calls=%d, %v", copyRow, calls.Load(), err)
	}
}
