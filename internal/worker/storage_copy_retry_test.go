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
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestCopyRetryPullReachesCommittedWithFreshAuthorization(t *testing.T) {
	client := &testutil.MockStorageClient{}
	nonces := &testutil.MockCommitNonces{}
	var submissions atomic.Int64
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: client, policy: cache.EvictionPolicyNone, commitNonces: nonces, commitMaxPieces: 1,
		parkedPieces: parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
			if submissions.Load() > 1 {
				return synapse.ParkedPieceReady, nil
			}
			return synapse.ParkedPieceMissing, nil
		}),
		register: func(h *worker.TaskHandlers, r *taskengine.Registry) error { return h.RegisterStorage(r) },
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "retry.bin", ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize, ETag: "retry", ContentType: "application/octet-stream"}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	provider := newRegistrationProvider(t, pipeline.target.ProviderID.SDK(), pipeline.targetSet.DataSetID.SDK(), pipeline.targetClient, nonces)
	provider.target.SubmitPullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
		if submissions.Add(1) == 1 {
			return pullStatusResult(request, storage.PullStatusFailed), nil
		}
		return pullStatusResult(request, storage.PullStatusComplete), nil
	}
	client.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return provider.target, nil
	}
	old := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStoragePull)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, old.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusFailed })
	if failed.LastError == nil || !strings.Contains(*failed.LastError, pipeline.target.ProviderID.String()) || !strings.Contains(*failed.LastError, pipeline.source.ProviderID.String()) {
		t.Fatalf("failure=%+v", failed)
	}
	// A second readable provider gives the new attempt a different source.
	bucket, err := runtime.repos.Buckets.GetByID(t.Context(), pipeline.upload.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	three := 3
	if _, err := runtime.repos.Buckets.UpdateCopyPolicy(t.Context(), repository.UpdateBucketCopyPolicyInput{Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: &three}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("requested_copies = 3").Where("id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	other, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: pipeline.upload.BucketID, ProviderID: testOnChainID(t, 99001), CopyIndex: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: other.ID, DataSetID: testOnChainID(t, 99002)}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(t.Context(), pipeline.upload.ID, []repository.UploadCopyBindingInput{{StorageDataSetID: other.ID, CopyIndex: 2, ProviderID: other.ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull}}); err != nil {
		t.Fatal(err)
	}
	otherCopy, err := runtime.repos.Contents.GetUploadCopy(t.Context(), pipeline.upload.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	otherPiece := testOnChainID(t, 0)
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, testutil.CommitCopyInput{StorageCopyID: otherCopy.ID, ContentID: pipeline.upload.ID, CopyIndex: 2, PieceCID: pipeline.pieceCID.String(), PieceID: &otherPiece, RetrievalURL: "https://other.example/piece"})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, stopServer := context.WithCancel(t.Context())
	serverDone := make(chan error, 1)
	server := admin.New("", runtime.db, runtime.cache, runtime.gate, runtime.tracker, 0, runtime.repos, nil, nil, 2, slog.Default()).WithTaskService(runtime.service)
	go func() { serverDone <- server.Serve(serverCtx, listener) }()
	defer func() {
		stopServer()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("admin server shutdown: %v", err)
			}
		case <-time.After(handlerTestLeaseDuration):
			t.Error("admin server did not stop")
		}
	}()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fmt.Sprintf("http://%s/api/v1/storage-copies/%d/retry", listener.Addr(), pipeline.target.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Timeout: handlerTestLeaseDuration}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("retry status=%d body=%s", response.StatusCode, body)
	}
	var accepted struct {
		CopyID int64 `json:"copy_id"`
		TaskID int64 `json:"task_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.CopyID != pipeline.target.ID || accepted.TaskID == 0 || accepted.TaskID == old.ID {
		t.Fatalf("recovery work=%+v", accepted)
	}
	waitForCommitted(t, runtime, []*model.StorageCopy{pipeline.target})
	waitForTask(t, runtime.repos, accepted.TaskID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
	var attempts []storagepull.Attempt
	if err := runtime.db.NewSelect().Model(&attempts).Where("content_id = ?", pipeline.upload.ID).OrderExpr("attempted_at ASC").Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0].AttemptID == attempts[1].AttemptID || attempts[0].ExtraDataHex == attempts[1].ExtraDataHex || attempts[0].Status != storagepull.AttemptStatusAbandoned || attempts[1].ResolvedAt == nil {
		t.Fatalf("attempts=%+v", attempts)
	}
	if attempts[1].SourceProviderID.Equal(attempts[0].SourceProviderID) || !attempts[1].SourceProviderID.Equal(other.ProviderID) {
		t.Fatalf("retry did not rotate source: %+v", attempts)
	}
	history, err := runtime.repos.Tasks.GetByID(t.Context(), old.ID)
	if err != nil || history.Status != model.TaskStatusFailed || history.AcknowledgedAt == nil || !bytes.Equal(history.Checkpoint, failed.Checkpoint) {
		t.Fatalf("history=%+v err=%v", history, err)
	}
}

type failedPullCacheDependency struct {
	repository.StorageContentRepository
}

func (failedPullCacheDependency) IsPendingReplacementCopy(context.Context, int64) (bool, error) {
	return false, errors.New("replacement lookup unavailable")
}

func TestConfirmedPullDependencyFailureRetainsAttempt(t *testing.T) {
	limit := 0
	runtime, pipeline, row := newPullTask(t, func(_ context.Context, r storage.PullRequest) (*storage.PullResult, error) {
		return pullStatusResult(r, storage.PullStatusFailed), nil
	}, nil, &limit)
	runtime.repos.Contents = failedPullCacheDependency{runtime.repos.Contents}
	result := runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
	attempt, err := runtime.repos.Contents.GetUnresolvedPullAttempt(t.Context(), pipeline.upload.ID, pipeline.target.StorageDataSetID)
	if err != nil || attempt.ResolvedAt != nil || result.FailureReason == nil || *result.FailureReason != storagepull.FailureOutcomeUnknown {
		t.Fatalf("failure=%+v attempt=%+v err=%v", result, attempt, err)
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != row.ID {
		t.Fatalf("copy=%+v err=%v", copyRow, err)
	}
}

func TestCacheRestoreMissingCacheOwnership(t *testing.T) {
	for _, tt := range []struct {
		name     string
		taskType model.TaskType
		sent     bool
	}{
		{"plan", model.TaskTypeStorageTransferPlan, false}, {"store not sent", model.TaskTypeStorageStore, false}, {"store sent", model.TaskTypeStorageStore, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runtime, pipeline, old := newPullTask(t, nil, nil, nil)
			version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "missing-cache.bin", ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize, ETag: "missing-cache", ContentType: "application/octet-stream"}
			if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
				t.Fatal(err)
			}
			// Replace the unused initial pull with the task under test.
			initial := pipeline.target
			current, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), initial.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.Contents.CompleteCopyTask(t.Context(), current.ID, current.WorkGeneration, *current.ActiveTaskID); err != nil {
				t.Fatal(err)
			}
			runOneStorageTask(t, runtime, old, model.TaskStatusCancelled)
			query := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).Set("transfer_method = ?", model.StorageCopyTransferMethodCacheRestore).Where("id = ?", current.ID)
			if tt.sent {
				query = query.Set("ingress_store_attempt = 1")
			}
			if _, err := query.Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			runtime.cache.(*testutil.MockCache).GetFunc = func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
				return nil, nil, os.ErrNotExist
			}
			row := bindCopyTask(t, runtime, current, tt.taskType)
			runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), current.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tt.sent {
				if copyRow.ActiveTaskID == nil || copyRow.Status != model.StorageCopyStatusPending {
					t.Fatalf("sent copy=%+v", copyRow)
				}
			} else if copyRow.ActiveTaskID != nil || copyRow.Status != model.StorageCopyStatusFailed {
				t.Fatalf("unattempted copy=%+v", copyRow)
			}
		})
	}
}

func TestConfirmedPullCacheFallbackReachesCommitted(t *testing.T) {
	client := &testutil.MockStorageClient{}
	nonces := &testutil.MockCommitNonces{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: client, policy: cache.EvictionPolicyNone, commitNonces: nonces, commitMaxPieces: 1,
		register: func(h *worker.TaskHandlers, r *taskengine.Registry) error { return h.RegisterStorage(r) },
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	payload := bytes.Repeat([]byte("r"), int(pipeline.upload.ContentSize))
	identity, err := piece.Calculate(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).Set("piece_cid = ?", identity.CIDv2.String()).Where("id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID, Key: "fallback.bin", ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize, ETag: "fallback", ContentType: "application/octet-stream"}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Objects.SetVersionCachePresence(t.Context(), version.VersionID, true); err != nil {
		t.Fatal(err)
	}
	runtime.cache.(*testutil.MockCache).ExistsFunc = func(context.Context, string, string) bool { return true }
	runtime.cache.(*testutil.MockCache).GetFunc = func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(bytes.NewReader(payload)), &cache.ObjectInfo{Size: int64(len(payload))}, nil
	}
	provider := newRegistrationProvider(t, pipeline.target.ProviderID.SDK(), pipeline.targetSet.DataSetID.SDK(), pipeline.targetClient, nonces)
	var pulls, stores atomic.Int64
	provider.target.SubmitPullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
		pulls.Add(1)
		return pullStatusResult(request, storage.PullStatusFailed), nil
	}
	provider.target.StoreFunc = func(_ context.Context, reader io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
		data, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(data, payload) || !options.PieceCID.Equals(identity.CIDv2) {
			return nil, errors.New("fallback stored different content")
		}
		stores.Add(1)
		return &storage.StoreResult{PieceCID: options.PieceCID, Size: int64(len(data))}, nil
	}
	client.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return provider.target, nil
	}
	row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStoragePull)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForCommitted(t, runtime, []*model.StorageCopy{pipeline.target})
	waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.TransferMethod != model.StorageCopyTransferMethodCacheRestore || pulls.Load() != 1 || stores.Load() != 1 {
		t.Fatalf("fallback copy=%+v pulls=%d stores=%d err=%v", copyRow, pulls.Load(), stores.Load(), err)
	}
	var attempt storagepull.Attempt
	if err := runtime.db.NewSelect().Model(&attempt).Where("content_id = ? AND storage_data_set_id = ?", pipeline.upload.ID, pipeline.target.StorageDataSetID).Scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if attempt.Status != storagepull.AttemptStatusAbandoned || attempt.ResolvedAt == nil || attempt.LastError == nil || !strings.Contains(*attempt.LastError, pipeline.source.ProviderID.String()) {
		t.Fatalf("fallback attempt=%+v", attempt)
	}
}

func TestConfirmedPullCacheFallback(t *testing.T) {
	for _, tt := range []struct {
		name          string
		evict, cancel bool
	}{{name: "available"}, {name: "eviction pending", evict: true}, {name: "cancelled", cancel: true}} {
		t.Run(tt.name, func(t *testing.T) {
			runtime, pipeline, row := newPullTask(t, func(_ context.Context, r storage.PullRequest) (*storage.PullResult, error) {
				return pullStatusResult(r, storage.PullStatusFailed), nil
			}, nil, nil)
			bucket, err := runtime.repos.Buckets.GetByID(t.Context(), pipeline.upload.BucketID)
			if err != nil {
				t.Fatal(err)
			}
			runtime.cache.(*testutil.MockCache).ExistsFunc = func(context.Context, string, string) bool { return true }
			version := &model.ObjectVersion{VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "cache.bin", ContentID: &pipeline.upload.ID, Size: 128, ETag: "cache", ContentType: "application/octet-stream"}
			if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
				t.Fatal(err)
			}
			if err := runtime.repos.Objects.SetVersionCachePresence(t.Context(), version.VersionID, true); err != nil {
				t.Fatal(err)
			}
			if tt.evict {
				if _, err := runtime.db.NewUpdate().Table("object_cache").Set("cache_active_task_id = ?", row.ID).Where("content_id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			// A reserved request must keep its recovery fence during cancellation.
			if tt.cancel {
				sources, err := runtime.repos.Contents.ListReadableCommittedCopies(t.Context(), pipeline.upload.ID)
				if err != nil {
					t.Fatal(err)
				}
				source := sources[0]
				if err := runtime.repos.Contents.ReservePullRequest(t.Context(), repository.ReservePullRequestInput{CopyID: pipeline.target.ID, Generation: 1, TaskID: row.ID, AttemptID: "cancelled-pull", SourceProviderID: &source.ProviderID, SourceDataSetID: &source.DataSetID, SourcePieceID: &source.PieceID, SourcePieceCID: source.PieceCID, SourceRetrievalURL: source.RetrievalURL, ExtraDataHex: hex.EncodeToString(testutil.CommitExtraData(7))}); err != nil {
					t.Fatal(err)
				}
				if err := runtime.repos.Tasks.RequestCancellation(t.Context(), row.ID, "cancel"); err != nil {
					t.Fatal(err)
				}
			}
			want := model.TaskStatusCompleted
			if tt.evict || tt.cancel {
				want = model.TaskStatusFailed
			}
			result := runOneStorageTask(t, runtime, row, want)
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil {
				t.Fatal(err)
			}
			if want == model.TaskStatusCompleted {
				if copyRow.TransferMethod != model.StorageCopyTransferMethodCacheRestore || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID == row.ID {
					t.Fatalf("fallback=%+v", copyRow)
				}
				successor, err := runtime.repos.Tasks.GetByID(t.Context(), *copyRow.ActiveTaskID)
				if err != nil || successor.Type != model.TaskTypeStorageStore {
					t.Fatalf("successor=%+v err=%v", successor, err)
				}
			} else if tt.cancel {
				if result.FailureReason == nil || *result.FailureReason != storagepull.FailureCancelOutcomeUnknown || copyRow.ActiveTaskID == nil || copyRow.TransferMethod != model.StorageCopyTransferMethodPeerPull {
					t.Fatalf("cancelled=%+v copy=%+v", result, copyRow)
				}
			} else if result.FailureReason == nil || *result.FailureReason != "pull_failed" || copyRow.ActiveTaskID != nil {
				t.Fatalf("failed=%+v copy=%+v", result, copyRow)
			}
		})
	}
}
