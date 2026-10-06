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

	"github.com/ipfs/go-cid"
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

func pullStatusResult(request storage.PullRequest, status storage.PullStatus) *storage.PullResult {
	result := &storage.PullResult{Status: status}
	for _, pieceCID := range request.Pieces {
		result.Pieces = append(result.Pieces, storage.PullPieceResult{PieceCID: pieceCID, Status: status})
	}
	return result
}

func TestPullSubmissionOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name        string
		status      storage.PullStatus
		overall     storage.PullStatus
		state       synapse.ParkedPieceState
		nilResult   bool
		omitPiece   bool
		wrongPiece  bool
		statusError bool
		wantStatus  model.TaskStatus
		wantFailure string
	}{
		{name: "pending", status: storage.PullStatusPending, wantStatus: model.TaskStatusPending},
		{name: "in progress", status: storage.PullStatusInProgress, wantStatus: model.TaskStatusPending},
		{name: "retrying", status: storage.PullStatusRetrying, wantStatus: model.TaskStatusPending},
		{name: "complete but processing", status: storage.PullStatusComplete, state: synapse.ParkedPieceProcessing, wantStatus: model.TaskStatusPending},
		{name: "complete after piece GC", status: storage.PullStatusComplete, state: synapse.ParkedPieceMissing, wantStatus: model.TaskStatusFailed, wantFailure: "pull_failed"},
		{name: "overall complete with requested piece failed", status: storage.PullStatusFailed, overall: storage.PullStatusComplete, wantStatus: model.TaskStatusFailed, wantFailure: "pull_failed"},
		{name: "nil result", nilResult: true, wantStatus: model.TaskStatusFailed, wantFailure: storagepull.FailureOutcomeUnknown},
		{name: "terminal without requested piece", status: storage.PullStatusComplete, omitPiece: true, wantStatus: model.TaskStatusFailed, wantFailure: storagepull.FailureOutcomeUnknown},
		{name: "wrong piece", status: storage.PullStatusComplete, wrongPiece: true, wantStatus: model.TaskStatusFailed, wantFailure: storagepull.FailureOutcomeUnknown},
		{name: "conflicting terminal statuses", status: storage.PullStatusComplete, overall: storage.PullStatusFailed, wantStatus: model.TaskStatusFailed, wantFailure: storagepull.FailureOutcomeUnknown},
		{name: "piece query failed", status: storage.PullStatusComplete, statusError: true, wantStatus: model.TaskStatusFailed, wantFailure: storagepull.FailureOutcomeUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			limit := 0
			var queries atomic.Int64
			var submissionContext context.Context
			otherCID := testPieceCID(t, "other pull piece")
			runtime, pipeline, row := newPullTask(t, func(ctx context.Context, request storage.PullRequest) (*storage.PullResult, error) {
				submissionContext = ctx
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 30*time.Second {
					t.Errorf("pull submission has no 30-second bound")
				}
				if tt.nilResult {
					return nil, nil
				}
				result := pullStatusResult(request, tt.status)
				if tt.overall != "" {
					result.Status = tt.overall
				}
				if tt.omitPiece {
					result.Pieces = nil
				}
				if tt.wrongPiece {
					result.Pieces[0].PieceCID = otherCID
				}
				return result, nil
			}, parkedPieceCheckerFunc(func(ctx context.Context, _ string, _ cid.Cid) (synapse.ParkedPieceState, error) {
				queries.Add(1)
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 4*time.Second || ctx.Err() != nil || submissionContext.Err() == nil {
					t.Errorf("piece query did not use a fresh 4-second context")
				}
				if tt.statusError {
					return "", errors.New("piece query disconnected")
				}
				return tt.state, nil
			}), &limit)
			result := runOneStorageTask(t, runtime, row, tt.wantStatus)
			if tt.wantFailure != "" && (result.FailureReason == nil || *result.FailureReason != tt.wantFailure) {
				t.Fatalf("failure = %#v, want %s", result, tt.wantFailure)
			}
			if tt.wantStatus == model.TaskStatusPending && (result.ResumeMode != model.TaskResumeModeRecover || result.RetryCount != 0) {
				t.Fatalf("pending pull = %#v, want recovery without retry consumption", result)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil {
				t.Fatal(err)
			}
			protected := tt.wantStatus == model.TaskStatusPending || tt.wantFailure == storagepull.FailureOutcomeUnknown
			attempt, err := runtime.repos.Contents.GetUnresolvedPullAttempt(t.Context(), pipeline.target.ContentID, pipeline.target.StorageDataSetID)
			if protected {
				if err != nil || attempt.ResolvedAt != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != row.ID {
					t.Fatalf("protected outcome = %#v, %#v, %v", copyRow, attempt, err)
				}
			} else if !errors.Is(err, repository.ErrNotFound) || copyRow.Status != model.StorageCopyStatusFailed || copyRow.ActiveTaskID != nil {
				t.Fatalf("confirmed failure = %#v, %#v, %v", copyRow, attempt, err)
			}
			wantQueries := int64(0)
			if tt.state != "" || tt.statusError {
				wantQueries = 1
			}
			if queries.Load() != wantQueries {
				t.Fatalf("piece queries = %d, want %d", queries.Load(), wantQueries)
			}
			if count, err := runtime.db.NewSelect().Model((*storagecommit.Request)(nil)).Where("storage_data_set_id = ?", pipeline.targetSet.ID).Count(t.Context()); err != nil || count != 0 {
				t.Fatalf("Commit requests before readiness = %d, %v", count, err)
			}
		})
	}
}

func TestPullQueueFullRetainsRequestAndRetryAfter(t *testing.T) {
	for _, retryAfter := range []time.Duration{2 * time.Minute, 0, -time.Second} {
		t.Run(retryAfter.String(), func(t *testing.T) {
			limit := 0
			var calls atomic.Int64
			var firstExtra, firstSource, firstCID string
			runtime, pipeline, row := newPullTask(t, func(ctx context.Context, request storage.PullRequest) (*storage.PullResult, error) {
				if calls.Add(1) == 1 {
					firstExtra, firstSource, firstCID = hex.EncodeToString(request.ExtraData), request.From(request.Pieces[0]), request.Pieces[0].String()
				} else if firstExtra != hex.EncodeToString(request.ExtraData) || firstSource != request.From(request.Pieces[0]) || firstCID != request.Pieces[0].String() {
					t.Error("queue-full replay changed the request")
				}
				err := fmt.Errorf("submit pull: %w", errors.Join(pdp.ErrPullQueueFull, &pdp.HTTPError{StatusCode: 429, RetryAfter: retryAfter}, io.ErrUnexpectedEOF))
				return nil, synapse.NormalizeProviderOperationError(ctx, err)
			}, nil, &limit)
			before := time.Now()
			waiting := runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			delay := retryAfter
			if delay <= 0 {
				delay = time.Minute
			}
			if waiting.WaitReason == nil || *waiting.WaitReason != storagepull.WaitQueueFull || waiting.RetryCount != 0 || waiting.ResumeMode != model.TaskResumeModeRecover ||
				waiting.AvailableAt.Before(before.Add(delay)) || waiting.AvailableAt.After(time.Now().Add(delay)) {
				t.Fatalf("queue-full wait = %#v", waiting)
			}
			attempt, err := runtime.repos.Contents.GetUnresolvedPullAttempt(t.Context(), pipeline.target.ContentID, pipeline.target.StorageDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			if woken, err := runtime.repos.Tasks.WakePending(t.Context(), []int64{row.ID}); err != nil || woken != 0 {
				t.Fatalf("early queue-full wake = %d, %v", woken, err)
			}
			for step := range 2 {
				if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				waiting = runOneStorageTask(t, runtime, waiting, model.TaskStatusPending)
				if step == 0 && (waiting.ResumeMode != model.TaskResumeModeExecute || calls.Load() != 1) {
					t.Fatalf("queue-full recovery sent a request: %#v", waiting)
				}
			}
			replayed, err := runtime.repos.Contents.GetUnresolvedPullAttempt(t.Context(), pipeline.target.ContentID, pipeline.target.StorageDataSetID)
			if err != nil || calls.Load() != 2 || replayed.AttemptID != attempt.AttemptID || !replayed.AttemptedAt.Equal(attempt.AttemptedAt) || waiting.RetryCount != 0 {
				t.Fatalf("queue-full replay = %#v, %#v, %v; calls=%d", attempt, replayed, err, calls.Load())
			}
		})
	}
}

func TestPullPendingMissingPieceStillObservesProviderFailure(t *testing.T) {
	limit := 0
	var calls, queries atomic.Int64
	var firstExtra, firstSource, firstCID string
	runtime, pipeline, row := newPullTask(t, func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
		call := calls.Add(1)
		if call == 1 {
			firstExtra, firstSource, firstCID = hex.EncodeToString(request.ExtraData), request.From(request.Pieces[0]), request.Pieces[0].String()
		} else if firstExtra != hex.EncodeToString(request.ExtraData) || firstSource != request.From(request.Pieces[0]) || firstCID != request.Pieces[0].String() {
			t.Error("pending pull replay changed the request")
		}
		status := storage.PullStatusPending
		if call == 3 {
			status = storage.PullStatusFailed
		}
		return pullStatusResult(request, status), nil
	}, parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		queries.Add(1)
		return synapse.ParkedPieceMissing, nil
	}), &limit)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	attempt, err := runtime.repos.Contents.GetUnresolvedPullAttempt(t.Context(), pipeline.target.ContentID, pipeline.target.StorageDataSetID)
	if err != nil {
		t.Fatal(err)
	}
	for step := range 4 {
		if _, err := runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
		status := model.TaskStatusPending
		if step == 3 {
			status = model.TaskStatusFailed
		}
		row = runOneStorageTask(t, runtime, row, status)
		if row.RetryCount != 0 {
			t.Fatalf("provider waiting consumed retries: %#v", row)
		}
		if step%2 == 0 && (row.ResumeMode != model.TaskResumeModeExecute || calls.Load() != int64(1+step/2)) {
			t.Fatalf("recovery submitted a request: %#v, calls=%d", row, calls.Load())
		}
	}
	resolved, err := runtime.repos.Contents.GetPullAttempt(t.Context(), attempt.AttemptID, attempt.ContentID, attempt.StorageDataSetID)
	if err != nil || resolved.ResolvedAt == nil || resolved.Status != storagepull.AttemptStatusAbandoned ||
		row.FailureReason == nil || *row.FailureReason != "pull_failed" || calls.Load() != 3 || queries.Load() != 2 {
		t.Fatalf("pending pull did not settle its confirmed failure: %#v, %#v, %v; calls=%d, queries=%d", row, resolved, err, calls.Load(), queries.Load())
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.ActiveTaskID != nil || copyRow.Status != model.StorageCopyStatusFailed {
		t.Fatalf("confirmed failure retained the transfer owner: %#v, %v", copyRow, err)
	}
}

type pullEvidenceFailureRepository struct {
	repository.StorageContentRepository
	stage     string
	remaining atomic.Int64
}

func (r *pullEvidenceFailureRepository) fail(stage string) error {
	if r.stage == stage && r.remaining.Add(-1) >= 0 {
		return errors.New("injected pull evidence read failure")
	}
	return nil
}

func (r *pullEvidenceFailureRepository) AuthorizeCopyTask(ctx context.Context, copyID, generation, taskID, claimGeneration int64) (*model.StorageCopy, error) {
	if err := r.fail("authorization"); err != nil {
		return nil, err
	}
	return r.StorageContentRepository.AuthorizeCopyTask(ctx, copyID, generation, taskID, claimGeneration)
}

func (r *pullEvidenceFailureRepository) GetPullAttempt(ctx context.Context, attemptID string, contentID, dataSetID int64) (*storagepull.Attempt, error) {
	if err := r.fail("attempt"); err != nil {
		return nil, err
	}
	return r.StorageContentRepository.GetPullAttempt(ctx, attemptID, contentID, dataSetID)
}

func (r *pullEvidenceFailureRepository) GetUnresolvedPullAttempt(ctx context.Context, contentID, dataSetID int64) (*storagepull.Attempt, error) {
	if err := r.fail("attempt"); err != nil {
		return nil, err
	}
	return r.StorageContentRepository.GetUnresolvedPullAttempt(ctx, contentID, dataSetID)
}

func TestPullCancellationPreservesIntentAcrossEvidenceFailures(t *testing.T) {
	for _, stage := range []string{"authorization", "attempt"} {
		for _, attempted := range []bool{false, true} {
			for _, exhausted := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/attempted=%v/exhausted=%v", stage, attempted, exhausted), func(t *testing.T) {
					limit := 1
					var calls, queries atomic.Int64
					runtime, pipeline, row := newPullTask(t, func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
						calls.Add(1)
						return pullStatusResult(request, storage.PullStatusPending), nil
					}, parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
						queries.Add(1)
						return synapse.ParkedPieceMissing, nil
					}), &limit)
					if attempted {
						row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
					}
					faults := &pullEvidenceFailureRepository{StorageContentRepository: runtime.repos.Contents, stage: stage}
					failures := int64(1)
					if exhausted {
						failures++
					}
					faults.remaining.Store(failures)
					runtime.repos.Contents = faults
					if err := runtime.repos.Tasks.RequestCancellation(t.Context(), row.ID, "cancel pull"); err != nil {
						t.Fatal(err)
					}
					row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
					if row.RetryCount != 1 || !row.CancellationRequested() || row.ResumeMode != model.TaskResumeModeRecover {
						t.Fatalf("evidence failure did not retry cancellation: %#v", row)
					}
					if exhausted {
						row = runOneStorageTask(t, runtime, row, model.TaskStatusFailed)
						if row.FailureReason == nil || *row.FailureReason != storagepull.FailureRecoveryBlocked || !runtime.service.Retryable(row) {
							t.Fatalf("blocked cancellation = %#v", row)
						}
						copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
						if err != nil || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != row.ID || copyRow.Status != model.StorageCopyStatusPending {
							t.Fatalf("blocked cancellation lost its owner: %#v, %v", copyRow, err)
						}
						if err := runtime.service.Retry(t.Context(), row.ID); err != nil {
							t.Fatal(err)
						}
						retried, err := runtime.repos.Tasks.GetByID(t.Context(), row.ID)
						if err != nil || !retried.CancellationRequested() || retried.CancellationReason == nil || *retried.CancellationReason != "cancel pull" ||
							retried.RetryCount != 0 || retried.ResumeMode != model.TaskResumeModeRecover || !bytes.Equal(retried.Checkpoint, row.Checkpoint) {
							t.Fatalf("manual retry lost cancellation intent: %#v, %v", retried, err)
						}
						row = retried
					}
					wantCalls := int64(0)
					if attempted {
						wantCalls = 1
					}
					if calls.Load() != wantCalls || queries.Load() != 0 {
						t.Fatalf("blocked recovery reached provider: calls=%d, queries=%d", calls.Load(), queries.Load())
					}
					status := model.TaskStatusCancelled
					if attempted {
						status = model.TaskStatusFailed
					}
					row = runOneStorageTask(t, runtime, row, status)
					if !row.CancellationRequested() || calls.Load() != wantCalls || (attempted && (queries.Load() != 1 || row.FailureReason == nil || *row.FailureReason != storagepull.FailureCancelOutcomeUnknown)) {
						t.Fatalf("recovered cancellation = %#v, calls=%d, queries=%d", row, calls.Load(), queries.Load())
					}
				})
			}
		}
	}
}

func TestPullCancellationPreservesUnresolvedOutcome(t *testing.T) {
	for _, tt := range []struct {
		name       string
		attempted  bool
		state      synapse.ParkedPieceState
		queryError bool
		wantStatus model.TaskStatus
	}{
		{name: "not attempted", wantStatus: model.TaskStatusCancelled},
		{name: "ready", attempted: true, state: synapse.ParkedPieceReady, wantStatus: model.TaskStatusCompleted},
		{name: "processing", attempted: true, state: synapse.ParkedPieceProcessing, wantStatus: model.TaskStatusFailed},
		{name: "missing", attempted: true, state: synapse.ParkedPieceMissing, wantStatus: model.TaskStatusFailed},
		{name: "query failed", attempted: true, queryError: true, wantStatus: model.TaskStatusFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			limit := 0
			var calls atomic.Int64
			runtime, pipeline, row := newPullTask(t, func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
				calls.Add(1)
				return nil, errors.Join(pdp.ErrPullQueueFull, &pdp.HTTPError{StatusCode: 429, RetryAfter: time.Hour})
			}, parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				if tt.queryError {
					return "", errors.New("provider unavailable during cancellation")
				}
				return tt.state, nil
			}), &limit)
			if tt.attempted {
				row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
			}
			if tt.state == synapse.ParkedPieceReady {
				version := &model.ObjectVersion{
					VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID,
					Key: "cancelled-ready-pull", ContentID: &pipeline.upload.ID, Size: pipeline.upload.ContentSize,
					ETag: "cancelled-ready-pull", ContentType: "application/octet-stream",
				}
				if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
					t.Fatal(err)
				}
			}
			if err := runtime.repos.Tasks.RequestCancellation(t.Context(), row.ID, "cancel pull"); err != nil {
				t.Fatal(err)
			}
			result := runOneStorageTask(t, runtime, row, tt.wantStatus)
			wantCalls := int64(0)
			if tt.attempted {
				wantCalls = 1
			}
			if calls.Load() != wantCalls {
				t.Fatalf("cancellation submitted %d requests, want %d", calls.Load(), wantCalls)
			}
			if tt.wantStatus != model.TaskStatusFailed {
				if _, err := runtime.repos.Contents.GetUnresolvedPullAttempt(t.Context(), pipeline.target.ContentID, pipeline.target.StorageDataSetID); !errors.Is(err, repository.ErrNotFound) {
					t.Fatalf("settled cancellation retains unresolved pull: %v", err)
				}
				return
			}
			if result.FailureReason == nil || *result.FailureReason != storagepull.FailureCancelOutcomeUnknown || !runtime.service.Retryable(result) {
				t.Fatalf("cancelled outcome = %#v", result)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != row.ID || copyRow.Status != model.StorageCopyStatusPending {
				t.Fatalf("cancelled pull owner = %#v, %v", copyRow, err)
			}
			if err := runtime.service.Retry(t.Context(), row.ID); err != nil {
				t.Fatal(err)
			}
			retried, err := runtime.repos.Tasks.GetByID(t.Context(), row.ID)
			if err != nil || retried.CancellationRequestedAt != nil || retried.CancellationReason != nil || retried.RetryCount != 0 ||
				retried.ResumeMode != model.TaskResumeModeRecover || !bytes.Equal(retried.Checkpoint, result.Checkpoint) {
				t.Fatalf("cancelled pull retry = %#v, %v", retried, err)
			}
			status := model.TaskStatusPending
			if tt.queryError {
				status = model.TaskStatusFailed
			}
			afterRetry := runOneStorageTask(t, runtime, retried, status)
			if afterRetry.CancellationRequestedAt != nil || calls.Load() != 1 {
				t.Fatalf("manual recovery repeated cancellation or POST: %#v, calls=%d", afterRetry, calls.Load())
			}
			if _, err := runtime.repos.Contents.GetUnresolvedPullAttempt(t.Context(), pipeline.target.ContentID, pipeline.target.StorageDataSetID); err != nil {
				t.Fatalf("manual recovery lost pull evidence: %v", err)
			}
		})
	}
}

type pullSlotProbe struct {
	called chan struct{}
}

func (p *pullSlotProbe) Definition() taskengine.Definition {
	return taskengine.Definition{Type: "pull_slot_probe", InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect, Codec: taskengine.StrictJSONCodec(func(*struct{}) error { return nil })}
}

func (p *pullSlotProbe) Execute(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	err := execution.WithResource(ctx, taskengine.ResourceProviderMutation, func(context.Context) error {
		close(p.called)
		return nil
	})
	if err != nil {
		return taskengine.Fail(err, "probe_failed", nil)
	}
	return taskengine.Complete("", nil)
}

func (p *pullSlotProbe) Recover(context.Context, taskengine.Execution) taskengine.Result {
	return taskengine.Fail(errors.New("probe unexpectedly recovered"), "probe_failed", nil)
}

func TestPullWaitReleasesWorkerAndMutationSlot(t *testing.T) {
	for _, queueFull := range []bool{false, true} {
		t.Run(fmt.Sprintf("queue full=%v", queueFull), func(t *testing.T) {
			probe := &pullSlotProbe{called: make(chan struct{})}
			client := &testutil.MockStorageClient{}
			target := &testutil.MockStorageTarget{
				PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return testutil.CommitExtraData(7), nil },
				SubmitPullFunc: func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
					if queueFull {
						return nil, pdp.ErrPullQueueFull
					}
					return pullStatusResult(request, storage.PullStatusPending), nil
				},
			}
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
				storage: client, policy: cache.EvictionPolicyNone,
				register: func(h *worker.TaskHandlers, registry *taskengine.Registry) error {
					if err := h.RegisterStorage(registry); err != nil {
						return err
					}
					return registry.Register(probe)
				},
			})
			pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
			target.ProviderIDValue, target.ClientDataSetIDValue = pipeline.targetSet.ProviderID.SDK(), pipeline.targetClient
			dataSetID := pipeline.targetSet.DataSetID.SDK()
			target.DataSetIDValue = &dataSetID
			client.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
				return target, nil
			}
			row := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStoragePull)
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{
				Concurrency: 1, ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1,
				PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration, Retention: time.Hour,
			}, runtime.repos, runtime.registry, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := runEngine(t, engine)
			defer stopHandlerEngine(t, cancel, done)
			waitForTask(t, runtime.repos, row.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusPending && row.WaitReason != nil })
			probeTask, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{Type: "pull_slot_probe", IdempotencyKey: "probe", Input: struct{}{}})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-probe.called:
			case <-time.After(3 * time.Second):
				t.Fatal("waiting pull retained the sole worker or provider mutation slot")
			}
			waitForTask(t, runtime.repos, probeTask.ID, func(row *model.Task) bool { return row.Status == model.TaskStatusCompleted })
		})
	}
}

func TestPullCopiesJoinOneBatchWithIndependentAuthorizations(t *testing.T) {
	client := &testutil.MockStorageClient{}
	nonces := &testutil.MockCommitNonces{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		storage: client, policy: cache.EvictionPolicyNone, commitNonces: nonces,
		parkedPieces: parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
			return synapse.ParkedPieceReady, nil
		}),
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
	provider.target.SubmitPullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
		if len(request.Pieces) != 1 {
			t.Errorf("Pull members = %d, want 1", len(request.Pieces))
		}
		copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), copies[len(pulls)].ID)
		if err != nil || copyRow.CommitRequestID != nil {
			t.Errorf("copy before Pull = %#v, %v, want no Commit membership", copyRow, err)
		}
		pulls = append(pulls, append([]byte(nil), request.ExtraData...))
		return pullStatusResult(request, storage.PullStatusComplete), nil
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
		Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration,
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
	for _, scenario := range []string{"missing ledger", "foreign content", "foreign data set", "invalid authorization", "invalid CID", "resolved attempt", "reservation rollback", "reservation rollback exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int64
			var retryLimit *int
			if scenario == "reservation rollback exhausted" {
				retryLimit = new(0)
			}
			rollback := scenario == "reservation rollback" || scenario == "reservation rollback exhausted"
			runtime, pipeline, row := newPullErrorTask(t, nil, retryLimit, &calls)
			var input storagepipeline.CopyGenerationInput
			if err := json.Unmarshal(row.Input, &input); err != nil {
				t.Fatal(err)
			}
			if rollback {
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
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration, Retention: time.Hour, ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1}, &limitedRepos, runtime.registry, slog.Default())
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
			if rollback {
				count, err := runtime.db.NewSelect().Model((*storagepull.Attempt)(nil)).Count(t.Context())
				if err != nil || count != 0 || len(result.Checkpoint) != 0 {
					t.Fatalf("rollback evidence = %d, %s, %v", count, result.Checkpoint, err)
				}
				if retryLimit != nil {
					copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
					if err != nil || copyRow.Status != model.StorageCopyStatusFailed || copyRow.ActiveTaskID != nil ||
						result.FailureReason == nil || *result.FailureReason != "pull_checkpoint_failed" || runtime.service.Retryable(result) {
						t.Fatalf("unsubmitted failure retained ownership or reported an unknown outcome: %#v, %#v, %v", result, copyRow, err)
					}
				}
			} else if result.FailureReason == nil || *result.FailureReason != storagepull.FailureOutcomeUnknown {
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
	engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 1, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration, Retention: time.Hour, ProviderMutationConcurrency: 1, DestructiveMutationConcurrency: 1}, &repos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runEngine(t, engine)
	defer stopHandlerEngine(t, cancel, done)
	return waitForTask(t, runtime.repos, row.ID, func(current *model.Task) bool {
		return current.ClaimGeneration > row.ClaimGeneration && current.Status == status
	})
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
					runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
						cache: cacheStore, storage: client, policy: cache.EvictionPolicyNone,
						parkedPieces: parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
							return synapse.ParkedPieceReady, nil
						}),
						register: func(h *worker.TaskHandlers, r *taskengine.Registry) error { return h.RegisterStorage(r) },
					})
					copyRow, target, original := seedSealedReplacementBatch(t, runtime, members, phase)
					target.SubmitPullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
						if cacheRestore {
							return pullStatusResult(request, storage.PullStatusFailed), nil
						}
						return pullStatusResult(request, storage.PullStatusComplete), nil
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
			target.SubmitPullFunc = func(_ context.Context, request storage.PullRequest) (*storage.PullResult, error) {
				return pullStatusResult(request, storage.PullStatusFailed), nil
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
			engine, err := taskengine.NewEngine(taskengine.EngineConfig{Concurrency: 3, PollInterval: handlerTestPollInterval, LeaseDuration: handlerTestLeaseDuration, Retention: time.Hour, ProviderMutationConcurrency: 2, DestructiveMutationConcurrency: 1}, runtime.repos, runtime.registry, slog.Default())
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
