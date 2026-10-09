package task_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
)

func TestPeriodicManualRetryPreservesSubjectAndArchivesFailure(t *testing.T) {
	runtime := newScheduledCapacityRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyNone})
	const scheduleKey = "capacity-policy"
	source, _, err := runtime.service.Enqueue(t.Context(), taskengine.EnqueueRequest{
		Type: model.TaskTypeCacheCapacityReconcile, IdempotencyKey: scheduleKey + ":1", Input: systemtask.Input{},
		SubjectType: "system", SubjectKey: "cache-capacity",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.TaskSchedules.Ensure(t.Context(), scheduleKey, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.TaskSchedules.SetHead(t.Context(), scheduleKey, 0, 1, nil, &source.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	reason := "attempts_exhausted"
	settleHandlerTaskFixture(t, runtime, source.ID, model.TaskStatusFailed, &reason)
	successor, err := runtime.service.Retry(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := runtime.repos.TaskSchedules.GetByTaskID(t.Context(), successor.ID)
	if err != nil || schedule == nil || schedule.Generation != 1 || schedule.LatestTaskID == nil || *schedule.LatestTaskID != successor.ID ||
		successor.SubjectType == nil || *successor.SubjectType != "system" || successor.SubjectKey == nil || *successor.SubjectKey != "cache-capacity" {
		t.Fatalf("periodic recovery = %#v, schedule = %#v, error = %v", successor, schedule, err)
	}
	settleHandlerTaskFixture(t, runtime, successor.ID, model.TaskStatusFailed, &reason)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, successor.ID, func(row *model.Task) bool { return row.SupersededAt != nil })
	healthy, err := runtime.repos.Tasks.GetByIdentity(t.Context(), model.TaskTypeCacheCapacityReconcile, scheduleKey+":2")
	if err != nil || healthy == nil || healthy.Status != model.TaskStatusCompleted {
		t.Fatalf("healthy cycle did not finish the manual recovery failure: task=%#v, error=%v", healthy, err)
	}
	failures, err := runtime.repos.Tasks.ListCurrentFailedForSubject(t.Context(), "system", "cache-capacity", 0, 100, model.TaskTypeCacheCapacityReconcile)
	if err != nil || len(failures.Tasks) != 0 {
		t.Fatalf("healthy cycle left a current failure: %#v, error=%v", failures, err)
	}
}

func TestStaticManualRetryPreservesEvidenceStops(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{})
	for _, tt := range []struct {
		taskType model.TaskType
		reason   string
		allowed  bool
	}{
		{model.TaskTypeStorageStore, "invalid_checkpoint", false},
		{model.TaskTypeStoragePull, "invalid_checkpoint", false},
		{model.TaskTypeStorageTransferPlan, "invalid_checkpoint", false},
		{model.TaskTypeStorageStore, "store_cache_missing", true},
		{model.TaskTypeStoragePull, "pull_outcome_unknown", true},
		{model.TaskTypeStorageTransferPlan, "copy_cache_missing", true},
		{model.TaskTypeWalletOperation, "wallet_broadcast_unknown", false},
		{model.TaskTypeWalletOperation, "wallet_transaction_reverted", false},
		{model.TaskTypeWalletOperation, "invalid_checkpoint", false},
		{model.TaskTypeWalletOperation, "handler_panic", true},
		{model.TaskTypeWalletOperation, "dependency_unavailable", true},
	} {
		row := &model.Task{Type: tt.taskType, Status: model.TaskStatusFailed, FailureReason: &tt.reason}
		if allowed := runtime.service.Retryable(row); allowed != tt.allowed {
			t.Errorf("%s / %s retryable = %v, want %v", tt.taskType, tt.reason, allowed, tt.allowed)
		}
	}
}

func TestManualPullRetryPreservesEarliestRequestTime(t *testing.T) {
	var calls atomic.Int64
	limit := 5
	runtime, _, source := newPullTask(t, func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
		calls.Add(1)
		return nil, errors.Join(pdp.ErrPullQueueFull, &pdp.HTTPError{StatusCode: 429, RetryAfter: time.Hour})
	}, nil, &limit)
	if handlerTaskMaxAttempts(t, source) != 12 {
		t.Fatalf("new Pull budget = %s", source.Policy)
	}
	runOneStorageTask(t, runtime, source, model.TaskStatusPending)
	reason := "attempts_exhausted"
	settleHandlerTaskFixture(t, runtime, source.ID, model.TaskStatusFailed, &reason)
	failed, err := runtime.repos.Tasks.GetByID(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint struct {
		NextRequestAt time.Time `json:"next_request_at"`
	}
	if err := json.Unmarshal(failed.Checkpoint, &checkpoint); err != nil || checkpoint.NextRequestAt.Before(time.Now().Add(59*time.Minute)) {
		t.Fatalf("provider request deadline was not persisted: %s, %v", failed.Checkpoint, err)
	}
	child, err := runtime.service.Retry(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	runtime.testMaxAttempts = nil
	waiting := runOneStorageTask(t, runtime, child, model.TaskStatusPending)
	if calls.Load() != 1 || waiting.RetryCount != 0 || waiting.AvailableAt.Before(checkpoint.NextRequestAt) || handlerTaskMaxAttempts(t, failed) != 6 || handlerTaskMaxAttempts(t, waiting) != 12 {
		t.Fatalf("manual recovery bypassed provider delay: calls=%d task=%#v", calls.Load(), waiting)
	}
}

func TestManualRetryCreatesIndependentCopyRound(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyNone})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	source := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageTransferPlan)
	claim, err := runtime.repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claim == nil || claim.ID != source.ID {
		t.Fatalf("claim source = %#v, %v", claim, err)
	}
	checkpoint := []byte(`{"recovery_evidence":"preserved"}`)
	if err := runtime.repos.Tasks.WriteCheckpoint(t.Context(), claim.ID, claim.ClaimGeneration, checkpoint); err != nil {
		t.Fatal(err)
	}
	reason, message := "attempts_exhausted", "provider unavailable"
	if err := runtime.repos.Tasks.Settle(t.Context(), claim.ID, claim.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason, LastError: &message}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Tasks.AcknowledgeFailed(t.Context(), source.ID); err != nil {
		t.Fatal(err)
	}
	before, err := runtime.repos.Tasks.GetByID(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	child, err := runtime.service.Retry(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if child.ID == source.ID || child.RetryOfTaskID == nil || *child.RetryOfTaskID != source.ID {
		t.Fatalf("new execution relationship = %#v", child)
	}
	if child.RetryCount != 0 || handlerTaskMaxAttempts(t, child) != 6 || child.AcknowledgedAt != nil || child.WorkStartedAt != nil || child.FinishedAt != nil {
		t.Fatalf("new execution budget/timing = %#v", child)
	}
	if !bytes.Equal(child.Checkpoint, before.Checkpoint) {
		t.Fatalf("recovery evidence changed: %s", child.Checkpoint)
	}
	after, err := runtime.repos.Tasks.GetByID(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != before.Status || after.RetryCount != before.RetryCount || after.AcknowledgedAt == nil || !after.AcknowledgedAt.Equal(*before.AcknowledgedAt) || !after.FinishedAt.Equal(*before.FinishedAt) || !bytes.Equal(after.Policy, before.Policy) || !bytes.Equal(after.Checkpoint, before.Checkpoint) || after.SupersededAt == nil {
		t.Fatalf("source execution changed: before=%#v after=%#v", before, after)
	}
	owned, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if owned.ActiveTaskID == nil || *owned.ActiveTaskID != child.ID {
		t.Fatalf("copy recovery owner = %#v", owned)
	}
	replay, err := runtime.service.Retry(t.Context(), source.ID)
	if err != nil || replay.ID != child.ID {
		t.Fatalf("replayed Retry = %#v, %v", replay, err)
	}
}

func TestHistoricalCopyFailureCannotAcquireNewerOwner(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{policy: cache.EvictionPolicyNone})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	source := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageTransferPlan)
	claim, err := runtime.repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	reason := "handler_panic"
	if err := runtime.repos.Tasks.Settle(t.Context(), claim.ID, claim.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason}); err != nil {
		t.Fatal(err)
	}
	child, err := runtime.service.Retry(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := runtime.service.RetryableContext(t.Context(), source); err != nil || allowed {
		t.Fatalf("historical source eligible=%v err=%v", allowed, err)
	}
	// A repeated request names its original child even after that child starts.
	claim, err = runtime.repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claim.ID != child.ID {
		t.Fatalf("claim child=%#v %v", claim, err)
	}
	replay, err := runtime.service.Retry(t.Context(), source.ID)
	if err != nil || replay.ID != child.ID {
		t.Fatalf("historical replay=%#v %v", replay, err)
	}
}

func TestCommitFailedOwnedMemberUsesCoordinatorBudget(t *testing.T) {
	for _, maxAttempts := range []int{2, 3} {
		t.Run(map[int]string{2: "exhausted", 3: "recovery"}[maxAttempts], func(t *testing.T) {
			parked := &droppedPieces{missing: map[string]bool{}}
			f := newRegistrationFixture(t, 1, parked, 0, func(options *handlerRuntimeOptions) {
				options.maxAttempts = &maxAttempts
			})
			f.provider.script = []sendOutcome{sendRefuse}
			parked.drop(f.pieceCID(t, f.copies[0]))
			requestID, taskID := f.collect(t)
			parent, err := f.runtime.repos.Tasks.GetByID(t.Context(), taskID)
			if err != nil {
				t.Fatal(err)
			}
			parent = runOneStorageTask(t, f.runtime, parent, model.TaskStatusPending)
			member, err := f.runtime.repos.Contents.GetUploadCopyByID(t.Context(), f.copies[0].ID)
			if err != nil || member.ActiveTaskID == nil || parent.RetryCount != 1 {
				t.Fatalf("first recovery member=%#v parent=%#v err=%v", member, parent, err)
			}
			failedID := *member.ActiveTaskID
			if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(time.Hour)).Where("id = ?", taskID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", failedID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			claim, err := f.runtime.repos.Tasks.ClaimNext(t.Context(), time.Minute)
			if err != nil || claim == nil || claim.ID != failedID {
				t.Fatalf("claim member=%#v err=%v", claim, err)
			}
			checkpoint := []byte(`{"unresolved_effect":"preserved"}`)
			if err := f.runtime.repos.Tasks.WriteCheckpoint(t.Context(), failedID, claim.ClaimGeneration, checkpoint); err != nil {
				t.Fatal(err)
			}
			reason := "attempts_exhausted"
			if err := f.runtime.repos.Tasks.Settle(t.Context(), failedID, claim.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason}); err != nil {
				t.Fatal(err)
			}
			before := f.request(t, requestID)
			if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("available_at = ?", time.Now().Add(-time.Second)).Where("id = ?", taskID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			wantStatus := model.TaskStatusPending
			if maxAttempts == 2 {
				wantStatus = model.TaskStatusCompleted
			}
			parent = runOneStorageTask(t, f.runtime, parent, wantStatus)
			member, err = f.runtime.repos.Contents.GetUploadCopyByID(t.Context(), member.ID)
			if err != nil || member.ActiveTaskID == nil {
				t.Fatalf("member lost execution owner: %#v %v", member, err)
			}
			source, err := f.runtime.repos.Tasks.GetByID(t.Context(), failedID)
			if err != nil {
				t.Fatal(err)
			}
			if maxAttempts == 2 {
				if parent.RetryCount != 1 || *member.ActiveTaskID != failedID || source.SupersededAt != nil || !bytes.Equal(source.Checkpoint, checkpoint) {
					t.Fatalf("exhausted coordinator reopened failed member: parent=%#v member=%#v source=%#v", parent, member, source)
				}
			} else {
				child, err := f.runtime.repos.Tasks.GetDirectSuccessor(t.Context(), failedID)
				if err != nil || child == nil || *member.ActiveTaskID != child.ID || parent.RetryCount != 2 || child.ResumeMode != model.TaskResumeModeRecover || child.RetryCount != 0 || !bytes.Equal(child.Checkpoint, checkpoint) || child.AvailableAt.Before(parent.AvailableAt) || source.SupersededAt == nil {
					t.Fatalf("member recovery bypassed budget or evidence: parent=%#v member=%#v child=%#v err=%v", parent, member, child, err)
				}
			}
			after := f.request(t, requestID)
			if after.ExtraDataHex == nil || before.ExtraDataHex == nil || *after.ExtraDataHex != *before.ExtraDataHex {
				t.Fatalf("member recovery changed signed request evidence: before=%#v after=%#v member=%#v", before, after, member)
			}
			if maxAttempts == 2 {
				if after.TaskID != nil || after.Status != storagecommit.RequestStatusAbandoned || member.CommitRequestID != nil || member.CommitPosition != nil {
					t.Fatalf("refused request was not abandoned safely: request=%#v member=%#v", after, member)
				}
			} else if after.TaskID == nil || *after.TaskID != taskID || after.Status != before.Status || member.CommitRequestID == nil || *member.CommitRequestID != requestID || member.CommitPosition == nil {
				t.Fatalf("recoverable member changed request ownership: request=%#v member=%#v", after, member)
			}
			if sends, _ := f.provider.sent(); len(sends) != 1 {
				t.Fatalf("failed member caused another submission: sends=%d", len(sends))
			}
		})
	}
}
