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
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
)

func TestManualPullRetryPreservesEarliestRequestTime(t *testing.T) {
	var calls atomic.Int64
	limit := 0
	runtime, _, source := newPullTask(t, func(context.Context, storage.PullRequest) (*storage.PullResult, error) {
		calls.Add(1)
		return nil, errors.Join(pdp.ErrPullQueueFull, &pdp.HTTPError{StatusCode: 429, RetryAfter: time.Hour})
	}, nil, &limit)
	failed := runOneStorageTask(t, runtime, source, model.TaskStatusFailed)
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
	waiting := runOneStorageTask(t, runtime, child, model.TaskStatusPending)
	if calls.Load() != 1 || waiting.RetryCount != 0 || waiting.AvailableAt.Before(checkpoint.NextRequestAt) {
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
	if child.ID == source.ID || child.RetryOfTaskID == nil || *child.RetryOfTaskID != source.ID || child.RetryGroupKey != source.RetryGroupKey {
		t.Fatalf("new execution relationship = %#v", child)
	}
	if child.RetryCount != 0 || child.RetryLimit == nil || *child.RetryLimit != 5 || child.AcknowledgedAt != nil || child.WorkStartedAt != nil || child.FinishedAt != nil {
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
				wantStatus = model.TaskStatusFailed
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
				if parent.FailureReason == nil || *parent.FailureReason != "attempts_exhausted" || parent.RetryCount != 1 || *member.ActiveTaskID != failedID || source.SupersededAt != nil {
					t.Fatalf("exhausted coordinator reopened failed member: parent=%#v member=%#v source=%#v", parent, member, source)
				}
			} else {
				child, err := f.runtime.repos.Tasks.GetDirectSuccessor(t.Context(), failedID)
				if err != nil || child == nil || *member.ActiveTaskID != child.ID || parent.RetryCount != 2 || child.ResumeMode != model.TaskResumeModeRecover || child.RetryCount != 0 || !bytes.Equal(child.Checkpoint, checkpoint) || child.AvailableAt.Before(parent.AvailableAt) || source.SupersededAt == nil {
					t.Fatalf("member recovery bypassed budget or evidence: parent=%#v member=%#v child=%#v err=%v", parent, member, child, err)
				}
			}
			after := f.request(t, requestID)
			if after.TaskID == nil || *after.TaskID != taskID || after.Status != before.Status || after.ExtraDataHex == nil || before.ExtraDataHex == nil || *after.ExtraDataHex != *before.ExtraDataHex || member.CommitRequestID == nil || *member.CommitRequestID != requestID || member.CommitPosition == nil {
				t.Fatalf("member recovery changed signed request evidence: before=%#v after=%#v member=%#v", before, after, member)
			}
			if sends, _ := f.provider.sent(); len(sends) != 1 {
				t.Fatalf("failed member caused another submission: sends=%d", len(sends))
			}
		})
	}
}
