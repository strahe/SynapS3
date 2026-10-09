package task_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func TestReplacementRecoversFailedCommitWithinCoordinatorBudget(t *testing.T) {
	for _, attempts := range []int{1, 2} {
		t.Run(map[int]string{1: "exhausted", 2: "recovery"}[attempts], func(t *testing.T) {
			f := newRegistrationFixture(t, 1, nil, 0, func(options *handlerRuntimeOptions) { options.maxAttempts = &attempts })
			requestID, commitID := f.collect(t)
			if _, err := f.runtime.repos.Contents.SealCommitRequest(t.Context(), repository.SealCommitRequestInput{
				RequestID: requestID, TaskID: commitID, ExtraDataHex: hex.EncodeToString(testutil.CommitExtraData(90)),
				Members: []repository.SealMember{{CopyID: f.copies[0].ID, ContentID: f.copies[0].ContentID, PieceCID: f.pieceCID(t, f.copies[0])}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.repos.Contents.BeginCommitSubmission(t.Context(), repository.BeginCommitSubmissionInput{RequestID: requestID, TaskID: commitID}); err != nil {
				t.Fatal(err)
			}
			checkpoint := json.RawMessage(`{"request_id":"` + requestID + `","sends":1}`)
			if _, err := f.runtime.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", checkpoint).Where("id = ?", commitID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			reason := "attempts_exhausted"
			settleHandlerTaskFixture(t, f.runtime, commitID, model.TaskStatusFailed, &reason)
			row, _, err := f.runtime.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
				BucketID: f.dataSet.BucketID, SourceDataSetID: f.dataSet.ID, SelectionMode: storagereplacement.SelectionModeManual,
				TargetProviderID: testOnChainID(t, 990001), ClientRequestID: "failed-commit-recovery",
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{ID: row.TargetDataSetID, DataSetID: testOnChainID(t, 990002)}); err != nil {
				t.Fatal(err)
			}
			coordinator, _, err := f.runtime.service.EnqueueTx(t.Context(), taskengine.EnqueueRequest{
				Type: model.TaskTypeProviderReplacementCoordinate, IdempotencyKey: storagereplacement.CoordinateTaskKey(row.ID, row.TaskGeneration),
				Input: storagereplacement.CoordinateInput{ReplacementID: row.ID, Generation: row.TaskGeneration},
			}, func(ctx context.Context, repos *repository.Repositories, task *model.Task, _ bool) error {
				return repos.Replacements.BindTask(ctx, row.ID, row.TaskGeneration, task.ID)
			})
			if err != nil {
				t.Fatal(err)
			}
			status := model.TaskStatusPending
			if attempts == 1 {
				status = model.TaskStatusFailed
			}
			result := runOneStorageTask(t, f.runtime, coordinator, status)
			successor, err := f.runtime.repos.Tasks.GetDirectSuccessor(t.Context(), commitID)
			if err != nil {
				t.Fatal(err)
			}
			request := f.request(t, requestID)
			source, err := f.runtime.repos.Contents.GetDataSetBindingByID(t.Context(), f.dataSet.ID)
			if err != nil || !source.IsCurrent {
				t.Fatalf("source switched before Commit recovery: %#v, %v", source, err)
			}
			if attempts == 1 {
				if successor != nil || request.TaskID == nil || *request.TaskID != commitID || result.FailureReason == nil || *result.FailureReason != "attempts_exhausted" {
					t.Fatalf("last opportunity reopened Commit: task=%#v request=%#v successor=%#v", result, request, successor)
				}
				return
			}
			if successor == nil || successor.Type != model.TaskTypeStorageCommit || successor.ResumeMode != model.TaskResumeModeRecover || successor.RetryCount != 0 || !bytes.Equal(successor.Checkpoint, checkpoint) ||
				request.TaskID == nil || *request.TaskID != successor.ID || result.RetryCount != 1 || successor.AvailableAt.Before(result.AvailableAt) || successor.AvailableAt.Before(time.Now().Add(-time.Second)) {
				t.Fatalf("Commit recovery lost its evidence or budget: task=%#v request=%#v successor=%#v", result, request, successor)
			}
		})
	}
}
