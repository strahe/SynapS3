package repository_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestResumeFailedStoreCopyPreservesProgressAndFence(t *testing.T) {
	for _, scenario := range []string{"resume", "generation", "checkpoint", "historical source", "destination type", "another ingress"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCommitFixture(t, testDB(t))
			copyRow := f.transferredCopy(t, "resume-session")
			progressAt := time.Now().UTC().Truncate(time.Microsecond)
			if _, err := f.db.NewUpdate().Model((*model.StorageCopy)(nil)).
				Set("ingress_store_attempt = 2").Set("ingress_bytes_transferred = 7").Set("progress_updated_at = ?", progressAt).
				Where("id = ?", copyRow.ID).Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			checkpoint := json.RawMessage(`{"session_id":"7cd9c1bb-7e18-4006-a8f6-a3b6dc75bfa3","stage":"finalize"}`)
			source, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
				Type: model.TaskTypeStorageStore, InputVersion: 1, Generation: new(int64(1)),
				SubjectType: model.TaskSubjectStorageCopy, SubjectKey: fmt.Sprint(copyRow.ID),
				Input: []byte(fmt.Sprintf(`{"copy_id":%d,"generation":1}`, copyRow.ID)), Checkpoint: checkpoint,
			}))
			if err != nil {
				t.Fatal(err)
			}
			if err := f.repos.Contents.BindCopyTask(t.Context(), copyRow.ID, 1, source.ID); err != nil {
				t.Fatal(err)
			}
			claim, err := f.repos.Tasks.ClaimNext(t.Context(), time.Minute, repository.TaskClaimFilter{})
			if err != nil || claim == nil || claim.ID != source.ID {
				t.Fatalf("claim=%+v, error=%v", claim, err)
			}
			reason := "store_processing_timeout"
			if err := f.repos.Tasks.Settle(t.Context(), source.ID, claim.ClaimGeneration, repository.TaskTransition{
				Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: &reason,
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
				StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex, LastError: "interrupted",
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.repos.Contents.CompleteCopyTask(t.Context(), copyRow.ID, 1, source.ID); err != nil {
				t.Fatal(err)
			}
			if err := f.repos.Tasks.SupersedeTerminal(t.Context(), source.ID); err != nil {
				t.Fatal(err)
			}
			if scenario == "historical source" {
				if _, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
					Type: model.TaskTypeStorageTransferPlan, InputVersion: 1, SubjectType: source.SubjectType,
					SubjectKey: source.SubjectKey, Input: source.Input,
				})); err != nil {
					t.Fatal(err)
				}
			}
			childType := model.TaskTypeStorageStore
			if scenario == "destination type" {
				childType = model.TaskTypeStorageTransferPlan
			}
			child, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
				Type: childType, InputVersion: 1, Generation: new(int64(2)), RetryOfTaskID: &source.ID,
				SubjectType: source.SubjectType, SubjectKey: source.SubjectKey, ResumeMode: model.TaskResumeModeRecover,
				Input: []byte(fmt.Sprintf(`{"copy_id":%d,"generation":2}`, copyRow.ID)), Checkpoint: checkpoint,
			}))
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "another ingress" {
				binding, err := f.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
					BucketID: f.bucket.ID, CopyIndex: 1, ProviderID: onChainID(t, "702"),
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := f.repos.Contents.CreateUploadCopiesForBindings(t.Context(), copyRow.ContentID, []repository.UploadCopyBindingInput{{
					StorageDataSetID: binding.ID, CopyIndex: 1, ProviderID: binding.ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress,
				}}); err != nil {
					t.Fatal(err)
				}
			}
			input := repository.ResumeFailedStoreCopyInput{
				CopyID: copyRow.ID, Generation: 1, SourceTaskID: source.ID, TaskID: child.ID,
				SourceCheckpoint: checkpoint, Checkpoint: checkpoint,
			}
			if scenario == "generation" {
				input.Generation++
			}
			if scenario == "checkpoint" {
				input.Checkpoint = json.RawMessage(`{"session_id":"another-session"}`)
			}
			release, err := cacheaccess.NewGate().HoldRead(t.Context(), model.ContentCacheKey(copyRow.ContentID))
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if scenario == "resume" {
				results := make(chan error, 2)
				for range 2 {
					go func() { results <- f.repos.Contents.ResumeFailedStoreCopy(t.Context(), input) }()
				}
				first, second := <-results, <-results
				if first != nil {
					first, second = second, first
				}
				if first != nil || !errors.Is(second, repository.ErrConflict) {
					t.Fatalf("concurrent recovery errors=%v, %v", first, second)
				}
				err = first
			} else {
				err = f.repos.Contents.ResumeFailedStoreCopy(t.Context(), input)
			}
			stored := f.copy(t, copyRow.ID)
			if scenario != "resume" {
				if !errors.Is(err, repository.ErrConflict) || stored.Status != model.StorageCopyStatusFailed || stored.ActiveTaskID != nil || stored.WorkGeneration != 1 {
					t.Fatalf("unsafe session recovery copy=%+v, error=%v", stored, err)
				}
				return
			}
			if err != nil || stored.Status != model.StorageCopyStatusPending || stored.ActiveTaskID == nil || *stored.ActiveTaskID != child.ID || stored.WorkGeneration != 2 ||
				stored.TransferMethod != model.StorageCopyTransferMethodIngress || stored.IngressStoreAttempt != 2 || stored.IngressBytesTransferred != 7 ||
				stored.ProgressUpdatedAt == nil || !stored.ProgressUpdatedAt.Equal(progressAt) {
				t.Fatalf("session recovery copy=%+v, error=%v", stored, err)
			}
			if _, err := f.repos.Contents.RecordIngressStoreProgress(t.Context(), repository.RecordIngressStoreProgressInput{
				CopyID: copyRow.ID, Generation: 1, TaskID: source.ID, Attempt: 2, BytesUploaded: 9,
			}); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("old owner still records progress: %v", err)
			}
		})
	}
}
