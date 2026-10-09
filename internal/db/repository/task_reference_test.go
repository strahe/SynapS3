package repository_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestCopyTaskBindingValidatesLogicalReference(t *testing.T) {
	for _, mismatch := range []string{"missing", "type", "subject", "generation", "archived"} {
		t.Run(mismatch, func(t *testing.T) {
			f := newCommitFixture(t, testDB(t))
			copyRow := f.transferredCopy(t, "logical-reference")
			generation := copyRow.WorkGeneration + 1
			taskID := int64(999999)
			if mismatch != "missing" {
				row := repositoryTestTask(&model.Task{
					Type: model.TaskTypeStorageStore, IdempotencyKey: "logical-reference", InputVersion: 1,
					Input: []byte(fmt.Sprintf(`{"copy_id":%d,"generation":%d}`, copyRow.ID, generation)), InputHash: "logical-reference",
					SubjectType: new(model.TaskSubjectStorageCopy), SubjectKey: new(fmt.Sprint(copyRow.ID)),
				})
				switch mismatch {
				case "type":
					row.Type = model.TaskTypeWalletOperation
				case "subject":
					row.SubjectKey = new(fmt.Sprint(copyRow.ID + 1))
				case "generation":
					row.Input = []byte(fmt.Sprintf(`{"copy_id":%d,"generation":%d}`, copyRow.ID, generation+1))
				}
				row, _, err := f.repos.Tasks.Enqueue(t.Context(), row)
				if err != nil {
					t.Fatal(err)
				}
				taskID = row.ID
				if mismatch == "archived" {
					claimed, err := f.repos.Tasks.ClaimNext(t.Context(), time.Minute)
					if err != nil || claimed == nil || claimed.ID != row.ID {
						t.Fatalf("claim = %#v, %v", claimed, err)
					}
					if err := f.repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := f.repos.Contents.BindCopyTask(t.Context(), copyRow.ID, generation, taskID); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("invalid task binding = %v, want conflict", err)
			}
			stored, err := f.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil || stored.ActiveTaskID != nil || stored.WorkGeneration != copyRow.WorkGeneration {
				t.Fatalf("invalid binding changed owner: %#v, %v", stored, err)
			}
		})
	}
}

func TestCopyTaskOwnerTransferRetainsUnverifiableSource(t *testing.T) {
	for _, missingSource := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing source=%v", missingSource), func(t *testing.T) {
			f := newCommitFixture(t, testDB(t))
			copyRow := f.transferredCopy(t, "transfer-reference")
			generation := copyRow.WorkGeneration + 1
			enqueue := func(key string) *model.Task {
				t.Helper()
				row, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
					Type: model.TaskTypeStorageStore, IdempotencyKey: key, InputVersion: 1,
					Input: []byte(fmt.Sprintf(`{"copy_id":%d,"generation":%d}`, copyRow.ID, generation)), InputHash: key,
					SubjectType: new(model.TaskSubjectStorageCopy), SubjectKey: new(fmt.Sprint(copyRow.ID)),
				}))
				if err != nil {
					t.Fatal(err)
				}
				return row
			}
			previous, next := enqueue("previous"), enqueue("next")
			if err := f.repos.Contents.BindCopyTask(t.Context(), copyRow.ID, generation, previous.ID); err != nil {
				t.Fatal(err)
			}
			if missingSource {
				if _, err := f.db.NewRaw(`UPDATE tasks SET retry_of_task_id = ? WHERE id = ?`, previous.ID, next.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.NewRaw(`DELETE FROM tasks WHERE id = ?`, previous.ID).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.repos.Contents.TransferCopyTaskOwner(t.Context(), copyRow.ID, generation, previous.ID, next.ID); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("unverifiable source transfer = %v, want conflict", err)
			}
			stored, err := f.repos.Contents.GetUploadCopyByID(t.Context(), copyRow.ID)
			if err != nil || stored.ActiveTaskID == nil || *stored.ActiveTaskID != previous.ID || stored.WorkGeneration != generation {
				t.Fatalf("unverifiable transfer changed owner: %#v, %v", stored, err)
			}
		})
	}
}
