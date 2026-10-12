package repository_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func repositoryTestTask(task *model.Task) *model.Task {
	if len(task.Runtime) == 0 {
		task.Runtime = json.RawMessage(`{}`)
	}
	return task
}

func repositorySuccessor(t *testing.T, repos *repository.Repositories, source *model.Task) *model.Task {
	t.Helper()
	var result *model.Task
	if err := repos.WithTx(t.Context(), func(tx *repository.Repositories) error {
		var err error
		source, err = tx.Tasks.GetForUpdate(t.Context(), source.ID)
		if err != nil {
			return err
		}
		if err := tx.Tasks.SupersedeTerminal(t.Context(), source.ID); err != nil {
			return err
		}
		child := repositoryTestTask(&model.Task{Type: source.Type, Generation: source.Generation, InputVersion: source.InputVersion, Input: source.Input, Checkpoint: source.Checkpoint, Runtime: json.RawMessage(`{}`), RetryOfTaskID: &source.ID, SubjectType: source.SubjectType, SubjectKey: source.SubjectKey, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now()})
		result, _, err = tx.Tasks.Enqueue(t.Context(), child)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return result
}
