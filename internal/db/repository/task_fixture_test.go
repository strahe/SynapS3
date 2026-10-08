package repository_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func repositoryTestTask(task *model.Task) *model.Task {
	if task.RetryGroupKey == "" {
		task.RetryGroupKey = fmt.Sprintf("test:%s:%s", task.Type, task.IdempotencyKey)
	}
	if task.RetryLimit == nil {
		task.RetryLimit = new(5)
	}
	if len(task.Policy) == 0 {
		task.Policy = json.RawMessage(`{"version":1,"backoff":{"initial_delay":10000000000,"multiplier":2,"maximum_delay":300000000000,"jitter":0.2},"invocation_timeout":0,"observation_window":0}`)
	}
	if len(task.Runtime) == 0 {
		task.Runtime = json.RawMessage(`{}`)
	}
	return task
}

func repositorySuccessor(t *testing.T, repos *repository.Repositories, source *model.Task, retainCancellation bool) *model.Task {
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
		child := repositoryTestTask(&model.Task{Type: source.Type, IdempotencyKey: source.IdempotencyKey, InputVersion: source.InputVersion, InputHash: source.InputHash, Input: source.Input, Checkpoint: source.Checkpoint, Policy: source.Policy, Runtime: json.RawMessage(`{}`), RetryOfTaskID: &source.ID, RetryGroupKey: source.RetryGroupKey, SubjectType: source.SubjectType, SubjectKey: source.SubjectKey, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now()})
		if retainCancellation {
			child.CancellationRequestedAt = source.CancellationRequestedAt
			child.CancellationReason = source.CancellationReason
		}
		result, _, err = tx.Tasks.Enqueue(t.Context(), child)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return result
}
