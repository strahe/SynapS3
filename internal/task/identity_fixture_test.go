package task_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/testutil"
)

func testStorageExecutionIdentity() storagepipeline.ExecutionIdentity {
	return storagepipeline.NewExecutionIdentity(testutil.DefaultContextIdentity, testutil.DefaultPDPVerifierAddress)
}

func markTestDataSetReady(t *testing.T, runtime handlerTestRuntime, ctx context.Context, input repository.MarkDataSetReadyInput) error {
	t.Helper()
	binding, err := runtime.repos.Contents.GetDataSetBindingByID(ctx, input.ID)
	if err != nil {
		return err
	}
	if binding != nil && binding.EnsureTaskID == nil {
		raw, err := json.Marshal(storagepipeline.DataSetInput{DataSetID: binding.ID, ExecutionIdentity: testStorageExecutionIdentity()})
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		generation := int64(0)
		task := &model.Task{Type: model.TaskTypeStorageDataSetEnsure, InputVersion: 1, SubjectType: "storage_data_set", SubjectKey: strconv.FormatInt(binding.ID, 10), Generation: &generation, Input: raw, Runtime: json.RawMessage(`{}`), Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover, AvailableAt: now, FinishedAt: &now}
		if _, err := runtime.db.NewInsert().Model(task).Exec(ctx); err != nil {
			return err
		}
		if _, err := runtime.db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("ensure_task_id = ?", task.ID).Where("id = ? AND ensure_task_id IS NULL", binding.ID).Exec(ctx); err != nil {
			return err
		}
	}
	return runtime.repos.Contents.MarkDataSetReady(ctx, input)
}
