package repository

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

// Logical task references are validated under the transaction that binds them.
func taskForBinding(ctx context.Context, db bun.IDB, taskID int64, subjectType, subjectKey string, types ...model.TaskType) (*model.Task, error) {
	if taskID < 1 {
		return nil, ErrInvalidInput
	}
	task, err := (&BunTaskRepo{db: db}).GetForUpdate(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if task == nil || (task.Status != model.TaskStatusPending && task.Status != model.TaskStatusRunning) || task.SupersededAt != nil || !slices.Contains(types, task.Type) {
		return nil, ErrConflict
	}
	if task.SubjectType == nil || task.SubjectKey == nil || *task.SubjectType != subjectType || *task.SubjectKey != subjectKey {
		return nil, ErrConflict
	}
	return task, nil
}

func validateTransferredTask(ctx context.Context, db bun.IDB, oldID, newID int64) error {
	if oldID < 1 || newID <= oldID {
		return ErrInvalidInput
	}
	repo := &BunTaskRepo{db: db}
	previous, err := repo.GetForUpdate(ctx, oldID)
	if err != nil {
		return err
	}
	next, err := repo.GetForUpdate(ctx, newID)
	if err != nil {
		return err
	}
	if previous == nil || next == nil || next.RetryOfTaskID == nil || *next.RetryOfTaskID != previous.ID {
		return ErrConflict
	}
	return nil
}

func validateCopyTaskBinding(ctx context.Context, db bun.IDB, copyID, generation, taskID int64) error {
	task, err := taskForBinding(ctx, db, taskID, model.TaskSubjectStorageCopy, strconv.FormatInt(copyID, 10), model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull)
	if err != nil {
		return err
	}
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(task.Input, &input); err != nil || input.CopyID != copyID || input.Generation != generation {
		return ErrConflict
	}
	return nil
}

func validateDataSetTaskBinding(ctx context.Context, db bun.IDB, dataSetID, generation, taskID int64, taskType model.TaskType) error {
	task, err := taskForBinding(ctx, db, taskID, "storage_data_set", strconv.FormatInt(dataSetID, 10), taskType)
	if err != nil {
		return err
	}
	if taskType == model.TaskTypeStorageDataSetEnsure {
		var input storagepipeline.DataSetInput
		if err := json.Unmarshal(task.Input, &input); err != nil || input.DataSetID != dataSetID {
			return ErrConflict
		}
		return nil
	}
	input, err := storagereplacement.ParseRetireInput(task)
	if err != nil || input.DataSetID != dataSetID || input.Generation != generation {
		return ErrConflict
	}
	return nil
}
