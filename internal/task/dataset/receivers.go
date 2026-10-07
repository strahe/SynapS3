package dataset

import (
	"context"
	"errors"
	"fmt"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type EnsureReceiverDependencies struct{ Scheduler *taskengine.Scheduler }

func NewEnsureDataSetReceiver(deps EnsureReceiverDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("data set ensure scheduler is required")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		msg, ok := message.(storagepipeline.EnsureDataSet)
		if !ok || msg.BindingID < 1 {
			return repository.ErrInvalidInput
		}
		row, err := tx.Contents.GetDataSetBindingByID(ctx, msg.BindingID)
		if err != nil {
			return err
		}
		if row == nil {
			return repository.ErrNotFound
		}
		taskRow, _, err := deps.Scheduler.EnqueueInTransaction(ctx, tx, taskengine.EnqueueRequest{
			Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(row.ID),
			Input: storagepipeline.DataSetInput{DataSetID: row.ID}, SubjectType: "storage_data_set", SubjectKey: fmt.Sprintf("%d", row.ID),
		})
		if err != nil {
			return err
		}
		return tx.Contents.BindDataSetEnsureTask(ctx, row.ID, taskRow.ID)
	}, nil
}

type RetireReceiverDependencies struct{ Scheduler *taskengine.Scheduler }

func NewRetireDataSetReceiver(deps RetireReceiverDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("data set retirement scheduler is required")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		msg, ok := message.(storagereplacement.RetireDataSet)
		if !ok || msg.BindingID < 1 || msg.ReplacementID < 1 {
			return repository.ErrInvalidInput
		}
		row, err := tx.Contents.GetDataSetBindingByID(ctx, msg.BindingID)
		if err != nil {
			return err
		}
		if row == nil {
			return repository.ErrNotFound
		}
		if row.RetirementTaskID != nil || row.Status == model.StorageDataSetStatusRetired {
			return nil
		}
		generation, err := tx.Contents.NextDataSetRetirementGeneration(ctx, row.ID)
		if err != nil {
			return err
		}
		taskRow, _, err := deps.Scheduler.EnqueueInTransaction(ctx, tx, taskengine.EnqueueRequest{
			Type: model.TaskTypeStorageDataSetRetire, IdempotencyKey: storagereplacement.RetireTaskKey(row.ID, generation),
			Input:       storagereplacement.RetireInput{ReplacementID: msg.ReplacementID, DataSetID: row.ID, Generation: generation},
			SubjectType: "storage_data_set", SubjectKey: fmt.Sprintf("%d", row.ID),
		})
		if err != nil {
			return err
		}
		return tx.Contents.BindDataSetRetirementTask(ctx, row.ID, generation, taskRow.ID)
	}, nil
}
