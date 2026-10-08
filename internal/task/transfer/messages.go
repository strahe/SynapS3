package transfer

import (
	"context"
	"errors"
	"fmt"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func NewStartCopyTransferReceiver(coordinator *CopyCoordinator) (taskengine.MessageHandler, error) {
	if coordinator == nil {
		return nil, errors.New("start copy transfer requires a copy coordinator")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		request, ok := message.(storagepipeline.StartCopyTransfer)
		if !ok || request.CopyID < 1 {
			return errors.New("invalid start copy transfer message")
		}
		if request.RecoveryTaskID != 0 {
			return coordinator.enqueueRecoveryCopyTaskAt(ctx, tx, request.CopyID, request.RecoveryTaskID, request.AvailableAt)
		}
		return coordinator.enqueueCopyTaskAt(ctx, tx, request.CopyID, model.TaskTypeStorageTransferPlan, request.AvailableAt)
	}, nil
}

func NewFailCopyReceiver(coordinator *CopyCoordinator) (taskengine.MessageHandler, error) {
	if coordinator == nil {
		return nil, errors.New("fail copy requires a copy coordinator")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		request, ok := message.(storagepipeline.FailCopy)
		if !ok || request.CopyID < 1 {
			return errors.New("invalid fail copy message")
		}
		copyRow, err := tx.Contents.GetUploadCopyByID(ctx, request.CopyID)
		if err != nil {
			return err
		}
		if copyRow == nil {
			return repository.ErrNotFound
		}
		return coordinator.failCopy(ctx, tx, copyRow, request.Message, request.PullAttemptID)
	}, nil
}

func NewDataSetReadySubscriber(coordinator *CopyCoordinator) (taskengine.MessageHandler, error) {
	if coordinator == nil {
		return nil, errors.New("data set ready continuation requires a copy coordinator")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		ready, ok := message.(storagepipeline.DataSetReady)
		if !ok || ready.BindingID < 1 {
			return errors.New("invalid data set ready message")
		}
		binding, err := tx.Contents.GetDataSetBindingByID(ctx, ready.BindingID)
		if err != nil {
			return err
		}
		if binding == nil || binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || binding.DataSetID.IsZero() {
			return fmt.Errorf("data set is not ready: %w", repository.ErrConflict)
		}
		copies, err := tx.Contents.ListIncompleteCopiesForDataSet(ctx, ready.BindingID)
		if err != nil {
			return err
		}
		ids := make([]int64, 0, len(copies))
		for i := range copies {
			copyRow := &copies[i]
			if copyRow.ContinuationKind() != model.CopyContinuationTransfer {
				continue
			}
			if copyRow.ActiveTaskID != nil {
				ids = append(ids, *copyRow.ActiveTaskID)
				continue
			}
			if err := coordinator.enqueueInitialCopyTask(ctx, tx, copyRow.ID, model.TaskTypeStorageTransferPlan); err != nil && !errors.Is(err, repository.ErrConflict) {
				return err
			}
		}
		_, err = coordinator.wake(ctx, tx, ids)
		return err
	}, nil
}

func NewContentCommittedSubscriber(coordinator *CopyCoordinator) (taskengine.MessageHandler, error) {
	if coordinator == nil {
		return nil, errors.New("content commit continuation requires a copy coordinator")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		committed, ok := message.(storagepipeline.ContentCommitted)
		if !ok || committed.ContentID < 1 || committed.BucketID < 1 {
			return errors.New("invalid content committed message")
		}
		if committed.IngressCommitted {
			failed, err := tx.Contents.ReopenFailedIngressForPull(ctx, committed.ContentID)
			if err != nil {
				return err
			}
			for _, copyRow := range failed {
				if err := coordinator.enqueueInitialCopyTask(ctx, tx, copyRow.ID, model.TaskTypeStorageTransferPlan); err != nil {
					return err
				}
			}
		}
		return coordinator.wakePeerPullPlans(ctx, tx, committed.ContentID)
	}, nil
}
