package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
)

const (
	replicaRepairDataSetIDKey = "storage_data_set_id"
	replicaRepairCopyIDKey    = "storage_upload_copy_id"
)

type replicaRepairPayload struct {
	dataSetID int64
	copyID    int64
}

func replicaRepairTaskKey(dataSetID int64) string {
	return fmt.Sprintf("upload:repair-data-set:%d", dataSetID)
}

func newReplicaRepairPayload(dataSetID, copyID int64) map[string]interface{} {
	return map[string]interface{}{
		replicaRepairDataSetIDKey: dataSetID,
		replicaRepairCopyIDKey:    copyID,
	}
}

func parseReplicaRepairPayload(task *model.Task) (replicaRepairPayload, error) {
	if task == nil {
		return replicaRepairPayload{}, errors.New("replica repair task is required")
	}
	dataSetID, err := payloadInt64(task.Payload, replicaRepairDataSetIDKey)
	if err != nil {
		return replicaRepairPayload{}, fmt.Errorf("invalid %s: %w", replicaRepairDataSetIDKey, err)
	}
	if dataSetID <= 0 {
		return replicaRepairPayload{}, fmt.Errorf("invalid %s: must be positive", replicaRepairDataSetIDKey)
	}
	copyID, err := payloadInt64(task.Payload, replicaRepairCopyIDKey)
	if err != nil {
		return replicaRepairPayload{}, fmt.Errorf("invalid %s: %w", replicaRepairCopyIDKey, err)
	}
	if copyID <= 0 {
		return replicaRepairPayload{}, fmt.Errorf("invalid %s: must be positive", replicaRepairCopyIDKey)
	}
	return replicaRepairPayload{dataSetID: dataSetID, copyID: copyID}, nil
}

func (u *Uploader) ensureReplicaRepairTask(ctx context.Context, binding *model.StorageDataSet, maxRetries int) error {
	return ensureReplicaRepairTask(ctx, u.repos, binding, maxRetries)
}

func ensureReplicaRepairTask(ctx context.Context, repos *repository.Repositories, binding *model.StorageDataSet, maxRetries int) error {
	if binding == nil || binding.ID <= 0 || !dataSetBindingEstablished(binding) {
		return nil
	}
	if binding.Status != model.StorageDataSetStatusUnavailable && binding.Status != model.StorageDataSetStatusReady {
		return nil
	}
	copyRow, err := repos.Uploads.NextFinalizableCopyForDataSet(ctx, binding.ID)
	if err != nil {
		return fmt.Errorf("select replica finalization copy for data set %d: %w", binding.ID, err)
	}
	if copyRow == nil {
		copyRow, err = repos.Uploads.NextIncompleteCopyForDataSet(ctx, binding.ID)
		if err != nil {
			return fmt.Errorf("select replica repair copy for data set %d: %w", binding.ID, err)
		}
	}
	if copyRow == nil {
		return nil
	}
	upload, err := repos.Uploads.GetByID(ctx, copyRow.UploadID)
	if err != nil {
		return fmt.Errorf("load replica repair upload %d: %w", copyRow.UploadID, err)
	}
	if upload == nil || upload.SourceVersionID == "" {
		return fmt.Errorf("load replica repair upload %d: %w", copyRow.UploadID, repository.ErrNotFound)
	}
	stage := uploadStageRepairReplica
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "bucket",
		RefID:          binding.BucketID,
		RefVersionID:   upload.SourceVersionID,
		IdempotencyKey: replicaRepairTaskKey(binding.ID),
		Payload:        newReplicaRepairPayload(binding.ID, copyRow.ID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    time.Now(),
	}
	if _, err := repos.Tasks.EnsureRecurring(ctx, task); err != nil {
		return fmt.Errorf("ensure replica repair task for data set %d: %w", binding.ID, err)
	}
	return nil
}

func (u *Uploader) processReplicaRepairTask(ctx context.Context, task *model.Task, logger *slog.Logger) {
	payload, err := parseReplicaRepairPayload(task)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "parse replica repair task", err)
		return
	}
	binding, err := u.repos.Uploads.GetDataSetBindingByID(ctx, payload.dataSetID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load replica repair data set", err)
		return
	}
	if binding == nil {
		u.advanceReplicaRepairTask(ctx, task, payload.dataSetID, logger)
		return
	}
	if task.RefType != "bucket" || task.RefID != binding.BucketID {
		u.handleTaskFailure(ctx, task, logger, "validate replica repair task", errors.New("task bucket identity does not match its data set"))
		return
	}
	copyRow, err := u.repos.Uploads.GetUploadCopyByID(ctx, payload.copyID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load replica repair copy", err)
		return
	}
	if copyRow == nil || copyRow.StorageDataSetID == nil || *copyRow.StorageDataSetID != binding.ID || copyRow.Status == model.StorageUploadCopyStatusFailed {
		u.advanceReplicaRepairTask(ctx, task, binding.ID, logger)
		return
	}
	if copyRow.CopyIndex != binding.CopyIndex {
		u.handleTaskFailure(ctx, task, logger, "validate replica repair copy", errors.New("copy slot does not match its data set"))
		return
	}
	if !copyCommitted(copyRow) {
		ordinaryTaskRunning, err := u.repos.Tasks.HasEarlierRunningUploadCopyTask(ctx, task, copyRow.UploadID, copyRow.CopyIndex)
		if err != nil {
			u.handleTaskFailure(ctx, task, logger, "check ordinary upload copy task", err)
			return
		}
		if ordinaryTaskRunning {
			u.waitForStorageDependency(ctx, task, logger, "Waiting for the active upload operation to finish")
			return
		}
	}
	if binding.DataSetID == nil || binding.DataSetID.IsZero() {
		u.handleTaskFailure(ctx, task, logger, "validate replica repair data set", errors.New("established data set has no data set ID"))
		return
	}
	alreadyCommitted := copyCommitted(copyRow)
	switch binding.Status {
	case model.StorageDataSetStatusReady, model.StorageDataSetStatusUnavailable:
	case model.StorageDataSetStatusDraining:
		if !alreadyCommitted {
			completeWorkerTask(ctx, u.repos, task, "uploader", logger)
			return
		}
	case model.StorageDataSetStatusRetired, model.StorageDataSetStatusFailed:
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	default:
		u.handleTaskFailure(ctx, task, logger, "validate replica repair data set", fmt.Errorf("data set status %s cannot be recovered in place", binding.Status))
		return
	}
	upload, err := u.repos.Uploads.GetByID(ctx, copyRow.UploadID)
	if err != nil || upload == nil {
		if err == nil {
			err = fmt.Errorf("storage upload %d not found", copyRow.UploadID)
		}
		u.handleTaskFailure(ctx, task, logger, "load replica repair upload", err)
		return
	}
	if upload.BucketID != binding.BucketID {
		u.handleTaskFailure(ctx, task, logger, "validate replica repair upload", errors.New("upload bucket does not match its data set"))
		return
	}
	bucket, err := u.repos.Buckets.GetByID(ctx, upload.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = fmt.Errorf("bucket %d not found", upload.BucketID)
		}
		u.handleTaskFailure(ctx, task, logger, "load replica repair bucket", err)
		return
	}
	if alreadyCommitted {
		if binding.Status == model.StorageDataSetStatusUnavailable {
			if _, err := u.contextForReadyBinding(ctx, binding, bucket.Name); err != nil {
				u.handleReplicaRepairDataSetFailure(ctx, task, binding, logger, "verify committed replica context", err)
				return
			}
			recovered, err := u.repos.Uploads.RecoverDataSet(ctx, repository.MarkDataSetReadyInput{
				ID:        binding.ID,
				UploadID:  upload.ID,
				DataSetID: *binding.DataSetID,
			})
			if err != nil {
				u.handleTaskFailure(ctx, task, logger, "recover committed replica data set", err)
				return
			}
			if !recovered {
				latest, loadErr := u.repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
				if loadErr != nil {
					u.handleTaskFailure(ctx, task, logger, "reload committed replica data set", loadErr)
					return
				}
				if latest == nil {
					u.handleTaskFailure(ctx, task, logger, "reload committed replica data set", repository.ErrNotFound)
					return
				}
				switch latest.Status {
				case model.StorageDataSetStatusReady:
					binding = latest
				case model.StorageDataSetStatusUnavailable:
					u.waitForStorageDependency(ctx, task, logger, "Waiting for the assigned storage provider to recover")
					return
				case model.StorageDataSetStatusDraining:
					binding = latest
				case model.StorageDataSetStatusRetired, model.StorageDataSetStatusFailed:
					completeWorkerTask(ctx, u.repos, task, "uploader", logger)
					return
				default:
					u.handleTaskFailure(ctx, task, logger, "recover committed replica data set", fmt.Errorf("data set status changed to %s", latest.Status))
					return
				}
			} else {
				binding.Status = model.StorageDataSetStatusReady
			}
		}
		if err := u.finishReplicaRepairItem(ctx, task, upload, binding.ID, logger); err != nil {
			u.handleTaskFailure(ctx, task, logger, "finalize committed replica repair", err)
		}
		return
	}
	storageCtx, err := u.contextForReadyBinding(ctx, binding, bucket.Name)
	if err != nil {
		u.handleReplicaRepairDataSetFailure(ctx, task, binding, logger, "restore replica context", err)
		return
	}
	if err := u.repairReplicaCopy(ctx, task, upload, bucket, binding, copyRow, storageCtx, logger); err != nil {
		u.handleReplicaRepairDataSetFailure(ctx, task, binding, logger, "repair replica copy", err)
	}
}

func (u *Uploader) repairReplicaCopy(
	ctx context.Context,
	task *model.Task,
	upload *model.StorageUpload,
	bucket *model.Bucket,
	binding *model.StorageDataSet,
	copyRow *model.StorageUploadCopy,
	storageCtx synapse.UploadContext,
	logger *slog.Logger,
) error {
	readableCopies, err := u.repos.Uploads.ListReadableCommittedCopies(ctx, upload.ID)
	if err != nil {
		return err
	}
	var pieceCID cid.Cid
	var pieceCIDString string
	if !copyHasPiece(copyRow) {
		var extraHex string
		if len(readableCopies) > 0 {
			sourceCopy := readableCopies[0]
			pieceCID, err = cid.Decode(sourceCopy.PieceCID)
			if err != nil {
				return fmt.Errorf("decode source piece CID: %w", err)
			}
			pieceCIDString = sourceCopy.PieceCID
			pieces := []storage.PieceInput{{PieceCID: pieceCID}}
			extraData, encodedExtra, err := u.extraDataForCopy(ctx, storageCtx, upload.ID, copyRow.CopyIndex, pieces)
			if err != nil {
				return err
			}
			extraHex = encodedExtra
			if _, err := storageCtx.Pull(ctx, storage.PullRequest{
				Pieces:    []cid.Cid{pieceCID},
				ExtraData: extraData,
				From: func(cid.Cid) string {
					return sourceCopy.RetrievalURL
				},
			}); err != nil {
				return err
			}
		} else {
			version, err := u.repos.Objects.GetVersionByID(ctx, upload.SourceVersionID)
			if err != nil {
				return err
			}
			if version == nil {
				u.waitForReplicaRepairSource(ctx, task, logger)
				return nil
			}
			rc, _, err := u.cache.Get(ctx, bucket.Name, version.CacheKey)
			if err != nil {
				if os.IsNotExist(err) {
					if version.InCache {
						if markErr := u.repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); markErr != nil {
							logger.Warn("failed to mark cache location absent", "versionID", version.VersionID, "error", markErr)
						}
					}
					u.waitForReplicaRepairSource(ctx, task, logger)
					return nil
				}
				return fmt.Errorf("open retained cache data: %w", err)
			}
			result, storeErr := storageCtx.Store(ctx, rc, &storage.StoreOptions{})
			closeErr := rc.Close()
			if storeErr != nil {
				return storeErr
			}
			if closeErr != nil {
				return fmt.Errorf("close retained cache data: %w", closeErr)
			}
			if result == nil || !result.PieceCID.Defined() {
				return errors.New("replica repair store returned no piece CID")
			}
			pieceCID = result.PieceCID
			pieceCIDString = pieceCID.String()
			_, extraHex, err = u.extraDataForCopy(
				ctx,
				storageCtx,
				upload.ID,
				copyRow.CopyIndex,
				[]storage.PieceInput{{PieceCID: pieceCID}},
			)
			if err != nil {
				return err
			}
		}
		if err := u.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			StorageUploadCopyID: copyRow.ID,
			UploadID:            upload.ID,
			CopyIndex:           copyRow.CopyIndex,
			PieceCID:            pieceCIDString,
			RetrievalURL:        storageCtx.PieceURL(pieceCID),
		}); err != nil {
			return err
		}
		if err := u.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
			StorageUploadCopyID: copyRow.ID,
			UploadID:            upload.ID,
			CopyIndex:           copyRow.CopyIndex,
			CommitExtraDataHex:  extraHex,
		}); err != nil {
			return err
		}
		copyRow.Status = model.StorageUploadCopyStatusPieceReady
		copyRow.CommitExtraDataHex = &extraHex
	} else {
		if upload.PieceCID == nil || *upload.PieceCID == "" {
			u.waitForReplicaRepairSource(ctx, task, logger)
			return nil
		}
		pieceCIDString = *upload.PieceCID
		pieceCID, err = cid.Decode(pieceCIDString)
		if err != nil {
			return fmt.Errorf("decode stored piece CID: %w", err)
		}
	}
	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	result, err := u.commitReplicaRepairCopy(ctx, upload, binding, copyRow, storageCtx, pieces)
	if err != nil {
		return err
	}
	if result == nil || len(result.PieceIDs) == 0 {
		return errors.New("replica repair commit returned no piece ID")
	}
	pieceID := idtypes.OnChainIDFromSDK(result.PieceIDs[0])
	if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copyRow.ID,
		UploadID:            upload.ID,
		CopyIndex:           copyRow.CopyIndex,
		PieceCID:            pieceCIDString,
		PieceID:             &pieceID,
		RetrievalURL:        storageCtx.PieceURL(pieceCID),
		CommitExtraDataHex:  derefString(copyRow.CommitExtraDataHex),
		CommitTransactionID: result.TransactionID,
	}); err != nil {
		return err
	}
	if binding.Status == model.StorageDataSetStatusUnavailable {
		recovered, err := u.repos.Uploads.RecoverDataSet(ctx, repository.MarkDataSetReadyInput{
			ID:        binding.ID,
			UploadID:  upload.ID,
			DataSetID: *binding.DataSetID,
		})
		if err != nil {
			return fmt.Errorf("mark replica data set recovered: %w", err)
		}
		if !recovered {
			latest, loadErr := u.repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
			if loadErr != nil {
				return fmt.Errorf("reload replica data set after recovery conflict: %w", loadErr)
			}
			if latest == nil {
				return fmt.Errorf("reload replica data set after recovery conflict: %w", repository.ErrNotFound)
			}
			switch latest.Status {
			case model.StorageDataSetStatusReady:
				binding = latest
			case model.StorageDataSetStatusUnavailable:
				u.waitForStorageDependency(ctx, task, logger, "Waiting for the assigned storage provider to recover")
				return nil
			case model.StorageDataSetStatusDraining:
				binding = latest
			case model.StorageDataSetStatusRetired, model.StorageDataSetStatusFailed:
				completeWorkerTask(ctx, u.repos, task, "uploader", logger)
				return nil
			default:
				return fmt.Errorf("replica data set status changed to %s during recovery", latest.Status)
			}
		} else {
			binding.Status = model.StorageDataSetStatusReady
		}
	}
	return u.finishReplicaRepairItem(ctx, task, upload, binding.ID, logger)
}

func (u *Uploader) finishReplicaRepairItem(
	ctx context.Context,
	task *model.Task,
	upload *model.StorageUpload,
	dataSetID int64,
	logger *slog.Logger,
) error {
	if _, err := u.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    upload.BucketID,
		ContentSize: upload.ContentSize,
		Checksum:    upload.Checksum,
	}); err != nil {
		return err
	}
	version, err := u.repos.Objects.GetVersionByID(ctx, upload.SourceVersionID)
	if err != nil {
		return err
	}
	if version == nil {
		return fmt.Errorf("load source version %s: %w", upload.SourceVersionID, repository.ErrNotFound)
	}
	ref := repository.ObjectVersionRef{ObjectID: version.ObjectID, VersionID: version.VersionID}
	_, needsPreparation, err := u.scheduleRemainingPeerCopies(ctx, ref, upload.BucketID, upload.ID, task.MaxRetries)
	if err != nil {
		return err
	}
	if needsPreparation {
		if err := u.enqueueRepairUploadForVersion(ctx, ref, task.MaxRetries, upload.ID); err != nil {
			return err
		}
	}
	if _, _, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, u.finalizeUploadInput(upload.ID)); err != nil {
		return err
	}
	u.advanceReplicaRepairTask(ctx, task, dataSetID, logger)
	return nil
}

func (u *Uploader) waitForReplicaRepairSource(ctx context.Context, task *model.Task, logger *slog.Logger) {
	u.waitForStorageDependency(ctx, task, logger, "Waiting for a readable replica or retained cache data")
}

func (u *Uploader) commitReplicaRepairCopy(
	ctx context.Context,
	upload *model.StorageUpload,
	binding *model.StorageDataSet,
	copyRow *model.StorageUploadCopy,
	storageCtx synapse.UploadContext,
	pieces []storage.PieceInput,
) (*storage.CommitResult, error) {
	if copyCommitSubmitted(copyRow) {
		transactionID := *copyRow.CommitTransactionID
		result, err := u.waitForSubmittedCommit(ctx, storageCtx, binding, transactionID, len(pieces))
		if errors.Is(err, errCommitRejected) {
			if resetErr := u.resetRejectedSubmittedCommit(ctx, upload.ID, copyRow.CopyIndex, transactionID, err); resetErr != nil {
				return nil, fmt.Errorf("reset rejected replica repair commit: %w", resetErr)
			}
		}
		return result, err
	}
	extraData, extraHex, err := u.extraDataForCopy(ctx, storageCtx, upload.ID, copyRow.CopyIndex, pieces)
	if err != nil {
		return nil, err
	}
	var submittedTx string
	var submitErr error
	result, err := storageCtx.Commit(ctx, storage.CommitRequest{
		Pieces:    pieces,
		ExtraData: extraData,
		OnSubmitted: func(txHash string) {
			submittedTx = txHash
			submitErr = u.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
				StorageUploadCopyID: copyRow.ID,
				UploadID:            upload.ID,
				CopyIndex:           copyRow.CopyIndex,
				CommitExtraDataHex:  extraHex,
				CommitTransactionID: txHash,
			})
		},
	})
	if submittedTx != "" && submitErr != nil {
		return nil, fmt.Errorf("save replica repair commit submission: %w", submitErr)
	}
	if errors.Is(err, errCommitRejected) && submittedTx != "" {
		if resetErr := u.resetRejectedSubmittedCommit(ctx, upload.ID, copyRow.CopyIndex, submittedTx, err); resetErr != nil {
			return nil, fmt.Errorf("reset rejected replica repair commit: %w", resetErr)
		}
	}
	return result, err
}

func (u *Uploader) handleReplicaRepairDataSetFailure(ctx context.Context, task *model.Task, binding *model.StorageDataSet, logger *slog.Logger, stage string, err error) {
	if u.waitForPendingSubmittedCommit(ctx, task, logger, err) {
		return
	}
	switch {
	case dataSetWriteBlockedError(err), synapse.IsDataSetServiceEnded(err):
		latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusDraining, err.Error())
		if markErr != nil {
			u.handleTaskFailure(ctx, task, logger, "mark replica data set draining", markErr)
			return
		}
		switch latest.Status {
		case model.StorageDataSetStatusDraining, model.StorageDataSetStatusRetired, model.StorageDataSetStatusFailed:
			completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		case model.StorageDataSetStatusReady, model.StorageDataSetStatusUnavailable:
			u.waitForStorageDependency(ctx, task, logger, "Waiting to retry the storage operation")
		default:
			u.handleTaskFailure(ctx, task, logger, stage, fmt.Errorf("data set status changed to %s: %w", latest.Status, err))
		}
	case synapse.IsProviderUnavailable(err), synapse.IsNoProviderCandidates(err):
		latest, markErr := u.markDataSetStatus(ctx, binding, model.StorageDataSetStatusUnavailable, err.Error())
		if markErr != nil {
			u.handleTaskFailure(ctx, task, logger, "mark replica data set unavailable", markErr)
			return
		}
		switch latest.Status {
		case model.StorageDataSetStatusUnavailable:
			u.waitForStorageDependency(ctx, task, logger, "Waiting for the assigned storage provider to recover")
		case model.StorageDataSetStatusReady:
			u.waitForStorageDependency(ctx, task, logger, "Waiting to retry the storage operation")
		case model.StorageDataSetStatusDraining, model.StorageDataSetStatusRetired, model.StorageDataSetStatusFailed:
			completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		default:
			u.handleTaskFailure(ctx, task, logger, stage, fmt.Errorf("data set status changed to %s: %w", latest.Status, err))
		}
	default:
		u.handleTaskFailure(ctx, task, logger, stage, err)
	}
}

func (u *Uploader) advanceReplicaRepairTask(ctx context.Context, task *model.Task, dataSetID int64, logger *slog.Logger) {
	var nextCopyID int64
	err := u.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if err := txRepos.Tasks.LockRunningClaim(ctx, task); err != nil {
			return err
		}
		next, err := txRepos.Uploads.NextFinalizableCopyForDataSet(ctx, dataSetID)
		if err != nil {
			return err
		}
		if next == nil {
			next, err = txRepos.Uploads.NextIncompleteCopyForDataSet(ctx, dataSetID)
			if err != nil {
				return err
			}
		}
		if next == nil {
			return txRepos.Tasks.Complete(ctx, task)
		}
		upload, err := txRepos.Uploads.GetByID(ctx, next.UploadID)
		if err != nil {
			return err
		}
		if upload == nil || upload.SourceVersionID == "" {
			return fmt.Errorf("load next replica repair upload %d: %w", next.UploadID, repository.ErrNotFound)
		}
		nextCopyID = next.ID
		return txRepos.Tasks.ContinueRunning(ctx, task, upload.SourceVersionID, newReplicaRepairPayload(dataSetID, next.ID))
	})
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "advance replica repair task", err)
		return
	}
	if nextCopyID > 0 {
		logger.Debug("queued next replica repair item", "dataSetID", dataSetID, "copyID", nextCopyID)
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}
