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
	"github.com/strahe/synaps3/internal/objectlimits"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
)

// One bounded window of upload history per seeding pass. Small enough that the
// write stays short on SQLite's single writer, large enough that a long history
// still drains in a reasonable number of passes.
const replacementSeedBatchSize = 200

// processReplacementTask advances one approved replacement by exactly one step.
// The coordinator is a singleton per replacement, so a replacement can never
// have more than one item in flight and ordinary uploads keep their place in
// the queue.
func (u *Uploader) processReplacementTask(ctx context.Context, task *model.Task, logger *slog.Logger) {
	if task == nil || task.ClaimedAt == nil {
		return
	}
	payload, err := storagereplacement.ParseMigratePayload(task)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "parse replacement migration task", err)
		return
	}
	replacement, err := u.repos.Replacements.GetByID(ctx, payload.ReplacementID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load provider replacement", err)
		return
	}
	if replacement == nil || replacement.Status == storagereplacement.StatusCompleted || replacement.Status.Retryable() {
		// A finished or operator-owned replacement has no work the coordinator
		// may do on its own.
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	if replacement.Status == storagereplacement.StatusSuperseded {
		// The successor owns the slot. This coordinator's leftover is the unused
		// target, which must still be ended even if confirmation already queued it.
		if err := u.ensureAbandonedTargetTask(ctx, replacement.ID, replacement.BucketID, task.MaxRetries); err != nil {
			u.handleReplacementTaskFailure(ctx, task, 0, logger, "queue abandoned replacement cleanup", err)
			return
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	target, err := u.repos.Uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "load replacement target", err)
		return
	}
	if target == nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "load replacement target",
			fmt.Errorf("target data set %d: %w", replacement.TargetDataSetID, repository.ErrNotFound))
		return
	}

	switch replacementPhase(replacement.Status, target) {
	case storagereplacement.PhasePrepare:
		u.prepareReplacementTarget(ctx, task, replacement, target, logger)
	case storagereplacement.PhaseMigrate:
		u.migrateReplacementItem(ctx, task, replacement, target, payload, logger)
	case storagereplacement.PhaseRetire:
		// Retirement belongs to the cleanup worker; the migration coordinator's
		// job is finished once it has handed over.
		if err := u.ensureReplacementRetirementTask(ctx, replacement.ID, replacement.BucketID, task.MaxRetries); err != nil {
			u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "queue replacement retirement", err)
			return
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
	default:
		// An unrecognised phase is a bug, not an outage. Stop the task without
		// touching the replacement so the record still describes reality.
		u.failReplacementTaskWithoutMutation(ctx, task, logger,
			fmt.Errorf("replacement %d has no coordinator work in status %s", replacement.ID, replacement.Status))
	}
}

// A waiting replacement resumes at the phase it was waiting in, which is
// recovered from the data rather than remembered in the status.
func replacementPhase(status storagereplacement.Status, target *model.StorageDataSet) storagereplacement.Phase {
	if phase := storagereplacement.PhaseFor(status); phase != storagereplacement.PhaseNone {
		return phase
	}
	if status != storagereplacement.StatusWaiting {
		return storagereplacement.PhaseNone
	}
	if target != nil && target.IsCurrent {
		return storagereplacement.PhaseMigrate
	}
	return storagereplacement.PhasePrepare
}

// prepareReplacementTarget creates the approved service and, once it is
// writable, switches the slot over. Until that switch every write still goes to
// the source, so a failure here costs nothing but time.
func (u *Uploader) prepareReplacementTarget(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	logger *slog.Logger,
) {
	bucket, err := u.repos.Buckets.GetByID(ctx, replacement.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = fmt.Errorf("bucket %d: %w", replacement.BucketID, repository.ErrNotFound)
		}
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "load replacement bucket", err)
		return
	}
	if target.Status != model.StorageDataSetStatusReady {
		if target.Status == model.StorageDataSetStatusPending || target.Status == model.StorageDataSetStatusFailed {
			if !u.ensureReplacementFundingReady(ctx, task, replacement, target, bucket, logger) {
				return
			}
		}
		ready, err := u.createReplacementDataSet(ctx, replacement, target, bucket)
		if err != nil {
			if errors.Is(err, storagereplacement.ErrTargetInUse) {
				// Retrying cannot change this: the operator has to pick a
				// provider that is free. The source still owns the replica at
				// this point, so a new confirmation is available to them.
				u.failReplacementTarget(ctx, task, replacement.ID, logger, err.Error())
				return
			}
			u.handleReplacementProviderFailure(ctx, task, replacement, logger, "create replacement service", err)
			return
		}
		if !ready {
			u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonTargetCreating, logger,
				storagereplacement.WaitReasonTargetCreating.Message())
			return
		}
	}
	if err := u.repos.Replacements.Activate(ctx, replacement.ID); err != nil {
		if errors.Is(err, repository.ErrConflict) {
			u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonTargetWritable, logger,
				storagereplacement.WaitReasonTargetWritable.Message())
			return
		}
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "activate replacement target", err)
		return
	}
	logger.Info("provider replacement activated",
		"replacementID", replacement.ID, "bucketID", replacement.BucketID, "copyIndex", replacement.CopyIndex)
	u.continueReplacementTask(ctx, task, replacement.ID, 0, 0, "", logger)
}

// createReplacementDataSet drives one data set creation step and reports
// whether the service is ready. It mirrors the ordinary upload path but does
// not touch any copy row, because the target holds no data yet.
func (u *Uploader) createReplacementDataSet(
	ctx context.Context,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	bucket *model.Bucket,
) (bool, error) {
	storageCtx, err := u.contextForBindingProvider(ctx, target, bucket.Name)
	if err != nil {
		return false, err
	}
	switch target.Status {
	case model.StorageDataSetStatusPending, model.StorageDataSetStatusFailed:
		// Nothing has been submitted for this generation yet, so a context that
		// already carries a data set can only be somebody else's: this provider
		// still runs a live service for this bucket, a generation released
		// locally without being terminated on chain. A replacement must open its
		// own paid service -- attaching here would leave it paying for, and
		// later retiring, a service it does not own.
		//
		// A creation this replacement did submit is resumed by the creating
		// branch below, which resolves the recorded transaction instead.
		if dataSetID := storageCtx.DataSetID(); dataSetID != nil {
			return false, fmt.Errorf("provider %s already runs data set %s for this bucket: %w",
				target.ProviderID.String(), idtypes.OnChainIDFromSDK(*dataSetID).String(),
				storagereplacement.ErrTargetInUse)
		}
		var submitted storage.CreateDataSetSubmission
		var submitErr error
		result, err := storageCtx.CreateDataSet(ctx, &storage.CreateDataSetOptions{
			OnSubmitted: func(sub storage.CreateDataSetSubmission) {
				submitted = sub
				submitErr = u.repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
					ID:              target.ID,
					TransactionID:   sub.TransactionID,
					StatusURL:       sub.StatusURL,
					ClientDataSetID: onChainIDPtrFromSDKPtr(sub.ClientDataSetID),
				})
			},
		})
		if submitted.TransactionID != "" && submitErr != nil {
			return false, fmt.Errorf("save replacement service submission: %w", submitErr)
		}
		if err != nil {
			return false, err
		}
		return true, u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID:              target.ID,
			DataSetID:       idtypes.OnChainIDFromSDK(result.DataSetID),
			ClientDataSetID: onChainIDPtrFromSDK(result.ClientDataSetID),
		})
	case model.StorageDataSetStatusCreating:
		if target.CreateTransactionID == nil || target.CreateStatusURL == nil || target.ClientDataSetID == nil {
			return false, errDataSetCreationIncomplete
		}
		result, err := storageCtx.WaitForDataSetCreated(ctx, storage.CreateDataSetSubmission{
			TransactionID:   *target.CreateTransactionID,
			StatusURL:       *target.CreateStatusURL,
			ClientDataSetID: sdkBigIntPtr(target.ClientDataSetID),
		})
		if err != nil {
			return false, err
		}
		return true, u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID:              target.ID,
			DataSetID:       idtypes.OnChainIDFromSDK(result.DataSetID),
			ClientDataSetID: onChainIDPtrFromSDK(result.ClientDataSetID),
		})
	default:
		return false, fmt.Errorf("replacement %d target status %s cannot be prepared", replacement.ID, target.Status)
	}
}

// migrateReplacementItem copies one piece of stored content to the target. It
// seeds work lazily so a large history never blocks the first transfer, and it
// holds exactly one item at a time.
func (u *Uploader) migrateReplacementItem(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	payload storagereplacement.MigratePayload,
	logger *slog.Logger,
) {
	if !replacement.SeedingComplete {
		if _, _, err := u.repos.Replacements.SeedMigrationBatch(ctx, replacement.ID, replacementSeedBatchSize); err != nil {
			u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "seed replacement migration", err)
			return
		}
	}
	item, err := u.nextReplacementItem(ctx, replacement.ID, payload.ItemID)
	if err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "select replacement item", err)
		return
	}
	if item == nil {
		u.finishReplacementMigration(ctx, task, replacement, logger)
		return
	}
	snapshot, err := u.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: replacement.ID,
		ItemID:        item.ID,
		TaskID:        task.ID,
		TaskClaimedAt: *task.ClaimedAt,
	})
	if err != nil {
		switch {
		case errors.Is(err, storagereplacement.ErrItemDeferred) && item.Status == storagereplacement.ItemStatusWaitingSource:
			// Already-parked work is only reached once nothing executable is
			// left, so re-queueing here would spin the coordinator against the
			// same item and starve ordinary uploads. Wait for the source
			// instead; the item is rechecked on the next tick.
			u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonReadableSource, logger,
				storagereplacement.WaitReasonReadableSource.Message())
		case errors.Is(err, storagereplacement.ErrItemCancelled), errors.Is(err, storagereplacement.ErrItemDeferred):
			// Either this content no longer needs migrating, or it just moved
			// out of the executable set. Both mean: make progress elsewhere.
			u.continueReplacementTask(ctx, task, replacement.ID, 0, 0, "", logger)
		case errors.Is(err, repository.ErrTaskClaimLost):
			return
		default:
			u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "acquire replacement item", err)
		}
		return
	}

	// An ordinary upload already writing this slot owns it first; the
	// coordinator waits rather than racing it.
	ordinaryRunning, err := u.repos.Tasks.HasEarlierRunningUploadCopyTask(ctx, task, snapshot.Upload.ID, replacement.CopyIndex)
	if err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "check ordinary upload copy task", err)
		return
	}
	if ordinaryRunning {
		u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonSourceWrites, logger,
			storagereplacement.WaitReasonSourceWrites.Message())
		return
	}

	copyRow, err := u.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: replacement.ID,
		ItemID:        item.ID,
		UploadID:      snapshot.Upload.ID,
	})
	if err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "attach replacement target copy", err)
		return
	}
	if copyCommitted(copyRow) {
		u.completeReplacementItem(ctx, task, replacement, &snapshot.Upload, item.ID, logger)
		return
	}

	bucket, err := u.repos.Buckets.GetByID(ctx, replacement.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = fmt.Errorf("bucket %d: %w", replacement.BucketID, repository.ErrNotFound)
		}
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "load replacement bucket", err)
		return
	}
	storageCtx, err := u.contextForReadyBinding(ctx, target, bucket.Name)
	if err != nil {
		u.handleReplacementProviderFailure(ctx, task, replacement, logger, "open replacement target context", err)
		return
	}
	if err := u.copyReplacementItem(ctx, task, replacement, snapshot, copyRow, storageCtx, bucket, logger); err != nil {
		u.handleReplacementProviderFailure(ctx, task, replacement, logger, "copy replacement item", err)
	}
}

// nextReplacementItem keeps the item the payload already names, so a retry
// resumes the same transfer instead of starting a different one.
func (u *Uploader) nextReplacementItem(ctx context.Context, replacementID, assignedItemID int64) (*storagereplacement.Item, error) {
	if assignedItemID > 0 {
		item, err := u.repos.Replacements.NextExecutableItem(ctx, replacementID)
		if err != nil {
			return nil, err
		}
		if item != nil && item.ID == assignedItemID {
			return item, nil
		}
	}
	return u.repos.Replacements.NextExecutableItem(ctx, replacementID)
}

func (u *Uploader) copyReplacementItem(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	snapshot *repository.ReplacementItemSnapshot,
	copyRow *model.StorageUploadCopy,
	storageCtx synapse.UploadContext,
	bucket *model.Bucket,
	logger *slog.Logger,
) error {
	upload := &snapshot.Upload
	version := &snapshot.Version
	var pieceCID cid.Cid
	var pieceCIDString string
	extraHex := derefString(copyRow.CommitExtraDataHex)

	if !copyHasPiece(copyRow) {
		readable, err := u.repos.Uploads.ListReadableCommittedCopies(ctx, upload.ID)
		if err != nil {
			return err
		}
		pulled, pullErr := u.pullReplacementItem(ctx, storageCtx, upload, copyRow,
			orderReplacementSources(readable, snapshot.Target.CopyIndex))
		if pullErr != nil {
			return pullErr
		}
		if pulled != nil {
			pieceCID = pulled.pieceCID
			pieceCIDString = pulled.pieceCIDString
			extraHex = pulled.extraHex
		} else {
			// No remote replica could serve the content, so fall back to data this
			// node still holds.
			stored, storedPieceCID, err := u.storeReplacementItemFromCache(ctx, storageCtx, bucket, version, logger)
			if err != nil {
				return err
			}
			if !stored {
				// Nothing can supply this content yet. Park the item so the
				// coordinator keeps making progress on everything else, and
				// revisit it later.
				if err := u.repos.Replacements.MarkItemWaitingSource(ctx, snapshot.Item.ID,
					"no readable replica or retained cache data"); err != nil {
					return err
				}
				u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonReadableSource, logger,
					storagereplacement.WaitReasonReadableSource.Message())
				return nil
			}
			pieceCID = storedPieceCID
			pieceCIDString = pieceCID.String()
			_, extraHex, err = u.extraDataForCopy(ctx, storageCtx, copyRow,
				[]storage.PieceInput{{PieceCID: pieceCID}})
			if err != nil {
				return err
			}
		}
		if err := u.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			StorageUploadCopyID: copyRow.ID,
			RequireEligibleCopy: true,
			UploadID:            upload.ID,
			CopyIndex:           copyRow.CopyIndex,
			PieceCID:            pieceCIDString,
			RetrievalURL:        storageCtx.PieceURL(pieceCID),
		}); err != nil {
			return err
		}
		if err := u.repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
			StorageUploadCopyID: copyRow.ID,
			RequireEligibleCopy: true,
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
			return fmt.Errorf("storage upload %d has no piece CID", upload.ID)
		}
		pieceCIDString = *upload.PieceCID
		decoded, err := cid.Decode(pieceCIDString)
		if err != nil {
			return fmt.Errorf("decode stored piece CID: %w", err)
		}
		pieceCID = decoded
	}

	pieces := []storage.PieceInput{{PieceCID: pieceCID}}
	result, err := u.commitReplicaRepairCopy(ctx, upload, &snapshot.Target, copyRow, storageCtx, pieces)
	if err != nil {
		return err
	}
	if result == nil || len(result.PieceIDs) == 0 {
		return errors.New("replacement commit returned no piece ID")
	}
	pieceID := idtypes.OnChainIDFromSDK(result.PieceIDs[0])
	if err := u.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copyRow.ID,
		RequireEligibleCopy: true,
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
	u.completeReplacementItem(ctx, task, replacement, upload, snapshot.Item.ID, logger)
	return nil
}

type pulledReplacementPiece struct {
	pieceCID       cid.Cid
	pieceCIDString string
	extraHex       string
}

// pullReplacementItem tries each readable replica in turn. A provider that
// cannot serve the piece is skipped rather than retried, because another
// replica or the local cache may still have it. A nil result with no error
// means no remote replica could supply the content.
func (u *Uploader) pullReplacementItem(
	ctx context.Context,
	storageCtx synapse.UploadContext,
	upload *model.StorageUpload,
	copyRow *model.StorageUploadCopy,
	sources []repository.ReadableStorageCopy,
) (*pulledReplacementPiece, error) {
	var lastErr error
	for i := range sources {
		source := &sources[i]
		pieceCID, err := cid.Decode(source.PieceCID)
		if err != nil {
			lastErr = fmt.Errorf("decode source piece CID: %w", err)
			continue
		}
		extraData, extraHex, err := u.extraDataForCopy(ctx, storageCtx, copyRow,
			[]storage.PieceInput{{PieceCID: pieceCID}})
		if err != nil {
			return nil, err
		}
		// Provider-to-provider transfer keeps the bytes off this node.
		if _, err := storageCtx.Pull(ctx, storage.PullRequest{
			Pieces:    []cid.Cid{pieceCID},
			ExtraData: extraData,
			From: func(cid.Cid) string {
				return source.RetrievalURL
			},
		}); err != nil {
			if replacementSourceUnusable(err) {
				lastErr = err
				continue
			}
			return nil, err
		}
		return &pulledReplacementPiece{pieceCID: pieceCID, pieceCIDString: source.PieceCID, extraHex: extraHex}, nil
	}
	if lastErr != nil && len(sources) > 0 {
		// Every candidate refused. Let the caller try retained cache data before
		// deciding the content has no source at all.
		return nil, nil
	}
	return nil, nil
}

// A source that cannot serve a read is not a reason to fail the migration; it
// only means this replica is not usable right now.
func replacementSourceUnusable(err error) bool {
	return synapse.IsProviderUnavailable(err) ||
		synapse.IsDataSetServiceEnded(err) ||
		synapse.IsNoProviderCandidates(err)
}

// orderReplacementSources prefers a replica on another slot over the draining
// source, and never offers the target itself. A source that recovered
// mid-migration therefore becomes usable again with no special case.
func orderReplacementSources(copies []repository.ReadableStorageCopy, targetCopyIndex int) []repository.ReadableStorageCopy {
	preferred := make([]repository.ReadableStorageCopy, 0, len(copies))
	sameSlot := make([]repository.ReadableStorageCopy, 0, len(copies))
	for i := range copies {
		candidate := copies[i]
		if candidate.PieceCID == "" || candidate.RetrievalURL == "" {
			continue
		}
		if candidate.CopyIndex == targetCopyIndex {
			sameSlot = append(sameSlot, candidate)
			continue
		}
		preferred = append(preferred, candidate)
	}
	return append(preferred, sameSlot...)
}

func (u *Uploader) storeReplacementItemFromCache(
	ctx context.Context,
	storageCtx synapse.UploadContext,
	bucket *model.Bucket,
	version *model.ObjectVersion,
	logger *slog.Logger,
) (bool, cid.Cid, error) {
	if version == nil || version.CacheKey == "" {
		return false, cid.Undef, nil
	}
	rc, _, err := u.cache.Get(ctx, bucket.Name, version.CacheKey)
	if err != nil {
		if os.IsNotExist(err) {
			if version.InCache {
				if markErr := u.repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); markErr != nil {
					logger.Warn("failed to mark cache location absent", "versionID", version.VersionID, "error", markErr)
				}
			}
			return false, cid.Undef, nil
		}
		return false, cid.Undef, fmt.Errorf("open retained cache data: %w", err)
	}
	result, storeErr := storageCtx.Store(ctx, rc, &storage.StoreOptions{})
	closeErr := rc.Close()
	if storeErr != nil {
		return false, cid.Undef, storeErr
	}
	if closeErr != nil {
		return false, cid.Undef, fmt.Errorf("close retained cache data: %w", closeErr)
	}
	if result == nil || !result.PieceCID.Defined() {
		return false, cid.Undef, errors.New("replacement store returned no piece CID")
	}
	return true, result.PieceCID, nil
}

func (u *Uploader) completeReplacementItem(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	upload *model.StorageUpload,
	itemID int64,
	logger *slog.Logger,
) {
	if err := u.repos.Replacements.MarkItemCopied(ctx, itemID); err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "record replacement item copied", err)
		return
	}
	// The target now holds another readable slot, which can complete the
	// upload's durability target.
	if _, _, err := u.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, u.finalizeUploadInput(upload.ID)); err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "finalize migrated upload", err)
		return
	}
	// The coordinator is bucket-scoped. Recording the migrated version here
	// would make the task list point at one arbitrary object.
	u.continueReplacementTask(ctx, task, replacement.ID, 0, 0, "", logger)
}

// finishReplacementMigration hands over to retirement once nothing is owed.
// Seeding must have finished first, otherwise "no items" only means "none
// discovered yet".
func (u *Uploader) finishReplacementMigration(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	logger *slog.Logger,
) {
	if !replacement.SeedingComplete {
		u.continueReplacementTask(ctx, task, replacement.ID, 0, 0, "", logger)
		return
	}
	if err := u.repos.Replacements.BeginRetirement(ctx, replacement.ID); err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "begin replacement retirement", err)
		return
	}
	if err := u.ensureReplacementRetirementTask(ctx, replacement.ID, replacement.BucketID, task.MaxRetries); err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "queue replacement retirement", err)
		return
	}
	logger.Info("provider replacement migration complete", "replacementID", replacement.ID)
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) ensureReplacementRetirementTask(ctx context.Context, replacementID, bucketID int64, maxRetries int) error {
	_, err := u.repos.Tasks.EnsureRecurring(ctx, storagereplacement.NewRetireTask(replacementID, bucketID, maxRetries, time.Now()))
	if err != nil {
		return fmt.Errorf("ensure replacement retirement task for replacement %d: %w", replacementID, err)
	}
	return nil
}

func (u *Uploader) ensureAbandonedTargetTask(ctx context.Context, replacementID, bucketID int64, maxRetries int) error {
	_, err := u.repos.Tasks.EnsureRecurring(ctx, storagereplacement.NewAbandonedTargetTask(replacementID, bucketID, maxRetries, time.Now()))
	if err != nil {
		return fmt.Errorf("ensure abandoned replacement cleanup for replacement %d: %w", replacementID, err)
	}
	return nil
}

// ensureReplacementFundingReady waits, without burning retries, until the wallet
// can pay for the approved service. Ordinary uploads already do this; creating
// a replacement service is the same kind of paid work.
func (u *Uploader) ensureReplacementFundingReady(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	bucket *model.Bucket,
	logger *slog.Logger,
) bool {
	storageCtx, err := u.contextForBindingProvider(ctx, target, bucket.Name)
	if err != nil {
		u.handleReplacementProviderFailure(ctx, task, replacement, logger, "open replacement funding context", err)
		return false
	}
	costs, err := u.storage.PrepareUpload(ctx, uint64(objectlimits.MinFOCUploadSize), []synapse.UploadContext{storageCtx})
	if err != nil {
		u.handleReplacementProviderFailure(ctx, task, replacement, logger, "prepare replacement funding", err)
		return false
	}
	if costs == nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "prepare replacement funding",
			errors.New("missing storage cost estimate"))
		return false
	}
	if costs.Ready {
		return true
	}
	u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonFunding, logger,
		uploadFundingWaitMessage(costs))
	return false
}

// continueReplacementTask re-queues the coordinator at the tail of the upload
// queue. Ordinary uploads that were already due are claimed first, which is how
// migration interleaves without a priority system.
func (u *Uploader) continueReplacementTask(
	ctx context.Context,
	task *model.Task,
	replacementID, itemID, copyID int64,
	versionID string,
	logger *slog.Logger,
) {
	err := u.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if err := txRepos.Tasks.LockRunningClaim(ctx, task); err != nil {
			return err
		}
		return txRepos.Tasks.ContinueRunning(ctx, task, versionID,
			storagereplacement.NewMigratePayload(replacementID, itemID, copyID))
	})
	if err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacementID, logger, "advance replacement task", err)
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

// waitForReplacementDependency records why progress paused without consuming
// retry budget. Waiting is never a failure.
func (u *Uploader) waitForReplacementDependency(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	reason storagereplacement.WaitReason,
	logger *slog.Logger,
	message string,
) {
	if err := u.repos.Replacements.MarkWaiting(ctx, replacement.ID, reason); err != nil {
		logger.Warn("failed to record replacement wait reason",
			"replacementID", replacement.ID, "reason", reason, "error", err)
	}
	u.waitForStorageDependency(ctx, task, logger, message)
}

// handleReplacementProviderFailure keeps a recoverable provider problem in
// waiting and reserves retries for genuinely unknown failures.
func (u *Uploader) handleReplacementProviderFailure(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	logger *slog.Logger,
	stage string,
	err error,
) {
	if u.waitForPendingSubmittedCommit(ctx, task, logger, err) {
		return
	}
	switch {
	case synapse.IsProviderUnavailable(err), synapse.IsNoProviderCandidates(err):
		u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonTarget, logger,
			storagereplacement.WaitReasonTarget.Message())
	case dataSetWriteBlockedError(err), synapse.IsDataSetServiceEnded(err):
		// The approved target itself ended. That needs a new confirmation, so it
		// is operator work rather than another attempt.
		u.markReplacementFailed(ctx, replacement.ID, logger, fmt.Sprintf("%s: %v", stage, err))
		u.failReplacementTaskWithoutMutation(ctx, task, logger, err)
	default:
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, stage, err)
	}
}

// handleReplacementTaskFailure retries, and records the replacement as failed
// only once the task has genuinely run out of attempts.
func (u *Uploader) handleReplacementTaskFailure(
	ctx context.Context,
	task *model.Task,
	replacementID int64,
	logger *slog.Logger,
	stage string,
	err error,
) {
	logger.Error(stage+" failed", "replacementID", replacementID, "error", err)
	status := scheduleTaskRetry(ctx, u.repos, task, "uploader", logger, err)
	if status == model.TaskStatusExhausted && replacementID > 0 {
		// Use an independent context so the record still lands during shutdown.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalFailureCleanupTimeout)
		defer cancel()
		u.markReplacementFailed(cleanupCtx, replacementID, logger,
			fmt.Sprintf("%s: %v (max retries reached)", stage, err))
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) markReplacementFailed(ctx context.Context, replacementID int64, logger *slog.Logger, message string) {
	if err := u.repos.Replacements.MarkFailed(ctx, replacementID, nil, message); err != nil {
		logger.Error("failed to record provider replacement failure", "replacementID", replacementID, "error", err)
	}
}

// failReplacementTarget records an approved target that can never work and
// stops its coordinator in the same transaction, so the record never shows work
// in progress with nothing queued to do it. Retrying is pointless here, so the
// retry budget is not spent first.
func (u *Uploader) failReplacementTarget(ctx context.Context, task *model.Task, replacementID int64, logger *slog.Logger, message string) {
	err := u.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		reason := storagereplacement.FailureReasonTargetInUse
		if err := txRepos.Replacements.MarkFailed(ctx, replacementID, &reason, message); err != nil {
			return err
		}
		return txRepos.Tasks.FailRunning(ctx, task, message)
	})
	if err != nil {
		logger.Error("failed to record unusable replacement target",
			"replacementID", replacementID, "error", err)
	} else {
		logger.Warn("provider replacement needs a different provider",
			"replacementID", replacementID, "reason", message)
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

// failReplacementTaskWithoutMutation stops a task that must not retry while
// leaving the replacement record exactly as it is.
func (u *Uploader) failReplacementTaskWithoutMutation(ctx context.Context, task *model.Task, logger *slog.Logger, err error) {
	logger.Error("replacement coordinator stopped", "taskID", task.ID, "error", err)
	if failErr := u.repos.Tasks.FailRunning(ctx, task, err.Error()); failErr != nil {
		logger.Error("failed to stop replacement coordinator", "taskID", task.ID, "error", failErr)
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

// deferToReplacement yields an ordinary upload stage to the replacement
// coordinator when both target the same copy. It mirrors the recovered-replica
// gate so the two coordinators and normal uploads never write one row at once.
func (u *Uploader) deferToReplacement(ctx context.Context, task *model.Task, bucketID, uploadID int64, copyIndex int, logger *slog.Logger) bool {
	copyRow, err := u.taskUploadCopy(ctx, task, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load upload copy for replacement coordination", err)
		return true
	}
	// The write belongs to the generation its copy is bound to. Asking the slot
	// instead would stop deferring the moment the replacement takes the slot,
	// which is exactly when the two writers overlap.
	binding, err := u.taskCopyDataSet(ctx, task, bucketID, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load upload data set", err)
		return true
	}
	if binding == nil {
		return false
	}
	replacement, err := u.repos.Replacements.GetActiveForDataSet(ctx, binding.ID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "check provider replacement", err)
		return true
	}
	if replacement == nil {
		return false
	}
	coordinator, err := u.repos.Tasks.GetByIdempotencyKey(ctx, storagereplacement.MigrateTaskKey(replacement.ID))
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "check replacement coordinator task", err)
		return true
	}
	if coordinator == nil || coordinator.Status != model.TaskStatusRunning {
		return false
	}
	if copyRow == nil {
		u.handleTaskFailure(ctx, task, logger, "load upload copy for replacement coordination",
			fmt.Errorf("upload copy %d not found", copyIndex))
		return true
	}
	claimedCopyID := copyRow.ID
	// Mutual exclusion is per copy row, and the coordinator's row is recorded on
	// the item it holds. Standing down for anything else would idle every
	// ordinary upload on this bucket for the length of each transfer, which also
	// keeps the retiring service billable for longer.
	heldCopyID, err := u.repos.Replacements.HeldItemCopyID(ctx, replacement.ID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "check replacement item in progress", err)
		return true
	}
	if heldCopyID == 0 || heldCopyID != claimedCopyID {
		return false
	}
	if !taskClaimPrecedes(coordinator, task) {
		return false
	}
	u.waitForStorageDependency(ctx, task, logger, "Waiting for the approved provider replacement")
	return true
}
