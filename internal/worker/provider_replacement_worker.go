package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
)

const (
	replacementItemLeaseTTL       = 10 * time.Minute
	replacementLeaseRetryDelay    = time.Second
	replacementSourceRecheckDelay = time.Minute
	replacementUploadYieldDelay   = time.Second
)

var (
	errReplacementSourceUnavailable = errors.New("replacement item has no readable source")
	errReplacementUploadPrecedes    = errors.New("ordinary upload claim precedes replacement item")
	errReplacementCommitPending     = errors.New("replacement storage confirmation is pending")
	errReplacementCommitAttention   = errors.New("replacement storage confirmation needs review")
	errReplacementCommitObserving   = errors.New("replacement storage confirmation remains under observation")
	errReplacementOwnerTerminal     = errors.New("replacement owner is terminal")
)

type replacementMigrationPauser interface {
	PauseMigration(context.Context, int64, int64, storagereplacement.WaitReason) error
}

type pulledReplacementPiece struct {
	pieceCID       cid.Cid
	pieceCIDString string
	extraHex       string
}

func replacementSourceUnusable(err error) bool {
	return synapse.IsProviderUnavailable(err) ||
		synapse.IsDataSetServiceEnded(err) ||
		synapse.IsNoProviderCandidates(err)
}

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

// ProviderReplacementWorker processes provider replacement items.
type ProviderReplacementWorker struct {
	repos        *repository.Repositories
	executor     *ReplacementTransferExecutor
	concurrency  int
	pollInterval time.Duration
	leaseTTL     time.Duration
	logger       *slog.Logger
	*livenessTracker
}

func NewProviderReplacementWorker(
	repos *repository.Repositories,
	uploadSupport *Uploader,
	concurrency int,
	pollInterval time.Duration,
	logger *slog.Logger,
) *ProviderReplacementWorker {
	registry := newReplacementTargetContextRegistry()
	return &ProviderReplacementWorker{
		repos:           repos,
		executor:        NewReplacementTransferExecutor(repos, uploadSupport, registry, logger),
		concurrency:     concurrency,
		pollInterval:    pollInterval,
		leaseTTL:        replacementItemLeaseTTL,
		logger:          logger,
		livenessTracker: newLivenessTracker(pollInterval),
	}
}

func (w *ProviderReplacementWorker) Name() string { return "provider_replacement" }

func (w *ProviderReplacementWorker) Healthy() bool { return w.healthy() }

func (w *ProviderReplacementWorker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for range w.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.runSlot(ctx)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

func (w *ProviderReplacementWorker) runSlot(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		w.recordTick()
		item, err := w.repos.Replacements.ClaimReadyReplacementItem(ctx, w.leaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("claiming provider replacement item", "error", err)
			if !sleepUntilNextWorkerPoll(ctx, w.pollInterval) {
				return
			}
			continue
		}
		if item == nil {
			if !sleepUntilNextWorkerPoll(ctx, w.pollInterval) {
				return
			}
			continue
		}
		w.processItem(ctx, item)
	}
}

func (w *ProviderReplacementWorker) processItem(parent context.Context, item *storagereplacement.Item) {
	if item == nil || item.ClaimedAt == nil {
		return
	}
	w.recordWorkStarted()
	defer w.recordWorkFinished()
	started := time.Now()
	defer func() {
		admin.WorkerTaskDuration.WithLabelValues(w.Name()).Observe(time.Since(started).Seconds())
	}()

	token := storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
	itemCtx, cancel := context.WithCancel(parent)
	leaseLost, stopRenewal := w.startLeaseRenewal(itemCtx, cancel, token, item.LeaseUntil)
	expectedStateVersion, err := w.executor.Execute(itemCtx, item)
	stopRenewal()
	cancel()
	if leaseLost() || errors.Is(err, repository.ErrItemClaimLost) {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), taskLeaseOperationTimeout)
		releaseErr := w.repos.Replacements.ReleaseReplacementItemClaim(releaseCtx, token)
		releaseCancel()
		if releaseErr != nil && !errors.Is(releaseErr, repository.ErrItemClaimLost) {
			w.logItemTransitionFailure(item, "releasing lost replacement item claim", releaseErr)
		}
		admin.WorkerTasksProcessed.WithLabelValues(w.Name(), "claim_lost").Inc()
		return
	}
	if parent.Err() != nil {
		// Worker shutdown must not leave a live lease behind. Use an independent,
		// bounded context because the runtime context is already cancelled.
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), taskLeaseOperationTimeout)
		releaseErr := w.repos.Replacements.ReleaseReplacementItemClaim(releaseCtx, token)
		releaseCancel()
		if releaseErr != nil && !errors.Is(releaseErr, repository.ErrItemClaimLost) {
			w.logItemTransitionFailure(item, "releasing replacement item during shutdown", releaseErr)
			admin.WorkerTasksProcessed.WithLabelValues(w.Name(), "failure").Inc()
			return
		}
		admin.WorkerTasksProcessed.WithLabelValues(w.Name(), "shutdown_released").Inc()
		return
	}

	result := "success"
	switch {
	case err == nil, errors.Is(err, storagereplacement.ErrItemCancelled), errors.Is(err, storagereplacement.ErrItemDeferred):
		// The repository already settled the claim.
	case errors.Is(err, errReplacementUploadPrecedes):
		if transitionErr := w.repos.Replacements.DeferReplacementItemClaim(parent, token, time.Now().Add(replacementUploadYieldDelay)); transitionErr != nil {
			w.logItemTransitionFailure(item, "yielding replacement item", transitionErr)
			result = "failure"
		}
	case errors.Is(err, errReplacementCommitPending):
		if transitionErr := w.repos.Replacements.DeferReplacementItemClaim(parent, token, time.Now().Add(w.commitPollDelay())); transitionErr != nil {
			w.logItemTransitionFailure(item, "waiting for replacement storage confirmation", transitionErr)
			result = "failure"
		} else {
			result = "confirmation_wait"
		}
	case errors.Is(err, errReplacementCommitObserving):
		if transitionErr := w.repos.Replacements.DeferReplacementItemClaim(parent, token, time.Now().Add(commitObservationDelay(w.commitPollDelay()))); transitionErr != nil {
			w.logItemTransitionFailure(item, "observing replacement storage confirmation", transitionErr)
			result = "failure"
		} else {
			result = "confirmation_wait"
		}
	case errors.Is(err, errReplacementCommitAttention):
		if transitionErr := w.repos.Replacements.DeferReplacementItemClaim(parent, token, time.Now().Add(storageCommitAttentionDelay)); transitionErr != nil {
			w.logItemTransitionFailure(item, "parking replacement storage confirmation for review", transitionErr)
			result = "failure"
		} else {
			result = "confirmation_attention"
		}
	case errors.Is(err, errReplacementOwnerTerminal):
		if transitionErr := w.repos.Replacements.CancelReplacementItemClaim(parent, token); transitionErr != nil {
			w.logItemTransitionFailure(item, "cancelling terminal replacement item", transitionErr)
			result = "failure"
		} else {
			result = "cancelled"
		}
	case errors.Is(err, errReplacementSourceUnavailable):
		if transitionErr := w.repos.Replacements.WaitReplacementItemClaim(
			parent, token, time.Now().Add(replacementSourceRecheckDelay), safeReplacementItemError(err),
		); transitionErr != nil {
			w.logItemTransitionFailure(item, "waiting for replacement source", transitionErr)
			result = "failure"
		} else {
			result = "waiting_source"
		}
	case synapse.IsProviderUnavailable(err), synapse.IsNoProviderCandidates(err), synapse.IsDataSetServiceEnded(err), dataSetWriteBlockedError(err):
		pauseErr := pauseReplacementAtExecutionVersion(
			parent, w.repos.Replacements, item.ReplacementID, expectedStateVersion,
		)
		if pauseErr != nil && !errors.Is(pauseErr, repository.ErrConflict) {
			w.logItemTransitionFailure(item, "pausing unavailable replacement target", pauseErr)
		}
		if transitionErr := w.repos.Replacements.DeferReplacementItemClaim(parent, token, time.Now().Add(w.pollInterval)); transitionErr != nil && !errors.Is(transitionErr, repository.ErrItemClaimLost) {
			w.logItemTransitionFailure(item, "releasing paused replacement item", transitionErr)
		}
		result = "target_wait"
	default:
		status, transitionErr := w.repos.Replacements.RetryReplacementItemClaim(
			parent, token, time.Now().Add(retryDelay(item.RetryCount)), safeReplacementItemError(err),
		)
		if transitionErr != nil {
			w.logItemTransitionFailure(item, "scheduling replacement item retry", transitionErr)
			result = "failure"
		} else {
			result = string(status)
		}
	}
	admin.WorkerTasksProcessed.WithLabelValues(w.Name(), result).Inc()
}

func (w *ProviderReplacementWorker) commitPollDelay() time.Duration {
	if w != nil && w.pollInterval > 0 {
		return w.pollInterval
	}
	return storageCommitPollDelay
}

func pauseReplacementAtExecutionVersion(
	ctx context.Context,
	pauser replacementMigrationPauser,
	replacementID, expectedStateVersion int64,
) error {
	if pauser == nil || replacementID <= 0 || expectedStateVersion <= 0 {
		return repository.ErrConflict
	}
	return pauser.PauseMigration(ctx, replacementID, expectedStateVersion, storagereplacement.WaitReasonTarget)
}

func (w *ProviderReplacementWorker) logItemTransitionFailure(item *storagereplacement.Item, message string, err error) {
	w.logger.Error(message, "replacementID", item.ReplacementID, "itemID", item.ID, "error", err)
}

func safeReplacementItemError(err error) string {
	if err == nil {
		return ""
	}
	// Normal APIs do not return this raw diagnostic.
	return err.Error()
}

func (w *ProviderReplacementWorker) startLeaseRenewal(
	ctx context.Context,
	cancel context.CancelFunc,
	token storagereplacement.ClaimToken,
	initialLeaseUntil *time.Time,
) (func() bool, func()) {
	var mu sync.Mutex
	lost := false
	done := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(done)
		leaseUntil := time.Time{}
		if initialLeaseUntil != nil {
			leaseUntil = *initialLeaseUntil
		}
		timer := time.NewTimer(taskLeaseRenewInterval(w.leaseTTL))
		defer timer.Stop()
		markLost := func() {
			mu.Lock()
			lost = true
			mu.Unlock()
			cancel()
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-timer.C:
				if leaseUntil.IsZero() || !time.Now().Before(leaseUntil) {
					markLost()
					return
				}
				attemptedAt := time.Now()
				opCtx, opCancel := context.WithTimeout(context.Background(), taskLeaseOperationTimeout)
				err := w.repos.Replacements.RenewReplacementItemLease(opCtx, token, w.leaseTTL)
				opCancel()
				if err == nil {
					leaseUntil = attemptedAt.Add(w.leaseTTL)
					timer.Reset(taskLeaseRenewInterval(w.leaseTTL))
					continue
				}
				if ctx.Err() != nil {
					return
				}
				if errors.Is(err, repository.ErrItemClaimLost) || !time.Now().Before(leaseUntil) {
					markLost()
					return
				}
				w.logger.Warn("renewing provider replacement item lease", "itemID", token.ItemID, "error", err)
				retryDelay := min(replacementLeaseRetryDelay, taskLeaseRenewInterval(w.leaseTTL), time.Until(leaseUntil)/2)
				if retryDelay <= 0 {
					markLost()
					return
				}
				timer.Reset(retryDelay)
			}
		}
	}()
	var once sync.Once
	return func() bool {
			mu.Lock()
			defer mu.Unlock()
			return lost
		}, func() {
			once.Do(func() { close(stop) })
			<-done
		}
}

// ReplacementTransferExecutor processes one claimed replacement item.
type ReplacementTransferExecutor struct {
	repos    *repository.Repositories
	support  *Uploader
	registry *replacementTargetContextRegistry
	logger   *slog.Logger
}

func NewReplacementTransferExecutor(
	repos *repository.Repositories,
	uploadSupport *Uploader,
	registry *replacementTargetContextRegistry,
	logger *slog.Logger,
) *ReplacementTransferExecutor {
	return &ReplacementTransferExecutor{repos: repos, support: uploadSupport, registry: registry, logger: logger}
}

func (e *ReplacementTransferExecutor) Execute(ctx context.Context, item *storagereplacement.Item) (int64, error) {
	if item == nil || item.ClaimedAt == nil {
		return 0, repository.ErrItemClaimLost
	}
	token := storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
	snapshot, err := e.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: item.ReplacementID,
		ItemID:        item.ID,
		ItemClaimedAt: token.ClaimedAt,
	})
	if err != nil {
		return 0, err
	}
	expectedStateVersion := snapshot.Replacement.StateVersion
	copyRow, err := e.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: snapshot.Replacement.ID,
		ItemID:        item.ID,
		UploadID:      snapshot.Upload.ID,
		ItemClaimedAt: token.ClaimedAt,
	})
	if err != nil {
		return expectedStateVersion, err
	}
	// The copy must be visible before claim ordering is checked, or a concurrent
	// upload could miss this item.
	earlierUpload, err := e.repos.Tasks.HasEarlierRunningUploadCopyClaim(
		ctx, token.ClaimedAt, snapshot.Upload.ID, snapshot.Replacement.CopyIndex,
	)
	if err != nil {
		return expectedStateVersion, err
	}
	if earlierUpload {
		return expectedStateVersion, errReplacementUploadPrecedes
	}
	if copyCommitted(copyRow) {
		return expectedStateVersion, e.finish(ctx, token, &snapshot.Upload)
	}
	if !snapshot.Replacement.Status.Active() && copyRow.CommitAttemptID != nil &&
		*copyRow.CommitAttemptID != "" && copyRow.CommitAttemptedAt == nil {
		advance, err := (&storagecommit.Advancer{Store: e.repos.Uploads}).ReleaseTerminalReservation(
			ctx, *copyRow, snapshot.Target,
		)
		if err != nil {
			return expectedStateVersion, err
		}
		if advance.State == storagecommit.AdvanceReleased && advance.ReleaseReason == storagecommit.ReleaseOwnerTerminal {
			return expectedStateVersion, errReplacementOwnerTerminal
		}
		return expectedStateVersion, errors.New("terminal replacement reservation returned an unexpected state")
	}

	bucket, err := e.repos.Buckets.GetByID(ctx, snapshot.Replacement.BucketID)
	if err != nil {
		return expectedStateVersion, err
	}
	if bucket == nil {
		return expectedStateVersion, fmt.Errorf("bucket %d: %w", snapshot.Replacement.BucketID, repository.ErrNotFound)
	}
	handle, err := e.registry.acquire(ctx, snapshot.Target.ID, func() (synapse.DataSetTarget, error) {
		return e.support.contextForReadyBinding(ctx, &snapshot.Target)
	})
	if err != nil {
		if copyCommitSubmitted(copyRow) {
			advance, advanceErr := (&storagecommit.Advancer{Store: e.repos.Uploads}).AdvanceUnavailable(
				ctx, *copyRow, snapshot.Target,
			)
			if advanceErr != nil {
				return expectedStateVersion, advanceErr
			}
			switch {
			case advance.State == storagecommit.AdvancePending:
				return expectedStateVersion, errReplacementCommitPending
			case advance.State == storagecommit.AdvanceNeedsAttention && advance.Continue:
				return expectedStateVersion, errReplacementCommitObserving
			case advance.State == storagecommit.AdvanceNeedsAttention:
				return expectedStateVersion, errReplacementCommitAttention
			default:
				return expectedStateVersion, errors.New("unavailable replacement commit returned an unexpected state")
			}
		}
		return expectedStateVersion, err
	}
	defer handle.release()
	return expectedStateVersion, e.copy(ctx, token, snapshot, copyRow, handle, bucket)
}

func (e *ReplacementTransferExecutor) copy(
	ctx context.Context,
	token storagereplacement.ClaimToken,
	snapshot *repository.ReplacementItemSnapshot,
	copyRow *model.StorageUploadCopy,
	handle *replacementTargetContextHandle,
	bucket *model.Bucket,
) error {
	upload := &snapshot.Upload
	version := &snapshot.Version
	storageCtx := handle.storageCtx
	var pieceCID cid.Cid
	var pieceCIDString string
	extraHex := derefString(copyRow.CommitExtraDataHex)

	if !copyHasPiece(copyRow) {
		readable, err := e.repos.Uploads.ListReadableCommittedCopies(ctx, upload.ID)
		if err != nil {
			return err
		}
		pulled, err := e.pull(ctx, storageCtx, copyRow, orderReplacementSources(readable, snapshot.Target.CopyIndex))
		if err != nil {
			return err
		}
		if pulled != nil {
			pieceCID = pulled.pieceCID
			pieceCIDString = pulled.pieceCIDString
			extraHex = pulled.extraHex
		} else {
			stored, storedCID, err := e.storeFromCache(ctx, storageCtx, bucket, version)
			if err != nil {
				return err
			}
			if !stored {
				return errReplacementSourceUnavailable
			}
			pieceCID = storedCID
			pieceCIDString = pieceCID.String()
			_, extraHex, err = e.support.extraDataForCopy(ctx, storageCtx, copyRow, []storage.PieceInput{{PieceCID: pieceCID}})
			if err != nil {
				return err
			}
		}
		evidenceCtx, evidenceCancel := providerEvidenceContext(ctx)
		err = e.repos.Uploads.MarkUploadCopyPieceReady(evidenceCtx, repository.MarkUploadCopyPieceReadyInput{
			StorageUploadCopyID: copyRow.ID,
			RequireEligibleCopy: true,
			UploadID:            upload.ID,
			CopyIndex:           copyRow.CopyIndex,
			PieceCID:            pieceCIDString,
			RetrievalURL:        storageCtx.PieceURL(pieceCID),
			CommitExtraDataHex:  extraHex,
		})
		evidenceCancel()
		if err != nil {
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
	advance, err := e.support.commitReplicaRepairCopy(
		ctx, upload, &snapshot.Target, copyRow, storageCtx, pieces, !snapshot.Replacement.Status.Active(),
	)
	if err != nil && advance.State == storagecommit.AdvancePending && synapse.IsProviderUnavailable(err) {
		return err
	}
	switch {
	case advance.State == storagecommit.AdvanceWaitingCapacity,
		advance.State == storagecommit.AdvanceSubmitted,
		advance.State == storagecommit.AdvancePending:
		if err != nil {
			e.logger.Warn("storage commit evidence remains fenced", "stage", "provider replacement commit", "error", err)
		}
		return errReplacementCommitPending
	case advance.State == storagecommit.AdvanceNeedsAttention && advance.Continue:
		return errReplacementCommitObserving
	case advance.State == storagecommit.AdvanceNeedsAttention:
		return errReplacementCommitAttention
	case advance.State == storagecommit.AdvanceReleased && advance.ReleaseReason == storagecommit.ReleaseDataSetUnavailable:
		return commitReleaseCause(advance)
	case advance.State == storagecommit.AdvanceReleased && advance.ReleaseReason == storagecommit.ReleaseOwnerTerminal:
		return errReplacementOwnerTerminal
	case advance.State == storagecommit.AdvanceReleased:
		return errReplacementCommitPending
	case advance.State == storagecommit.AdvanceRejected:
		return errCommitRejected
	case err != nil:
		return err
	case advance.State != storagecommit.AdvanceConfirmed || advance.Confirmation == nil || len(advance.Confirmation.PieceIDs) == 0:
		return errors.New("replacement commit returned no piece ID")
	}
	result := advance.Confirmation
	pieceID := idtypes.OnChainIDFromSDK(result.PieceIDs[0])
	evidenceCtx, evidenceCancel := providerEvidenceContext(ctx)
	err = e.repos.Uploads.MarkUploadCopyCommitted(evidenceCtx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID:          copyRow.ID,
		RequireEligibleCopy:          true,
		UploadID:                     upload.ID,
		CopyIndex:                    copyRow.CopyIndex,
		PieceCID:                     pieceCIDString,
		PieceID:                      &pieceID,
		RetrievalURL:                 storageCtx.PieceURL(pieceCID),
		CommitExtraDataHex:           derefString(copyRow.CommitExtraDataHex),
		CommitTransactionID:          result.TransactionID,
		CommitAttemptID:              advance.AttemptID,
		CommitConfirmedTransactionID: result.ConfirmedTransactionID,
	})
	evidenceCancel()
	if err != nil {
		return err
	}
	return e.finish(ctx, token, upload)
}

func (e *ReplacementTransferExecutor) pull(
	ctx context.Context,
	storageCtx synapse.DataSetTarget,
	copyRow *model.StorageUploadCopy,
	sources []repository.ReadableStorageCopy,
) (*pulledReplacementPiece, error) {
	for i := range sources {
		source := &sources[i]
		pieceCID, err := cid.Decode(source.PieceCID)
		if err != nil {
			continue
		}
		extraData, extraHex, err := e.support.extraDataForCopy(
			ctx, storageCtx, copyRow, []storage.PieceInput{{PieceCID: pieceCID}},
		)
		if err != nil {
			return nil, err
		}
		if _, err := storageCtx.Pull(ctx, storage.PullRequest{
			Pieces:    []cid.Cid{pieceCID},
			ExtraData: extraData,
			From: func(cid.Cid) string {
				return source.RetrievalURL
			},
		}); err != nil {
			if replacementSourceUnusable(err) {
				continue
			}
			return nil, err
		}
		return &pulledReplacementPiece{pieceCID: pieceCID, pieceCIDString: source.PieceCID, extraHex: extraHex}, nil
	}
	return nil, nil
}

func (e *ReplacementTransferExecutor) storeFromCache(
	ctx context.Context,
	storageCtx synapse.DataSetTarget,
	bucket *model.Bucket,
	version *model.ObjectVersion,
) (bool, cid.Cid, error) {
	if version == nil || version.CacheKey == "" {
		return false, cid.Undef, nil
	}
	rc, _, err := e.support.cache.Get(ctx, bucket.Name, version.CacheKey)
	if err != nil {
		if os.IsNotExist(err) {
			if version.InCache {
				if markErr := e.repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); markErr != nil {
					e.logger.Warn("failed to mark cache location absent", "versionID", version.VersionID, "error", markErr)
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

func (e *ReplacementTransferExecutor) finish(
	ctx context.Context,
	token storagereplacement.ClaimToken,
	upload *model.StorageUpload,
) error {
	// Finalize first so a crash leaves the claim recoverable.
	return finalizeReplacementItem(
		func() error {
			_, _, err := e.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, e.support.finalizeUploadInput(upload.ID))
			return err
		},
		func() error { return e.repos.Replacements.CompleteReplacementItemClaim(ctx, token) },
	)
}

func finalizeReplacementItem(finalizeUpload, completeClaim func() error) error {
	if err := finalizeUpload(); err != nil {
		return err
	}
	return completeClaim()
}

type replacementTargetContextRegistry struct {
	mu      sync.Mutex
	entries map[int64]*replacementTargetContextEntry
}

type replacementTargetContextEntry struct {
	ready      chan struct{}
	storageCtx synapse.DataSetTarget
	err        error
	refs       int
}

type replacementTargetContextHandle struct {
	registry   *replacementTargetContextRegistry
	targetID   int64
	entry      *replacementTargetContextEntry
	storageCtx synapse.DataSetTarget
	once       sync.Once
}

func newReplacementTargetContextRegistry() *replacementTargetContextRegistry {
	return &replacementTargetContextRegistry{entries: make(map[int64]*replacementTargetContextEntry)}
}

func (r *replacementTargetContextRegistry) acquire(
	ctx context.Context,
	targetID int64,
	create func() (synapse.DataSetTarget, error),
) (*replacementTargetContextHandle, error) {
	r.mu.Lock()
	entry, exists := r.entries[targetID]
	if !exists {
		entry = &replacementTargetContextEntry{
			ready: make(chan struct{}),
		}
		r.entries[targetID] = entry
	}
	// Count creators and waiters before publishing the unlocked entry. A handle
	// cannot be evicted while another acquire is still waiting for creation.
	entry.refs++
	r.mu.Unlock()

	if !exists {
		entry.storageCtx, entry.err = create()
		if entry.err != nil {
			r.mu.Lock()
			if r.entries[targetID] == entry {
				delete(r.entries, targetID)
			}
			r.mu.Unlock()
		}
		close(entry.ready)
	}
	select {
	case <-ctx.Done():
		r.release(targetID, entry)
		return nil, context.Cause(ctx)
	case <-entry.ready:
	}
	if entry.err != nil {
		r.release(targetID, entry)
		return nil, entry.err
	}
	return &replacementTargetContextHandle{
		registry: r, targetID: targetID, entry: entry, storageCtx: entry.storageCtx,
	}, nil
}

func (r *replacementTargetContextRegistry) release(targetID int64, entry *replacementTargetContextEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs == 0 && r.entries[targetID] == entry {
		delete(r.entries, targetID)
	}
}

func (h *replacementTargetContextHandle) release() {
	if h == nil || h.registry == nil || h.entry == nil {
		return
	}
	h.once.Do(func() {
		h.registry.release(h.targetID, h.entry)
	})
}
