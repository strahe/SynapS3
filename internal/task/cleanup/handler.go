package cleanup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectdeletion"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
)

type ReceiptChecker interface {
	TransactionReceipt(context.Context, common.Hash) (*ethtypes.Receipt, error)
}
type Dependencies struct {
	Repositories         *repository.Repositories
	Storage              synapse.StorageClient
	Receipts             ReceiptChecker
	WalletReceiptTimeout time.Duration
	Cache                cache.Cache
	CacheGate            *cacheaccess.Gate
	CacheTracker         *cacheaccess.Tracker
	MaxRetries           int
	Logger               *slog.Logger
}
type Handler struct {
	*taskengine.FuncHandler
	deps Dependencies
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("cleanup handler requires repositories")
	}
	if deps.MaxRetries < 0 {
		return nil, errors.New("task retry limit cannot be negative")
	}
	if deps.WalletReceiptTimeout < 0 {
		return nil, errors.New("wallet timeouts cannot be negative")
	}
	if deps.WalletReceiptTimeout == 0 {
		deps.WalletReceiptTimeout = 15 * time.Second
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	h := &Handler{deps: deps}
	h.FuncHandler = h.newHandler()
	return h, nil
}
func (h *Handler) retryLimit() *int { value := h.deps.MaxRetries; return &value }

const (
	dependencyWait        = time.Minute
	externalPollInterval  = 5 * time.Second
	cleanupPollInterval   = time.Minute
	cleanupAttentionAfter = 24 * time.Hour
)

type cleanupCheckpoint struct {
	CopyID        int64     `json:"copy_id"`
	AttemptedAt   time.Time `json:"attempted_at"`
	RetryOfTxHash string    `json:"retry_of_tx_hash,omitempty"`
	// Finalized records that the content's rows were deleted.
	Finalized bool `json:"finalized,omitempty"`
}

func (h *Handler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageCleanup, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		Codec:      taskengine.StrictJSONCodec(func(input *storagecleanup.Input) error { return storagecleanup.ValidateInput(*input) }),
		RetryLimit: h.retryLimit(), AllowRetry: true,
		Subject: taskengine.SubjectFromInput(model.TaskSubjectStorageContent, func(input storagecleanup.Input) int64 {
			return input.ContentID
		}),
	}
	return taskengine.NewFuncHandler(definition, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.runStorageCleanup(ctx, execution, true)
	},
		func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runStorageCleanup(ctx, execution, false)
		},
	)
}

func (h *Handler) runStorageCleanup(ctx context.Context, execution taskengine.Execution, allowDelete bool) taskengine.Result {
	input, err := taskengine.DecodeInput[storagecleanup.Input](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeStorageCleanup), err)
	}
	if h.deps.Storage == nil {
		return taskengine.Fail(errors.New("storage cleanup client is unavailable"), "dependency_unavailable", nil)
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[cleanupCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	// Finalizing deleted the content's rows, so there is nothing left to
	// authorize against.
	if checkpoint.Finalized {
		return taskengine.Complete("Stored data removed", nil)
	}
	copies, err := h.deps.Repositories.StorageCleanup.AuthorizeTask(ctx, input.ContentID, input.Generation, execution.ID())
	if err != nil {
		if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
			return taskengine.Cancel("Remote cleanup was superseded", nil)
		}
		return retryTask(err, "cleanup_authorization_failed")
	}
	hasReferences, err := h.deps.Repositories.StorageCleanup.UploadHasObjectReferences(ctx, input.ContentID)
	if err == nil && !hasReferences {
		hasReferences, err = h.deps.Repositories.StorageCleanup.CleanupHasObjectReferences(ctx, input.ContentID)
	}
	if err != nil {
		return retryTask(err, "cleanup_reference_check_failed")
	}
	if hasReferences {
		return taskengine.Suspend(model.TaskResumeModeRecover, dependencyWait, "references", "Waiting for stored data references", nil)
	}
	for i := range copies {
		copyRow := copies[i]
		//exhaustive:enforce
		switch copyRow.Status {
		// A deletion the provider cannot perform stays recorded as unsupported
		// and does not keep the content from being finalized.
		case model.StorageCleanupCopyStatusRemoved, model.StorageCleanupCopyStatusUnsupported:
			continue
		case model.StorageCleanupCopyStatusPending, model.StorageCleanupCopyStatusDeleteScheduled, model.StorageCleanupCopyStatusFailed:
			// Resolved against the chain below.
		default:
			// A status this version does not know may record a paid request it
			// cannot judge, so the copy is left untouched.
			return taskengine.Suspend(model.TaskResumeModeRecover, dependencyWait, "cleanup_status",
				"Waiting for a newer version that supports this copy's cleanup record", nil)
		}
		// Zero is a legal on-chain ID; a missing data set is the only
		// identity gap that prevents an exact piece-ID lookup.
		if copyRow.DataSetID == nil {
			message := "Storage provider details are incomplete"
			if err := h.deps.Repositories.StorageCleanup.MarkCopyUnsupported(ctx, copyRow.ID, message); err != nil {
				return retryTask(err, "cleanup_evidence_failed")
			}
			continue
		}
		state, err := h.deps.Storage.DeletionState(ctx, copyRow.DataSetID.SDK(), copyRow.PieceID.SDK())
		if err != nil {
			return taskengine.Suspend(model.TaskResumeModeRecover, cleanupPollInterval, "provider_confirmation", "Checking remote cleanup", nil)
		}
		if !state.Live {
			if err := h.deps.Repositories.StorageCleanup.MarkCopyRemoved(ctx, copyRow.ID); err != nil {
				return retryTask(err, "cleanup_evidence_failed")
			}
			continue
		}
		if state.Queued {
			return taskengine.Suspend(model.TaskResumeModeRecover, cleanupPollInterval, "provider_confirmation", "Waiting for remote cleanup", nil)
		}
		previousHash := ""
		if copyRow.DeleteTxHash != nil {
			previousHash = *copyRow.DeleteTxHash
		}
		retryingFailedCopy := copyRow.Status == model.StorageCleanupCopyStatusFailed
		// Older attempts could checkpoint a retry without changing the failed row.
		// Do not treat that in-flight attempt as a fresh manual retry.
		if retryingFailedCopy && hasCheckpoint && checkpoint.CopyID == copyRow.ID &&
			checkpoint.RetryOfTxHash == previousHash && !checkpoint.AttemptedAt.IsZero() &&
			!checkpoint.AttemptedAt.Before(copyRow.UpdatedAt) {
			return waitForStorageCleanupOutcome(copyRow, checkpoint, hasCheckpoint, "Waiting for remote cleanup")
		}
		if !retryingFailedCopy && previousHash != "" {
			hashBytes, hashErr := hexutil.Decode(previousHash)
			if h.deps.Receipts == nil || hashErr != nil || len(hashBytes) != common.HashLength {
				return waitForStorageCleanupOutcome(copyRow, checkpoint, hasCheckpoint, "Checking remote cleanup")
			}
			requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletReceiptTimeout)
			receipt, receiptErr := h.deps.Receipts.TransactionReceipt(requestCtx, common.BytesToHash(hashBytes))
			cancel()
			if receiptErr != nil || receipt == nil || receipt.BlockNumber == nil || receipt.BlockNumber.Uint64() > state.BlockNumber {
				return waitForStorageCleanupOutcome(copyRow, checkpoint, hasCheckpoint, "Waiting for remote cleanup")
			}
			message := "Removal was not queued. Recover may submit another paid request."
			reason := "cleanup_transaction_not_scheduled"
			if receipt.Status == ethtypes.ReceiptStatusFailed {
				message = "Removal transaction failed. Recover may submit another paid request."
				reason = "cleanup_transaction_reverted"
			}
			return taskengine.Fail(errors.New(message), reason, func(ctx context.Context, repos *repository.Repositories) error {
				return repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, message)
			})
		} else if !retryingFailedCopy && (copyRow.Status != model.StorageCleanupCopyStatusPending || (hasCheckpoint && checkpoint.CopyID == copyRow.ID)) {
			return waitForStorageCleanupOutcome(copyRow, checkpoint, hasCheckpoint, "Waiting for remote cleanup")
		}
		if !allowDelete {
			return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Remote cleanup is ready", nil)
		}
		providerID := copyRow.ProviderID.SDK()
		cleanupContext, err := h.deps.Storage.OpenCleanupContext(ctx, copyRow.DataSetID.SDK(), storage.NewDataSetContextOptions{ProviderID: &providerID})
		if err != nil {
			return retryTask(err, "cleanup_context_failed")
		}
		checkpoint = cleanupCheckpoint{CopyID: copyRow.ID, AttemptedAt: time.Now().UTC(), RetryOfTxHash: previousHash}
		var settlement taskengine.Settlement
		if retryingFailedCopy {
			settlement = func(ctx context.Context, repos *repository.Repositories) error {
				return repos.StorageCleanup.BeginFailedCopyRetry(ctx, copyRow.ID, previousHash)
			}
		}
		var txHash string
		attempted, err := execution.WithCheckpointedEffect(ctx, taskengine.ResourceDestructiveMutation, checkpoint, settlement, func(ctx context.Context) error {
			result, deleteErr := cleanupContext.DeletePieceByID(ctx, copyRow.PieceID.SDK())
			if result != nil && result.Hash != (common.Hash{}) {
				txHash = result.Hash.String()
			}
			return deleteErr
		})
		if txHash != "" {
			err = errors.Join(err, h.deps.Repositories.StorageCleanup.MarkCopyDeleteScheduled(ctx, copyRow.ID, txHash))
		}
		if err != nil {
			if !attempted {
				if errors.Is(err, taskengine.ErrResourceBusy) {
					return taskengine.ResourceWait("Waiting for other removal operations to finish")
				}
				return retryTask(err, "cleanup_not_started")
			}
			if ctx.Err() == nil {
				h.deps.Logger.Warn("remote cleanup request failed",
					"task_id", execution.ID(), "cleanup_copy_id", copyRow.ID, "provider_id", copyRow.ProviderID,
					"error", synapse.ErrorSummary(err))
			}
			return taskengine.SuspendWithError(model.TaskResumeModeRecover, externalPollInterval, "provider_confirmation", "Checking remote cleanup",
				synapse.SummarizedError(err), nil)
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, externalPollInterval, "provider_confirmation", "Waiting for remote cleanup", nil)
	}
	return h.finishStorageCleanup(ctx, execution, input)
}

func waitForStorageCleanupOutcome(copyRow model.StorageCleanupCopy, checkpoint cleanupCheckpoint, hasCheckpoint bool, waitingMessage string) taskengine.Result {
	attemptedAt := time.Time{}
	if hasCheckpoint && checkpoint.CopyID == copyRow.ID {
		attemptedAt = checkpoint.AttemptedAt
	}
	if attemptedAt.IsZero() && copyRow.ScheduledAt != nil {
		attemptedAt = *copyRow.ScheduledAt
	}
	if attemptedAt.IsZero() || time.Since(attemptedAt) >= cleanupAttentionAfter {
		message := "removal unconfirmed after 24 hours. Recover may submit another paid request"
		if attemptedAt.IsZero() {
			message = "removal outcome cannot be confirmed. Recover may submit another paid request"
		}
		return taskengine.Fail(errors.New(message), "cleanup_outcome_unknown", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, message)
		})
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, cleanupPollInterval, "provider_confirmation", waitingMessage, nil)
}

// finishStorageCleanup releases the content's cached bytes and then deletes its
// current-state rows, so writing the same bytes again starts new content. The
// ledgers keep their rows, including any remote deletion left unsupported.
func (h *Handler) finishStorageCleanup(ctx context.Context, execution taskengine.Execution, input storagecleanup.Input) (result taskengine.Result) {
	var workStartedAt time.Time
	defer func() { result = result.WithWorkStartedAt(workStartedAt) }()
	if h.deps.Cache == nil || h.deps.CacheGate == nil || h.deps.CacheTracker == nil {
		return taskengine.Fail(errors.New("cache dependencies are unavailable"), "dependency_unavailable", nil)
	}
	content, err := h.deps.Repositories.Contents.GetByID(ctx, input.ContentID)
	if err != nil || content == nil {
		return retryTask(errors.Join(err, repository.ErrNotFound), "cleanup_content_load_failed")
	}
	bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, content.BucketID)
	if err != nil || bucket == nil {
		return retryTask(errors.Join(err, repository.ErrNotFound), "cleanup_bucket_load_failed")
	}
	if _, err := objectdeletion.ReleaseContentCache(
		ctx, h.deps.Cache, h.deps.CacheGate, h.deps.CacheTracker, h.deps.Repositories.Objects, bucket.Name, input.ContentID,
		func() { workStartedAt = time.Now().UTC() },
	); err != nil {
		return retryTask(err, "cleanup_cache_release_failed")
	}
	finalized := cleanupCheckpoint{AttemptedAt: time.Now().UTC(), Finalized: true}
	err = execution.WriteCheckpointWith(ctx, finalized, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.StorageCleanup.FinalizeContent(ctx, input.ContentID, input.Generation, execution.ID())
	})
	if errors.Is(err, repository.ErrContentCleanupNotReady) {
		return taskengine.Suspend(model.TaskResumeModeRecover, dependencyWait, "references", "Waiting for other work on the stored data to finish", nil)
	}
	if err != nil {
		return retryTask(err, "cleanup_finalize_failed")
	}
	return taskengine.Complete("Stored data removed", nil)
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
