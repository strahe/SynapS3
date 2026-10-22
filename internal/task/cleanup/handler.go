package cleanup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectdeletion"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storageremoval"
	"github.com/strahe/synaps3/internal/synapse"
	bindingtask "github.com/strahe/synaps3/internal/task/binding"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type Dependencies struct {
	Repositories *repository.Repositories
	Storage      synapse.StorageClient
	Scheduler    *taskengine.Scheduler
	Cache        cache.Cache
	CacheGate    *cacheaccess.Gate
	CacheTracker *cacheaccess.Tracker
	Logger       *slog.Logger
}
type Handler struct {
	*taskengine.FuncHandler
	deps Dependencies
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil || deps.Scheduler == nil {
		return nil, errors.New("cleanup handler requires repositories and scheduler")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	h := &Handler{deps: deps}
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageCleanup, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect, MaxConcurrency: 2, AllowRetry: true,
		Codec:    taskengine.StrictJSONCodec(func(input *storagecleanup.Input) error { return storagecleanup.ValidateInput(*input) }),
		Policy:   taskengine.ExecutionPolicy{MaxAttempts: 10, Backoff: taskengine.BackoffPolicy{InitialDelay: 30 * time.Second, Multiplier: 2, MaximumDelay: 5 * time.Minute, Jitter: 0.2}},
		Identity: taskengine.IdentityFromInput(model.TaskSubjectStorageContent, func(input storagecleanup.Input) (int64, int64) { return input.ContentID, input.Generation }),
	}
	definition.InspectRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
		var input storagecleanup.Input
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return err
		}
		_, err := repos.StorageCleanup.AuthorizeTask(ctx, input.ContentID, input.Generation, source.ID)
		return err
	}
	definition.BindSuccessor = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
		var input storagecleanup.Input
		if err := json.Unmarshal(old.Input, &input); err != nil {
			return err
		}
		if err := repos.StorageCleanup.TransferTaskOwner(ctx, input.ContentID, input.Generation, old.ID, next.ID); err != nil {
			return err
		}
		return repos.StorageCleanup.ReauthorizeFailedCopies(ctx, input.ContentID, input.Generation, next.ID)
	}
	h.FuncHandler = taskengine.NewFuncHandler(definition, h.runStorageCleanup, h.runStorageCleanup)
	return h, nil
}

// cleanupCheckpoint is the persisted recovery contract for individual copies.
type cleanupCheckpoint struct {
	storagepipeline.ExecutionIdentity
	CopyID          int64     `json:"copy_id"`
	AttemptedAt     time.Time `json:"attempted_at"`
	RetryOfTxHash   string    `json:"retry_of_tx_hash,omitempty"`
	TransactionHash string    `json:"transaction_hash,omitempty"`
	Finalized       bool      `json:"finalized,omitempty"`
}

type pendingGroup struct {
	identity storagepipeline.ExecutionIdentity
	copyIDs  []int64
}

func (h *Handler) runStorageCleanup(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[storagecleanup.Input](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeStorageCleanup), err)
	}
	cp, hasCheckpoint, err := taskengine.DecodeCheckpoint[cleanupCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if cp.Finalized {
		return taskengine.Complete("Stored data removed", nil)
	}
	copies, err := h.deps.Repositories.StorageCleanup.AuthorizeTask(ctx, input.ContentID, input.Generation, execution.ID())
	if err != nil {
		if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
			return taskengine.Cancel("Remote cleanup was superseded", nil)
		}
		return cleanupWait(err, nil)
	}
	references, err := h.deps.Repositories.StorageCleanup.UploadHasObjectReferences(ctx, input.ContentID)
	if err == nil && !references {
		references, err = h.deps.Repositories.StorageCleanup.CleanupHasObjectReferences(ctx, input.ContentID)
	}
	if err != nil {
		return cleanupWait(err, nil)
	}
	if references {
		return taskengine.Wait(model.TaskResumeModeRecover, "references", "Waiting for stored data references", nil)
	}
	groups := map[int64]*pendingGroup{}
	var adopt []repository.AdoptLegacyCleanupCopyInput
	var unsupported []int64
	hasFailed, hasActive := false, false
	clearCheckpoint := false
	for _, copyRow := range copies {
		//exhaustive:enforce
		switch copyRow.Status {
		case model.StorageCleanupCopyStatusRemoved, model.StorageCleanupCopyStatusUnsupported:
			clearCheckpoint = clearCheckpoint || hasCheckpoint && cp.CopyID == copyRow.ID
			continue
		case model.StorageCleanupCopyStatusFailed:
			hasFailed = true
			continue
		case model.StorageCleanupCopyStatusPending, model.StorageCleanupCopyStatusDeleteScheduled:
			hasActive = true
		default:
			return taskengine.Wait(model.TaskResumeModeRecover, "cleanup_status", "Waiting for support for this cleanup record", nil)
		}
		if copyRow.RemovalRequestID != nil {
			clearCheckpoint = clearCheckpoint || hasCheckpoint && cp.CopyID == copyRow.ID
			continue
		}
		if copyRow.DataSetID == nil {
			unsupported = append(unsupported, copyRow.ID)
			continue
		}
		if h.deps.Storage == nil {
			return taskengine.Fail(errors.New("storage cleanup client is unavailable"), "dependency_unavailable", nil)
		}
		row, err := h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
		if err != nil {
			return cleanupWait(err, nil)
		}
		if row == nil || row.DataSetID == nil || row.ClientDataSetID == nil || copyRow.ClientDataSetID == nil {
			return taskengine.Fail(storagepipeline.ErrExecutionIdentityMissing, "storage_identity_missing", nil)
		}
		if !row.DataSetID.Equal(*copyRow.DataSetID) || !row.ClientDataSetID.Equal(*copyRow.ClientDataSetID) || !row.ProviderID.Equal(copyRow.ProviderID) {
			return taskengine.Fail(storagepipeline.ErrExecutionIdentityChanged, "storage_identity_changed", nil)
		}
		identity, err := bindingtask.RecordedIdentity(ctx, h.deps.Repositories, row)
		if err == nil {
			err = identity.Match(bindingtask.CurrentIdentity(h.deps.Storage))
		}
		if err != nil {
			if storagepipeline.IsExecutionIdentityError(err) {
				return taskengine.Fail(err, storagepipeline.ExecutionIdentityFailure(err), nil)
			}
			return cleanupWait(err, nil)
		}
		matchesCheckpoint := hasCheckpoint && cp.CopyID == copyRow.ID
		if matchesCheckpoint {
			if err := cp.Match(identity); err != nil {
				return taskengine.Fail(err, storagepipeline.ExecutionIdentityFailure(err), nil)
			}
		}
		if copyRow.Status == model.StorageCleanupCopyStatusDeleteScheduled || copyRow.DeleteTxHash != nil || matchesCheckpoint && !cp.AttemptedAt.IsZero() {
			legacy := repository.AdoptLegacyCleanupCopyInput{ContentID: input.ContentID, Generation: input.Generation, ContentTaskID: execution.ID(), CopyID: copyRow.ID, Identity: identity, Enqueue: h.enqueueRemoval}
			if matchesCheckpoint {
				legacy.AttemptedAt = cp.AttemptedAt
				legacy.RetryOfTxHash = cp.RetryOfTxHash
				legacy.TransactionHash = cp.TransactionHash
			}
			adopt = append(adopt, legacy)
			clearCheckpoint = clearCheckpoint || matchesCheckpoint
			continue
		}
		group := groups[row.ID]
		if group == nil {
			group = &pendingGroup{identity: identity}
			groups[row.ID] = group
		}
		group.copyIDs = append(group.copyIDs, copyRow.ID)
	}
	if !hasActive && !hasFailed {
		return h.finishStorageCleanup(ctx, execution, input)
	}
	dataSetIDs := make([]int64, 0, len(groups))
	for id := range groups {
		dataSetIDs = append(dataSetIDs, id)
	}
	slices.Sort(dataSetIDs)
	enqueue := func(ctx context.Context, repos *repository.Repositories) error {
		for _, row := range adopt {
			if _, err := repos.StorageRemoval.AdoptLegacyCleanupCopy(ctx, row); err != nil {
				return err
			}
		}
		for _, id := range unsupported {
			if err := repos.StorageCleanup.MarkCopyUnsupported(ctx, id, "Storage provider details are incomplete"); err != nil {
				return err
			}
		}
		for _, id := range dataSetIDs {
			group := groups[id]
			if _, err := repos.StorageRemoval.JoinOrCreateCollecting(ctx, repository.JoinRemovalInput{ContentID: input.ContentID, Generation: input.Generation, ContentTaskID: execution.ID(), StorageDataSetID: id, CopyIDs: group.copyIDs, Identity: group.identity, Enqueue: h.enqueueRemoval}); err != nil {
				return err
			}
		}
		if clearCheckpoint {
			if err := repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), json.RawMessage(`{}`)); err != nil {
				return err
			}
			row, err := repos.Tasks.GetByID(ctx, execution.ID())
			if err != nil {
				return err
			}
			var runtime model.TaskRuntime
			if err := json.Unmarshal(row.Runtime, &runtime); err != nil {
				return err
			}
			key := fmt.Sprintf("cleanup:%d", cp.CopyID)
			if runtime.OperationKey == key {
				return repos.Tasks.ResolveOperation(ctx, execution.ID(), execution.ClaimGeneration(), key)
			}
		}
		return nil
	}
	if hasFailed {
		return taskengine.Fail(errors.New("some copies could not be removed; retry to try again"), "cleanup_copy_failed", enqueue)
	}
	return cleanupWait(nil, enqueue)
}

func (h *Handler) enqueueRemoval(ctx context.Context, repos *repository.Repositories, input storageremoval.Input) (int64, error) {
	row, _, err := h.deps.Scheduler.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{Type: model.TaskTypeStorageRemoval, Input: input})
	if err != nil {
		return 0, err
	}
	return row.ID, nil
}

func cleanupWait(err error, settlement taskengine.Settlement) taskengine.Result {
	return taskengine.Wait(model.TaskResumeModeRecover, "remote_cleanup", "Waiting for remote removal", settlement).WithDiagnostic(err)
}

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
		return taskengine.Wait(model.TaskResumeModeRecover, "references", "Waiting for other work on the stored data to finish", nil)
	}
	if err != nil {
		return retryTask(err, "cleanup_finalize_failed")
	}
	return taskengine.Complete("Stored data removed", nil)
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.Retry(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
