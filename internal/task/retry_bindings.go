package task

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synaps3/internal/walletoperation"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type retryBoundHandler struct {
	taskengine.Handler
	definition taskengine.Definition
}

func (h *retryBoundHandler) Definition() taskengine.Definition { return h.definition }

func bindRetryContract(handler taskengine.Handler, deps Dependencies) taskengine.Handler {
	definition := handler.Definition()
	bind := func(context.Context, *repository.Repositories, *model.Task, *model.Task) error { return nil }
	inspect := func(context.Context, *repository.Repositories, *model.Task) error { return nil }
	prepare := func(_ context.Context, _ *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
		return taskengine.RetryPreparation{Request: taskengine.EnqueueRequest{Type: source.Type, IdempotencyKey: source.IdempotencyKey, Input: json.RawMessage(source.Input)}, Checkpoint: source.Checkpoint, ResumeMode: model.TaskResumeModeRecover, Bind: bind}, nil
	}
	if definition.InspectRetry != nil {
		inspect = definition.InspectRetry
	}
	if definition.LegacyHandoff != nil {
		bind = definition.LegacyHandoff
	}
	if definition.PrepareRetry != nil {
		prepare = definition.PrepareRetry
	}
	switch definition.Type {
	case model.TaskTypeBucketProvision:
		definition.Subject = taskengine.SubjectFromInput("bucket", func(input bucketlifecycle.ProvisionInput) int64 { return input.BucketID })
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input bucketlifecycle.ProvisionInput
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			bucket, err := repos.Buckets.GetByID(ctx, input.BucketID)
			if err != nil {
				return err
			}
			if bucket == nil || source.IdempotencyKey != bucketlifecycle.ProvisionKey(bucket.ID, bucket.DefaultCopies) {
				return repository.ErrConflict
			}
			return nil
		}
	case model.TaskTypeUploadPlan:
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input storagepipeline.UploadPlanInput
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			content, err := repos.Contents.GetByID(ctx, input.ContentID)
			if err != nil {
				return err
			}
			if content == nil || content.AcceptedAt != nil {
				return repository.ErrConflict
			}
			unreferenced, err := repos.Objects.ContentIsUnreferenced(ctx, input.ContentID)
			if err != nil {
				return err
			}
			if unreferenced {
				return repository.ErrConflict
			}
			return nil
		}
	case model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull:
		definition.CanManualRetry = nil
		inspect = inspectCopyRetry
		bind = transferCopyOwner
		prepare = prepareCopyRetry(deps)
	case model.TaskTypeStorageDataSetEnsure:
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input storagepipeline.DataSetInput
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			row, err := repos.Contents.GetDataSetBindingByID(ctx, input.DataSetID)
			if err != nil {
				return err
			}
			if row == nil || row.EnsureTaskID == nil || *row.EnsureTaskID != source.ID {
				return repository.ErrConflict
			}
			return nil
		}
		bind = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			var input storagepipeline.DataSetInput
			if err := json.Unmarshal(old.Input, &input); err != nil {
				return err
			}
			row, err := repos.Contents.GetDataSetBindingByID(ctx, input.DataSetID)
			if err != nil {
				return err
			}
			if row == nil {
				return repository.ErrConflict
			}
			return repos.Contents.TransferEnsureTaskOwner(ctx, row.ID, row.Generation, old.ID, next.ID)
		}
	case model.TaskTypeStorageCommit:
		definition.CanManualRetry = func(source *model.Task) bool {
			return source != nil && source.FailureReason != nil && *source.FailureReason != "invalid_checkpoint"
		}
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input storagepipeline.CommitRequestInput
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			row, err := repos.Contents.GetCommitRequest(ctx, input.RequestID)
			if err != nil {
				return err
			}
			if row == nil || row.TaskID == nil || *row.TaskID != source.ID {
				return repository.ErrConflict
			}
			if row.AttentionCode != nil && !storagecommit.AttentionCode(*row.AttentionCode).Valid() {
				return repository.ErrConflict
			}
			return nil
		}
		bind = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			var input storagepipeline.CommitRequestInput
			if err := json.Unmarshal(old.Input, &input); err != nil {
				return err
			}
			return repos.Contents.TransferCommitTaskOwner(ctx, input.RequestID, old.ID, next.ID)
		}
	case model.TaskTypeStorageCleanup:
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input storagecleanup.Input
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			_, err := repos.StorageCleanup.AuthorizeTask(ctx, input.ContentID, input.Generation, source.ID)
			return err
		}
		bind = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			var input storagecleanup.Input
			if err := json.Unmarshal(old.Input, &input); err != nil {
				return err
			}
			return repos.StorageCleanup.TransferTaskOwner(ctx, input.ContentID, input.Generation, old.ID, next.ID)
		}
	case model.TaskTypeWalletOperation:
		definition.CanManualRetry = nil
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input walletoperation.Input
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			row, err := repos.WalletOperations.GetByID(ctx, input.OperationID)
			if err != nil {
				return err
			}
			if row == nil || row.TaskID == nil || *row.TaskID != source.ID || row.Status == model.WalletOperationStatusUnknown || row.Status == model.WalletOperationStatusConfirmed {
				return repository.ErrConflict
			}
			if row.BroadcastAttemptedAt != nil && (row.TxHash == nil || *row.TxHash == "") {
				return repository.ErrConflict
			}
			return nil
		}
		bind = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			var input walletoperation.Input
			if err := json.Unmarshal(old.Input, &input); err != nil {
				return err
			}
			return repos.WalletOperations.TransferTaskOwner(ctx, input.OperationID, old.ID, next.ID)
		}
	case model.TaskTypeProviderReplacementCoordinate:
		definition.Subject = taskengine.SubjectFromInput("storage_replacement", func(i storagereplacement.CoordinateInput) int64 { return i.ReplacementID })
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input storagereplacement.CoordinateInput
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			row, err := repos.Replacements.GetByID(ctx, input.ReplacementID)
			if err != nil {
				return err
			}
			if row == nil || row.TaskID == nil || *row.TaskID != source.ID || row.TaskGeneration != input.Generation {
				return repository.ErrConflict
			}
			if row.Status == storagereplacement.StatusFailed {
				err := repos.Replacements.RetryEligibility(ctx, row.ID)
				if errors.Is(err, storagereplacement.ErrNotRetryable) || errors.Is(err, storagereplacement.ErrSuperseded) || errors.Is(err, storagereplacement.ErrTargetInUse) {
					return errors.Join(repository.ErrConflict, err)
				}
				return err
			}
			if row.Status == storagereplacement.StatusCompleted || row.Status == storagereplacement.StatusSuperseded || row.Status == storagereplacement.StatusRetiring || row.Status == storagereplacement.StatusCleanupAttention {
				return repository.ErrConflict
			}
			return nil
		}
		bind = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			var input storagereplacement.CoordinateInput
			if err := json.Unmarshal(old.Input, &input); err != nil {
				return err
			}
			return repos.Replacements.ResumeCoordinatorTask(ctx, input.ReplacementID, input.Generation, old.ID, next.ID)
		}
		definition.LegacyHandoff = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			var input storagereplacement.CoordinateInput
			if err := json.Unmarshal(old.Input, &input); err != nil {
				return err
			}
			return repos.Replacements.TransferTaskOwner(ctx, input.ReplacementID, input.Generation, old.ID, next.ID)
		}
	case model.TaskTypeCacheEvict:
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input cacheeviction.EvictInput
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			row, err := repos.CacheEvictions.GetCacheEntry(ctx, input.ContentID)
			if err != nil {
				return err
			}
			if row == nil || row.CacheActiveTaskID == nil || *row.CacheActiveTaskID != source.ID || row.CacheOperationGeneration != input.Generation {
				return repository.ErrConflict
			}
			return nil
		}
		bind = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			var input cacheeviction.EvictInput
			if err := json.Unmarshal(old.Input, &input); err != nil {
				return err
			}
			return repos.CacheEvictions.TransferEvictionTaskOwner(ctx, input.ContentID, input.Generation, old.ID, next.ID)
		}
	case model.TaskTypeCacheReconcileDurability:
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input cacheeviction.DurabilityInput
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			bucket, err := repos.Buckets.GetByID(ctx, input.BucketID)
			if err != nil {
				return err
			}
			if bucket == nil || bucket.DurabilityTaskID == nil || *bucket.DurabilityTaskID != source.ID || bucket.DurabilityGeneration != input.Generation {
				return repository.ErrConflict
			}
			return nil
		}
		bind = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			var input cacheeviction.DurabilityInput
			if err := json.Unmarshal(old.Input, &input); err != nil {
				return err
			}
			return repos.CacheEvictions.TransferDurabilityTaskOwner(ctx, input.BucketID, input.Generation, old.ID, next.ID)
		}
	case model.TaskTypeProviderUploadSpeedTest:
		definition.Subject = func(raw json.RawMessage) (taskengine.Subject, error) {
			var input providerbenchmark.Input
			if err := json.Unmarshal(raw, &input); err != nil {
				return taskengine.Subject{}, err
			}
			return taskengine.Subject{Type: "provider", Key: input.ProviderID}, nil
		}
		inspect = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
			var input providerbenchmark.Input
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return err
			}
			latest, err := repos.Tasks.LatestForSubject(ctx, "provider", input.ProviderID, model.TaskTypeProviderUploadSpeedTest)
			if err != nil {
				return err
			}
			if latest == nil || latest.ID != source.ID {
				return repository.ErrConflict
			}
			row, err := repos.ProviderUploadSpeed.Get(ctx, input.ProviderID)
			if err != nil {
				return err
			}
			if row != nil && row.State == providerbenchmark.StateTesting {
				return repository.ErrConflict
			}
			return nil
		}
		prepare = func(ctx context.Context, repos *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
			var input providerbenchmark.Input
			if err := json.Unmarshal(source.Input, &input); err != nil {
				return taskengine.RetryPreparation{}, err
			}
			id, err := types.ParseOnChainID("provider_id", input.ProviderID)
			if err != nil {
				return taskengine.RetryPreparation{}, err
			}
			url, eligible, err := providerbenchmark.CurrentServiceURL(ctx, deps.Observability, id)
			if err != nil {
				return taskengine.RetryPreparation{}, err
			}
			if !eligible {
				return taskengine.RetryPreparation{}, repository.ErrConflict
			}
			input.ServiceURLHash = providerbenchmark.URLHash(url)
			return taskengine.RetryPreparation{Request: taskengine.EnqueueRequest{Type: source.Type, IdempotencyKey: source.IdempotencyKey, Input: input}, ResumeMode: model.TaskResumeModeExecute, Bind: func(ctx context.Context, repos *repository.Repositories, _, next *model.Task) error {
				return repos.ProviderUploadSpeed.Begin(ctx, input.ProviderID, input.ServiceURLHash, next.ID)
			}}, nil
		}
		definition.LegacyHandoff = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
			return repos.ProviderUploadSpeed.TransferTaskOwner(ctx, old.ID, next.ID)
		}
	}
	definition.InspectRetry = inspect
	definition.PrepareRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
		return prepare(ctx, repos, source)
	}
	if definition.LegacyHandoff == nil {
		definition.LegacyHandoff = bind
	}
	return &retryBoundHandler{Handler: handler, definition: definition}
}

func transferCopyOwner(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(old.Input, &input); err != nil {
		return err
	}
	return repos.Contents.TransferCopyTaskOwner(ctx, input.CopyID, input.Generation, old.ID, next.ID)
}

func inspectCopyRetry(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
	if source.FailureReason != nil && *source.FailureReason == "invalid_checkpoint" {
		return repository.ErrConflict
	}
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(source.Input, &input); err != nil {
		return err
	}
	row, err := repos.Contents.GetUploadCopyByID(ctx, input.CopyID)
	if err != nil {
		return err
	}
	if row == nil || row.WorkGeneration != input.Generation {
		return repository.ErrConflict
	}
	if row.ActiveTaskID != nil {
		if *row.ActiveTaskID == source.ID {
			return nil
		}
		return repository.ErrConflict
	}
	latest, err := repos.Tasks.LatestForSubject(ctx, model.TaskSubjectStorageCopy, strconv.FormatInt(input.CopyID, 10), model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull)
	if err != nil {
		return err
	}
	if latest == nil || latest.ID != source.ID {
		return repository.ErrConflict
	}
	states, err := repos.Contents.CopyRetryStates(ctx, []int64{input.CopyID})
	if err != nil {
		return err
	}
	if !states[input.CopyID].Available {
		return repository.ErrConflict
	}
	return nil
}

func prepareCopyRetry(deps Dependencies) func(context.Context, *repository.Repositories, *model.Task) (taskengine.RetryPreparation, error) {
	return func(ctx context.Context, repos *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
		var input storagepipeline.CopyGenerationInput
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return taskengine.RetryPreparation{}, err
		}
		row, err := repos.Contents.GetUploadCopyByID(ctx, input.CopyID)
		if err != nil {
			return taskengine.RetryPreparation{}, err
		}
		if row == nil {
			return taskengine.RetryPreparation{}, repository.ErrNotFound
		}
		preparation := taskengine.RetryPreparation{Request: taskengine.EnqueueRequest{Type: source.Type, IdempotencyKey: source.IdempotencyKey, Input: json.RawMessage(source.Input)}, Checkpoint: source.Checkpoint, ResumeMode: model.TaskResumeModeRecover, Bind: transferCopyOwner}
		if row.ActiveTaskID != nil {
			return preparation, nil
		}
		if deps.CacheGate == nil {
			return taskengine.RetryPreparation{}, errors.New("cache admission unavailable")
		}
		preparation.Release = deps.CacheGate.HoldRead(model.ContentCacheKey(row.ContentID))
		nextInput := storagepipeline.CopyGenerationInput{CopyID: input.CopyID, Generation: input.Generation + 1}
		preparation.Request = taskengine.EnqueueRequest{Type: model.TaskTypeStorageTransferPlan, IdempotencyKey: storagepipeline.TransferPlanKey(nextInput.CopyID, nextInput.Generation), Input: nextInput}
		preparation.Checkpoint = nil
		preparation.ResumeMode = model.TaskResumeModeExecute
		preparation.Bind = func(ctx context.Context, repos *repository.Repositories, _, next *model.Task) error {
			row, err := repos.Contents.RetryFailedCopy(ctx, input.CopyID)
			if err != nil {
				return err
			}
			if row == nil || row.WorkGeneration != input.Generation {
				return repository.ErrConflict
			}
			return repos.Contents.BindCopyTask(ctx, input.CopyID, nextInput.Generation, next.ID)
		}
		return preparation, nil
	}
}
