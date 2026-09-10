package worker

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectlimits"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	idtypes "github.com/strahe/synaps3/internal/types"
)

func (h *TaskHandlers) bucketProvisionHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeBucketProvision, InputVersion: 1,
		Codec: taskengine.StrictJSONCodec(func(input *bucketlifecycle.ProvisionInput) error {
			return bucketlifecycle.ValidateProvisionInput(*input)
		}),
		RetryLimit: h.retryLimit(), AllowRetry: true,
	}
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, err := taskengine.DecodeInput[bucketlifecycle.ProvisionInput](execution)
		if err != nil {
			return decodeFailure(string(definition.Type), err)
		}
		if h.deps.Storage == nil || h.taskService == nil {
			return taskengine.Fail(errors.New("storage task dependencies are unavailable"), "dependency_unavailable", nil)
		}
		bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, input.BucketID)
		if err != nil {
			return retryTask(err, "bucket_load_failed")
		}
		if bucket == nil {
			return taskengine.Cancel("Bucket no longer exists", nil)
		}
		// A ready bucket is still provisioned here after its replica target
		// grows: readiness is decided by the data sets below, not by the status
		// the bucket happened to have when this task was enqueued.

		required := h.effectiveBucketCopies(bucket)
		bindings, err := h.deps.Repositories.Contents.ListDataSetBindings(ctx, bucket.ID)
		if err != nil {
			return retryTask(err, "dataset_bindings_load_failed")
		}
		// A generation that ended gives up its slot, so only live generations
		// reach the body of this loop and provisioning always has a next move.
		ready := 0
		covered := 0
		allPendingWorkBound := true
		for i := range bindings {
			binding := &bindings[i]
			if !binding.IsCurrent || binding.CopyIndex >= required {
				continue
			}
			covered++
			if binding.Status == model.StorageDataSetStatusReady {
				ready++
			} else if (binding.Status == model.StorageDataSetStatusPending || binding.Status == model.StorageDataSetStatusCreating) && binding.EnsureTaskID == nil {
				allPendingWorkBound = false
			}
		}
		if ready >= required {
			return taskengine.Complete("Bucket storage is ready", func(ctx context.Context, repos *repository.Repositories) error {
				_, err := repos.Buckets.PromoteReadyIfProvisioned(ctx, bucket.ID, required)
				return err
			})
		}
		if covered == required && allPendingWorkBound {
			return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "storage_service", "Preparing bucket storage", nil)
		}

		selected, err := h.selectBucketBindings(ctx, bucket, required)
		if err != nil {
			if synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
				return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "providers", "Waiting for storage providers", nil)
			}
			return retryTask(err, "storage_selection_failed")
		}
		if len(selected) < required {
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "providers", "Waiting for storage providers", nil)
		}
		targets := make([]synapse.StorageTarget, 0, len(selected))
		for i := range selected {
			targets = append(targets, selected[i].target)
		}
		costs, err := h.deps.Storage.PrepareUpload(ctx, uint64(objectlimits.MinFOCUploadSize), targets)
		if err != nil {
			if synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
				return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "funding", "Waiting for storage funding", nil)
			}
			return retryTask(err, "storage_funding_failed")
		}
		if costs == nil || !costs.Ready {
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "funding", uploadFundingWaitMessage(costs), nil)
		}

		plan := make([]uploadBindingPlan, 0, len(selected))
		for i := range selected {
			entry := selected[i]
			frozen := uploadBindingPlan{
				copyIndex: entry.copyIndex,
				provider:  idtypes.OnChainIDFromSDK(entry.target.ProviderID()),
			}
			if ref, ok := entry.target.DataSetRef(); ok {
				frozen.dataSet = &ref
			}
			plan = append(plan, frozen)
		}

		settlement := func(ctx context.Context, repos *repository.Repositories) error {
			created := make([]model.StorageDataSet, 0, len(plan))
			for i := range plan {
				entry := plan[i]
				binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
					BucketID: bucket.ID, ProviderID: entry.provider, CopyIndex: entry.copyIndex,
				})
				if err != nil {
					return err
				}
				if entry.dataSet != nil {
					dataSetID, clientDataSetID, err := dataSetRefIDs(binding, *entry.dataSet)
					if err != nil {
						return err
					}
					if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
						ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
					}); err != nil {
						return err
					}
					binding.Status = model.StorageDataSetStatusReady
				}
				created = append(created, *binding)
			}
			for i := range created {
				binding := &created[i]
				if binding.Status == model.StorageDataSetStatusReady || binding.EnsureTaskID != nil {
					continue
				}
				if err := h.enqueueDataSetEnsure(ctx, repos, binding); err != nil && !errors.Is(err, repository.ErrConflict) {
					return err
				}
			}
			_, err := repos.Buckets.PromoteReadyIfProvisioned(ctx, bucket.ID, required)
			return err
		}
		allResolved := true
		for i := range plan {
			if plan[i].dataSet == nil {
				allResolved = false
				break
			}
		}
		if allResolved {
			return taskengine.Complete("Bucket storage is ready", settlement)
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "storage_service", "Preparing bucket storage", settlement)
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func (h *TaskHandlers) effectiveBucketCopies(bucket *model.Bucket) int {
	if bucket == nil {
		return model.ClampStorageCopies(h.deps.DefaultCopies)
	}
	return model.ClampStorageCopies(bucket.DefaultCopies)
}
