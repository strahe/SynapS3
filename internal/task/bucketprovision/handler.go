package bucketprovision

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"

	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"

	"github.com/strahe/synaps3/internal/bucketlifecycle"

	"github.com/strahe/synaps3/internal/objectlimits"

	"github.com/strahe/synaps3/internal/storagepipeline"

	idtypes "github.com/strahe/synaps3/internal/types"
	sdkcosts "github.com/strahe/synapse-go/costs"

	bindingtask "github.com/strahe/synaps3/internal/task/binding"
)

type Dependencies struct {
	Repositories  *repository.Repositories
	Storage       synapse.StorageClient
	Selector      *bindingtask.Selector
	Messenger     *taskengine.Messenger
	DefaultCopies int
	Logger        *slog.Logger
}

const (
	storageDependencyWait = time.Minute
	storagePollInterval   = 5 * time.Second
)

type Handler struct {
	*taskengine.FuncHandler
	deps Dependencies
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("task repositories are required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Messenger == nil || deps.Selector == nil {
		return nil, errors.New("bucket task scheduling dependencies are required")
	}
	h := &Handler{deps: deps}
	h.FuncHandler = h.bucketProvisionHandler()
	return h, nil
}

func (h *Handler) bucketProvisionHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeBucketProvision, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec: taskengine.StrictJSONCodec(func(input *bucketlifecycle.ProvisionInput) error {
			return bucketlifecycle.ValidateProvisionInput(*input)
		}),
		// Provisioning re-reads bucket state on every wake and can wait a long
		// time for providers, so transient errors must not exhaust it.
		RetryLimit: nil, AllowRetry: true,
	}
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, err := taskengine.DecodeInput[bucketlifecycle.ProvisionInput](execution)
		if err != nil {
			return decodeFailure(string(definition.Type), err)
		}
		if h.deps.Storage == nil || h.deps.Messenger == nil {
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
				stopped, err := h.deps.Repositories.Contents.DataSetCreationStopped(ctx, binding.ID)
				if err != nil {
					return retryTask(err, "dataset_permission_load_failed")
				}
				if !stopped {
					allPendingWorkBound = false
				}
			}
		}
		if ready >= required {
			return taskengine.Complete("Bucket storage is ready", func(ctx context.Context, repos *repository.Repositories) error {
				return h.promoteBucketReady(ctx, repos, bucket.ID, required)
			})
		}
		if covered == required && allPendingWorkBound {
			return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "storage_service", "Preparing bucket storage", nil)
		}

		selected, err := h.deps.Selector.SelectBucketBindings(ctx, bucket, required)
		if err != nil {
			if errors.Is(err, providerselect.ErrNoTrustedProvider) {
				return taskengine.Fail(providerselect.ErrNoTrustedProvider, "required_provider_unavailable", nil)
			}
			if synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
				return h.waitForStorageDependency(ctx, execution, "providers", "Waiting for storage providers", err)
			}
			return retryTask(err, "storage_selection_failed")
		}
		if len(selected) < required {
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "providers", "Waiting for storage providers", nil)
		}
		targets := make([]synapse.StorageTarget, 0, len(selected))
		for i := range selected {
			targets = append(targets, selected[i].Target)
		}
		costs, err := h.deps.Storage.PrepareUpload(ctx, uint64(objectlimits.MinFOCUploadSize), targets)
		if err != nil {
			if synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
				return h.waitForStorageDependency(ctx, execution, "funding", "Waiting for storage funding", err)
			}
			return retryTask(err, "storage_funding_failed")
		}
		if costs == nil || !costs.Ready {
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "funding", uploadFundingWaitMessage(costs), nil)
		}

		plan := make([]bindingtask.Plan, 0, len(selected))
		for i := range selected {
			entry := selected[i]
			frozen := bindingtask.Plan{
				CopyIndex: entry.CopyIndex,
				Admission: entry.Admission,
				Provider:  idtypes.OnChainIDFromSDK(entry.Target.ProviderID()),
			}
			if ref, ok := entry.Target.DataSetRef(); ok {
				frozen.DataSet = &ref
			}
			plan = append(plan, frozen)
		}

		settlement := func(ctx context.Context, repos *repository.Repositories) error {
			// Replacement activation locks the bucket before the data set it
			// drains, and promotion below updates the bucket after these data
			// sets are locked, so the bucket is locked first here too.
			if err := repos.Buckets.LockByID(ctx, bucket.ID); err != nil {
				return err
			}
			if err := bindingtask.ValidateSelection(ctx, repos, bucket.ID, plan); err != nil {
				return err
			}
			created := make([]model.StorageDataSet, 0, len(plan))
			for i := range plan {
				entry := plan[i]
				binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
					BucketID: bucket.ID, ProviderID: entry.Provider, CopyIndex: entry.CopyIndex,
				})
				if err != nil {
					return err
				}
				if entry.DataSet != nil {
					dataSetID, clientDataSetID, err := bindingtask.DataSetRefIDs(binding, *entry.DataSet)
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
				stopped, err := repos.Contents.DataSetCreationStopped(ctx, binding.ID)
				if err != nil {
					return err
				}
				if stopped {
					continue
				}
				if err := h.enqueueDataSetEnsure(ctx, repos, binding); err != nil && !errors.Is(err, repository.ErrConflict) {
					return err
				}
			}
			return h.promoteBucketReady(ctx, repos, bucket.ID, required)
		}
		allResolved := true
		for i := range plan {
			if plan[i].DataSet == nil {
				allResolved = false
				break
			}
		}
		if allResolved {
			return taskengine.Complete("Bucket storage is ready", settlement)
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "storage_service", "Preparing bucket storage", settlement)
	}
	return taskengine.NewFuncHandler(definition, run, run)
}

func (h *Handler) promoteBucketReady(ctx context.Context, tx *repository.Repositories, bucketID int64, required int) error {
	return promoteBucketReady(ctx, tx, h.deps.Messenger, bucketID, required)
}

func (h *Handler) effectiveBucketCopies(bucket *model.Bucket) int {
	if bucket == nil {
		return model.ClampStorageCopies(h.deps.DefaultCopies)
	}
	return model.ClampStorageCopies(bucket.DefaultCopies)
}

func (h *Handler) waitForStorageDependency(ctx context.Context, execution taskengine.Execution, reason, message string, err error) taskengine.Result {
	summary := synapse.SummarizedError(err)
	level := slog.LevelWarn
	if execution.LastError() == summary.Error() {
		level = slog.LevelDebug
	}
	h.deps.Logger.Log(ctx, level, "storage task waiting for dependency",
		"task_id", execution.ID(), "task_type", execution.Type(), "wait_reason", reason, "error", summary)
	return taskengine.SuspendWithError(model.TaskResumeModeExecute, storageDependencyWait, reason, message, summary, nil)
}

func uploadFundingWaitMessage(costs *sdkcosts.MultiContextCosts) string {
	parts := make([]string, 0, 2)
	if costs != nil && costs.DepositNeeded != nil && costs.DepositNeeded.Sign() > 0 {
		parts = append(parts, fmt.Sprintf("deposit %s USDFC base units", costs.DepositNeeded.String()))
	}
	if costs != nil && costs.NeedsFWSSMaxApproval {
		parts = append(parts, "approve FWSS spending")
	}
	if len(parts) == 0 {
		return "Waiting for Filecoin payment funding"
	}
	return "Waiting for Filecoin payment funding: " + strings.Join(parts, "; ")
}

func (h *Handler) enqueueDataSetEnsure(ctx context.Context, tx *repository.Repositories, row *model.StorageDataSet) error {
	if row == nil {
		return repository.ErrInvalidInput
	}
	return h.deps.Messenger.Handover(ctx, tx, storagepipeline.EnsureDataSet{BindingID: row.ID})
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}
