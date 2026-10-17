package uploadplan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"

	"github.com/strahe/synaps3/internal/objectlimits"

	"github.com/strahe/synaps3/internal/storagepipeline"

	"github.com/strahe/synaps3/internal/storagereplacement"
	idtypes "github.com/strahe/synaps3/internal/types"
	sdkcosts "github.com/strahe/synapse-go/costs"

	bindingtask "github.com/strahe/synaps3/internal/task/binding"
)

type Dependencies struct {
	Repositories *repository.Repositories
	Storage      synapse.StorageClient
	Selector     *bindingtask.Selector
	Messenger    *taskengine.Messenger

	Logger *slog.Logger
}

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
		return nil, errors.New("upload plan scheduling dependencies are required")
	}
	h := &Handler{deps: deps}
	h.FuncHandler = h.uploadPlanHandler()
	return h, nil
}

func (h *Handler) uploadPlanHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeUploadPlan, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec: taskengine.StrictJSONCodec(func(input *storagepipeline.UploadPlanInput) error {
			return storagepipeline.ValidateUploadPlanInput(*input)
		}),
		Policy: taskengine.ExecutionPolicy{MaxAttempts: 10, Backoff: taskengine.BackoffPolicy{InitialDelay: 30 * time.Second, Multiplier: 2, MaximumDelay: 5 * time.Minute, Jitter: 0.2}}, AllowRetry: true,
		Identity: taskengine.IdentityFromInput(model.TaskSubjectStorageContent, func(input storagepipeline.UploadPlanInput) (int64, int64) {
			return input.ContentID, 0
		}),
	}
	definition.InspectRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
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
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, err := taskengine.DecodeInput[storagepipeline.UploadPlanInput](execution)
		if err != nil {
			return decodeFailure(string(definition.Type), err)
		}
		if h.deps.Storage == nil || h.deps.Messenger == nil {
			return taskengine.Fail(errors.New("storage task dependencies are unavailable"), "dependency_unavailable", nil)
		}
		// Ingest is planned for the bytes, not for one version of them, so the
		// content row is the whole subject of this task.
		upload, err := h.deps.Repositories.Contents.GetByID(ctx, input.ContentID)
		if err != nil {
			return retryTask(err, "upload_load_failed")
		}
		if upload == nil {
			return taskengine.Cancel("Storage is no longer required", nil)
		}
		unreferenced, err := h.deps.Repositories.Objects.ContentIsUnreferenced(ctx, upload.ID)
		if err != nil {
			return retryTask(err, "upload_reference_check_failed")
		}
		if unreferenced {
			return taskengine.Cancel("Storage is no longer required", nil)
		}
		bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, upload.BucketID)
		if err != nil {
			return retryTask(err, "upload_bucket_load_failed")
		}
		if bucket == nil {
			return taskengine.Fail(repository.ErrNotFound, "upload_bucket_missing", nil)
		}
		if upload.AcceptedAt != nil {
			return taskengine.Complete("Replicas are ready", nil)
		}

		plan, err := h.deps.Selector.SelectBucketBindings(ctx, bucket, model.ClampStorageCopies(upload.RequestedCopies))
		if err != nil {
			if bindingtask.SelectionNeedsWait(err) {
				reason, message := bindingtask.SelectionWait(err)
				return taskengine.WaitForObservation(model.TaskResumeModeExecute, reason, message, bindingtask.SelectionWaitSettlement(execution.ID(), err))
			}
			if errors.Is(err, providerselect.ErrCandidatesTemporarilyUnavailable) {
				return taskengine.Retry(h.logStorageDependencyFailure(ctx, execution, "providers", err), "provider_selection_unavailable", bindingtask.SelectionSettlement(execution.ID(), err)).WithResumeMode(model.TaskResumeModeExecute)
			}
			if errors.Is(err, providerselect.ErrNoTrustedProvider) || synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
				return h.waitForStorageDependency(ctx, execution, "providers", "Waiting for storage providers", err)
			}
			return taskengine.Retry(synapse.SummarizedError(err), "storage_selection_failed", bindingtask.SelectionSettlement(execution.ID(), err))
		}
		if len(plan) < model.ClampStorageCopies(upload.RequestedCopies) {
			waiting, attention, err := uploadReplacementWait(ctx, h.deps.Repositories, bucket.ID, upload.RequestedCopies, plan)
			if err != nil {
				return retryTask(err, "upload_dependency_check_failed")
			}
			if waiting {
				message := "Waiting for the replacement provider to finish setup"
				if attention {
					message = "Storage replacement needs attention. Check it on the bucket page."
				}
				return taskengine.Wait(model.TaskResumeModeExecute, storagepipeline.UploadPlanReplacementWaitReason, message, func(ctx context.Context, repos *repository.Repositories) error {
					if err := repos.Buckets.LockByID(ctx, bucket.ID); err != nil {
						return err
					}
					currentWaiting, currentAttention, err := uploadReplacementWait(ctx, repos, bucket.ID, upload.RequestedCopies, plan)
					if err != nil {
						return err
					}
					// Activation wakes pending plans under this same bucket lock;
					// if it won the race, recover without committing a stale wait.
					if !currentWaiting || currentAttention != attention {
						return repository.ErrConflict
					}
					return nil
				})
			}
			return taskengine.WaitForObservation(model.TaskResumeModeExecute, "providers", "Waiting for storage providers", nil)
		}
		targets := make([]synapse.StorageTarget, 0, len(plan))
		for i := range plan {
			targets = append(targets, plan[i].Target)
		}
		dataSize := uint64(objectlimits.MinFOCUploadSize)
		if upload.ContentSize > int64(dataSize) {
			dataSize = uint64(upload.ContentSize)
		}
		costs, err := h.deps.Storage.PrepareUpload(ctx, dataSize, targets)
		if err != nil {
			if synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
				return h.waitForStorageDependency(ctx, execution, "funding", "Waiting for storage funding", err)
			}
			return retryTask(err, "storage_funding_failed")
		}
		if costs == nil || !costs.Ready {
			return taskengine.WaitForObservation(model.TaskResumeModeExecute, "funding", uploadFundingWaitMessage(costs), nil)
		}
		bindingPlan := make([]bindingtask.Plan, 0, len(plan))
		for i := range plan {
			entry := plan[i]
			frozen := bindingtask.Plan{
				CopyIndex: entry.CopyIndex,
				Admission: entry.Admission,
				Provider:  idtypes.OnChainIDFromSDK(entry.Target.ProviderID()),
			}
			if ref, ok := entry.Target.DataSetRef(); ok {
				frozen.DataSet = &ref
			}
			bindingPlan = append(bindingPlan, frozen)
		}
		sort.Slice(bindingPlan, func(i, j int) bool { return bindingPlan[i].CopyIndex < bindingPlan[j].CopyIndex })
		ingressProviderID, err := h.ingressProviderID(ctx, bindingPlan)
		if err != nil {
			return retryTask(err, "ingress_selection_failed")
		}

		return taskengine.Complete("Storage work scheduled", func(ctx context.Context, repos *repository.Repositories) error {
			if err := repos.Buckets.LockByID(ctx, bucket.ID); err != nil {
				return err
			}
			if err := bindingtask.ValidateSelection(ctx, repos, bucket.ID, bindingPlan); err != nil {
				return err
			}
			bindings := make([]model.StorageDataSet, 0, len(bindingPlan))
			for i := range bindingPlan {
				entry := bindingPlan[i]
				binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
					BucketID: bucket.ID, ProviderID: entry.Provider,
					CopyIndex: entry.CopyIndex, CreatedByContentID: upload.ID,
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
					binding.DataSetID = &dataSetID
					binding.ClientDataSetID = &clientDataSetID
				}
				bindings = append(bindings, *binding)
			}
			sort.Slice(bindings, func(i, j int) bool { return bindings[i].CopyIndex < bindings[j].CopyIndex })
			ingress, err := repos.Contents.GetIngressCopy(ctx, upload.ID)
			if err != nil {
				return err
			}
			copyBindings := make([]repository.UploadCopyBindingInput, 0, len(bindings))
			for i := range bindings {
				method := model.StorageCopyTransferMethodPeerPull
				if (ingress != nil && bindings[i].ProviderID.Equal(ingress.ProviderID)) || (ingress == nil && bindings[i].ProviderID.Equal(ingressProviderID)) {
					method = model.StorageCopyTransferMethodIngress
				}
				copyBindings = append(copyBindings, repository.UploadCopyBindingInput{
					StorageDataSetID: bindings[i].ID, CopyIndex: bindings[i].CopyIndex,
					TransferMethod: method, ProviderID: bindings[i].ProviderID,
				})
			}
			if err := repos.Contents.CreateUploadCopiesForBindings(ctx, upload.ID, copyBindings); err != nil {
				return err
			}
			for i := range bindings {
				binding := &bindings[i]
				if binding.Status != model.StorageDataSetStatusReady {
					if binding.EnsureTaskID == nil {
						if err := h.enqueueDataSetEnsure(ctx, repos, binding); err != nil && !errors.Is(err, repository.ErrConflict) {
							return err
						}
					}
					continue
				}
				copyRow, err := repos.Contents.GetUploadCopyForDataSet(ctx, upload.ID, binding.ID)
				if err != nil {
					return err
				}
				if copyRow != nil && copyRow.WorkTaskID() == nil && copyRow.Status == model.StorageCopyStatusPending {
					if err := h.startCopyTransfer(ctx, repos, copyRow.ID); err != nil && !errors.Is(err, repository.ErrConflict) {
						return err
					}
				}
			}
			return nil
		})
	}
	return taskengine.NewFuncHandler(definition, run, run)
}

func uploadReplacementWait(ctx context.Context, repos *repository.Repositories, bucketID int64, requestedCopies int, plan []bindingtask.SelectedBinding) (bool, bool, error) {
	requestedCopies = model.ClampStorageCopies(requestedCopies)
	selected := make(map[int]bool, len(plan))
	for _, entry := range plan {
		selected[entry.CopyIndex] = true
	}
	bindings, err := repos.Contents.ListDataSetBindings(ctx, bucketID)
	if err != nil {
		return false, false, err
	}
	blocked := 0
	attention := false
	for _, binding := range bindings {
		if !binding.IsCurrent || binding.CopyIndex >= requestedCopies || selected[binding.CopyIndex] || binding.DataSetID != nil {
			continue
		}
		stopped, err := repos.Contents.DataSetCreationStopped(ctx, binding.ID)
		if err != nil {
			return false, false, err
		}
		if !stopped {
			continue
		}
		_, _, eligibilityErr := repos.Replacements.SourceEligibility(ctx, binding.ID)
		if eligibilityErr != nil && storagereplacement.Code(eligibilityErr) == "" {
			return false, false, eligibilityErr
		}
		blocked++
		attention = attention || !errors.Is(eligibilityErr, storagereplacement.ErrActiveReplacement)
	}
	return blocked > 0 && blocked == requestedCopies-len(selected), attention, nil
}

func (h *Handler) waitForStorageDependency(ctx context.Context, execution taskengine.Execution, reason, message string, err error) taskengine.Result {
	if (synapse.IsProviderCandidateWait(err) || errors.Is(err, providerselect.ErrNoTrustedProvider)) && !bindingtask.SelectionFailed(err) {
		return taskengine.WaitForObservation(model.TaskResumeModeExecute, reason, message, bindingtask.SelectionWaitSettlement(execution.ID(), err))
	}
	return taskengine.RetryInMode(h.logStorageDependencyFailure(ctx, execution, reason, err), reason, model.TaskResumeModeExecute, bindingtask.SelectionSettlement(execution.ID(), err))
}

func (h *Handler) logStorageDependencyFailure(ctx context.Context, execution taskengine.Execution, reason string, err error) error {
	summary := synapse.SummarizedError(err)
	level := slog.LevelWarn
	if execution.LastError() == errtext.Redact(summary.Error()) {
		level = slog.LevelDebug
	}
	h.deps.Logger.Log(ctx, level, "storage task waiting for dependency",
		"task_id", execution.ID(), "task_type", execution.Type(), "wait_reason", reason, "error", summary)
	return summary
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

func (h *Handler) ingressProviderID(ctx context.Context, plan []bindingtask.Plan) (idtypes.OnChainID, error) {
	if len(plan) == 0 {
		return idtypes.OnChainID{}, repository.ErrInvalidInput
	}
	ids := make([]idtypes.OnChainID, len(plan))
	for i := range plan {
		ids[i] = plan[i].Provider
	}
	speeds, err := h.deps.Repositories.ProviderRankingSpeeds(ctx, ids)
	if err != nil {
		return idtypes.OnChainID{}, err
	}
	entries := make([]providerselect.IngressEntry, len(plan))
	for i := range plan {
		entries[i] = providerselect.IngressEntry{CopyIndex: plan[i].CopyIndex, BytesPerSecond: speeds[plan[i].Provider.String()]}
	}
	best := providerselect.RankIngress(entries)[0]
	for _, entry := range plan {
		if entry.CopyIndex == best.CopyIndex {
			return entry.Provider, nil
		}
	}
	return idtypes.OnChainID{}, repository.ErrNotFound
}

func (h *Handler) enqueueDataSetEnsure(ctx context.Context, tx *repository.Repositories, row *model.StorageDataSet) error {
	if row == nil {
		return repository.ErrInvalidInput
	}
	return h.deps.Messenger.Handover(ctx, tx, storagepipeline.EnsureDataSet{BindingID: row.ID})
}

func (h *Handler) startCopyTransfer(ctx context.Context, tx *repository.Repositories, copyID int64) error {
	return h.deps.Messenger.Handover(ctx, tx, storagepipeline.StartCopyTransfer{CopyID: copyID})
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.Retry(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
