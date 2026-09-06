package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectlimits"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

const (
	storageDependencyWait = time.Minute
	storagePollInterval   = 5 * time.Second
	dataSetAttentionAfter = 15 * time.Minute
)

type dataSetCreationCheckpoint struct {
	AttemptedAt     time.Time `json:"attempted_at"`
	TransactionID   string    `json:"transaction_id,omitempty"`
	StatusURL       string    `json:"status_url,omitempty"`
	ClientDataSetID string    `json:"client_data_set_id,omitempty"`
}

type storeCheckpoint struct {
	AttemptedAt time.Time `json:"attempted_at"`
	PieceCID    string    `json:"piece_cid,omitempty"`
}

type pullCheckpoint struct {
	AttemptedAt time.Time `json:"attempted_at"`
	// AttemptID names the ledger row this request was recorded in, so recovery
	// resolves the same row the first execute created.
	AttemptID          string `json:"attempt_id"`
	PieceCID           string `json:"piece_cid"`
	SourceProviderID   string `json:"source_provider_id"`
	SourceDataSetID    string `json:"source_data_set_id"`
	SourcePieceID      string `json:"source_piece_id"`
	SourceRetrievalURL string `json:"source_retrieval_url"`
	CommitExtraDataHex string `json:"commit_extra_data_hex"`
}

type selectedBinding struct {
	copyIndex int
	target    synapse.StorageTarget
}

type uploadBindingPlan struct {
	copyIndex int
	provider  idtypes.OnChainID
	dataSet   *storage.DataSetRef
}

func (h *TaskHandlers) uploadPlanHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeUploadPlan, InputVersion: 1,
		Codec: taskengine.StrictJSONCodec(func(input *storagepipeline.UploadPlanInput) error {
			return storagepipeline.ValidateUploadPlanInput(*input)
		}),
		RetryLimit: h.retryLimit(), AllowRetry: true,
	}
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, err := taskengine.DecodeInput[storagepipeline.UploadPlanInput](execution)
		if err != nil {
			return decodeFailure(string(definition.Type), err)
		}
		if h.deps.Storage == nil || h.taskService == nil {
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
			return taskengine.Complete("Storage copies are ready", nil)
		}

		plan, err := h.selectUploadBindings(ctx, bucket, upload)
		if err != nil {
			if synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
				return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "providers", "Waiting for storage providers", nil)
			}
			return retryTask(err, "storage_selection_failed")
		}
		if len(plan) < model.ClampStorageCopies(upload.RequestedCopies) {
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "providers", "Waiting for storage providers", nil)
		}
		targets := make([]synapse.StorageTarget, 0, len(plan))
		for i := range plan {
			targets = append(targets, plan[i].target)
		}
		dataSize := uint64(objectlimits.MinFOCUploadSize)
		if upload.ContentSize > int64(dataSize) {
			dataSize = uint64(upload.ContentSize)
		}
		costs, err := h.deps.Storage.PrepareUpload(ctx, dataSize, targets)
		if err != nil {
			if synapse.IsProviderUnavailable(err) || synapse.IsNoProviderCandidates(err) {
				return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "funding", "Waiting for storage funding", nil)
			}
			return retryTask(err, "storage_funding_failed")
		}
		if costs == nil || !costs.Ready {
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "funding", uploadFundingWaitMessage(costs), nil)
		}
		bindingPlan := make([]uploadBindingPlan, 0, len(plan))
		for i := range plan {
			entry := plan[i]
			frozen := uploadBindingPlan{
				copyIndex: entry.copyIndex,
				provider:  idtypes.OnChainIDFromSDK(entry.target.ProviderID()),
			}
			if ref, ok := entry.target.DataSetRef(); ok {
				frozen.dataSet = &ref
			}
			bindingPlan = append(bindingPlan, frozen)
		}

		return taskengine.Complete("Storage work scheduled", func(ctx context.Context, repos *repository.Repositories) error {
			bindings := make([]model.StorageDataSet, 0, len(bindingPlan))
			for i := range bindingPlan {
				entry := bindingPlan[i]
				binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
					BucketID: bucket.ID, ProviderID: entry.provider,
					CopyIndex: entry.copyIndex, CreatedByContentID: upload.ID,
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
						ID: binding.ID, ContentID: upload.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
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
			copyBindings := make([]repository.UploadCopyBindingInput, 0, len(bindings))
			for i := range bindings {
				method := model.StorageCopyTransferMethodPeerPull
				if i == 0 {
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
				if copyRow != nil && copyRow.ActiveTaskID == nil {
					if err := h.enqueueInitialCopyTask(ctx, repos, copyRow.ID, model.TaskTypeStorageTransferPlan); err != nil && !errors.Is(err, repository.ErrConflict) {
						return err
					}
				}
			}
			return nil
		})
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func uploadFundingWaitMessage(costs *storage.MultiContextCosts) string {
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

func (h *TaskHandlers) selectUploadBindings(ctx context.Context, bucket *model.Bucket, upload *model.StorageContent) ([]selectedBinding, error) {
	return h.selectBucketBindings(ctx, bucket, model.ClampStorageCopies(upload.RequestedCopies))
}

func (h *TaskHandlers) selectBucketBindings(ctx context.Context, bucket *model.Bucket, targetCount int) ([]selectedBinding, error) {
	bindings, err := h.deps.Repositories.Contents.ListDataSetBindings(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	targetCount = model.ClampStorageCopies(targetCount)
	selected := make([]selectedBinding, 0, targetCount)
	excluded := make([]sdktypes.BigInt, 0, len(bindings))
	usedIndexes := make(map[int]struct{}, len(bindings))
	for i := range bindings {
		binding := &bindings[i]
		excluded = append(excluded, binding.ProviderID.SDK())
		if !binding.IsCurrent {
			continue
		}
		usedIndexes[binding.CopyIndex] = struct{}{}
		if binding.CopyIndex >= targetCount || len(selected) >= targetCount || (binding.Status != model.StorageDataSetStatusReady && binding.Status != model.StorageDataSetStatusPending && binding.Status != model.StorageDataSetStatusCreating) {
			continue
		}
		target, err := h.openBindingTarget(ctx, bucket.Name, binding)
		if err != nil {
			return nil, err
		}
		selected = append(selected, selectedBinding{copyIndex: binding.CopyIndex, target: target})
	}
	missing := targetCount - len(selected)
	if missing <= 0 {
		sort.Slice(selected, func(i, j int) bool { return selected[i].copyIndex < selected[j].copyIndex })
		return selected, nil
	}
	// Free positions come from the bucket's own slot rows, not from the global
	// maximum: a data set can only exist on a slot the bucket opened, and the
	// foreign key would reject anything else.
	slots, err := h.deps.Repositories.Buckets.ActiveReplicaSlots(ctx, bucket.ID)
	if err != nil {
		return nil, err
	}
	indexes := make([]int, 0, missing)
	for _, copyIndex := range slots {
		if len(indexes) >= missing {
			break
		}
		if _, exists := usedIndexes[copyIndex]; !exists {
			indexes = append(indexes, copyIndex)
		}
	}
	if len(indexes) == 0 {
		sort.Slice(selected, func(i, j int) bool { return selected[i].copyIndex < selected[j].copyIndex })
		return selected, nil
	}
	if missing > len(indexes) {
		missing = len(indexes)
	}
	targets, selectErr := h.deps.Storage.SelectUploadTargets(ctx, storage.SelectUploadContextsOptions{
		Copies: missing, ExcludeProviderIDs: excluded, DataSetMetadata: map[string]string{"bucket": bucket.Name},
	})
	for i := range targets {
		if i >= len(indexes) || targets[i] == nil {
			break
		}
		selected = append(selected, selectedBinding{copyIndex: indexes[i], target: targets[i]})
	}
	if selectErr != nil && len(targets) == 0 {
		return nil, selectErr
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].copyIndex < selected[j].copyIndex })
	return selected, nil
}

func (h *TaskHandlers) dataSetEnsureHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageDataSetEnsure, InputVersion: 1,
		Codec: taskengine.StrictJSONCodec(func(input *storagepipeline.DataSetInput) error {
			return storagepipeline.ValidateDataSetInput(*input)
		}),
		RetryLimit: h.retryLimit(), AllowRetry: true,
	}
	return taskHandler{
		definition: definition,
		execute: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runDataSetEnsure(ctx, execution, true)
		},
		recover: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runDataSetEnsure(ctx, execution, false)
		},
	}
}

func (h *TaskHandlers) runDataSetEnsure(ctx context.Context, execution taskengine.Execution, mayCreate bool) taskengine.Result {
	input, err := taskengine.DecodeInput[storagepipeline.DataSetInput](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeStorageDataSetEnsure), err)
	}
	if h.deps.Storage == nil {
		return taskengine.Fail(errors.New("storage client is unavailable"), "dependency_unavailable", nil)
	}
	binding, err := h.deps.Repositories.Contents.AuthorizeDataSetEnsureTask(ctx, input.DataSetID, execution.ID())
	if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
		return taskengine.Cancel("Storage service setup was superseded", nil)
	}
	if err != nil {
		return retryTask(err, "dataset_authorization_failed")
	}
	if binding.Status == model.StorageDataSetStatusReady && binding.DataSetID != nil && !binding.DataSetID.IsZero() {
		return taskengine.Complete("Storage service is ready", func(ctx context.Context, repos *repository.Repositories) error {
			return h.finishDataSetEnsure(ctx, repos, binding, execution.ID())
		})
	}
	bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, binding.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return retryTask(err, "dataset_bucket_load_failed")
	}
	provider, err := h.deps.Storage.OpenProviderTarget(ctx, binding.ProviderID.SDK(), storage.NewProviderContextOptions{
		DataSetMetadata: map[string]string{"bucket": bucket.Name},
	})
	if err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "provider", "Waiting for storage provider", nil)
	}
	matching, err := h.deps.Storage.FindMatchingDataSet(ctx, binding.ProviderID.SDK(), map[string]string{"bucket": bucket.Name}, provider.CDNEnabled())
	if err == nil && matching != nil {
		dataSetID, clientDataSetID, identityErr := dataSetRefIDs(binding, *matching)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, clientDataSetID)
	}
	if err != nil {
		if synapse.IsProviderUnavailable(err) {
			return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "provider", "Waiting for storage provider", nil)
		}
		return retryTask(err, "dataset_discovery_failed")
	}

	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[dataSetCreationCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if !hasCheckpoint && binding.CreateTransactionID != nil && binding.CreateStatusURL != nil && binding.ClientDataSetID != nil {
		checkpoint = dataSetCreationCheckpoint{
			AttemptedAt: time.Now().UTC(), TransactionID: *binding.CreateTransactionID,
			StatusURL: *binding.CreateStatusURL, ClientDataSetID: binding.ClientDataSetID.String(),
		}
		hasCheckpoint = true
	}
	if hasCheckpoint && checkpoint.TransactionID != "" {
		return h.waitDataSetCreation(ctx, execution, binding, provider, checkpoint)
	}
	if hasCheckpoint {
		if time.Since(checkpoint.AttemptedAt) >= dataSetAttentionAfter {
			err := errors.New("storage service creation outcome could not be recovered")
			return taskengine.Fail(err, "dataset_creation_unknown", dataSetFailureSettlement(binding.ID, execution.ID(), err.Error(), false))
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage service creation", nil)
	}
	if !mayCreate {
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage service is ready to create", nil)
	}

	checkpoint = dataSetCreationCheckpoint{AttemptedAt: time.Now().UTC()}
	if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
		return retryTask(err, "dataset_checkpoint_failed")
	}
	var (
		created     *storage.CreateDataSetResult
		createErr   error
		evidenceErr error
		submission  storage.CreateDataSetSubmission
	)
	createCtx, cancelCreate := context.WithCancel(ctx)
	createErr = execution.WithResource(ctx, taskengine.ResourceProviderMutation, func(context.Context) error {
		created, createErr = provider.CreateDataSet(createCtx, &storage.CreateDataSetOptions{OnSubmitted: func(sub storage.CreateDataSetSubmission) {
			submission = sub
			checkpoint.TransactionID = sub.TransactionID
			checkpoint.StatusURL = sub.StatusURL
			if sub.ClientDataSetID != nil {
				checkpoint.ClientDataSetID = sub.ClientDataSetID.String()
			}
			evidenceErr = execution.WriteCheckpointWith(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
					ID: binding.ID, ContentID: derefInt64(binding.CreatedByContentID), TransactionID: sub.TransactionID,
					StatusURL: sub.StatusURL, ClientDataSetID: onChainIDPtr(sub.ClientDataSetID),
				})
			})
			cancelCreate()
		}})
		return createErr
	})
	cancelCreate()
	if evidenceErr != nil {
		clientDataSetID := onChainIDPtr(submission.ClientDataSetID)
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Recording storage service creation", func(ctx context.Context, repos *repository.Repositories) error {
			if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, binding.ID, execution.ID()); err != nil {
				return err
			}
			return repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
				ID: binding.ID, ContentID: derefInt64(binding.CreatedByContentID), TransactionID: submission.TransactionID,
				StatusURL: submission.StatusURL, ClientDataSetID: clientDataSetID,
			})
		})
	}
	if created != nil {
		dataSetID, clientDataSetID, identityErr := dataSetResultIDs(binding, created)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, clientDataSetID)
	}
	if submission.TransactionID != "" || createErr != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage service creation", nil)
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage service creation", nil)
}

func (h *TaskHandlers) waitDataSetCreation(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	checkpoint dataSetCreationCheckpoint,
) taskengine.Result {
	clientID, err := idtypes.ParseOnChainID("clientDataSetID", checkpoint.ClientDataSetID)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	clientSDK := clientID.SDK()
	result, err := provider.WaitForDataSetCreated(ctx, storage.CreateDataSetSubmission{
		ProviderID: binding.ProviderID.SDK(), TransactionID: checkpoint.TransactionID,
		StatusURL: checkpoint.StatusURL, ClientDataSetID: &clientSDK,
	})
	if err != nil {
		if errors.Is(err, pdp.ErrTxRejected) {
			return taskengine.Fail(err, "dataset_creation_rejected", dataSetFailureSettlement(binding.ID, execution.ID(), err.Error(), true))
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage service", nil)
	}
	dataSetID, clientDataSetID, err := dataSetResultIDs(binding, result)
	if err != nil {
		return taskengine.Fail(err, "dataset_identity_mismatch", nil)
	}
	return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, clientDataSetID)
}

// dataSetFailureSettlement records that a generation could not be created.
// rejected says the chain refused the creation, which is proof no data set
// exists; an unknown outcome is not, and leaves the row in place so an operator
// still has the provider and the transaction to work from.
func dataSetFailureSettlement(dataSetID, taskID int64, message string, rejected bool) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, dataSetID, taskID); err != nil {
			return err
		}
		if err := repos.Contents.MarkDataSetFailed(ctx, dataSetID, message); err != nil {
			return err
		}
		if !rejected {
			// The outcome is unknown, so the generation may still exist on the
			// provider. Retrying this task rediscovers it through
			// FindMatchingDataSet and continues the copies bound to it, and
			// continuation skips copies that already failed — so they are left
			// alone rather than terminated on a guess.
			return nil
		}
		// The chain refused the creation, so nothing will ever serve these
		// copies: the data set they name will not exist, and continuation would
		// have nothing to rediscover. Failing them keeps their objects from
		// sitting at "uploading" forever.
		if err := failDataSetCopies(ctx, repos, dataSetID, message); err != nil {
			return err
		}
		// The generation ends here rather than being deleted. Retiring it frees
		// the provider for the bucket while keeping the record that this one was
		// tried, and it is the only status the provider reservation index lets
		// go of.
		if _, err := repos.Contents.RetireRejectedDataSet(ctx, dataSetID); err != nil {
			return err
		}
		return nil
	}
}

func failDataSetCopies(ctx context.Context, repos *repository.Repositories, dataSetID int64, message string) error {
	copies, err := repos.Contents.ListIncompleteCopiesForDataSet(ctx, dataSetID)
	if err != nil {
		return err
	}
	for i := range copies {
		copyRow := &copies[i]
		if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{
			StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex,
			LastError: message,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (h *TaskHandlers) completeDataSetEnsure(binding *model.StorageDataSet, taskID int64, dataSetID, clientDataSetID idtypes.OnChainID) taskengine.Result {
	return taskengine.Complete("Storage service is ready", func(ctx context.Context, repos *repository.Repositories) error {
		if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID: binding.ID, ContentID: derefInt64(binding.CreatedByContentID), DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
		}); err != nil {
			return err
		}
		return h.finishDataSetEnsure(ctx, repos, binding, taskID)
	})
}

func (h *TaskHandlers) finishDataSetEnsure(ctx context.Context, repos *repository.Repositories, binding *model.StorageDataSet, taskID int64) error {
	if err := repos.Contents.CompleteDataSetEnsureTask(ctx, binding.ID, taskID); err != nil {
		return err
	}
	if err := h.continueDataSetCopies(ctx, repos, binding.ID); err != nil {
		return err
	}
	bucket, err := repos.Buckets.GetByID(ctx, binding.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return err
	}
	required := h.effectiveBucketCopies(bucket)
	if _, err := repos.Buckets.PromoteReadyIfProvisioned(ctx, bucket.ID, required); err != nil {
		return err
	}
	provisionTask, err := repos.Tasks.GetByIdentity(ctx, model.TaskTypeBucketProvision, bucketlifecycle.ProvisionKey(bucket.ID, bucket.DefaultCopies))
	if err != nil {
		return err
	}
	if provisionTask == nil {
		return nil
	}
	_, err = h.taskService.WakeInTransaction(ctx, repos, []int64{provisionTask.ID})
	return err
}

func (h *TaskHandlers) continueDataSetCopies(ctx context.Context, repos *repository.Repositories, dataSetID int64) error {
	if h.taskService == nil {
		return errors.New("task service is unavailable")
	}
	copies, err := repos.Contents.ListIncompleteCopiesForDataSet(ctx, dataSetID)
	if err != nil {
		return err
	}
	wakeIDs := make([]int64, 0, len(copies))
	for i := range copies {
		copyRow := &copies[i]
		if copyRow.ActiveTaskID != nil {
			wakeIDs = append(wakeIDs, *copyRow.ActiveTaskID)
			continue
		}
		if err := h.enqueueInitialCopyTask(ctx, repos, copyRow.ID, model.TaskTypeStorageTransferPlan); err != nil && !errors.Is(err, repository.ErrConflict) {
			return err
		}
	}
	_, err = h.taskService.WakeInTransaction(ctx, repos, wakeIDs)
	return err
}

func (h *TaskHandlers) transferPlanHandler() taskengine.Handler {
	definition := copyDefinition(model.TaskTypeStorageTransferPlan, h.retryLimit())
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, copyRow, handled, result := h.authorizeCopyTask(ctx, execution)
		if handled {
			return result
		}
		if copyRow.Status == model.StorageCopyStatusCommitted {
			return h.completeCopyTask(input, execution.ID(), "Storage copy is complete")
		}
		if copyRow.Status == model.StorageCopyStatusPieceReady || copyRow.Status == model.StorageCopyStatusCommitting {
			return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageCommitCoordinate, "Storage copy is ready to register")
		}
		binding, err := h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "dataset_load_failed")
		}
		if binding == nil {
			return h.failCopyTask(execution, input, copyRow, repository.ErrNotFound, "dataset_missing")
		}
		if binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || binding.DataSetID.IsZero() {
			return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "dataset", "Waiting for storage service", nil)
		}
		unreferenced, err := h.deps.Repositories.Objects.ContentIsUnreferenced(ctx, copyRow.ContentID)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "copy_owner_load_failed")
		}
		if unreferenced {
			return h.completeCopyTask(input, execution.ID(), "Storage copy is no longer required")
		}
		if copyRow.TransferMethod == model.StorageCopyTransferMethodPeerPull {
			sources, err := h.deps.Repositories.Contents.ListReadableCommittedCopies(ctx, copyRow.ContentID)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "copy_source_load_failed")
			}
			if len(sources) > 0 {
				return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStoragePull, "Storage copy is ready to transfer")
			}
		}
		// Cached bytes are named by the content, so residency is asked of the
		// content's cache entry rather than of a version that happens to exist.
		cacheEntry, err := h.deps.Repositories.CacheEvictions.GetCacheEntry(ctx, copyRow.ContentID)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "copy_cache_load_failed")
		}
		if cacheEntry != nil && cacheEntry.InCache && h.deps.Cache != nil {
			bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, copyRow.BucketID)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "copy_bucket_load_failed")
			}
			if bucket != nil && h.deps.Cache.Exists(ctx, bucket.Name, model.ContentCacheKey(copyRow.ContentID)) {
				return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageStore, "Storage copy is ready to transfer")
			}
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for a readable storage source", nil)
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func (h *TaskHandlers) storeHandler() taskengine.Handler {
	definition := copyDefinition(model.TaskTypeStorageStore, h.retryLimit())
	return taskHandler{
		definition: definition,
		execute: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runStore(ctx, execution, true)
		},
		recover: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runStore(ctx, execution, false)
		},
	}
}

func (h *TaskHandlers) runStore(ctx context.Context, execution taskengine.Execution, mayStore bool) taskengine.Result {
	input, copyRow, handled, result := h.authorizeCopyTask(ctx, execution)
	if handled {
		return result
	}
	if copyRow.Status == model.StorageCopyStatusCommitted {
		return h.completeCopyTask(input, execution.ID(), "Storage copy is complete")
	}
	if copyRow.Status == model.StorageCopyStatusPieceReady || copyRow.Status == model.StorageCopyStatusCommitting {
		return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageCommitCoordinate, "Storage copy is ready to register")
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[storeCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if hasCheckpoint && checkpoint.PieceCID == "" && !mayStore {
		// Store is content addressed. With no domain or checkpoint result, replaying
		// the same immutable bytes cannot create a second logical copy.
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage transfer is ready to resume", nil)
	}
	binding, target, content, bucket, err := h.copyContext(ctx, copyRow)
	if err != nil {
		return h.copyContextFailure(execution, input, copyRow, err, true)
	}
	if checkpoint.PieceCID != "" {
		pieceCID, err := cid.Parse(checkpoint.PieceCID)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		return h.finishPieceTransfer(ctx, execution, input, copyRow, target, pieceCID)
	}
	if !mayStore {
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage transfer is ready", nil)
	}
	if h.deps.Cache == nil || h.deps.CacheGate == nil {
		return h.failCopyTask(execution, input, copyRow, errors.New("cache reader is unavailable"), "dependency_unavailable")
	}
	cacheKey := model.ContentCacheKey(content.ID)
	opened, err := h.deps.CacheGate.Open(cacheKey, func() (io.ReadCloser, *cache.ObjectInfo, error) {
		return h.deps.Cache.Get(ctx, bucket.Name, cacheKey)
	})
	if err != nil {
		if os.IsNotExist(err) {
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for retained cache data", nil)
		}
		return h.retryCopyTask(execution, input, copyRow, err, "cache_open_failed")
	}
	defer func() { _ = opened.Body.Close() }()
	checkpoint = storeCheckpoint{AttemptedAt: time.Now().UTC()}
	if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
		return h.retryCopyTask(execution, input, copyRow, err, "store_checkpoint_failed")
	}
	var stored *storage.StoreResult
	var progress *uploadProgressReporter
	err = execution.WithResource(ctx, taskengine.ResourceProviderMutation, func(ctx context.Context) error {
		if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress {
			progress = h.beginIngressProgress(ctx, execution.ID(), content, bucket)
		}
		options := &storage.StoreOptions{}
		if progress != nil {
			options.OnProgress = progress.OnProgress
		}
		var storeErr error
		stored, storeErr = target.Store(ctx, opened.Body, options)
		return storeErr
	})
	defer progress.Close()
	if err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
	}
	if stored == nil || !stored.PieceCID.Defined() {
		return h.failCopyTask(execution, input, copyRow, errors.New("storage provider returned no piece identity"), "store_result_invalid")
	}
	progress.Flush(content.ContentSize, true)
	checkpoint.PieceCID = stored.PieceCID.String()
	if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Recording storage transfer", nil)
	}
	_ = binding
	return h.finishPieceTransfer(ctx, execution, input, copyRow, target, stored.PieceCID)
}

func (h *TaskHandlers) pullHandler() taskengine.Handler {
	definition := copyDefinition(model.TaskTypeStoragePull, h.retryLimit())
	return taskHandler{
		definition: definition,
		execute: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runPull(ctx, execution, true)
		},
		recover: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runPull(ctx, execution, false)
		},
	}
}

func (h *TaskHandlers) runPull(ctx context.Context, execution taskengine.Execution, mayPull bool) taskengine.Result {
	input, copyRow, handled, result := h.authorizeCopyTask(ctx, execution)
	if handled {
		return result
	}
	if copyRow.Status == model.StorageCopyStatusCommitted {
		return h.completeCopyTask(input, execution.ID(), "Storage copy is complete")
	}
	if copyRow.Status == model.StorageCopyStatusPieceReady || copyRow.Status == model.StorageCopyStatusCommitting {
		return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageCommitCoordinate, "Storage copy is ready to register")
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[pullCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if !hasCheckpoint && !mayPull {
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage transfer is ready", nil)
	}
	_, target, _, _, err := h.copyContext(ctx, copyRow)
	if err != nil {
		return h.copyContextFailure(execution, input, copyRow, err, !hasCheckpoint)
	}
	if !hasCheckpoint {
		sources, err := h.deps.Repositories.Contents.ListReadableCommittedCopies(ctx, copyRow.ContentID)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "copy_source_load_failed")
		}
		if len(sources) == 0 {
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for a readable storage source", nil)
		}
		source := sources[0]
		pieceCID, err := cid.Parse(source.PieceCID)
		if err != nil {
			return h.failCopyTask(execution, input, copyRow, err, "source_identity_invalid")
		}
		extra, err := target.PresignForCommit(ctx, []storage.PieceInput{{PieceCID: pieceCID}})
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "pull_presign_failed")
		}
		attemptID, err := newAttemptID()
		if err != nil {
			return h.failCopyTask(execution, input, copyRow, err, "pull_identity_failed")
		}
		checkpoint = pullCheckpoint{
			AttemptedAt: time.Now().UTC(), AttemptID: attemptID, PieceCID: source.PieceCID,
			SourceProviderID: source.ProviderID.String(), SourceDataSetID: source.DataSetID.String(),
			SourcePieceID: source.PieceID.String(), SourceRetrievalURL: source.RetrievalURL,
			CommitExtraDataHex: hex.EncodeToString(extra),
		}
		if err := execution.WriteCheckpointWith(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.ReservePullRequest(ctx, repository.ReservePullRequestInput{
				CopyID: copyRow.ID, Generation: input.Generation, TaskID: execution.ID(),
				AttemptID:        checkpoint.AttemptID,
				SourceProviderID: source.ProviderID, SourceDataSetID: source.DataSetID, SourcePieceID: source.PieceID,
				SourcePieceCID: source.PieceCID, SourceRetrievalURL: source.RetrievalURL,
				CommitExtraDataHex: checkpoint.CommitExtraDataHex,
			})
		}); err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "pull_checkpoint_failed")
		}
	}
	if !mayPull {
		pieceCID, parseErr := cid.Parse(checkpoint.PieceCID)
		if parseErr != nil {
			return taskengine.Fail(parseErr, "invalid_checkpoint", nil)
		}
		status, statusErr := target.PieceStatus(ctx, pieceCID)
		if statusErr == nil && status != nil && status.Exists {
			return h.finishPieceTransferWithExtra(execution, input, copyRow, target, pieceCID, checkpoint.CommitExtraDataHex, checkpoint.AttemptID)
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
	}
	pieceCID, err := cid.Parse(checkpoint.PieceCID)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	extra, err := hex.DecodeString(checkpoint.CommitExtraDataHex)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	err = execution.WithResource(ctx, taskengine.ResourceProviderMutation, func(ctx context.Context) error {
		_, pullErr := target.Pull(ctx, storage.PullRequest{
			Pieces: []cid.Cid{pieceCID}, ExtraData: extra,
			From: func(cid.Cid) string { return checkpoint.SourceRetrievalURL },
		})
		return pullErr
	})
	if err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
	}
	return h.finishPieceTransferWithExtra(execution, input, copyRow, target, pieceCID, checkpoint.CommitExtraDataHex, checkpoint.AttemptID)
}

func (h *TaskHandlers) commitCoordinateHandler() taskengine.Handler {
	definition := copyDefinition(model.TaskTypeStorageCommitCoordinate, h.retryLimit())
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, copyRow, handled, result := h.authorizeCopyTask(ctx, execution)
		if handled {
			return result
		}
		if copyRow.Status == model.StorageCopyStatusCommitted {
			return h.completeCopyTask(input, execution.ID(), "Storage copy is complete")
		}
		if copyRow.Status != model.StorageCopyStatusPieceReady && copyRow.Status != model.StorageCopyStatusCommitting {
			return h.failCopyTask(execution, input, copyRow, errors.New("storage copy has no transferable piece"), "piece_not_ready")
		}
		return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageCommit, "Storage registration scheduled")
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func (h *TaskHandlers) commitHandler() taskengine.Handler {
	definition := copyDefinition(model.TaskTypeStorageCommit, h.retryLimit())
	return taskHandler{
		definition: definition,
		execute: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runCommit(ctx, execution, true)
		},
		recover: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runCommit(ctx, execution, false)
		},
	}
}

func (h *TaskHandlers) runCommit(ctx context.Context, execution taskengine.Execution, maySubmit bool) taskengine.Result {
	input, copyRow, handled, result := h.authorizeCopyTask(ctx, execution)
	if handled {
		return result
	}
	if copyRow.Status == model.StorageCopyStatusCommitted {
		return h.completeCopyTask(input, execution.ID(), "Storage copy is complete")
	}
	binding, err := h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
	if err != nil || binding == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return h.retryCopyTask(execution, input, copyRow, err, "dataset_load_failed")
	}
	upload, err := h.deps.Repositories.Contents.GetByID(ctx, copyRow.ContentID)
	if err != nil || upload == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return h.retryCopyTask(execution, input, copyRow, err, "upload_load_failed")
	}
	if upload.PieceCID == nil || *upload.PieceCID == "" {
		return h.failCopyTask(execution, input, copyRow, errors.New("storage upload has no piece identity"), "piece_identity_missing")
	}
	if copyRow.CommitAttemptedAt == nil && !maySubmit {
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage registration is ready", nil)
	}
	pieceCID, err := cid.Parse(*upload.PieceCID)
	if err != nil {
		return h.failCopyTask(execution, input, copyRow, err, "piece_identity_invalid")
	}
	target, err := h.openReadyDataSet(ctx, binding)
	if err != nil {
		if copyRow.CommitAttemptedAt != nil && copyRow.CommitAttemptID != nil {
			advancer := storagecommit.Advancer{Store: h.deps.Repositories.Contents, StatusChecker: h.deps.CommitStatus}
			advanced, advanceErr := advancer.AdvanceUnavailable(ctx, *copyRow, *binding)
			if advanceErr != nil {
				return h.retryCopyTask(execution, input, copyRow, advanceErr, "commit_recovery_failed")
			}
			if advanced.State == storagecommit.AdvanceNeedsAttention {
				if advanced.Continue && advanced.AttentionCode.Valid() {
					return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage registration", nil)
				}
				return taskengine.Fail(errors.New("storage registration requires attention"), commitAttentionFailureReason(advanced.AttentionCode), nil)
			}
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage registration", nil)
	}
	advancer := storagecommit.Advancer{Store: h.deps.Repositories.Contents, StatusChecker: h.deps.CommitStatus}
	advance := func(ctx context.Context) (storagecommit.AdvanceResult, error) {
		return advancer.Advance(ctx, storagecommit.AdvanceInput{
			Copy: *copyRow, Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}}, RequireEligibleCopy: true,
		})
	}
	var advanced storagecommit.AdvanceResult
	if copyRow.CommitAttemptedAt == nil {
		err = execution.WithResource(ctx, taskengine.ResourceProviderMutation, func(ctx context.Context) error {
			var advanceErr error
			advanced, advanceErr = advance(ctx)
			return advanceErr
		})
	} else {
		advanced, err = advance(ctx)
	}
	if err != nil {
		if advanced.State == storagecommit.AdvancePending || advanced.State == storagecommit.AdvanceSubmitted {
			return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage registration", nil)
		}
		return h.retryCopyTask(execution, input, copyRow, err, "commit_advance_failed")
	}
	switch advanced.State {
	case storagecommit.AdvanceWaitingCapacity:
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "capacity", "Waiting to register storage", nil)
	case storagecommit.AdvanceSubmitted, storagecommit.AdvancePending:
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage registration", nil)
	case storagecommit.AdvanceRejected:
		return h.retryResolvedCopyTask(execution, input, copyRow, pdp.ErrTxRejected, "commit_rejected")
	case storagecommit.AdvanceNeedsAttention:
		if advanced.Continue && advanced.AttentionCode.Valid() {
			return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage registration", nil)
		}
		return taskengine.Fail(errors.New("storage registration requires attention"), commitAttentionFailureReason(advanced.AttentionCode), nil)
	case storagecommit.AdvanceReleased:
		if !advanced.ReleaseReason.Valid() {
			return taskengine.Fail(fmt.Errorf("storage registration was released with unknown reason %q", advanced.ReleaseReason), "commit_release_reason_unknown", nil)
		}
		return h.retryResolvedCopyTask(execution, input, copyRow, errors.New("storage registration was released"), string(advanced.ReleaseReason))
	case storagecommit.AdvanceConfirmed:
		if advanced.Confirmation == nil || len(advanced.Confirmation.PieceIDs) != 1 {
			return taskengine.Fail(errors.New("storage confirmation has no unique piece identity"), "commit_confirmation_invalid", nil)
		}
		pieceID := idtypes.OnChainIDFromSDK(advanced.Confirmation.PieceIDs[0])
		retrievalURL := target.PieceURL(pieceCID)
		extraHex := taskDerefString(copyRow.CommitExtraDataHex)
		return taskengine.Complete("Storage copy registered", func(ctx context.Context, repos *repository.Repositories) error {
			if err := repos.Contents.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
				StorageCopyID: copyRow.ID, RequireEligibleCopy: true, ContentID: copyRow.ContentID,
				CopyIndex: copyRow.CopyIndex, PieceCID: pieceCID.String(), PieceID: &pieceID,
				RetrievalURL: retrievalURL, CommitExtraDataHex: extraHex,
				CommitTransactionID: advanced.Confirmation.TransactionID, CommitAttemptID: advanced.AttemptID,
				CommitConfirmedTransactionID: advanced.Confirmation.ConfirmedTransactionID,
			}); err != nil {
				return err
			}
			if err := repos.Contents.CompleteCopyTask(ctx, input.CopyID, input.Generation, execution.ID()); err != nil {
				return err
			}
			// The content is readable once this copy commits, independently of
			// which versions currently point at it.
			if _, err := repos.Contents.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
				ContentID: copyRow.ContentID, BucketID: copyRow.BucketID,
			}); err != nil {
				return err
			}
			_, refs, err := repos.Contents.FinalizeUploadIfTargetCopiesMet(ctx, repository.NewFinalizeUploadInput(copyRow.ContentID))
			if err != nil {
				return err
			}
			return h.enqueueAfterUploadEvictions(ctx, repos, refs)
		})
	default:
		return taskengine.Fail(fmt.Errorf("unknown storage commit result %q", advanced.State), "commit_result_invalid", nil)
	}
}

func copyDefinition(taskType model.TaskType, retryLimit *int) taskengine.Definition {
	return taskengine.Definition{
		Type: taskType, InputVersion: 1,
		Codec: taskengine.StrictJSONCodec(func(input *storagepipeline.CopyGenerationInput) error {
			return storagepipeline.ValidateCopyGenerationInput(*input)
		}),
		RetryLimit: retryLimit, AllowRetry: false,
	}
}

func (h *TaskHandlers) authorizeCopyTask(
	ctx context.Context,
	execution taskengine.Execution,
) (storagepipeline.CopyGenerationInput, *model.StorageCopy, bool, taskengine.Result) {
	input, err := taskengine.DecodeInput[storagepipeline.CopyGenerationInput](execution)
	if err != nil {
		return input, nil, true, decodeFailure(string(execution.Type()), err)
	}
	copyRow, err := h.deps.Repositories.Contents.AuthorizeCopyTask(ctx, input.CopyID, input.Generation, execution.ID())
	if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
		return input, nil, true, taskengine.Cancel("Storage work was superseded", nil)
	}
	if err != nil {
		if execution.RetryWillFail() {
			message := err.Error()
			return input, nil, true, taskengine.Fail(err, "copy_authorization_failed", func(ctx context.Context, repos *repository.Repositories) error {
				copyRow, authorizeErr := repos.Contents.AuthorizeCopyTask(ctx, input.CopyID, input.Generation, execution.ID())
				if errors.Is(authorizeErr, repository.ErrConflict) || errors.Is(authorizeErr, repository.ErrNotFound) {
					return nil
				}
				if authorizeErr != nil {
					return authorizeErr
				}
				if copyRow.CommitAttemptedAt != nil {
					return nil
				}
				return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message)
			})
		}
		return input, nil, true, retryTask(err, "copy_authorization_failed")
	}
	return input, copyRow, false, taskengine.Result{}
}

func (h *TaskHandlers) advanceCopyTask(
	input storagepipeline.CopyGenerationInput,
	taskID int64,
	nextType model.TaskType,
	message string,
) taskengine.Result {
	return taskengine.Complete(message, func(ctx context.Context, repos *repository.Repositories) error {
		return h.enqueueSuccessorCopyTask(ctx, repos, input, taskID, nextType)
	})
}

func (h *TaskHandlers) completeCopyTask(input storagepipeline.CopyGenerationInput, taskID int64, message string) taskengine.Result {
	return taskengine.Complete(message, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Contents.CompleteCopyTask(ctx, input.CopyID, input.Generation, taskID)
	})
}

func (h *TaskHandlers) enqueueInitialCopyTask(ctx context.Context, repos *repository.Repositories, copyID int64, taskType model.TaskType) error {
	if h.taskService == nil {
		return errors.New("task service is unavailable")
	}
	generation, err := repos.Contents.NextCopyWorkGeneration(ctx, copyID)
	if err != nil {
		return err
	}
	input := storagepipeline.CopyGenerationInput{CopyID: copyID, Generation: generation}
	taskRow, _, err := h.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type: taskType, IdempotencyKey: copyTaskKey(taskType, copyID, generation), Input: input,
		SubjectType: "storage_copy", SubjectKey: fmt.Sprintf("%d", copyID),
	})
	if err != nil {
		return err
	}
	return repos.Contents.BindCopyTask(ctx, copyID, generation, taskRow.ID)
}

func (h *TaskHandlers) enqueueSuccessorCopyTask(
	ctx context.Context,
	repos *repository.Repositories,
	current storagepipeline.CopyGenerationInput,
	currentTaskID int64,
	nextType model.TaskType,
) error {
	if h.taskService == nil {
		return errors.New("task service is unavailable")
	}
	next := storagepipeline.CopyGenerationInput{CopyID: current.CopyID, Generation: current.Generation + 1}
	taskRow, _, err := h.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type: nextType, IdempotencyKey: copyTaskKey(nextType, next.CopyID, next.Generation), Input: next,
		SubjectType: "storage_copy", SubjectKey: fmt.Sprintf("%d", next.CopyID),
	})
	if err != nil {
		return err
	}
	return repos.Contents.ReplaceCopyTask(ctx, current.CopyID, current.Generation, currentTaskID, next.Generation, taskRow.ID)
}

func (h *TaskHandlers) enqueueDataSetEnsure(ctx context.Context, repos *repository.Repositories, binding *model.StorageDataSet) error {
	if h.taskService == nil || binding == nil {
		return errors.New("data set task dependencies are unavailable")
	}
	taskRow, _, err := h.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageDataSetEnsure, IdempotencyKey: storagepipeline.DataSetEnsureKey(binding.ID),
		Input:       storagepipeline.DataSetInput{DataSetID: binding.ID},
		SubjectType: "storage_data_set", SubjectKey: fmt.Sprintf("%d", binding.ID),
	})
	if err != nil {
		return err
	}
	return repos.Contents.BindDataSetEnsureTask(ctx, binding.ID, taskRow.ID)
}

func copyTaskKey(taskType model.TaskType, copyID, generation int64) string {
	switch taskType {
	case model.TaskTypeStorageTransferPlan:
		return storagepipeline.TransferPlanKey(copyID, generation)
	case model.TaskTypeStorageStore:
		return storagepipeline.StoreKey(copyID, generation)
	case model.TaskTypeStoragePull:
		return storagepipeline.PullKey(copyID, generation)
	case model.TaskTypeStorageCommitCoordinate:
		return storagepipeline.CommitCoordinateKey(copyID, generation)
	case model.TaskTypeStorageCommit:
		return storagepipeline.CommitKey(copyID, generation)
	default:
		panic(fmt.Sprintf("unsupported copy task type %q", taskType))
	}
}

// copyContext resolves what a copy task acts on. The unit of work is the
// content: its bytes, its cache file and its size are what the provider
// receives, so no object version needs to exist for the transfer to be valid.
func (h *TaskHandlers) copyContext(
	ctx context.Context,
	copyRow *model.StorageCopy,
) (*model.StorageDataSet, synapse.DataSetTarget, *model.StorageContent, *model.Bucket, error) {
	if copyRow == nil {
		return nil, nil, nil, nil, errors.New("storage copy has no data set")
	}
	binding, err := h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
	if err != nil || binding == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return binding, nil, nil, nil, err
	}
	target, err := h.openReadyDataSet(ctx, binding)
	if err != nil {
		return binding, nil, nil, nil, err
	}
	content, err := h.deps.Repositories.Contents.GetByID(ctx, copyRow.ContentID)
	if err != nil || content == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return binding, target, content, nil, err
	}
	bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, content.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return binding, target, content, bucket, err
	}
	return binding, target, content, bucket, nil
}

func (h *TaskHandlers) copyContextFailure(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	err error,
	settleSafe bool,
) taskengine.Result {
	if errors.Is(err, repository.ErrNotFound) {
		if settleSafe {
			return h.failCopyTask(execution, input, copyRow, err, "copy_owner_missing")
		}
		return taskengine.Fail(err, "copy_owner_missing", nil)
	}
	if synapse.IsProviderUnavailable(err) || errors.Is(err, storage.ErrDataSetUnavailable) {
		return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "provider", "Waiting for storage provider", nil)
	}
	if settleSafe {
		return h.retryCopyTask(execution, input, copyRow, err, "copy_context_failed")
	}
	return retryTask(err, "copy_context_failed")
}

func (h *TaskHandlers) finishPieceTransfer(
	ctx context.Context,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	target synapse.DataSetTarget,
	pieceCID cid.Cid,
) taskengine.Result {
	extra, err := target.PresignForCommit(ctx, []storage.PieceInput{{PieceCID: pieceCID}})
	if err != nil {
		return h.retryCopyTask(execution, input, copyRow, err, "commit_presign_failed")
	}
	return h.finishPieceTransferWithExtra(execution, input, copyRow, target, pieceCID, hex.EncodeToString(extra), "")
}

// finishPieceTransferWithExtra settles a completed transfer. pullAttemptID is
// empty for a store, which sent no request to a source provider.
func (h *TaskHandlers) finishPieceTransferWithExtra(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	target synapse.DataSetTarget,
	pieceCID cid.Cid,
	extraHex string,
	pullAttemptID string,
) taskengine.Result {
	pieceCIDString := pieceCID.String()
	retrievalURL := target.PieceURL(pieceCID)
	canonicalExtraHex := strings.ToLower(extraHex)
	return taskengine.Complete("Storage transfer completed", func(ctx context.Context, repos *repository.Repositories) error {
		if err := repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			StorageCopyID: copyRow.ID, RequireEligibleCopy: true, ContentID: copyRow.ContentID,
			CopyIndex: copyRow.CopyIndex, PieceCID: pieceCIDString, RetrievalURL: retrievalURL,
			CommitExtraDataHex: canonicalExtraHex, PullAttemptID: pullAttemptID,
		}); err != nil {
			return err
		}
		return h.enqueueSuccessorCopyTask(ctx, repos, input, execution.ID(), model.TaskTypeStorageCommitCoordinate)
	})
}

func (h *TaskHandlers) retryCopyTask(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	err error,
	reason string,
) taskengine.Result {
	if !execution.RetryWillFail() {
		return retryTask(err, reason)
	}
	return h.failCopyTask(execution, input, copyRow, err, reason)
}

func (h *TaskHandlers) retryResolvedCopyTask(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	err error,
	reason string,
) taskengine.Result {
	if !execution.RetryWillFail() {
		return retryTask(err, reason)
	}
	resolvedCopy := *copyRow
	resolvedCopy.CommitAttemptedAt = nil
	return h.failCopyTask(execution, input, &resolvedCopy, err, reason)
}

func (h *TaskHandlers) failCopyTask(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	err error,
	reason string,
) taskengine.Result {
	if copyRow == nil || copyRow.CommitAttemptedAt != nil {
		return taskengine.Fail(err, reason, nil)
	}
	message := err.Error()
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message)
	})
}

func (h *TaskHandlers) settleCopyFailure(
	ctx context.Context,
	repos *repository.Repositories,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	message string,
) error {
	if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{
		StorageCopyID: copyRow.ID,
		ContentID:     copyRow.ContentID,
		CopyIndex:     copyRow.CopyIndex,
		LastError:     message,
	}); err != nil {
		return err
	}
	if err := repos.Contents.CompleteCopyTask(ctx, input.CopyID, input.Generation, execution.ID()); err != nil {
		return err
	}
	upload, err := repos.Contents.GetByID(ctx, copyRow.ContentID)
	if err != nil || upload == nil {
		return errors.Join(err, repository.ErrNotFound)
	}
	// The content row is shared by every version of these bytes, so recording
	// the failure once covers all of them; there are no followers to fan out to.
	return repos.Contents.RecordContentFailure(ctx, upload.ID, message)
}

func commitAttentionFailureReason(code storagecommit.AttentionCode) string {
	if code == "" {
		return "commit_attention_unknown"
	}
	return string(code)
}

func (h *TaskHandlers) openBindingTarget(ctx context.Context, bucketName string, binding *model.StorageDataSet) (synapse.StorageTarget, error) {
	if binding.DataSetID != nil && !binding.DataSetID.IsZero() {
		return h.openReadyDataSet(ctx, binding)
	}
	return h.deps.Storage.OpenProviderTarget(ctx, binding.ProviderID.SDK(), storage.NewProviderContextOptions{
		DataSetMetadata: map[string]string{"bucket": bucketName},
	})
}

func (h *TaskHandlers) openReadyDataSet(ctx context.Context, binding *model.StorageDataSet) (synapse.DataSetTarget, error) {
	if binding == nil || binding.DataSetID == nil || binding.DataSetID.IsZero() {
		return nil, errors.New("storage data set is not ready")
	}
	providerID := binding.ProviderID.SDK()
	target, err := h.deps.Storage.OpenDataSetTarget(ctx, binding.DataSetID.SDK(), storage.NewDataSetContextOptions{ProviderID: &providerID})
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, errors.New("storage client returned no data set context")
	}
	if got := idtypes.OnChainIDFromSDK(target.ProviderID()); !got.Equal(binding.ProviderID) {
		return nil, fmt.Errorf("data set resolved provider %s, want %s", got.String(), binding.ProviderID.String())
	}
	ref, ok := target.DataSetRef()
	if !ok {
		return nil, errors.New("storage client did not resolve the requested data set")
	}
	dataSetID, clientDataSetID, err := dataSetRefIDs(binding, ref)
	if err != nil {
		return nil, err
	}
	if !dataSetID.Equal(*binding.DataSetID) {
		return nil, fmt.Errorf("data set resolved %s, want %s", dataSetID.String(), binding.DataSetID.String())
	}
	if binding.ClientDataSetID == nil {
		if err := h.deps.Repositories.Contents.BackfillClientDataSetID(ctx, repository.BackfillClientDataSetIDInput{
			ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: clientDataSetID,
		}); err != nil {
			return nil, err
		}
	} else if !binding.ClientDataSetID.Equal(clientDataSetID) {
		return nil, errors.New("storage client data set identity changed")
	}
	return target, nil
}

func dataSetRefIDs(binding *model.StorageDataSet, ref storage.DataSetRef) (idtypes.OnChainID, idtypes.OnChainID, error) {
	if binding == nil {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, errors.New("storage data set binding is missing")
	}
	providerID := idtypes.OnChainIDFromSDK(ref.ProviderID())
	if !providerID.Equal(binding.ProviderID) {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, fmt.Errorf("data set resolved provider %s, want %s", providerID.String(), binding.ProviderID.String())
	}
	dataSetID := idtypes.OnChainIDFromSDK(ref.DataSetID())
	if dataSetID.IsZero() {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, errors.New("data set resolved a zero identity")
	}
	return dataSetID, idtypes.OnChainIDFromSDK(ref.ClientDataSetID()), nil
}

func dataSetResultIDs(binding *model.StorageDataSet, result *storage.CreateDataSetResult) (idtypes.OnChainID, idtypes.OnChainID, error) {
	if result == nil {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, errors.New("storage provider returned no data set result")
	}
	return dataSetRefIDs(binding, result.DataSet)
}

func onChainIDPtr(value *sdktypes.BigInt) *idtypes.OnChainID {
	if value == nil {
		return nil
	}
	converted := idtypes.OnChainIDFromSDK(*value)
	return &converted
}

func (h *TaskHandlers) enqueueAfterUploadEvictions(ctx context.Context, repos *repository.Repositories, refs []repository.ObjectVersionRef) error {
	if h.deps.EvictionPolicy != cache.EvictionPolicyAfterUpload || h.taskService == nil {
		return nil
	}
	// Cache residency is content-addressed, so several refs can name the same
	// file. One eviction per content, not per version.
	seen := make(map[int64]struct{}, len(refs))
	for _, ref := range refs {
		if ref.ContentID == nil {
			continue
		}
		contentID := *ref.ContentID
		if _, exists := seen[contentID]; exists {
			continue
		}
		seen[contentID] = struct{}{}
		generation, err := repos.CacheEvictions.NextEvictionGeneration(ctx, contentID)
		if err != nil {
			return err
		}
		taskRow, _, err := h.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
			Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(contentID, generation),
			Input:       cacheeviction.EvictInput{ContentID: contentID, Generation: generation},
			SubjectType: "storage_content", SubjectKey: strconv.FormatInt(contentID, 10),
		})
		if err != nil {
			return err
		}
		if err := repos.CacheEvictions.BindEvictionTask(ctx, contentID, generation, taskRow.ID); err != nil {
			return err
		}
	}
	return nil
}

func newAttemptID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("creating request identity: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func taskDerefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
