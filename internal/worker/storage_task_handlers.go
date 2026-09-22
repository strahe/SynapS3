package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
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
	sdkcosts "github.com/strahe/synapse-go/costs"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

const (
	storageDependencyWait = time.Minute
	storagePollInterval   = 5 * time.Second
	storeAttentionAfter   = 30 * time.Minute
	// A request whose outcome was never observed is checked on chain, and sent
	// again with the same identity, one minute after the first request,
	// doubling with each further request up to 30 minutes.
	unobservedOutcomeBaseDelay = time.Minute
	unobservedOutcomeMaxDelay  = 30 * time.Minute
	// Resolving a commit attempt wakes the head of its data set's queue; this
	// delay only covers a missed wake.
	commitCapacityBackstop = 5 * time.Minute
)

type dataSetCreationCheckpoint struct {
	// AttemptedAt is when the latest create request was sent.
	AttemptedAt     time.Time `json:"attempted_at"`
	TransactionID   string    `json:"transaction_id,omitempty"`
	StatusURL       string    `json:"status_url,omitempty"`
	ClientDataSetID string    `json:"client_data_set_id,omitempty"`
	// Identity is the payer, chain, and record keeper the request was signed
	// for; a client data set ID is only unique within it.
	Identity *storage.ContextIdentity `json:"identity,omitempty"`
	// Sends counts create requests carrying ClientDataSetID. A request is only
	// repeated after one whose outcome was never observed, so more than one
	// send means a rejection no longer proves that no data set exists.
	Sends int `json:"sends,omitempty"`
}

type storeCheckpoint struct {
	AttemptedAt        time.Time `json:"attempted_at"`
	IntendedPieceCID   string    `json:"intended_piece_cid"`
	ProviderServiceURL string    `json:"provider_service_url"`
	IngressAttempt     int       `json:"ingress_attempt,omitempty"`
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
		sort.Slice(bindingPlan, func(i, j int) bool { return bindingPlan[i].copyIndex < bindingPlan[j].copyIndex })
		ingressIndex := h.fastestIngressIndex(ctx, bindingPlan)
		ingressProviderID := bindingPlan[ingressIndex].provider

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
		// These outcomes already ended the generation: the chain refused the
		// creation, the chain resolved its ID to a record that is not ours, or
		// the row proved nothing was ever sent. It is retired and its fence
		// released, so a retry has nothing left to do. Everything else stays
		// retryable, because an operator who restores the wallet, the network,
		// or a missing dependency can finish a creation that was only interrupted.
		CanManualRetry: func(task *model.Task) bool {
			if task == nil || task.FailureReason == nil {
				return true
			}
			switch *task.FailureReason {
			case "dataset_creation_rejected", "dataset_correlation_conflict", "dataset_creation_unsent":
				return false
			default:
				return true
			}
		},
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
	binding, err := h.deps.Repositories.Contents.AuthorizeDataSetEnsureTask(ctx, input.DataSetID, execution.ID())
	if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
		return taskengine.Cancel("Storage service setup was superseded", nil)
	}
	if err != nil {
		return retryTask(err, "dataset_authorization_failed")
	}
	if h.deps.Storage == nil {
		return failDataSetEnsure(binding, execution.ID(), errors.New("storage client is unavailable"), "dependency_unavailable")
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
	// A request that already went out is resolved by the ID it used, before any
	// metadata search: every generation this bucket ever had at this provider
	// shares that metadata, so a match proves nothing about which one is ours.
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[dataSetCreationCheckpoint](execution)
	if err != nil {
		return h.recoverUnnamedCreation(ctx, execution, binding, provider, err)
	}
	if hasCheckpoint {
		// Whether or not its submission was recorded, a request is resolved only
		// under the identity it was signed for: a submission's status says
		// nothing about who pays for the data set it created.
		clientDataSetID, reason, err := checkpoint.requestIdentity(provider)
		if err != nil {
			if reason == "invalid_checkpoint" {
				return h.recoverUnnamedCreation(ctx, execution, binding, provider, err)
			}
			// The wallet or network moved. That proves only that this context
			// cannot look the ID up, wait on it, or resend it safely — the data
			// set the original identity asked for may exist and be billing — so
			// the generation, its copies, and its fence are all kept for an
			// operator who restores the configuration and retries.
			return taskengine.Fail(err, reason, nil)
		}
		if checkpoint.TransactionID != "" {
			return h.waitDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID)
		}
		// The request went out without its submission being recorded, so it may
		// or may not have created the data set. Only the chain can tell.
		if !mayCreate {
			return h.findRequestedDataSet(ctx, execution, binding, provider, checkpoint, clientDataSetID)
		}
		return h.sendDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID)
	}
	if dataSetCreationSent(binding) {
		// The row remembers a request this task no longer can: resolve it by that
		// ID rather than adopting a data set by metadata or minting a new one.
		return h.recoverUnnamedCreation(ctx, execution, binding, provider,
			errors.New("storage service creation checkpoint is missing"))
	}

	// Nothing was ever sent for this generation, so a data set the provider
	// already holds for the bucket may be adopted.
	matching, err := h.deps.Storage.FindMatchingDataSet(ctx, binding.ProviderID.SDK(), map[string]string{"bucket": bucket.Name}, provider.CDNEnabled())
	if err == nil && matching != nil {
		dataSetID, clientDataSetID, identityErr := dataSetRefIDs(binding, *matching)
		if identityErr != nil {
			return failDataSetEnsure(binding, execution.ID(), identityErr, "dataset_identity_mismatch")
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, clientDataSetID)
	}
	if err != nil {
		if synapse.IsProviderUnavailable(err) {
			return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "provider", "Waiting for storage provider", nil)
		}
		return retryTask(err, "dataset_discovery_failed")
	}
	if !mayCreate {
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage service is ready to create", nil)
	}
	identity := provider.ContextIdentity()
	if !contextIdentityComplete(identity) {
		return failDataSetEnsure(binding, execution.ID(), errors.New("storage signing identity is incomplete"), "dependency_unavailable")
	}
	clientDataSetID, err := newClientDataSetID()
	if err != nil {
		return retryTask(err, "dataset_creation_not_started")
	}
	checkpoint = dataSetCreationCheckpoint{ClientDataSetID: clientDataSetID.String(), Identity: &identity}
	return h.sendDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID)
}

// sendDataSetCreation records the generation's client data set ID, then sends a
// create request carrying it. Sending the same ID again is safe: the contract
// registers at most one data set per client data set ID and payer, so a request
// that already landed cannot create a second one.
func (h *TaskHandlers) sendDataSetCreation(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	checkpoint dataSetCreationCheckpoint,
	clientDataSetID sdktypes.BigInt,
) taskengine.Result {
	checkpoint.AttemptedAt = time.Now().UTC()
	checkpoint.Sends++
	recordedID := idtypes.OnChainIDFromSDK(clientDataSetID)
	var (
		created     *storage.CreateDataSetResult
		evidenceErr error
		submission  storage.CreateDataSetSubmission
	)
	createCtx, cancelCreate := context.WithCancel(ctx)
	attempted, createErr := execution.WithCheckpointedEffect(ctx, taskengine.ResourceProviderMutation, checkpoint,
		func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.RecordDataSetClientID(ctx, binding.ID, recordedID)
		},
		func(context.Context) error {
			var err error
			created, err = provider.CreateDataSet(createCtx, &storage.CreateDataSetOptions{
				ClientDataSetID: &clientDataSetID,
				OnSubmitted: func(sub storage.CreateDataSetSubmission) {
					submission = sub
					checkpoint.TransactionID = sub.TransactionID
					checkpoint.StatusURL = sub.StatusURL
					evidenceErr = execution.WriteCheckpointWith(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
						return repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
							ID: binding.ID, ContentID: derefInt64(binding.CreatedByContentID), TransactionID: sub.TransactionID,
							StatusURL: sub.StatusURL, ClientDataSetID: &recordedID,
						})
					})
					cancelCreate()
				},
			})
			return err
		})
	cancelCreate()
	if createErr != nil && !attempted {
		if errors.Is(createErr, taskengine.ErrResourceBusy) {
			return taskengine.ResourceWait("Waiting for other storage operations to finish")
		}
		return retryTask(createErr, "dataset_creation_not_started")
	}
	if evidenceErr != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Recording storage service creation", func(ctx context.Context, repos *repository.Repositories) error {
			if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, binding.ID, execution.ID()); err != nil {
				return err
			}
			return repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
				ID: binding.ID, ContentID: derefInt64(binding.CreatedByContentID), TransactionID: submission.TransactionID,
				StatusURL: submission.StatusURL, ClientDataSetID: &recordedID,
			})
		})
	}
	if created != nil {
		dataSetID, createdClientID, identityErr := dataSetResultIDs(binding, created)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, createdClientID)
	}
	if submission.TransactionID != "" {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage service creation", nil)
	}
	// The request may have reached the provider without its submission reaching
	// us, so the chain is checked before anything is sent again.
	return taskengine.Suspend(model.TaskResumeModeRecover, unobservedOutcomeDelay(checkpoint.Sends), "provider_confirmation", "Checking storage service creation", nil)
}

// findRequestedDataSet reads the chain for the data set a request with an
// unobserved outcome may have created. It never sends: once the latest request
// has had time to land and nothing is visible, execute sends the same ID again.
func (h *TaskHandlers) findRequestedDataSet(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	checkpoint dataSetCreationCheckpoint,
	clientDataSetID sdktypes.BigInt,
) taskengine.Result {
	ref, found, err := provider.FindDataSetByClientDataSetID(ctx, clientDataSetID)
	if errors.Is(err, storage.ErrDataSetCorrelationConflict) {
		// The ID is consumed on chain but does not resolve to this request's
		// data set, so it is never sent again.
		return taskengine.Fail(err, "dataset_correlation_conflict", dataSetFailureSettlement(binding.ID, execution.ID(), err.Error(), true))
	}
	if err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "provider_confirmation", "Checking storage service creation", nil)
	}
	if found {
		dataSetID, foundClientID, identityErr := dataSetRefIDs(binding, ref)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, foundClientID)
	}
	if wait := time.Until(checkpoint.AttemptedAt.Add(unobservedOutcomeDelay(checkpoint.Sends))); wait > 0 {
		return taskengine.Suspend(model.TaskResumeModeRecover, wait, "provider_confirmation", "Checking storage service creation", nil)
	}
	return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Retrying storage service creation", nil)
}

// requestIdentity returns the client data set ID a checkpointed request used,
// once the rebuilt context is confirmed to sign for the same payer, chain, and
// record keeper. Under another identity the ID would be looked up, or sent
// again, where the contract cannot tell that it was already used.
func (c dataSetCreationCheckpoint) requestIdentity(provider synapse.ProviderTarget) (sdktypes.BigInt, string, error) {
	clientID, err := idtypes.ParseOnChainID("clientDataSetID", c.ClientDataSetID)
	if err != nil || clientID.IsZero() || c.Identity == nil {
		return sdktypes.BigInt{}, "invalid_checkpoint", errors.New("storage service creation checkpoint has no request identity")
	}
	if provider.ContextIdentity() != *c.Identity {
		return sdktypes.BigInt{}, "dataset_identity_changed", errors.New("the wallet or network changed after storage service creation began")
	}
	return clientID.SDK(), "", nil
}

// newClientDataSetID draws a random nonzero uint256. It is never derived from
// local row IDs, which start over in a fresh database while IDs consumed on
// chain stay consumed.
func newClientDataSetID() (sdktypes.BigInt, error) {
	var raw [32]byte
	for {
		if _, err := rand.Read(raw[:]); err != nil {
			return sdktypes.BigInt{}, fmt.Errorf("creating client data set ID: %w", err)
		}
		if value := new(big.Int).SetBytes(raw[:]); value.Sign() != 0 {
			return sdktypes.BigIntFromBig(value)
		}
	}
}

func contextIdentityComplete(identity storage.ContextIdentity) bool {
	return identity.Payer != (common.Address{}) && identity.ChainID.IsValid() && identity.RecordKeeper != (common.Address{})
}

// unobservedOutcomeDelay spaces the chain checks and resends that follow
// requests whose outcome was never observed.
func unobservedOutcomeDelay(sends int) time.Duration {
	delay := unobservedOutcomeBaseDelay
	for i := 1; i < sends && delay < unobservedOutcomeMaxDelay; i++ {
		delay *= 2
	}
	return min(delay, unobservedOutcomeMaxDelay)
}

func (h *TaskHandlers) waitDataSetCreation(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	checkpoint dataSetCreationCheckpoint,
	clientDataSetID sdktypes.BigInt,
) taskengine.Result {
	result, err := provider.WaitForDataSetCreated(ctx, checkpoint.StatusURL, clientDataSetID)
	if err != nil {
		if errors.Is(err, synapse.ErrProviderTransactionRejected) {
			if checkpoint.Sends > 1 {
				// An earlier request with this ID had no observed outcome, so the
				// rejection may only mean that request created the data set first.
				// The dead submission is dropped and the ID is looked up instead.
				checkpoint.TransactionID, checkpoint.StatusURL = "", ""
				if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
					return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage service", nil)
				}
				return taskengine.Suspend(model.TaskResumeModeRecover, 0, "provider_confirmation", "Checking storage service creation", nil)
			}
			return taskengine.Fail(err, "dataset_creation_rejected", dataSetFailureSettlement(binding.ID, execution.ID(), err.Error(), true))
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage service", nil)
	}
	dataSetID, createdClientID, err := dataSetResultIDs(binding, result)
	if err != nil {
		return taskengine.Fail(err, "dataset_identity_mismatch", nil)
	}
	return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, createdClientID)
}

// dataSetCreationSent reports whether a create request may already have gone out
// for this generation. The client data set ID and the submission are both
// written before the provider call, so a row carrying neither proves nothing was
// sent — and a generation that never asked for a data set can be given up
// without risking one that exists and is billing.
func dataSetCreationSent(binding *model.StorageDataSet) bool {
	if binding == nil {
		return true
	}
	if binding.ClientDataSetID != nil && !binding.ClientDataSetID.IsZero() {
		return true
	}
	return binding.CreateTransactionID != nil && *binding.CreateTransactionID != ""
}

// failDataSetEnsure fails a creation that cannot go on. When the row proves no
// request was ever sent, nothing can exist: the generation is given up under
// dataset_creation_unsent, which offers no retry because a retry would have
// nothing to do. Otherwise the task fails with reason and keeps the generation,
// its copies, and its fence, because a data set may exist and may be billing.
func failDataSetEnsure(binding *model.StorageDataSet, taskID int64, err error, reason string) taskengine.Result {
	if dataSetCreationSent(binding) {
		return taskengine.Fail(err, reason, nil)
	}
	return taskengine.Fail(err, "dataset_creation_unsent", dataSetFailureSettlement(binding.ID, taskID, err.Error(), true))
}

// recoverUnnamedCreation resolves a creation whose checkpoint can no longer name
// its request. The client data set ID reaches the row before the provider call,
// so the row can still name it, and the lookup that follows only reads. Without
// that ID nothing was ever sent and the generation is given up instead.
func (h *TaskHandlers) recoverUnnamedCreation(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	cause error,
) taskengine.Result {
	if binding.ClientDataSetID == nil || binding.ClientDataSetID.IsZero() {
		return failDataSetEnsure(binding, execution.ID(), cause, "invalid_checkpoint")
	}
	ref, found, err := provider.FindDataSetByClientDataSetID(ctx, binding.ClientDataSetID.SDK())
	if errors.Is(err, storage.ErrDataSetCorrelationConflict) {
		return taskengine.Fail(err, "dataset_correlation_conflict",
			dataSetFailureSettlement(binding.ID, execution.ID(), err.Error(), true))
	}
	if err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "provider_confirmation", "Checking storage service creation", nil)
	}
	if found {
		dataSetID, foundClientID, identityErr := dataSetRefIDs(binding, ref)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, foundClientID)
	}
	// The chain holds nothing under that ID, and nothing here can prove which
	// identity signed the request, so it is never sent again from this task.
	return taskengine.Fail(cause, "invalid_checkpoint", nil)
}

// dataSetFailureSettlement records that a generation could not be created.
// terminal says this generation will never hold a data set of ours: the chain
// refused the creation, the ID its request used does not resolve to one, or the
// row proves no request was ever sent. It then fails the copies, retires the
// generation, and releases its creation fence. Anything short of that proof
// leaves the row in place so an operator still has the provider, the ID, and any
// transaction to work from.
func dataSetFailureSettlement(dataSetID, taskID int64, message string, terminal bool) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, dataSetID, taskID); err != nil {
			return err
		}
		if err := repos.Contents.MarkDataSetFailed(ctx, dataSetID, message); err != nil {
			return err
		}
		if !terminal {
			// The outcome is unknown, so the generation may still exist on the
			// provider. Retrying this task looks the recorded ID up again and
			// continues the copies bound to it, and continuation skips copies that
			// already failed — so they are left alone rather than terminated on a
			// guess.
			return nil
		}
		// Nothing will ever serve these copies: the data set they name will not
		// exist, and continuation would have nothing to rediscover. Failing them
		// keeps their objects from sitting at "uploading" forever.
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
		// Nothing will be created for the retired generation, so its creation
		// fence is released: a new replacement of the slot is refused while an
		// earlier target still holds one.
		return repos.Contents.CompleteDataSetEnsureTask(ctx, dataSetID, taskID)
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
	if err := h.promoteBucketReady(ctx, repos, bucket.ID, required); err != nil {
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

func (h *TaskHandlers) wakePeerPullPlans(ctx context.Context, repos *repository.Repositories, contentID int64) error {
	if h.taskService == nil {
		return errors.New("task service is unavailable")
	}
	copies, err := repos.Contents.ListCopies(ctx, contentID)
	if err != nil {
		return err
	}
	wakeIDs := make([]int64, 0, len(copies))
	for i := range copies {
		copyRow := &copies[i]
		if copyRow.Status != model.StorageCopyStatusPending ||
			copyRow.TransferMethod != model.StorageCopyTransferMethodPeerPull ||
			copyRow.ActiveTaskID == nil {
			continue
		}
		taskRow, err := repos.Tasks.GetByID(ctx, *copyRow.ActiveTaskID)
		if err != nil {
			return err
		}
		if taskRow != nil && taskRow.Type == model.TaskTypeStorageTransferPlan &&
			taskRow.Status == model.TaskStatusPending && taskRow.WaitReason != nil && *taskRow.WaitReason == "source" {
			wakeIDs = append(wakeIDs, taskRow.ID)
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
			return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageCommit, "Storage copy is ready to register")
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
			migration, err := h.deps.Repositories.Contents.IsPendingReplacementCopy(ctx, copyRow.ID)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "replacement_load_failed")
			}
			if !migration {
				return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "source", "Waiting for a readable storage source", nil)
			}
			available, err := h.copyCacheAvailable(ctx, copyRow)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "copy_cache_load_failed")
			}
			if !available {
				return taskengine.Fail(errors.New("stored content migration has no readable source or local cache"), "migration_cache_missing", nil)
			}
			return h.advanceToCacheRestore(input, execution.ID(), "Storage copy is recovering from cache", "")
		}
		available, err := h.copyCacheAvailable(ctx, copyRow)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "copy_cache_load_failed")
		}
		if available {
			return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageStore, "Storage copy is ready to transfer")
		}
		if copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
			return taskengine.Fail(errors.New("stored content migration cannot read its local cache"), "migration_cache_missing", nil)
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for a readable storage source", nil)
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func (h *TaskHandlers) copyCacheAvailable(ctx context.Context, copyRow *model.StorageCopy) (bool, error) {
	entry, err := h.deps.Repositories.CacheEvictions.GetCacheEntry(ctx, copyRow.ContentID)
	if err != nil || entry == nil || !entry.InCache || h.deps.Cache == nil {
		return false, err
	}
	bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, copyRow.BucketID)
	if err != nil || bucket == nil {
		return false, err
	}
	return h.deps.Cache.Exists(ctx, bucket.Name, model.ContentCacheKey(copyRow.ContentID)), nil
}

func (h *TaskHandlers) advanceToCacheRestore(input storagepipeline.CopyGenerationInput, taskID int64, message, pullAttemptID string) taskengine.Result {
	return taskengine.Complete(message, func(ctx context.Context, repos *repository.Repositories) error {
		if err := repos.Contents.SetCopyCacheRestore(ctx, input.CopyID, input.Generation, taskID, pullAttemptID); err != nil {
			return err
		}
		return h.enqueueSuccessorCopyTask(ctx, repos, input, taskID, model.TaskTypeStorageStore)
	})
}

func (h *TaskHandlers) pullWithoutSource(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput, copyRow *model.StorageCopy) taskengine.Result {
	migration, err := h.deps.Repositories.Contents.IsPendingReplacementCopy(ctx, copyRow.ID)
	if err != nil {
		return h.retryCopyTask(execution, input, copyRow, err, "replacement_load_failed")
	}
	if !migration {
		return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "source", "Waiting for a readable storage source", nil)
	}
	available, err := h.copyCacheAvailable(ctx, copyRow)
	if err != nil {
		return h.retryCopyTask(execution, input, copyRow, err, "copy_cache_load_failed")
	}
	if !available {
		return taskengine.Fail(errors.New("stored content migration has no readable source or local cache"), "migration_cache_missing", nil)
	}
	return h.advanceToCacheRestore(input, execution.ID(), "Storage copy is recovering from cache", "")
}

func (h *TaskHandlers) recoverMigrationFromCache(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput, copyRow *model.StorageCopy, pullAttemptID string) (taskengine.Result, bool) {
	migration, err := h.deps.Repositories.Contents.IsPendingReplacementCopy(ctx, copyRow.ID)
	if err != nil {
		return h.retryCopyTask(execution, input, copyRow, err, "replacement_load_failed"), true
	}
	if !migration {
		return taskengine.Result{}, false
	}
	available, err := h.copyCacheAvailable(ctx, copyRow)
	if err != nil {
		return h.retryCopyTask(execution, input, copyRow, err, "copy_cache_load_failed"), true
	}
	if !available {
		return taskengine.Fail(errors.New("stored content migration failed and its local cache is unavailable"), "migration_cache_missing", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.AbandonMigrationPull(ctx, copyRow.ID, input.Generation, execution.ID(), pullAttemptID)
		}), true
	}
	return h.advanceToCacheRestore(input, execution.ID(), "Storage copy is recovering from cache", pullAttemptID), true
}

func (h *TaskHandlers) storeHandler() taskengine.Handler {
	definition := copyDefinition(model.TaskTypeStorageStore, h.retryLimit())
	definition.AllowRetry = true
	definition.CanManualRetry = func(task *model.Task) bool {
		if task == nil || task.FailureReason == nil {
			return false
		}
		switch *task.FailureReason {
		case "store_not_started":
			return len(task.Checkpoint) == 0
		case "store_outcome_unknown", "copy_owner_missing", "copy_context_failed":
			return len(task.Checkpoint) > 0
		default:
			return false
		}
	}
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
		return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageCommit, "Storage copy is ready to register")
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[storeCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	_, target, content, bucket, err := h.copyContext(ctx, copyRow)
	if err != nil {
		return h.copyContextFailure(execution, input, copyRow, err, !hasCheckpoint)
	}
	if hasCheckpoint {
		return h.recoverStore(ctx, execution, input, copyRow, target, checkpoint)
	}
	if !mayStore {
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage transfer is ready", nil)
	}
	if h.deps.Cache == nil || h.deps.CacheGate == nil {
		return h.failCopyTask(execution, input, copyRow, errors.New("cache reader is unavailable"), "dependency_unavailable")
	}
	cacheKey := model.ContentCacheKey(content.ID)
	releaseCache := h.deps.CacheGate.HoldRead(cacheKey)
	defer releaseCache()
	// One provider slot spans identity calculation and the transfer, so a task
	// that has to wait for a slot never hashes the bytes first.
	var outcome taskengine.Result
	err = execution.WithResource(ctx, taskengine.ResourceProviderMutation, func(ctx context.Context) error {
		outcome = h.storeWithProviderSlot(ctx, execution, input, copyRow, target, content, bucket, cacheKey)
		return nil
	})
	if errors.Is(err, taskengine.ErrResourceBusy) {
		return taskengine.ResourceWait("Waiting for other storage operations to finish")
	}
	if err != nil {
		return h.retryStoreNotStarted(execution, err)
	}
	return outcome
}

// storeWithProviderSlot runs while the caller holds a provider mutation slot and
// the cache read gate for cacheKey.
func (h *TaskHandlers) storeWithProviderSlot(
	ctx context.Context,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	target synapse.DataSetTarget,
	content *model.StorageContent,
	bucket *model.Bucket,
	cacheKey string,
) taskengine.Result {
	calculateReader, _, err := h.deps.Cache.Get(ctx, bucket.Name, cacheKey)
	if err != nil {
		if os.IsNotExist(err) {
			if copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
				return taskengine.Fail(errors.New("stored content migration cannot read its local cache"), "migration_cache_missing", nil)
			}
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for retained cache data", nil)
		}
		return h.retryCopyTask(execution, input, copyRow, err, "cache_open_failed")
	}
	pieceInfo, calculateErr := piece.Calculate(calculateReader)
	closeErr := calculateReader.Close()
	if calculateErr != nil {
		return h.retryCopyTask(execution, input, copyRow, calculateErr, "store_identity_failed")
	}
	if closeErr != nil {
		return h.retryCopyTask(execution, input, copyRow, closeErr, "cache_close_failed")
	}
	if !pieceInfo.CIDv2.Defined() || content.ContentSize < 0 || pieceInfo.RawSize != uint64(content.ContentSize) {
		err := fmt.Errorf("calculated storage identity has size %d, expected %d", pieceInfo.RawSize, content.ContentSize)
		return h.failCopyTask(execution, input, copyRow, err, "store_identity_mismatch")
	}
	storeReader, _, err := h.deps.Cache.Get(ctx, bucket.Name, cacheKey)
	if err != nil {
		if os.IsNotExist(err) {
			if copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
				return taskengine.Fail(errors.New("stored content migration cannot read its local cache"), "migration_cache_missing", nil)
			}
			return taskengine.Suspend(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for retained cache data", nil)
		}
		return h.retryCopyTask(execution, input, copyRow, err, "cache_open_failed")
	}
	defer func() { _ = storeReader.Close() }()
	checkpoint := storeCheckpoint{
		AttemptedAt: time.Now().UTC(), IntendedPieceCID: pieceInfo.CIDv2.String(),
		ProviderServiceURL: target.ServiceURL(),
	}
	var checkpointSettlement taskengine.Settlement
	if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress || copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
		checkpoint.IngressAttempt = copyRow.IngressStoreAttempt + 1
		checkpointSettlement = func(ctx context.Context, repos *repository.Repositories) error {
			_, err := repos.Contents.BeginIngressStoreProgress(ctx, repository.BeginIngressStoreProgressInput{
				CopyID: copyRow.ID, Generation: input.Generation, TaskID: execution.ID(), Attempt: checkpoint.IngressAttempt,
			})
			return err
		}
	}
	var stored *storage.StoreResult
	var progress *uploadProgressReporter
	attempted, err := execution.WithCheckpointedEffect(ctx, taskengine.ResourceProviderMutation, checkpoint, checkpointSettlement, func(ctx context.Context) error {
		if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress || copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
			progress = h.newIngressProgressReporter(ctx, execution.ID(), input.Generation, copyRow.ID, checkpoint.IngressAttempt, content, bucket)
		}
		options := &storage.StoreOptions{PieceCID: pieceInfo.CIDv2}
		if progress != nil {
			options.OnProgress = progress.OnProgress
		}
		var storeErr error
		stored, storeErr = target.Store(ctx, storeReader, options)
		return storeErr
	})
	defer progress.Close()
	if err != nil {
		if !attempted {
			return h.retryStoreNotStarted(execution, err)
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
	}
	if stored == nil || !stored.PieceCID.Equals(pieceInfo.CIDv2) || stored.Size != content.ContentSize {
		err := errors.New("storage provider returned a mismatched piece identity or size")
		return h.failCopyTask(execution, input, copyRow, err, "store_result_invalid")
	}
	progress.Flush(content.ContentSize, true)
	return h.finishPieceTransfer(ctx, execution, input, copyRow, target, stored.PieceCID)
}

func (h *TaskHandlers) recoverStore(
	ctx context.Context,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	target synapse.DataSetTarget,
	checkpoint storeCheckpoint,
) taskengine.Result {
	if checkpoint.AttemptedAt.IsZero() || checkpoint.IntendedPieceCID == "" || checkpoint.ProviderServiceURL == "" {
		return taskengine.Fail(errors.New("storage transfer checkpoint is incomplete"), "invalid_checkpoint", nil)
	}
	pieceCID, err := cid.Parse(checkpoint.IntendedPieceCID)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	pieceInfo, err := piece.ParseV2(pieceCID)
	if err != nil || copyRow.ContentSize < 0 || pieceInfo.RawSize != uint64(copyRow.ContentSize) {
		if err == nil {
			err = fmt.Errorf("checkpointed storage identity has size %d, expected %d", pieceInfo.RawSize, copyRow.ContentSize)
		}
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if h.deps.ParkedPieces == nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "provider", "Waiting for storage provider", nil)
	}
	state, findErr := h.deps.ParkedPieces.FindParkedPiece(ctx, checkpoint.ProviderServiceURL, pieceCID)
	if findErr == nil && state == synapse.ParkedPieceReady {
		return h.finishPieceTransfer(ctx, execution, input, copyRow, target, pieceCID)
	}
	if time.Since(checkpoint.AttemptedAt) >= storeAttentionAfter {
		if findErr == nil {
			findErr = fmt.Errorf("storage transfer remains %s", state)
		}
		return taskengine.Fail(findErr, "store_outcome_unknown", nil)
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
}

func (h *TaskHandlers) retryStoreNotStarted(execution taskengine.Execution, err error) taskengine.Result {
	if execution.RetryWillFail() {
		return taskengine.Fail(err, "store_not_started", nil)
	}
	return retryTask(err, "store_not_started")
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
		return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageCommit, "Storage copy is ready to register")
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[pullCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress && !hasCheckpoint {
		return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageStore, "Storage copy is ready for ingress")
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
			return h.pullWithoutSource(ctx, execution, input, copyRow)
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
				SourceProviderID: &source.ProviderID, SourceDataSetID: &source.DataSetID, SourcePieceID: &source.PieceID,
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
		return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
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
	if errors.Is(err, taskengine.ErrResourceBusy) {
		return taskengine.ResourceWait("Waiting for other storage operations to finish")
	}
	if err != nil {
		switch synapse.ClassifyPullError(err) {
		case synapse.PullErrorRetryable:
			return taskengine.Suspend(model.TaskResumeModeExecute, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
		case synapse.PullErrorTerminal:
			if result, recovered := h.recoverMigrationFromCache(ctx, execution, input, copyRow, checkpoint.AttemptID); recovered {
				return result
			}
			return h.failPullTask(execution, input, copyRow, checkpoint.AttemptID, err, "pull_failed")
		default:
			return h.retryPullTask(execution, input, copyRow, checkpoint.AttemptID, err, "pull_request_failed")
		}
	}
	return h.finishPieceTransferWithExtra(execution, input, copyRow, target, pieceCID, checkpoint.CommitExtraDataHex, checkpoint.AttemptID)
}

// commitCoordinateHandler finishes relay tasks that an earlier build enqueued
// between transfer and commit; nothing enqueues this type any more.
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
	if copyRow.Status != model.StorageCopyStatusPieceReady && copyRow.Status != model.StorageCopyStatusCommitting {
		return h.failCopyTask(execution, input, copyRow, errors.New("storage copy has no transferable piece"), "piece_not_ready")
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
			advancer := storagecommit.Advancer{Store: h.deps.Repositories.Contents}
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
	advancer := storagecommit.Advancer{Store: h.deps.Repositories.Contents}
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
	if errors.Is(err, taskengine.ErrResourceBusy) {
		return taskengine.ResourceWait("Waiting for other storage operations to finish")
	}
	if err != nil {
		if advanced.State == storagecommit.AdvancePending || advanced.State == storagecommit.AdvanceSubmitted {
			return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage registration", nil)
		}
		return h.retryCopyTask(execution, input, copyRow, err, "commit_advance_failed")
	}
	switch advanced.State {
	case storagecommit.AdvanceWaitingCapacity:
		return taskengine.Suspend(model.TaskResumeModeRecover, commitCapacityBackstop, "capacity", "Waiting to register storage", nil)
	case storagecommit.AdvanceSubmitted, storagecommit.AdvancePending:
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage registration", nil)
	case storagecommit.AdvanceRejected:
		return h.retryResolvedCopyTask(execution, input, copyRow, synapse.ErrProviderTransactionRejected, "commit_rejected")
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
			if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress {
				failed, err := repos.Contents.ReopenFailedIngressForPull(ctx, copyRow.ContentID)
				if err != nil {
					return err
				}
				for _, failedCopy := range failed {
					if err := h.enqueueInitialCopyTask(ctx, repos, failedCopy.ID, model.TaskTypeStorageTransferPlan); err != nil {
						return err
					}
				}
			}
			// The content is readable once this copy commits, independently of
			// which versions currently point at it.
			if _, err := repos.Contents.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
				ContentID: copyRow.ContentID, BucketID: copyRow.BucketID,
			}); err != nil {
				return err
			}
			if err := h.wakePeerPullPlans(ctx, repos, copyRow.ContentID); err != nil {
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
	copyRow, err := h.deps.Repositories.Contents.AuthorizeCopyTask(ctx, input.CopyID, input.Generation, execution.ID(), execution.ClaimGeneration())
	if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
		return input, nil, true, taskengine.Cancel("Storage work was superseded", nil)
	}
	if err != nil {
		if execution.RetryWillFail() {
			message := err.Error()
			return input, nil, true, taskengine.Fail(err, "copy_authorization_failed", func(ctx context.Context, repos *repository.Repositories) error {
				copyRow, authorizeErr := repos.Contents.AuthorizeCopyTask(ctx, input.CopyID, input.Generation, execution.ID(), execution.ClaimGeneration())
				if errors.Is(authorizeErr, repository.ErrConflict) || errors.Is(authorizeErr, repository.ErrNotFound) {
					return nil
				}
				if authorizeErr != nil {
					return authorizeErr
				}
				if copyRow.CommitAttemptedAt != nil {
					return nil
				}
				return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message, "")
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
		return h.enqueueSuccessorCopyTask(ctx, repos, input, execution.ID(), model.TaskTypeStorageCommit)
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

func (h *TaskHandlers) retryPullTask(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	pullAttemptID string,
	err error,
	reason string,
) taskengine.Result {
	if !execution.RetryWillFail() {
		return retryTask(err, reason)
	}
	return h.failPullTask(execution, input, copyRow, pullAttemptID, err, reason)
}

func (h *TaskHandlers) failPullTask(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	pullAttemptID string,
	err error,
	reason string,
) taskengine.Result {
	if copyRow == nil || copyRow.CommitAttemptedAt != nil {
		return taskengine.Fail(err, reason, nil)
	}
	message := err.Error()
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message, pullAttemptID)
	})
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
		return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message, "")
	})
}

func (h *TaskHandlers) settleCopyFailure(
	ctx context.Context,
	repos *repository.Repositories,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	message string,
	pullAttemptID string,
) error {
	if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{
		StorageCopyID: copyRow.ID,
		ContentID:     copyRow.ContentID,
		CopyIndex:     copyRow.CopyIndex,
		LastError:     message,
		PullAttemptID: pullAttemptID,
	}); err != nil {
		return err
	}
	// The content itself is flagged by MarkUploadCopyFailed, and only once no
	// copy is left that holds or may still hold it.
	if err := repos.Contents.CompleteCopyTask(ctx, input.CopyID, input.Generation, execution.ID()); err != nil {
		return err
	}
	if copyRow.TransferMethod != model.StorageCopyTransferMethodIngress {
		return nil
	}
	readable, err := repos.Contents.HasReadableCommittedCopy(ctx, copyRow.ContentID)
	if err != nil {
		return err
	}
	if readable {
		failed, err := repos.Contents.ReopenFailedIngressForPull(ctx, copyRow.ContentID)
		if err != nil {
			return err
		}
		for _, failedCopy := range failed {
			if err := h.enqueueInitialCopyTask(ctx, repos, failedCopy.ID, model.TaskTypeStorageTransferPlan); err != nil {
				return err
			}
		}
		return nil
	}
	promoted, err := repos.Contents.PromotePendingIngress(ctx, copyRow.ContentID)
	if err != nil || promoted == nil {
		return err
	}
	if promoted.ActiveTaskID != nil && h.taskService != nil {
		_, err = h.taskService.WakeInTransaction(ctx, repos, []int64{*promoted.ActiveTaskID})
		return err
	}
	return h.enqueueInitialCopyTask(ctx, repos, promoted.ID, model.TaskTypeStorageTransferPlan)
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
	clientDataSetID := idtypes.OnChainIDFromSDK(ref.ClientDataSetID())
	// Once a request goes out, this generation owns the ID it used. A data set
	// resolved under any other ID belongs to a different generation, however
	// well its metadata matches.
	if binding.ClientDataSetID != nil && !binding.ClientDataSetID.IsZero() && !clientDataSetID.Equal(*binding.ClientDataSetID) {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, fmt.Errorf(
			"data set resolved client ID %s, want %s", clientDataSetID.String(), binding.ClientDataSetID.String())
	}
	return dataSetID, clientDataSetID, nil
}

func dataSetResultIDs(binding *model.StorageDataSet, result *storage.CreateDataSetResult) (idtypes.OnChainID, idtypes.OnChainID, error) {
	if result == nil {
		return idtypes.OnChainID{}, idtypes.OnChainID{}, errors.New("storage provider returned no data set result")
	}
	return dataSetRefIDs(binding, result.DataSet)
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
		reservation, err := repos.CacheEvictions.PrepareEviction(ctx, contentID)
		if err != nil {
			return err
		}
		if reservation.ActiveTaskID != nil {
			continue
		}
		generation := reservation.Generation
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
