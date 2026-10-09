package dataset

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	bindings "github.com/strahe/synaps3/internal/task/binding"
	idtypes "github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type EnsureDependencies struct {
	Repositories *repository.Repositories
	Storage      synapse.StorageClient
	Messenger    *taskengine.Messenger

	Logger *slog.Logger
}

const (
	storageDependencyWait      = time.Minute
	storagePollInterval        = 5 * time.Second
	unobservedOutcomeBaseDelay = time.Minute
	unobservedOutcomeMaxDelay  = 30 * time.Minute
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
	// Sends counts requests carrying ClientDataSetID. A later HTTP refusal and
	// absence check permit replacement, but cannot rule out an earlier request
	// succeeding late. A rejected transaction only resolves its own submission.
	Sends             int                       `json:"sends,omitempty"`
	ProviderRejection *dataSetProviderRejection `json:"provider_rejection,omitempty"`
}

type dataSetProviderRejection struct {
	StatusCode int       `json:"status_code"`
	RejectedAt time.Time `json:"rejected_at"`
	Message    string    `json:"message,omitempty"`
}

type EnsureHandler struct {
	*taskengine.FuncHandler
	deps EnsureDependencies
}

func NewEnsureHandler(deps EnsureDependencies) (*EnsureHandler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("task repositories are required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Messenger == nil {
		return nil, errors.New("data set messenger is required")
	}
	h := &EnsureHandler{deps: deps}
	h.FuncHandler = h.dataSetEnsureHandler()
	return h, nil
}

func (h *EnsureHandler) dataSetEnsureHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageDataSetEnsure, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		Codec: taskengine.StrictJSONCodec(func(input *storagepipeline.DataSetInput) error {
			return storagepipeline.ValidateDataSetInput(*input)
		}),
		Subject: taskengine.SubjectFromInput("storage_data_set", func(input storagepipeline.DataSetInput) int64 { return input.DataSetID }),
		Policy:  taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
		// A refusal checkpoint resumes queries; only settled terminal failures
		// have no remaining creation work to retry.
		CanManualRetry: func(task *model.Task) bool {
			if task == nil || task.FailureReason == nil {
				return true
			}
			switch *task.FailureReason {
			case "dataset_creation_rejected", "dataset_correlation_conflict", "dataset_creation_unsent", "dataset_provider_rejected":
				return false
			default:
				return true
			}
		},
	}
	definition.InspectRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
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
	definition.LegacyHandoff = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
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
	return taskengine.NewFuncHandler(definition,
		func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runDataSetEnsure(ctx, execution, true)
		},
		func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runDataSetEnsure(ctx, execution, false)
		})
}

func (h *EnsureHandler) runDataSetEnsure(ctx context.Context, execution taskengine.Execution, mayCreate bool) taskengine.Result {
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
		return taskengine.Wait(model.TaskResumeModeRecover, storageDependencyWait, "provider", "Waiting for storage provider", nil)
	}
	// A request that already went out is resolved by the ID it used, before any
	// metadata search: every generation this bucket ever had at this provider
	// shares that metadata, so a match proves nothing about which one is ours.
	if len(binding.CreationRejection) != 0 {
		evidence, err := binding.CreationRejectionEvidence()
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		identity := storage.ContextIdentity{Payer: evidence.Payer, ChainID: sdktypes.ChainID(evidence.ChainID), RecordKeeper: evidence.RecordKeeper}
		checkpoint := dataSetCreationCheckpoint{
			ClientDataSetID: evidence.ClientDataSetID.String(), Identity: &identity,
			ProviderRejection: &dataSetProviderRejection{StatusCode: evidence.StatusCode, RejectedAt: evidence.RejectedAt},
		}
		clientID, reason, err := checkpoint.requestIdentity(provider)
		if err != nil {
			if reason == "dataset_identity_changed" {
				return taskengine.Wait(model.TaskResumeModeRecover, storageDependencyWait, "provider_confirmation", "Restore the original wallet and network to check storage setup", nil)
			}
			return taskengine.Fail(err, reason, nil)
		}
		return h.resolveProviderRejection(ctx, execution, binding, provider, checkpoint, clientID)
	}
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
			if checkpoint.ProviderRejection != nil && reason == "dataset_identity_changed" {
				return taskengine.Wait(model.TaskResumeModeRecover, storageDependencyWait, "provider_confirmation", "Restore the original wallet and network to check storage setup", nil)
			}
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
		if checkpoint.ProviderRejection != nil {
			return h.resolveProviderRejection(ctx, execution, binding, provider, checkpoint, clientDataSetID)
		}
		if checkpoint.TransactionID != "" {
			startedAt := time.Now().UTC()
			return h.waitDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID).WithWorkStartedAt(startedAt)
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
		dataSetID, clientDataSetID, identityErr := bindings.DataSetRefIDs(binding, *matching)
		if identityErr != nil {
			return failDataSetEnsure(binding, execution.ID(), identityErr, "dataset_identity_mismatch")
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, clientDataSetID)
	}
	if err != nil {
		if synapse.IsProviderUnavailable(err) {
			return taskengine.Wait(model.TaskResumeModeRecover, storageDependencyWait, "provider", "Waiting for storage provider", nil)
		}
		return retryTask(err, "dataset_discovery_failed")
	}
	if !mayCreate {
		return taskengine.Wait(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage service is ready to create", nil)
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

func (h *EnsureHandler) sendDataSetCreation(
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
	attempted, createErr := execution.WithCheckpointedEffect(ctx, taskengine.ResourceProviderMutation, fmt.Sprintf("ensure:%d", binding.ID), checkpoint,
		func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.RecordDataSetClientID(ctx, binding.ID, execution.ID(), recordedID)
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
							ID: binding.ID, TransactionID: sub.TransactionID,
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
		if errors.Is(createErr, repository.ErrConflict) || errors.Is(createErr, repository.ErrNotFound) {
			return taskengine.Cancel("Storage service setup was superseded", nil)
		}
		if errors.Is(createErr, taskengine.ErrResourceBusy) {
			return taskengine.ResourceWait("Waiting for other storage operations to finish")
		}
		return retryTask(createErr, "dataset_creation_not_started")
	}
	if evidenceErr != nil {
		return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Recording storage service creation", func(ctx context.Context, repos *repository.Repositories) error {
			if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, binding.ID, execution.ID()); err != nil {
				return err
			}
			return repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
				ID: binding.ID, TransactionID: submission.TransactionID,
				StatusURL: submission.StatusURL, ClientDataSetID: &recordedID,
			})
		})
	}
	if created != nil {
		dataSetID, createdClientID, identityErr := bindings.DataSetResultIDs(binding, created)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, createdClientID)
	}
	if submission.TransactionID != "" {
		return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage service creation", nil)
	}
	if rejection, ok := errors.AsType[*synapse.DataSetProviderRejectionError](createErr); ok &&
		binding.CreateTransactionID == nil && binding.CreateStatusURL == nil && checkpoint.TransactionID == "" {
		checkpoint.ProviderRejection = &dataSetProviderRejection{StatusCode: rejection.StatusCode, RejectedAt: time.Now().UTC(), Message: synapse.ErrorSummary(createErr)}
		if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
			encoded, encodeErr := json.Marshal(checkpoint)
			if encodeErr != nil {
				return taskengine.Fail(encodeErr, "dataset_rejection_record_failed", nil)
			}
			return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Recording storage provider refusal",
				func(ctx context.Context, repos *repository.Repositories) error {
					return repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), encoded)
				})
		}
		return h.resolveProviderRejection(ctx, execution, binding, provider, checkpoint, clientDataSetID)
	}
	// The request may have reached the provider without its submission reaching
	// us, so the chain is checked before anything is sent again.
	delay := unobservedOutcomeDelay(checkpoint.Sends)
	if summary := synapse.ErrorSummary(createErr); summary != "" && ctx.Err() == nil {
		h.deps.Logger.Warn("storage service creation request failed",
			"task_id", execution.ID(), "storage_data_set_id", binding.ID, "provider_id", binding.ProviderID,
			"sends", checkpoint.Sends, "error", summary)
		return taskengine.RetryInMode(errors.New(summary), "provider_confirmation", model.TaskResumeModeRecover, delay, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.RecordDataSetCreationError(ctx, binding.ID, execution.ID(), summary)
		})
	}
	return taskengine.Wait(model.TaskResumeModeRecover, delay, "provider_confirmation", "Checking storage service creation", nil)
}

func (h *EnsureHandler) resolveProviderRejection(ctx context.Context, execution taskengine.Execution, binding *model.StorageDataSet, provider synapse.ProviderTarget, checkpoint dataSetCreationCheckpoint, clientID sdktypes.BigInt) taskengine.Result {
	refusal := checkpoint.ProviderRejection
	if refusal == nil || refusal.RejectedAt.IsZero() || (refusal.StatusCode != 400 && refusal.StatusCode != 401 && refusal.StatusCode != 403) ||
		checkpoint.TransactionID != "" || checkpoint.StatusURL != "" || binding.CreateTransactionID != nil || binding.CreateStatusURL != nil {
		return taskengine.Fail(errors.New("invalid storage provider refusal evidence"), "invalid_checkpoint", nil)
	}
	if binding.ClientDataSetID != nil && !binding.ClientDataSetID.Equal(idtypes.OnChainIDFromSDK(clientID)) {
		return taskengine.Fail(errors.New("storage service creation identity changed"), "dataset_identity_mismatch", nil)
	}
	ref, found, err := provider.FindDataSetByClientDataSetID(ctx, clientID)
	if errors.Is(err, storage.ErrDataSetCorrelationConflict) {
		return taskengine.Fail(err, "dataset_identity_mismatch", nil)
	}
	if err != nil {
		return retryTask(err, "dataset_observation_failed")
	}
	if found {
		dataSetID, foundClientID, err := bindings.DataSetRefIDs(binding, ref)
		if err != nil {
			return taskengine.Fail(err, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, foundClientID)
	}
	identity := checkpoint.Identity
	evidence := model.DataSetCreationRejection{
		Version: 1, StatusCode: refusal.StatusCode, RejectedAt: refusal.RejectedAt, AbsenceCheckedAt: time.Now().UTC(),
		ClientDataSetID: idtypes.OnChainIDFromSDK(clientID), Payer: identity.Payer, ChainID: uint64(identity.ChainID), RecordKeeper: identity.RecordKeeper,
	}
	message := storagereplacement.ProviderRejectedMessage
	if refusal.Message != "" {
		message = synapse.ErrorSummary(errors.New(refusal.Message))
	}
	return taskengine.Fail(errors.New(message), "dataset_provider_rejected",
		func(ctx context.Context, repos *repository.Repositories) error {
			if err := repos.Contents.RecordDataSetCreationRejection(ctx, binding.ID, binding.Generation, execution.ID(), evidence); err != nil {
				return err
			}
			return repos.Contents.RecordDataSetCreationError(ctx, binding.ID, execution.ID(), message)
		})
}

func (h *EnsureHandler) findRequestedDataSet(
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
		return retryTask(err, "dataset_observation_failed")
	}
	if found {
		dataSetID, foundClientID, identityErr := bindings.DataSetRefIDs(binding, ref)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, foundClientID)
	}
	if wait := time.Until(checkpoint.AttemptedAt.Add(unobservedOutcomeDelay(checkpoint.Sends))); wait > 0 {
		return taskengine.Wait(model.TaskResumeModeRecover, wait, "provider_confirmation", "Checking storage service creation", nil)
	}
	return taskengine.Wait(model.TaskResumeModeExecute, 0, "safe_to_execute", "Retrying storage service creation", nil)
}

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

func unobservedOutcomeDelay(sends int) time.Duration {
	delay := unobservedOutcomeBaseDelay
	for i := 1; i < sends && delay < unobservedOutcomeMaxDelay; i++ {
		delay *= 2
	}
	return min(delay, unobservedOutcomeMaxDelay)
}

func (h *EnsureHandler) waitDataSetCreation(
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
					return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage service", nil)
				}
				return taskengine.Wait(model.TaskResumeModeRecover, 0, "provider_confirmation", "Checking storage service creation", nil)
			}
			return taskengine.Fail(err, "dataset_creation_rejected", dataSetFailureSettlement(binding.ID, execution.ID(), err.Error(), true))
		}
		return taskengine.RetryInMode(synapse.SummarizedError(err), "provider_confirmation", model.TaskResumeModeRecover, storagePollInterval, nil)
	}
	if result == nil {
		return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage service", nil)
	}
	dataSetID, createdClientID, err := bindings.DataSetResultIDs(binding, result)
	if err != nil {
		return taskengine.Fail(err, "dataset_identity_mismatch", nil)
	}
	return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, createdClientID)
}

func dataSetCreationSent(binding *model.StorageDataSet) bool {
	if binding == nil {
		return true
	}
	if binding.ClientDataSetID != nil && !binding.ClientDataSetID.IsZero() {
		return true
	}
	return binding.CreateTransactionID != nil && *binding.CreateTransactionID != ""
}

func failDataSetEnsure(binding *model.StorageDataSet, taskID int64, err error, reason string) taskengine.Result {
	if dataSetCreationSent(binding) {
		return taskengine.Fail(err, reason, nil)
	}
	return taskengine.Fail(err, "dataset_creation_unsent", dataSetFailureSettlement(binding.ID, taskID, err.Error(), true))
}

func (h *EnsureHandler) recoverUnnamedCreation(
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
		return retryTask(err, "dataset_observation_failed")
	}
	if found {
		dataSetID, foundClientID, identityErr := bindings.DataSetRefIDs(binding, ref)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, foundClientID)
	}
	// The chain holds nothing under that ID, and nothing here can prove which
	// identity signed the request, so it is never sent again from this task.
	return taskengine.Fail(cause, "invalid_checkpoint", nil)
}

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

func (h *EnsureHandler) completeDataSetEnsure(binding *model.StorageDataSet, taskID int64, dataSetID, clientDataSetID idtypes.OnChainID) taskengine.Result {
	return taskengine.Complete("Storage service is ready", func(ctx context.Context, repos *repository.Repositories) error {
		if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID,
		}); err != nil {
			return err
		}
		return h.finishDataSetEnsure(ctx, repos, binding, taskID)
	})
}

func (h *EnsureHandler) finishDataSetEnsure(ctx context.Context, repos *repository.Repositories, binding *model.StorageDataSet, taskID int64) error {
	if err := repos.Contents.CompleteDataSetEnsureTask(ctx, binding.ID, taskID); err != nil {
		return err
	}
	ready, err := repos.Contents.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil {
		return err
	}
	if ready == nil || ready.Status != model.StorageDataSetStatusReady || ready.DataSetID == nil || ready.DataSetID.IsZero() {
		return repository.ErrConflict
	}
	return h.deps.Messenger.Notify(ctx, repos, storagepipeline.DataSetReady{BindingID: ready.ID})
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
