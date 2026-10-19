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

type dataSetCreationCheckpoint struct {
	storagepipeline.ExecutionIdentity
	// AttemptedAt is when the latest create request was sent.
	AttemptedAt     time.Time `json:"attempted_at"`
	TransactionID   string    `json:"transaction_id,omitempty"`
	StatusURL       string    `json:"status_url,omitempty"`
	ClientDataSetID string    `json:"client_data_set_id,omitempty"`
	// Sends counts admitted requests carrying ClientDataSetID.
	Sends             int                       `json:"sends,omitempty"`
	NextResendAt      time.Time                 `json:"next_resend_at,omitzero"`
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
		MaxConcurrency: 2,
		Codec: taskengine.StrictJSONCodec(func(input *storagepipeline.DataSetInput) error {
			return storagepipeline.ValidateDataSetInput(*input)
		}),
		Identity: taskengine.IdentityFromInput("storage_data_set", func(input storagepipeline.DataSetInput) (int64, int64) { return input.DataSetID, 0 }),
		Policy: taskengine.ExecutionPolicy{
			MaxAttempts: 10, Backoff: taskengine.BackoffPolicy{InitialDelay: 30 * time.Second, Multiplier: 2, MaximumDelay: 5 * time.Minute, Jitter: 0.2},
			ObservationWindow: 30 * time.Minute,
			ResendBackoff:     &taskengine.BackoffPolicy{InitialDelay: time.Minute, Multiplier: 2, MaximumDelay: 30 * time.Minute},
		}, AllowRetry: true,
		CanManualRetry: func(task *model.Task) bool {
			if task == nil || task.FailureReason == nil {
				return true
			}
			return *task.FailureReason != "dataset_correlation_conflict"
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
	definition.BindSuccessor = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
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
		return taskengine.Cancel("Data set setup was superseded", nil)
	}
	if err != nil {
		return retryTask(err, "dataset_authorization_failed")
	}
	if h.deps.Storage == nil {
		return retryTask(errors.New("storage client is unavailable"), "dependency_unavailable")
	}
	if binding.Status == model.StorageDataSetStatusReady && binding.DataSetID != nil && !binding.DataSetID.IsZero() {
		return taskengine.Complete("Data set is ready", func(ctx context.Context, repos *repository.Repositories) error {
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
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[dataSetCreationCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	identity := input.ExecutionIdentity
	if hasCheckpoint {
		if identity.Present() {
			if err := identity.Match(checkpoint.ExecutionIdentity); err != nil {
				return taskengine.Fail(err, storagepipeline.ExecutionIdentityFailure(err), nil)
			}
		}
		identity = checkpoint.ExecutionIdentity
	}
	if err := identity.Match(bindings.CurrentIdentity(h.deps.Storage)); err != nil {
		return taskengine.Fail(err, storagepipeline.ExecutionIdentityFailure(err), nil)
	}
	provider, err := h.deps.Storage.OpenProviderTarget(ctx, binding.ProviderID.SDK(), storage.NewProviderContextOptions{
		DataSetMetadata: map[string]string{"bucket": bucket.Name},
	})
	if err != nil {
		return retryTask(err, "provider_unavailable")
	}
	if hasCheckpoint {
		// Whether or not its submission was recorded, a request is resolved only
		// under the identity it was signed for: a submission's status says
		// nothing about who pays for the data set it created.
		clientDataSetID, reason, err := checkpoint.requestIdentity(provider)
		if err != nil {
			return taskengine.Fail(err, reason, nil)
		}
		if binding.ClientDataSetID != nil && !binding.ClientDataSetID.Equal(idtypes.OnChainIDFromSDK(clientDataSetID)) {
			return taskengine.Fail(errors.New("data set creation identity changed"), "dataset_identity_mismatch", nil)
		}
		if checkpoint.TransactionID != "" || checkpoint.StatusURL != "" {
			startedAt := time.Now().UTC()
			return h.waitDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID).WithWorkStartedAt(startedAt)
		}
		return h.findRequestedDataSet(ctx, execution, binding, provider, checkpoint, clientDataSetID, mayCreate)
	}
	if dataSetCreationSent(binding) {
		return taskengine.Fail(errors.New("data set creation checkpoint is missing"), "invalid_checkpoint", nil)
	}

	if !mayCreate {
		return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Data set is ready to create", nil)
	}
	if provider.ContextIdentity() != *identity.Identity {
		return taskengine.Fail(storagepipeline.ErrExecutionIdentityChanged, "storage_identity_changed", nil)
	}
	if !contextIdentityComplete(*identity.Identity) {
		return taskengine.Fail(errors.New("storage signing identity is incomplete"), "dependency_unavailable", nil)
	}
	clientDataSetID, err := newClientDataSetID()
	if err != nil {
		return retryTask(err, "dataset_creation_not_started")
	}
	checkpoint = dataSetCreationCheckpoint{ClientDataSetID: clientDataSetID.String(), ExecutionIdentity: identity}
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
	nextResendAt, err := execution.ResendAt(checkpoint.AttemptedAt, checkpoint.Sends)
	if err != nil {
		return taskengine.Fail(err, "invalid_policy", nil)
	}
	checkpoint.NextResendAt = nextResendAt
	recordedID := idtypes.OnChainIDFromSDK(clientDataSetID)
	var (
		created     *storage.CreateDataSetResult
		evidenceErr error
		submission  storage.CreateDataSetSubmission
	)
	createCtx, cancelCreate := context.WithCancel(ctx)
	attempted, createErr := execution.WithCheckpointedEffect(ctx, fmt.Sprintf("ensure:%d", binding.ID), checkpoint,
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
						if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, binding.ID, execution.ID()); err != nil {
							return err
						}
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
			return taskengine.Cancel("Data set setup was superseded", nil)
		}
		return retryTask(createErr, "dataset_creation_not_started")
	}
	binding.ClientDataSetID = &recordedID
	if created != nil {
		dataSetID, createdClientID, identityErr := bindings.DataSetResultIDs(binding, created)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, createdClientID)
	}
	if evidenceErr != nil {
		encoded, err := json.Marshal(checkpoint)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		return taskengine.RetryInMode(synapse.SummarizedError(evidenceErr), "dataset_submission_record_failed", model.TaskResumeModeRecover,
			func(ctx context.Context, repos *repository.Repositories) error {
				if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, binding.ID, execution.ID()); err != nil {
					return err
				}
				if err := repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), encoded); err != nil {
					return err
				}
				return repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
					ID: binding.ID, TransactionID: submission.TransactionID,
					StatusURL: submission.StatusURL, ClientDataSetID: &recordedID,
				})
			})
	}
	if submission.TransactionID != "" || submission.StatusURL != "" {
		return h.waitDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID)
	}
	if createErr == nil {
		createErr = errors.New("data set creation has no confirmed result")
	}
	summary := synapse.ErrorSummary(createErr)
	if ctx.Err() == nil {
		h.deps.Logger.Warn("storage service creation request failed",
			"task_id", execution.ID(), "storage_data_set_id", binding.ID, "provider_id", binding.ProviderID,
			"sends", checkpoint.Sends, "error", summary)
	}
	return taskengine.RetryInMode(synapse.SummarizedError(createErr), "provider_confirmation", model.TaskResumeModeRecover,
		func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.RecordDataSetCreationError(ctx, binding.ID, execution.ID(), summary)
		})
}

func (h *EnsureHandler) findRequestedDataSet(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	checkpoint dataSetCreationCheckpoint,
	clientDataSetID sdktypes.BigInt,
	mayCreate bool,
) taskengine.Result {
	ref, found, err := provider.FindDataSetByClientDataSetID(ctx, clientDataSetID)
	if errors.Is(err, storage.ErrDataSetCorrelationConflict) {
		return taskengine.Fail(err, "dataset_correlation_conflict", nil)
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
	if checkpoint.NextResendAt.IsZero() {
		checkpoint.NextResendAt, err = execution.ResendAt(checkpoint.AttemptedAt, checkpoint.Sends)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		if err := execution.WriteCheckpointWith(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
			_, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, binding.ID, execution.ID())
			return err
		}); err != nil {
			return retryTask(err, "dataset_checkpoint_record_failed")
		}
	}
	if time.Now().Before(checkpoint.NextResendAt) {
		return taskengine.WaitUntil(model.TaskResumeModeRecover, checkpoint.NextResendAt, "provider_confirmation", "Checking data set creation", nil)
	}
	if !mayCreate {
		return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Retrying data set creation", nil)
	}
	return h.sendDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID)
}

func (c dataSetCreationCheckpoint) requestIdentity(provider synapse.ProviderTarget) (sdktypes.BigInt, string, error) {
	clientID, err := idtypes.ParseOnChainID("clientDataSetID", c.ClientDataSetID)
	if err != nil || clientID.IsZero() || c.Identity == nil {
		return sdktypes.BigInt{}, "invalid_checkpoint", errors.New("data set creation checkpoint has no request identity")
	}
	if provider.ContextIdentity() != *c.Identity {
		return sdktypes.BigInt{}, "dataset_identity_changed", errors.New("the wallet or network changed after data set creation began")
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

func (h *EnsureHandler) waitDataSetCreation(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	checkpoint dataSetCreationCheckpoint,
	clientDataSetID sdktypes.BigInt,
) taskengine.Result {
	startedAt, err := execution.ObserveOperation(ctx, fmt.Sprintf("ensure:%d", binding.ID))
	if err != nil {
		return retryTask(err, "dataset_observation_failed")
	}
	result, err := provider.WaitForDataSetCreated(ctx, checkpoint.StatusURL, clientDataSetID)
	if errors.Is(err, synapse.ErrProviderTransactionRejected) {
		ref, found, lookupErr := provider.FindDataSetByClientDataSetID(ctx, clientDataSetID)
		if errors.Is(lookupErr, storage.ErrDataSetCorrelationConflict) {
			return taskengine.Fail(lookupErr, "dataset_correlation_conflict", nil)
		}
		if lookupErr != nil {
			return retryTask(lookupErr, "dataset_observation_failed")
		}
		if found {
			dataSetID, foundClientID, identityErr := bindings.DataSetRefIDs(binding, ref)
			if identityErr != nil {
				return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
			}
			return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, foundClientID)
		}
		history, encodeErr := json.Marshal(map[string]any{
			"transaction_id": checkpoint.TransactionID, "status_url": checkpoint.StatusURL,
			"client_data_set_id": checkpoint.ClientDataSetID, "identity": checkpoint.Identity,
			"sends": checkpoint.Sends, "attempted_at": checkpoint.AttemptedAt,
			"next_resend_at": checkpoint.NextResendAt, "error": synapse.ErrorSummary(err),
		})
		if encodeErr != nil {
			return taskengine.Fail(encodeErr, "invalid_checkpoint", nil)
		}
		checkpoint.TransactionID, checkpoint.StatusURL = "", ""
		encoded, encodeErr := json.Marshal(checkpoint)
		if encodeErr != nil {
			return taskengine.Fail(encodeErr, "invalid_checkpoint", nil)
		}
		recordedID := idtypes.OnChainIDFromSDK(clientDataSetID)
		return taskengine.RetryInMode(synapse.SummarizedError(err), "provider_confirmation", model.TaskResumeModeRecover,
			func(ctx context.Context, repos *repository.Repositories) error {
				if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, binding.ID, execution.ID()); err != nil {
					return err
				}
				if err := repos.Tasks.AppendEvent(ctx, execution.ID(), "dataset_transaction_rejected", history); err != nil {
					return err
				}
				if err := repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), encoded); err != nil {
					return err
				}
				return repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
					ID: binding.ID, ClientDataSetID: &recordedID,
				})
			})
	}
	if err != nil {
		return retryTask(err, "provider_confirmation")
	}
	if result == nil {
		if time.Since(startedAt) < execution.Policy().ObservationWindow {
			return taskengine.WaitForObservation(model.TaskResumeModeRecover, "provider_confirmation", "Waiting for data set confirmation", nil)
		}
		return retryTask(errors.New("data set creation is awaiting confirmation"), "provider_confirmation")
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

func (h *EnsureHandler) completeDataSetEnsure(binding *model.StorageDataSet, taskID int64, dataSetID, clientDataSetID idtypes.OnChainID) taskengine.Result {
	return taskengine.Complete("Data set is ready", func(ctx context.Context, repos *repository.Repositories) error {
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
	return taskengine.Retry(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
