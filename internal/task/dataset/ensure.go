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
	"github.com/strahe/synaps3/internal/errtext"
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
	AttemptedAt         time.Time `json:"attempted_at"`
	TxHash              string    `json:"transaction_id,omitempty"`
	ConfirmedTxHash     string    `json:"confirmed_tx_hash,omitempty"`
	TransactionRejected bool      `json:"transaction_rejected,omitempty"`
	StatusURL           string    `json:"status_url,omitempty"`
	ClientDataSetID     string    `json:"client_data_set_id,omitempty"`
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
		return h.recoverDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID, mayCreate)
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
		submitted   bool
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
					if !submitted && sub.TransactionID != "" {
						checkpoint.recordSubmission(sub.TransactionID)
						submitted = true
					}
					if evidenceErr = checkpoint.mergeEvidence(sub.TransactionID, "", sub.StatusURL); evidenceErr != nil {
						cancelCreate()
						return
					}
					evidenceErr = execution.RecordEffectEvidence(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
						if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, binding.ID, execution.ID()); err != nil {
							return err
						}
						return repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
							ID: binding.ID, TxHash: checkpoint.TxHash, ConfirmedTxHash: checkpoint.ConfirmedTxHash,
							StatusURL: checkpoint.StatusURL, ClientDataSetID: &recordedID,
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
		if !submitted && created.TransactionID != "" {
			checkpoint.recordSubmission(created.TransactionID)
		}
		if err := checkpoint.mergeEvidence(created.TransactionID, created.ConfirmedTransactionID, submission.StatusURL); err != nil {
			cause := errors.Join(createErr, evidenceErr, err)
			settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, cause)
			return taskengine.Fail(cause, "dataset_identity_mismatch", settlement).WithContentionSettlement(settlement)
		}
		if created.DataSet.DataSetID().IsZero() {
			if createErr == nil {
				createErr = errors.New("data set creation has no confirmed identity")
			}
			cause := errors.Join(createErr, evidenceErr)
			settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, cause)
			return taskengine.RetryInMode(cause, "provider_confirmation", model.TaskResumeModeRecover, settlement).WithContentionSettlement(settlement)
		}
		if _, _, identityErr := bindings.DataSetResultIDs(binding, created); identityErr != nil {
			cause := errors.Join(createErr, evidenceErr, identityErr)
			settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, cause)
			return taskengine.Fail(cause, "dataset_identity_mismatch", settlement).WithContentionSettlement(settlement)
		}
		ref, found, lookupErr := provider.FindDataSetByClientDataSetID(ctx, clientDataSetID)
		if errors.Is(lookupErr, storage.ErrDataSetCorrelationConflict) {
			cause := errors.Join(createErr, evidenceErr, lookupErr)
			settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, cause)
			return taskengine.Fail(cause, "dataset_correlation_conflict", settlement).WithContentionSettlement(settlement)
		}
		if lookupErr == nil && found {
			chainID, chainClientID, identityErr := bindings.DataSetRefIDs(binding, ref)
			if identityErr != nil {
				cause := errors.Join(createErr, evidenceErr, identityErr)
				settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, cause)
				return taskengine.Fail(cause, "dataset_identity_mismatch", settlement).WithContentionSettlement(settlement)
			}
			return h.completeDataSetEnsure(binding, execution.ID(), chainID, chainClientID, created)
		}
		settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, nil)
		return taskengine.WaitForObservation(model.TaskResumeModeRecover, "provider_confirmation", "Waiting for data set confirmation", settlement).
			WithDiagnostic(errors.Join(createErr, evidenceErr, lookupErr))
	}
	if evidenceErr != nil {
		cause := errors.Join(createErr, evidenceErr)
		settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, cause)
		return taskengine.RetryInMode(cause, "dataset_submission_record_failed", model.TaskResumeModeRecover, settlement).WithContentionSettlement(settlement)
	}
	if submission.StatusURL != "" {
		return h.recoverDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID, false)
	}
	if createErr == nil {
		createErr = errors.New("data set creation has no confirmed result")
	}
	summary := errtext.Format(createErr)
	if ctx.Err() == nil {
		h.deps.Logger.Warn("storage service creation request failed",
			"task_id", execution.ID(), "storage_data_set_id", binding.ID, "provider_id", binding.ProviderID,
			"sends", checkpoint.Sends, "error", summary)
	}
	return taskengine.RetryInMode(createErr, "provider_confirmation", model.TaskResumeModeRecover,
		func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.RecordDataSetCreationError(ctx, binding.ID, execution.ID(), summary)
		})
}

// recoverDataSetCreation settles a sent request from the chain: only the data
// set found by its client ID completes it. Provider status supplies transaction
// evidence and rejection, never completion.
func (h *EnsureHandler) recoverDataSetCreation(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	checkpoint dataSetCreationCheckpoint,
	clientDataSetID sdktypes.BigInt,
	mayCreate bool,
) taskengine.Result {
	ref, found, lookupErr := provider.FindDataSetByClientDataSetID(ctx, clientDataSetID)
	if errors.Is(lookupErr, storage.ErrDataSetCorrelationConflict) {
		return taskengine.Fail(lookupErr, "dataset_correlation_conflict", nil)
	}
	if lookupErr == nil && found {
		dataSetID, foundClientID, identityErr := bindings.DataSetRefIDs(binding, ref)
		if identityErr != nil {
			return taskengine.Fail(identityErr, "dataset_identity_mismatch", nil)
		}
		return h.completeDataSetEnsure(binding, execution.ID(), dataSetID, foundClientID, checkpoint.creationEvidence())
	}
	// A sent request that was not rejected is only observed, so a failed
	// lookup is a diagnostic of that observation rather than a spent attempt.
	if !checkpoint.TransactionRejected && checkpoint.StatusURL != "" {
		startedAt := time.Now().UTC()
		return h.observeDataSetCreation(ctx, execution, binding, provider, checkpoint, clientDataSetID, lookupErr).WithWorkStartedAt(startedAt)
	}
	if !checkpoint.TransactionRejected && (checkpoint.TxHash != "" || checkpoint.ConfirmedTxHash != "") {
		startedAt, err := execution.ObserveOperation(ctx, fmt.Sprintf("ensure:%d", binding.ID))
		if err != nil {
			return retryTask(errors.Join(err, lookupErr), "dataset_observation_failed")
		}
		// A missing status URL does not prove that the submitted transaction failed.
		if time.Since(startedAt) < execution.Policy().ObservationWindow {
			return taskengine.WaitForObservation(model.TaskResumeModeRecover, "provider_confirmation", "Waiting for data set confirmation", nil).WithDiagnostic(lookupErr)
		}
		return retryTask(errors.Join(errors.New("data set creation is awaiting confirmation without a status URL"), lookupErr), "provider_confirmation")
	}
	if lookupErr != nil {
		return retryTask(lookupErr, "dataset_observation_failed")
	}
	if checkpoint.NextResendAt.IsZero() {
		var err error
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

// observeDataSetCreation reads the provider's status after this round found no
// data set on chain, or could not look it up (lookupErr). It keeps the evidence
// and detects rejection.
func (h *EnsureHandler) observeDataSetCreation(
	ctx context.Context,
	execution taskengine.Execution,
	binding *model.StorageDataSet,
	provider synapse.ProviderTarget,
	checkpoint dataSetCreationCheckpoint,
	clientDataSetID sdktypes.BigInt,
	lookupErr error,
) taskengine.Result {
	startedAt, err := execution.ObserveOperation(ctx, fmt.Sprintf("ensure:%d", binding.ID))
	if err != nil {
		return retryTask(errors.Join(err, lookupErr), "dataset_observation_failed")
	}
	recordedID := idtypes.OnChainIDFromSDK(clientDataSetID)
	previous := checkpoint
	result, err := provider.WaitForDataSetCreated(ctx, checkpoint.StatusURL, clientDataSetID)
	err = errors.Join(lookupErr, err)
	if result != nil {
		if evidenceErr := checkpoint.mergeEvidence(result.TransactionID, result.ConfirmedTransactionID, ""); evidenceErr != nil {
			cause := errors.Join(err, evidenceErr)
			settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, cause)
			return taskengine.Fail(cause, "dataset_identity_mismatch", settlement).WithContentionSettlement(settlement)
		}
	}
	if errors.Is(err, synapse.ErrProviderTransactionRejected) {
		checkpoint.TransactionRejected = true
		recordEvidence := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, err)
		history, encodeErr := json.Marshal(map[string]any{
			"tx_hash": checkpoint.TxHash, "confirmed_tx_hash": checkpoint.ConfirmedTxHash, "status_url": checkpoint.StatusURL,
			"client_data_set_id": checkpoint.ClientDataSetID, "identity": checkpoint.Identity,
			"sends": checkpoint.Sends, "attempted_at": checkpoint.AttemptedAt,
			"next_resend_at": checkpoint.NextResendAt, "error": errtext.Format(err),
		})
		if encodeErr != nil {
			return taskengine.Fail(encodeErr, "invalid_checkpoint", nil)
		}
		return taskengine.RetryInMode(err, "provider_confirmation", model.TaskResumeModeRecover,
			func(ctx context.Context, repos *repository.Repositories) error {
				if err := recordEvidence(ctx, repos); err != nil {
					return err
				}
				return repos.Tasks.AppendEvent(ctx, execution.ID(), "dataset_transaction_rejected", history)
			}).WithContentionSettlement(recordEvidence)
	}
	if result != nil && !result.DataSet.DataSetID().IsZero() {
		if _, _, identityErr := bindings.DataSetResultIDs(binding, result); identityErr != nil {
			cause := errors.Join(err, identityErr)
			settlement := dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, cause)
			return taskengine.Fail(cause, "dataset_identity_mismatch", settlement).WithContentionSettlement(settlement)
		}
	}
	var settlement taskengine.Settlement
	if checkpoint.TxHash != previous.TxHash || checkpoint.ConfirmedTxHash != previous.ConfirmedTxHash {
		settlement = dataSetCreationEvidenceSettlement(execution, binding.ID, recordedID, checkpoint, nil)
	}
	// A confirmed provider status still waits for the chain lookup of a later round.
	if time.Since(startedAt) < execution.Policy().ObservationWindow {
		return taskengine.WaitForObservation(model.TaskResumeModeRecover, "provider_confirmation", "Waiting for data set confirmation", settlement).WithDiagnostic(err)
	}
	return taskengine.Retry(errors.Join(errors.New("data set creation is awaiting confirmation"), err), "provider_confirmation", settlement)
}

// creationEvidence returns the recorded transaction pair for a data set found on chain.
func (c dataSetCreationCheckpoint) creationEvidence() *storage.CreateDataSetResult {
	if c.TxHash == "" && c.ConfirmedTxHash == "" {
		return nil
	}
	return &storage.CreateDataSetResult{TransactionID: c.TxHash, ConfirmedTransactionID: c.ConfirmedTxHash}
}

func (c *dataSetCreationCheckpoint) recordSubmission(txHash string) {
	if txHash != "" && txHash != c.TxHash {
		c.TxHash, c.ConfirmedTxHash, c.StatusURL = txHash, "", ""
		c.TransactionRejected = false
	}
}

func (c *dataSetCreationCheckpoint) mergeEvidence(txHash, confirmedTxHash, statusURL string) error {
	if txHash != "" && c.TxHash != "" && txHash != c.TxHash {
		return fmt.Errorf("data set creation returned submission hash %q for recorded %q: %w", txHash, c.TxHash, repository.ErrConflict)
	}
	if confirmedTxHash != "" {
		if c.ConfirmedTxHash != "" && c.ConfirmedTxHash != confirmedTxHash {
			return fmt.Errorf("data set creation has conflicting confirmation hashes: %w", repository.ErrConflict)
		}
		c.ConfirmedTxHash = confirmedTxHash
	}
	if txHash != "" {
		c.TxHash = txHash
	}
	if statusURL != "" && (txHash != "" || c.TxHash == "") {
		c.StatusURL = statusURL
	}
	return nil
}

func dataSetCreationSent(binding *model.StorageDataSet) bool {
	if binding == nil {
		return true
	}
	if binding.ClientDataSetID != nil && !binding.ClientDataSetID.IsZero() {
		return true
	}
	return binding.TxHash != nil && *binding.TxHash != ""
}

func (h *EnsureHandler) completeDataSetEnsure(binding *model.StorageDataSet, taskID int64, dataSetID, clientDataSetID idtypes.OnChainID, result *storage.CreateDataSetResult) taskengine.Result {
	return taskengine.Complete("Data set is ready", func(ctx context.Context, repos *repository.Repositories) error {
		input := repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: dataSetID, ClientDataSetID: &clientDataSetID}
		if result != nil {
			input.TxHash, input.ConfirmedTxHash = result.TransactionID, result.ConfirmedTransactionID
		}
		if err := repos.Contents.MarkDataSetReady(ctx, input); err != nil {
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

func dataSetCreationEvidenceSettlement(execution taskengine.Execution, bindingID int64, clientID idtypes.OnChainID, checkpoint dataSetCreationCheckpoint, cause error) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		if _, err := repos.Contents.AuthorizeDataSetEnsureTask(ctx, bindingID, execution.ID()); err != nil {
			return err
		}
		raw, err := json.Marshal(checkpoint)
		if err != nil {
			return err
		}
		if err := repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), raw); err != nil {
			return err
		}
		if err := repos.Contents.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
			ID: bindingID, TxHash: checkpoint.TxHash, ConfirmedTxHash: checkpoint.ConfirmedTxHash,
			StatusURL: checkpoint.StatusURL, ClientDataSetID: &clientID,
		}); err != nil {
			return err
		}
		if cause != nil {
			return repos.Contents.RecordDataSetCreationError(ctx, bindingID, execution.ID(), errtext.Format(cause))
		}
		return nil
	}
}
