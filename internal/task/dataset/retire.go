package dataset

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	bindings "github.com/strahe/synaps3/internal/task/binding"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type RetireDependencies struct {
	Repositories *repository.Repositories
	Terminator   synapse.ServiceTerminator
	Epochs       synapse.ChainEpochReader

	Logger *slog.Logger
}

type retirementCheckpoint struct {
	// AttemptedAt is when the latest termination request was sent.
	AttemptedAt      time.Time `json:"attempted_at"`
	TerminationEpoch *int64    `json:"termination_epoch,omitempty"`
	TxHash           string    `json:"transaction_hash,omitempty"`
	ConfirmedTxHash  string    `json:"confirmed_tx_hash,omitempty"`
	AttemptNo        int64     `json:"attempt_no,omitempty"`
	ReceiptStatus    *uint64   `json:"receipt_status,omitempty"`
	// Only a manual Retry may authorize a new transaction after a failed receipt.
	RetryFailedTransaction bool `json:"retry_failed_transaction,omitempty"`
	// RetryUnresolvedRelay records a manual Retry authorizing the payer wallet
	// to complete an attempt whose relayed request may still land. Once that
	// transaction is admitted, the attempt is observed from it alone.
	RetryUnresolvedRelay bool `json:"retry_unresolved_relay,omitempty"`
	// Sends counts admitted termination requests.
	Sends                     int       `json:"sends,omitempty"`
	NextResendAt              time.Time `json:"next_resend_at,omitzero"`
	ProtocolNextObservationAt time.Time `json:"protocol_next_observation_at,omitzero"`
	ObservationDiagnostic     string    `json:"observation_diagnostic,omitempty"`
	// Unsent means the latest admission was released: none of its requests can
	// land, so the same attempt is admitted again.
	Unsent bool `json:"unsent,omitempty"`
	// RelayOnly means the latest admission can have sent only a provider relay
	// request. Only then does the provider reporting no request release it.
	RelayOnly bool `json:"relay_only,omitempty"`
	// Creation identity binds observations and termination requests to the same signing domain.
	Identity    *storage.ContextIdentity `json:"identity,omitempty"`
	PDPVerifier *common.Address          `json:"pdp_verifier,omitempty"`
}

type RetireHandler struct {
	*taskengine.FuncHandler
	deps RetireDependencies
}

func NewRetireHandler(deps RetireDependencies) (*RetireHandler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("task repositories are required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	h := &RetireHandler{deps: deps}
	h.FuncHandler = h.dataSetRetireHandler()
	return h, nil
}

func (h *RetireHandler) dataSetRetireHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageDataSetRetire, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		MaxConcurrency: 1,
		Codec: taskengine.StrictJSONCodec(func(input *storagereplacement.RetireInput) error {
			return storagereplacement.ValidateRetireInput(*input)
		}),
		Policy: taskengine.ExecutionPolicy{
			MaxAttempts: 10, Backoff: taskengine.BackoffPolicy{InitialDelay: 30 * time.Second, Multiplier: 2, MaximumDelay: 5 * time.Minute, Jitter: 0.2},
			ObservationWindow: 5 * time.Minute, ObservationInterval: 30 * time.Second,
		}, AllowRetry: true,
		Identity: taskengine.IdentityFromInput("storage_data_set", func(input storagereplacement.RetireInput) (int64, int64) {
			return input.DataSetID, input.Generation
		}),
		InspectRetry:  inspectRetirementRetry,
		BindSuccessor: transferRetirementOwner,
		PrepareRetry: func(_ context.Context, _ *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
			checkpoint := source.Checkpoint
			var recovery retirementCheckpoint
			authorized := false
			switch {
			case source.FailureReason == nil:
			case *source.FailureReason == "termination_confirmation_failed":
				if err := json.Unmarshal(checkpoint, &recovery); err != nil {
					return taskengine.RetryPreparation{}, err
				}
				recovery.RetryFailedTransaction, authorized = true, true
			case *source.FailureReason == "attempts_exhausted":
				// An unreadable checkpoint is left for the handler to report.
				if json.Unmarshal(checkpoint, &recovery) == nil && recovery.relayUnresolved() {
					recovery.RetryUnresolvedRelay, authorized = true, true
				}
			}
			if authorized {
				var err error
				checkpoint, err = json.Marshal(recovery)
				if err != nil {
					return taskengine.RetryPreparation{}, err
				}
			}
			return taskengine.RetryPreparation{Request: taskengine.EnqueueRequest{Type: source.Type, Input: json.RawMessage(source.Input)}, Checkpoint: checkpoint, ResumeMode: model.TaskResumeModeRecover, Bind: transferRetirementOwner}, nil
		},
		CanManualRetry: func(task *model.Task) bool {
			return task == nil || task.FailureReason == nil || *task.FailureReason != "termination_outcome_unknown"
		},
	}
	return taskengine.NewFuncHandler(definition,
		func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runDataSetRetirement(ctx, execution, true)
		},
		func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runDataSetRetirement(ctx, execution, false)
		})
}

func inspectRetirementRetry(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
	var input storagereplacement.RetireInput
	if err := json.Unmarshal(source.Input, &input); err != nil {
		return err
	}
	row, err := repos.Contents.GetDataSetBindingByID(ctx, input.DataSetID)
	if err != nil {
		return err
	}
	if row == nil || row.RetirementTaskID == nil || *row.RetirementTaskID != source.ID || row.RetirementGeneration != input.Generation {
		return repository.ErrConflict
	}
	return nil
}

func transferRetirementOwner(ctx context.Context, repos *repository.Repositories, source, next *model.Task) error {
	var input storagereplacement.RetireInput
	if err := json.Unmarshal(source.Input, &input); err != nil {
		return err
	}
	return repos.Contents.TransferRetireTaskOwner(ctx, input.DataSetID, input.Generation, source.ID, next.ID)
}

func (h *RetireHandler) runDataSetRetirement(ctx context.Context, execution taskengine.Execution, mayTerminate bool) taskengine.Result {
	input, err := taskengine.DecodeInput[storagereplacement.RetireInput](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeStorageDataSetRetire), err)
	}
	dataSet, err := h.deps.Repositories.Contents.AuthorizeDataSetRetirementTask(ctx, input.DataSetID, input.Generation, execution.ID())
	if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
		return taskengine.Cancel("Data set retirement was superseded", nil)
	}
	if err != nil {
		return h.retryRetirement(execution, input.ReplacementID, err, "retirement_authorization_failed")
	}
	row, err := h.deps.Repositories.Replacements.GetByID(ctx, input.ReplacementID)
	if err != nil || row == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return taskengine.Fail(err, "replacement_missing", nil)
	}
	abandoned := row.Status == storagereplacement.StatusSuperseded && input.DataSetID == row.TargetDataSetID
	if !abandoned && input.DataSetID != row.SourceDataSetID {
		return taskengine.Fail(errors.New("retirement data set does not belong to replacement"), "retirement_identity_mismatch", nil)
	}
	if dataSet.Status == model.StorageDataSetStatusRetired {
		return taskengine.Complete("Data set retired", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.CompleteDataSetRetirementTask(ctx, input.DataSetID, input.Generation, execution.ID())
		})
	}
	if dataSet.DataSetID == nil || dataSet.DataSetID.IsZero() {
		return taskengine.Fail(errors.New("data set has no remote identity"), "retirement_identity_missing", nil)
	}

	recordedIdentity, err := bindings.RecordedIdentity(ctx, h.deps.Repositories, dataSet)
	if err != nil {
		if storagepipeline.IsExecutionIdentityError(err) {
			return stopRetirementIdentity(abandoned, row.ID, err)
		}
		return retryTask(err, "storage_identity_load_failed")
	}
	if h.deps.Terminator == nil {
		return taskengine.Fail(errors.New("data set terminator is unavailable"), "dependency_unavailable", nil)
	}
	if err := recordedIdentity.Match(storagepipeline.NewExecutionIdentity(h.deps.Terminator.ContextIdentity(), h.deps.Terminator.PDPVerifierAddress())); err != nil {
		return stopRetirementIdentity(abandoned, row.ID, err)
	}
	var terminationEpoch *int64
	if abandoned {
		terminationEpoch = row.AbandonedTerminationEpoch
		sole, err := h.deps.Repositories.Replacements.CountAbandonedTargetSoleCopies(ctx, row.TargetDataSetID)
		if err != nil {
			return h.retryRetirement(execution, row.ID, err, "retirement_gate_failed")
		}
		if sole > 0 {
			return taskengine.Fail(fmt.Errorf("unused data set holds %d sole replicas", sole), "retirement_coverage", nil)
		}
	} else {
		terminationEpoch = row.TerminationEpoch
		gate, err := h.deps.Repositories.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
		if err != nil {
			return h.retryRetirement(execution, row.ID, err, "retirement_gate_failed")
		}
		if len(gate.Blockers) > 0 {
			if containsString(gate.Blockers, "slot_ownership") {
				err := fmt.Errorf("retirement safety gate failed: %s", strings.Join(gate.Blockers, ", "))
				return taskengine.Fail(err, "retirement_gate_invalid", func(ctx context.Context, repos *repository.Repositories) error {
					return repos.Replacements.MarkCleanupAttention(ctx, row.ID, errtext.Format(err))
				})
			}
			return taskengine.Wait(model.TaskResumeModeRecover, "retirement_gate", "Waiting for safe data set retirement", nil)
		}
	}

	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[retirementCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if !hasCheckpoint {
		checkpoint.Identity = recordedIdentity.Identity
		checkpoint.PDPVerifier = recordedIdentity.PDPVerifier
	}
	if hasCheckpoint {
		if err := (storagepipeline.ExecutionIdentity{Identity: checkpoint.Identity, PDPVerifier: checkpoint.PDPVerifier}).Match(recordedIdentity); err != nil {
			return stopRetirementIdentity(abandoned, row.ID, err)
		}
	}
	ledger := synapse.TerminationResult{EndEpoch: terminationEpoch}
	var attemptNo int64
	if abandoned {
		attemptNo = row.AbandonedTerminationAttemptNo
		ledger.ReceiptStatus = row.AbandonedTerminationReceiptStatus
		if row.AbandonedTerminationTxHash != nil {
			ledger.TxHash = *row.AbandonedTerminationTxHash
		}
		if row.AbandonedTerminationConfirmedTxHash != nil {
			ledger.ConfirmedTxHash = *row.AbandonedTerminationConfirmedTxHash
		}
	} else {
		attemptNo = row.TerminationAttemptNo
		ledger.ReceiptStatus = row.TerminationReceiptStatus
		if row.TerminationTxHash != nil {
			ledger.TxHash = *row.TerminationTxHash
		}
		if row.TerminationConfirmedTxHash != nil {
			ledger.ConfirmedTxHash = *row.TerminationConfirmedTxHash
		}
	}
	if checkpoint.AttemptNo != 0 && attemptNo != 0 && checkpoint.AttemptNo != attemptNo {
		return stopRetirement(abandoned, row.ID, synapse.ErrTerminationEvidenceConflict, "termination_evidence_conflict")
	}
	if checkpoint.AttemptNo == 0 {
		checkpoint.AttemptNo = attemptNo
	}
	if err := mergeRetirementEvidence(&checkpoint, &ledger); err != nil {
		return stopRetirement(abandoned, row.ID, err, "termination_evidence_conflict")
	}
	if terminationEpoch == nil && checkpoint.TerminationEpoch != nil {
		if *checkpoint.TerminationEpoch < 0 {
			return taskengine.Fail(errors.New("data set retirement checkpoint has an invalid epoch"), "invalid_checkpoint", nil)
		}
		return taskengine.Wait(model.TaskResumeModeRecover, "termination_epoch", "Recording data set retirement", retirementEvidenceSettlement(
			execution, input, abandoned, checkpoint,
		))
	}
	if terminationEpoch == nil {
		knownAttempt := checkpoint.attemptMayLand()
		if time.Now().Before(checkpoint.ProtocolNextObservationAt) {
			if knownAttempt {
				var diagnostic error
				if checkpoint.ObservationDiagnostic != "" {
					diagnostic = errors.New(checkpoint.ObservationDiagnostic)
				}
				return h.waitForRetirementEvidence(execution, input, abandoned, checkpoint, diagnostic)
			}
			return taskengine.WaitUntil(model.TaskResumeModeRecover, checkpoint.ProtocolNextObservationAt, "provider_confirmation", "Waiting to check data set retirement", nil)
		}
		var diagnostic error
		freshAttempt, resumeRelay := false, false
		var observe func(context.Context, sdktypes.BigInt, synapse.TerminationResult) (*synapse.TerminationResult, bool, error)
		if checkpoint.AttemptNo > 1 || (checkpoint.RetryUnresolvedRelay && !checkpoint.RelayOnly) {
			if observer, ok := h.deps.Terminator.(interface {
				ObserveTerminationDirect(context.Context, sdktypes.BigInt, synapse.TerminationResult) (*synapse.TerminationResult, bool, error)
			}); ok {
				observe = observer.ObserveTerminationDirect
			}
		} else if observer, ok := h.deps.Terminator.(interface {
			ObserveTermination(context.Context, sdktypes.BigInt, synapse.TerminationResult) (*synapse.TerminationResult, bool, error)
		}); ok {
			observe = observer.ObserveTermination
		}
		if observe != nil {
			observed, pending, err := observe(ctx, dataSet.DataSetID.SDK(), synapse.TerminationResult{
				TxHash: checkpoint.TxHash, ConfirmedTxHash: checkpoint.ConfirmedTxHash, ReceiptStatus: checkpoint.ReceiptStatus,
			})
			absentErr := err
			absent := errors.Is(err, synapse.ErrTerminationRequestAbsent)
			if absent {
				err = nil
			}
			if mergeErr := mergeRetirementEvidence(&checkpoint, observed); mergeErr != nil {
				return stopRetirement(abandoned, row.ID, mergeErr, "termination_evidence_conflict")
			}
			checkpoint.ObservationDiagnostic = errtext.Format(err)
			if errors.Is(err, synapse.ErrTerminationEvidenceConflict) {
				return stopRetirement(abandoned, row.ID, err, "termination_evidence_conflict")
			}
			if observed != nil && observed.EndEpoch != nil {
				return taskengine.Wait(model.TaskResumeModeRecover, "termination_epoch", "Recording data set retirement", retirementEvidenceSettlement(execution, input, abandoned, checkpoint))
			}
			if errors.Is(err, synapse.ErrTerminationTransactionFailed) {
				if !checkpoint.RetryFailedTransaction || checkpoint.ReceiptStatus == nil || *checkpoint.ReceiptStatus != 0 {
					return stopRetirement(abandoned, row.ID,
						fmt.Errorf("data set termination transaction reverted; Retry checks the service and submits a new paid transaction if it is still active: %w", err),
						"termination_confirmation_failed", retirementEvidenceSettlement(execution, input, abandoned, checkpoint))
				}
				if !mayTerminate {
					return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Data set is ready for another termination attempt", retirementEvidenceSettlement(execution, input, abandoned, checkpoint))
				}
				// The failed attempt remains in its ledger while this manual authorization
				// proceeds to a separately admitted transaction.
				knownAttempt = false
				freshAttempt, pending = true, false
				diagnostic = nil
				err = nil
			}
			if knownAttempt && errors.Is(err, synapse.ErrServicePaidByAnother) {
				return stopRetirement(abandoned, row.ID, err, "termination_payer_mismatch", retirementEvidenceSettlement(execution, input, abandoned, checkpoint))
			}
			if !freshAttempt {
				knownAttempt = checkpoint.attemptMayLand()
			}
			if absent && knownAttempt && checkpoint.RelayOnly && checkpoint.TxHash == "" && checkpoint.ConfirmedTxHash == "" {
				checkpoint.Unsent = true
				release := retirementReleaseSettlement(execution, input, abandoned, checkpoint)
				return taskengine.RetryInMode(fmt.Errorf("data set termination request was not received; sending it again: %w", absentErr),
					"termination_not_sent", model.TaskResumeModeExecute, release).WithContentionSettlement(release)
			}
			if knownAttempt && checkpoint.RetryUnresolvedRelay && checkpoint.relayUnresolved() && ctx.Err() == nil {
				if !mayTerminate {
					return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Data set is ready for a direct termination attempt", nil)
				}
				// The payer wallet completes this attempt, which the provider's
				// relayed request may still land alongside.
				knownAttempt, resumeRelay, pending = false, true, false
				diagnostic, err = nil, nil
			}
			if err != nil {
				diagnostic = err
				if httpErr, ok := errors.AsType[*pdp.HTTPError](err); ok && httpErr.RetryAfter > 0 && ctx.Err() == nil {
					checkpoint.ProtocolNextObservationAt = time.Now().UTC().Add(httpErr.RetryAfter)
					if knownAttempt {
						return h.waitForRetirementEvidence(execution, input, abandoned, checkpoint, err)
					}
					if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
						return retryTask(err, "termination_checkpoint_record_failed")
					}
					return taskengine.WaitUntil(model.TaskResumeModeRecover, checkpoint.ProtocolNextObservationAt, "provider_confirmation", "Waiting to check data set retirement", nil)
				}
				if !knownAttempt && (ctx.Err() != nil || !errors.Is(err, synapse.ErrTerminationObservationUnavailable)) {
					return retryTask(err, "termination_observation_failed")
				}
				if !knownAttempt {
					h.deps.Logger.Warn("storage service termination status unavailable",
						"task_id", execution.ID(), "storage_data_set_id", dataSet.ID, "error", errtext.Format(err))
				}
			}
			if pending && !knownAttempt {
				return taskengine.WaitForObservation(model.TaskResumeModeRecover, "provider_confirmation", "Waiting for data set retirement", nil).WithDiagnostic(diagnostic)
			}
		}
		if knownAttempt {
			return h.waitForRetirementEvidence(execution, input, abandoned, checkpoint, diagnostic)
		}
		if !mayTerminate {
			return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Data set is ready to retire", nil)
		}
		if h.deps.Terminator == nil {
			return taskengine.Fail(errors.New("data set terminator is unavailable"), "dependency_unavailable", nil)
		}
		identity := h.deps.Terminator.ContextIdentity()
		if !contextIdentityComplete(identity) {
			return taskengine.Fail(errors.New("storage signing identity is incomplete"), "dependency_unavailable", nil)
		}
		// Ownership is read before anything is recorded. The checkpoint below
		// binds the identity a request goes out under; a refusal after it would
		// bind an identity nothing was sent for, and an operator who put the right
		// wallet back could never retry past it.
		if err := h.deps.Terminator.VerifyServicePayer(ctx, dataSet.DataSetID.SDK()); err != nil {
			if errors.Is(err, synapse.ErrServicePaidByAnother) {
				return stopRetirement(abandoned, row.ID, err, "termination_payer_mismatch")
			}
			return retryTask(err, "termination_payer_observation_failed")
		}
		previous := checkpoint
		nextAttempt := checkpoint.AttemptNo
		if nextAttempt == 0 {
			nextAttempt = 1
		} else if freshAttempt {
			nextAttempt++
		}
		resumeAuthorized := previous.RetryUnresolvedRelay && !freshAttempt
		direct := freshAttempt || nextAttempt > 1 || resumeAuthorized
		checkpoint = retirementCheckpoint{
			AttemptedAt: time.Now().UTC(), AttemptNo: nextAttempt, Sends: checkpoint.Sends + 1, RelayOnly: !direct,
			RetryUnresolvedRelay: resumeAuthorized, Identity: &identity, PDPVerifier: recordedIdentity.PDPVerifier,
		}
		terminate := h.deps.Terminator.TerminateService
		if direct {
			directTerminator, ok := h.deps.Terminator.(interface {
				TerminateServiceDirect(context.Context, sdktypes.BigInt, synapse.TerminationHooks) (*synapse.TerminationResult, error)
			})
			if !ok {
				return stopRetirement(abandoned, row.ID, errors.New("direct data set termination is unavailable"), "dependency_unavailable")
			}
			terminate = directTerminator.TerminateServiceDirect
		}
		attempted, err := execution.WithCheckpointedEffect(ctx, fmt.Sprintf("retire:%d", dataSet.ID), checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
			if freshAttempt {
				if err := repos.Replacements.RecordTerminationEvidence(ctx, retirementEvidenceInput(input, execution.ID(), abandoned, previous)); err != nil {
					return err
				}
			}
			role := storagereplacement.TerminationRoleSource
			if abandoned {
				role = storagereplacement.TerminationRoleAbandonedTarget
			}
			return repos.Replacements.BeginTerminationAttempt(ctx, repository.BeginTerminationAttemptInput{
				ReplacementID: input.ReplacementID, DataSetID: input.DataSetID, Generation: input.Generation, TaskID: execution.ID(), Role: role,
				AttemptNo: nextAttempt, PreviousAttemptNo: previous.AttemptNo, ResumeUnresolved: resumeRelay, AttemptedAt: checkpoint.AttemptedAt,
			})
		}, func(ctx context.Context) error {
			result, terminateErr := terminate(ctx, dataSet.DataSetID.SDK(), synapse.TerminationHooks{
				OnSubmitted: func(hash string) error {
					if err := mergeRetirementEvidence(&checkpoint, &synapse.TerminationResult{TxHash: hash}); err != nil {
						return err
					}
					return execution.RecordEffectEvidence(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
						return repos.Replacements.RecordTerminationEvidence(ctx, retirementEvidenceInput(input, execution.ID(), abandoned, checkpoint))
					})
				},
				// Once the payer wallet may broadcast, the provider's answer does
				// not cover every transaction this attempt can put on chain.
				BeforeDirect: func() error {
					if !checkpoint.RelayOnly {
						return nil
					}
					checkpoint.RelayOnly = false
					return execution.WriteCheckpoint(ctx, checkpoint)
				},
			})
			return errors.Join(terminateErr, mergeRetirementEvidence(&checkpoint, result))
		})
		if err != nil && !attempted {
			return retryTask(err, "termination_not_started")
		}
		notSent := errors.Is(err, synapse.ErrTerminationNotSent)
		if !direct && (notSent || errors.Is(err, synapse.ErrTerminationTransactionNotSent)) {
			checkpoint.RelayOnly = true
		}
		settlement := retirementEvidenceSettlement(execution, input, abandoned, checkpoint)
		if errors.Is(err, synapse.ErrTerminationEvidenceConflict) {
			return stopRetirement(abandoned, row.ID, err, "termination_evidence_conflict", settlement)
		}
		if notSent {
			checkpoint.Unsent = true
			release := retirementReleaseSettlement(execution, input, abandoned, checkpoint)
			switch {
			case synapse.IsTerminationBlocked(err):
				return stopRetirement(abandoned, row.ID, err, "termination_blocked", release)
			case errors.Is(err, synapse.ErrServicePaidByAnother):
				return stopRetirement(abandoned, row.ID, err, "termination_payer_mismatch", release)
			default:
				return taskengine.RetryInMode(err, "termination_not_sent", model.TaskResumeModeExecute, release).WithContentionSettlement(release)
			}
		}
		if synapse.IsTerminationBlocked(err) {
			return stopRetirement(abandoned, row.ID, err, "termination_blocked", settlement)
		}
		if errors.Is(err, synapse.ErrServicePaidByAnother) {
			return stopRetirement(abandoned, row.ID, err, "termination_payer_mismatch", settlement)
		}
		if err != nil {
			if ctx.Err() == nil {
				h.deps.Logger.Warn("storage service termination request failed",
					"task_id", execution.ID(), "storage_data_set_id", dataSet.ID, "sends", checkpoint.Sends,
					"error", errtext.Format(err))
			}
			return taskengine.RetryInMode(err, "provider_confirmation", model.TaskResumeModeRecover, settlement).WithContentionSettlement(settlement)
		}
		if checkpoint.TerminationEpoch == nil {
			return taskengine.RetryInMode(errors.New("data set retirement has no confirmed result"), "provider_confirmation", model.TaskResumeModeRecover, settlement).WithContentionSettlement(settlement)
		}
		if *checkpoint.TerminationEpoch < 0 {
			checkpoint.TerminationEpoch = nil
			return stopRetirement(abandoned, row.ID,
				errors.New("data set termination returned an invalid epoch"), "termination_outcome_unknown", retirementEvidenceSettlement(execution, input, abandoned, checkpoint))
		}
		return taskengine.WaitForObservation(model.TaskResumeModeRecover, "termination_epoch", "Waiting for data set retirement", settlement)
	}
	if h.deps.Epochs == nil {
		return taskengine.Fail(errors.New("chain epoch reader is unavailable"), "dependency_unavailable", nil)
	}
	observedEpoch, err := h.deps.Epochs.CurrentEpoch(ctx)
	if err != nil {
		return retryTask(err, "termination_epoch_observation_failed")
	}
	if observedEpoch < *terminationEpoch {
		return taskengine.WaitForObservation(model.TaskResumeModeRecover, "termination_epoch", "Waiting for data set retirement", nil)
	}
	return taskengine.Complete("Data set retired", func(ctx context.Context, repos *repository.Repositories) error {
		if abandoned {
			if err := repos.Replacements.CompleteAbandonedTargetTermination(ctx, row.ID); err != nil {
				return err
			}
		} else {
			// A task retried after it stopped for attention finishes with the
			// replacement still marked for it. The retirement it was stopped on is
			// done, so the replacement returns to retiring and completes.
			if err := repos.Replacements.BeginRetirement(ctx, row.ID); err != nil && !errors.Is(err, repository.ErrConflict) {
				return err
			}
			if err := repos.Replacements.CompleteRetirement(ctx, row.ID, observedEpoch); err != nil {
				return err
			}
		}
		return repos.Contents.CompleteDataSetRetirementTask(ctx, input.DataSetID, input.Generation, execution.ID())
	})
}

// relayUnresolved reports whether the latest admission sent only a provider
// relay request whose outcome no evidence settles.
func (c retirementCheckpoint) relayUnresolved() bool {
	return c.RelayOnly && c.attemptMayLand() && c.TxHash == "" && c.ConfirmedTxHash == "" && c.ReceiptStatus == nil
}

// attemptMayLand reports whether the latest admission can still put a
// termination transaction on chain.
func (c retirementCheckpoint) attemptMayLand() bool {
	if c.TxHash != "" || c.ConfirmedTxHash != "" {
		return true
	}
	return !c.Unsent && (c.AttemptNo != 0 || c.Sends > 0 || !c.AttemptedAt.IsZero())
}

func mergeRetirementEvidence(checkpoint *retirementCheckpoint, result *synapse.TerminationResult) error {
	if result == nil {
		return nil
	}
	if (checkpoint.TxHash != "" && result.TxHash != "" && checkpoint.TxHash != result.TxHash) ||
		(checkpoint.ConfirmedTxHash != "" && result.ConfirmedTxHash != "" && checkpoint.ConfirmedTxHash != result.ConfirmedTxHash) ||
		(checkpoint.ReceiptStatus != nil && result.ReceiptStatus != nil && *checkpoint.ReceiptStatus != *result.ReceiptStatus) ||
		(checkpoint.TerminationEpoch != nil && result.EndEpoch != nil && *checkpoint.TerminationEpoch != *result.EndEpoch) {
		return synapse.ErrTerminationEvidenceConflict
	}
	if result.TxHash != "" {
		checkpoint.TxHash = result.TxHash
	}
	if result.ConfirmedTxHash != "" {
		checkpoint.ConfirmedTxHash = result.ConfirmedTxHash
	}
	if result.ReceiptStatus != nil {
		checkpoint.ReceiptStatus = result.ReceiptStatus
	}
	if result.EndEpoch != nil {
		checkpoint.TerminationEpoch = result.EndEpoch
	}
	return nil
}

func (h *RetireHandler) waitForRetirementEvidence(execution taskengine.Execution, input storagereplacement.RetireInput, abandoned bool, checkpoint retirementCheckpoint, diagnostic error) taskengine.Result {
	timeoutErr := fmt.Errorf("data set termination is still unconfirmed after %s", execution.Policy().ObservationWindow)
	if checkpoint.relayUnresolved() {
		timeoutErr = fmt.Errorf("data set termination is still unconfirmed after %s. Once retries run out, Retry may submit a paid termination from your wallet; the provider may still terminate the data set", execution.Policy().ObservationWindow)
	}
	return taskengine.WaitForObservation(model.TaskResumeModeRecover, "provider_confirmation", "Waiting for data set retirement", retirementEvidenceSettlement(execution, input, abandoned, checkpoint)).
		WithObservationWindow("termination_confirmation", timeoutErr, model.TaskResumeModeRecover).
		WithDiagnostic(diagnostic).
		WithRetrySettlements(nil, func(ctx context.Context, repos *repository.Repositories) error {
			if abandoned {
				return nil
			}
			return repos.Replacements.MarkCleanupAttention(ctx, input.ReplacementID, errtext.Format(errors.Join(timeoutErr, diagnostic)))
		})
}

func retirementEvidenceInput(input storagereplacement.RetireInput, taskID int64, abandoned bool, checkpoint retirementCheckpoint) repository.RecordTerminationEvidenceInput {
	role := storagereplacement.TerminationRoleSource
	if abandoned {
		role = storagereplacement.TerminationRoleAbandonedTarget
	}
	return repository.RecordTerminationEvidenceInput{
		ReplacementID: input.ReplacementID, DataSetID: input.DataSetID, Generation: input.Generation, TaskID: taskID, Role: role,
		AttemptNo: checkpoint.AttemptNo, ReceiptStatus: checkpoint.ReceiptStatus,
		TxHash: checkpoint.TxHash, ConfirmedTxHash: checkpoint.ConfirmedTxHash, Epoch: checkpoint.TerminationEpoch,
	}
}

func retirementEvidenceSettlement(execution taskengine.Execution, input storagereplacement.RetireInput, abandoned bool, checkpoint retirementCheckpoint) taskengine.Settlement {
	if checkpoint.AttemptNo == 0 && checkpoint.TxHash == "" && checkpoint.ConfirmedTxHash == "" && checkpoint.TerminationEpoch == nil {
		return nil
	}
	return func(ctx context.Context, repos *repository.Repositories) error {
		raw, err := json.Marshal(checkpoint)
		if err != nil {
			return err
		}
		if err := repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), raw); err != nil {
			return err
		}
		return repos.Replacements.RecordTerminationEvidence(ctx, retirementEvidenceInput(input, execution.ID(), abandoned, checkpoint))
	}
}

// retirementReleaseSettlement releases the latest admission together with the
// checkpoint that marks it unsent.
func retirementReleaseSettlement(execution taskengine.Execution, input storagereplacement.RetireInput, abandoned bool, checkpoint retirementCheckpoint) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		raw, err := json.Marshal(checkpoint)
		if err != nil {
			return err
		}
		if err := repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), raw); err != nil {
			return err
		}
		return repos.Replacements.RecordTerminationUnsent(ctx, retirementEvidenceInput(input, execution.ID(), abandoned, checkpoint))
	}
}

func (h *RetireHandler) retryRetirement(_ taskengine.Execution, _ int64, err error, reason string) taskengine.Result {
	return retryTask(err, reason)
}

func stopRetirement(abandoned bool, replacementID int64, err error, reason string, evidence ...taskengine.Settlement) taskengine.Result {
	persist := func(ctx context.Context, repos *repository.Repositories) error {
		for _, settlement := range evidence {
			if settlement != nil {
				if err := settlement(ctx, repos); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if abandoned {
		return taskengine.Fail(err, reason, persist).WithContentionSettlement(persist)
	}
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		if recordErr := persist(ctx, repos); recordErr != nil {
			return recordErr
		}
		return repos.Replacements.MarkCleanupAttention(ctx, replacementID, errtext.Format(err))
	}).WithContentionSettlement(persist)
}

func stopRetirementIdentity(abandoned bool, replacementID int64, err error) taskengine.Result {
	reason := storagepipeline.ExecutionIdentityFailure(err)
	if errors.Is(err, storagepipeline.ErrExecutionIdentityChanged) {
		reason = "termination_identity_changed"
	}
	return stopRetirement(abandoned, replacementID, err, reason)
}

func containsString(values []string, wanted string) bool {
	return slices.Contains(values, wanted)
}
