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

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
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
	TransactionHash  string    `json:"transaction_hash,omitempty"`
	// Sends counts termination requests; it paces the ones that follow an
	// unobserved outcome.
	Sends int `json:"sends,omitempty"`
	// Identity is what the first request signed for. A numeric data set ID means
	// something else on another chain, so recovery compares this before it reads
	// the chain or sends again.
	Identity *storage.ContextIdentity `json:"identity,omitempty"`
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
		Policy: taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
		Subject:       taskengine.SubjectFromInput("storage_data_set", func(input storagereplacement.RetireInput) int64 { return input.DataSetID }),
		InspectRetry:  inspectRetirementRetry,
		LegacyHandoff: transferRetirementOwner,
		PrepareRetry: func(_ context.Context, _ *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
			return taskengine.RetryPreparation{Request: taskengine.EnqueueRequest{Type: source.Type, IdempotencyKey: source.IdempotencyKey, Input: json.RawMessage(source.Input)}, Checkpoint: source.Checkpoint, ResumeMode: model.TaskResumeModeRecover, Bind: transferRetirementOwner}, nil
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
		return taskengine.Cancel("Storage service retirement was superseded", nil)
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
		return taskengine.Complete("Storage service retired", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.CompleteDataSetRetirementTask(ctx, input.DataSetID, input.Generation, execution.ID())
		})
	}
	if dataSet.DataSetID == nil || dataSet.DataSetID.IsZero() {
		return taskengine.Fail(errors.New("storage service has no remote identity"), "retirement_identity_missing", nil)
	}

	var terminationEpoch *int64
	if abandoned {
		terminationEpoch = row.AbandonedTerminationEpoch
		sole, err := h.deps.Repositories.Replacements.CountAbandonedTargetSoleCopies(ctx, row.TargetDataSetID)
		if err != nil {
			return h.retryRetirement(execution, row.ID, err, "retirement_gate_failed")
		}
		if sole > 0 {
			return taskengine.Fail(fmt.Errorf("unused storage service holds %d sole copies", sole), "retirement_coverage", nil)
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
					return repos.Replacements.MarkCleanupAttention(ctx, row.ID, err.Error())
				})
			}
			return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "retirement_gate", "Waiting for safe storage service retirement", nil)
		}
	}

	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[retirementCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if hasCheckpoint && checkpoint.Identity != nil && h.deps.Terminator != nil &&
		*checkpoint.Identity != h.deps.Terminator.ContextIdentity() {
		// The wallet or network moved after a request went out. The same numeric
		// data set ID names a different service on another chain, and an end
		// epoch read there would say nothing about ours, so this reads nothing
		// and sends nothing until the original configuration is back.
		return stopRetirement(abandoned, row.ID,
			errors.New("the wallet or network changed after storage service retirement began"), "termination_identity_changed")
	}
	if terminationEpoch == nil && checkpoint.TerminationEpoch != nil {
		if *checkpoint.TerminationEpoch < 0 {
			return taskengine.Fail(errors.New("storage service retirement checkpoint has an invalid epoch"), "invalid_checkpoint", nil)
		}
		return taskengine.Wait(model.TaskResumeModeRecover, 0, "termination_epoch", "Recording storage service retirement", retirementEvidenceSettlement(
			abandoned, row.ID, *checkpoint.TerminationEpoch, checkpoint.TransactionHash,
		))
	}
	if terminationEpoch == nil {
		if observer, ok := h.deps.Terminator.(interface {
			ObserveTermination(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, bool, error)
		}); ok {
			observed, pending, err := observer.ObserveTermination(ctx, dataSet.DataSetID.SDK())
			if err != nil {
				if ctx.Err() != nil || !errors.Is(err, synapse.ErrTerminationObservationUnavailable) {
					if httpErr, ok := errors.AsType[*pdp.HTTPError](err); ok && httpErr.RetryAfter > 0 {
						return taskengine.RetryInMode(synapse.SummarizedError(err), "termination_observation_failed",
							model.TaskResumeModeRecover, httpErr.RetryAfter, nil)
					}
					return retryTask(err, "termination_observation_failed")
				}
				h.deps.Logger.Warn("storage service termination status unavailable",
					"task_id", execution.ID(), "storage_data_set_id", dataSet.ID,
					"error", synapse.ErrorSummary(err))
			}
			if observed != nil {
				return taskengine.Wait(model.TaskResumeModeRecover, 0, "termination_epoch", "Recording storage service retirement", retirementEvidenceSettlement(abandoned, row.ID, observed.EndEpoch, observed.TxHash))
			}
			if pending {
				return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage service retirement", nil)
			}
		}
		if hasCheckpoint && !mayTerminate {
			// A request may already have been sent. Execute reads the chain before
			// sending another one, so resuming there cannot end the service twice;
			// the delay gives the earlier request time to land.
			wait := max(time.Until(checkpoint.AttemptedAt.Add(unobservedOutcomeDelay(checkpoint.Sends))), 0)
			return taskengine.Wait(model.TaskResumeModeExecute, wait, "provider_confirmation", "Checking storage service retirement", nil)
		}
		if !mayTerminate {
			return taskengine.Wait(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage service is ready to retire", nil)
		}
		if h.deps.Terminator == nil {
			return taskengine.Fail(errors.New("storage service terminator is unavailable"), "dependency_unavailable", nil)
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
		checkpoint = retirementCheckpoint{
			AttemptedAt: time.Now().UTC(), Sends: checkpoint.Sends + 1, Identity: &identity,
		}
		var terminationEpochValue int64
		var txHash string
		attempted, err := execution.WithCheckpointedEffect(ctx, fmt.Sprintf("retire:%d", dataSet.ID), checkpoint, nil, func(ctx context.Context) error {
			result, terminateErr := h.deps.Terminator.TerminateService(ctx, dataSet.DataSetID.SDK())
			if result != nil {
				terminationEpochValue = result.EndEpoch
				txHash = result.TxHash
			}
			return terminateErr
		})
		if err != nil && !attempted {
			return retryTask(err, "termination_not_started")
		}
		if synapse.IsTerminationBlocked(err) {
			// Settling payment debt is the operator's decision; it is never
			// retried automatically.
			return stopRetirement(abandoned, row.ID, err, "termination_blocked")
		}
		if errors.Is(err, synapse.ErrServicePaidByAnother) {
			return stopRetirement(abandoned, row.ID, err, "termination_payer_mismatch")
		}
		if err != nil {
			// The outcome is unknown, or the provider is still publishing it. The
			// next request reads the chain first and only goes out while the
			// service is still running there.
			if ctx.Err() == nil {
				h.deps.Logger.Warn("storage service termination request failed",
					"task_id", execution.ID(), "storage_data_set_id", dataSet.ID, "sends", checkpoint.Sends,
					"error", synapse.ErrorSummary(err))
			}
			return taskengine.RetryInMode(synapse.SummarizedError(err), "provider_confirmation", model.TaskResumeModeExecute, unobservedOutcomeDelay(checkpoint.Sends), nil)
		}
		if terminationEpochValue < 0 {
			return stopRetirement(abandoned, row.ID,
				errors.New("storage service termination returned an invalid epoch"), "termination_outcome_unknown")
		}
		checkpoint.TerminationEpoch = &terminationEpochValue
		checkpoint.TransactionHash = txHash
		settlement := retirementEvidenceSettlement(abandoned, row.ID, terminationEpochValue, txHash)
		if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
			return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "termination_epoch", "Recording storage service retirement", settlement)
		}
		return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "termination_epoch", "Waiting for storage service retirement", settlement)
	}
	if h.deps.Epochs == nil {
		return taskengine.Fail(errors.New("chain epoch reader is unavailable"), "dependency_unavailable", nil)
	}
	observedEpoch, err := h.deps.Epochs.CurrentEpoch(ctx)
	if err != nil {
		return retryTask(err, "termination_epoch_observation_failed")
	}
	if observedEpoch < *terminationEpoch {
		return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "termination_epoch", "Waiting for storage service retirement", nil)
	}
	return taskengine.Complete("Storage service retired", func(ctx context.Context, repos *repository.Repositories) error {
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

func retirementEvidenceSettlement(abandoned bool, replacementID, epoch int64, transactionHash string) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		evidence := repository.RecordTerminationEpochInput{
			ReplacementID: replacementID,
			TxHash:        transactionHash,
			Epoch:         epoch,
		}
		if abandoned {
			return repos.Replacements.RecordAbandonedTerminationEpoch(ctx, evidence)
		}
		return repos.Replacements.RecordTerminationEpoch(ctx, evidence)
	}
}

func (h *RetireHandler) retryRetirement(_ taskengine.Execution, _ int64, err error, reason string) taskengine.Result {
	return retryTask(err, reason)
}

func stopRetirement(abandoned bool, replacementID int64, err error, reason string) taskengine.Result {
	if abandoned {
		return taskengine.Fail(err, reason, nil)
	}
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Replacements.MarkCleanupAttention(ctx, replacementID, err.Error())
	})
}

func containsString(values []string, wanted string) bool {
	return slices.Contains(values, wanted)
}
