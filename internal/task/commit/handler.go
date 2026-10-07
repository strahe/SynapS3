package commit

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
)

const (
	commitQueueMessage           = "Waiting to register storage"
	commitCollectionPollInterval = 30 * time.Second
)

// commitCheckpoint records the latest send of a commit request. The request
// row holds the send history; the checkpoint only fences the send itself.
type commitCheckpoint struct {
	RequestID     string    `json:"request_id"`
	Sends         int       `json:"sends"`
	SentAt        time.Time `json:"sent_at"`
	TransactionID string    `json:"transaction_id,omitempty"`
}

func (h *Handler) commitHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageCommit, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		Codec: taskengine.StrictJSONCodec(func(input *storagepipeline.CommitRequestInput) error {
			return storagepipeline.ValidateCommitRequestInput(*input)
		}),
		// A request is given up only when the chain proves it can never land,
		// so passing failures back off without ever exhausting a budget.
		RetryLimit: nil, AllowRetry: true,
		// Retry runs in recover mode, which checks a stopped registration on
		// chain again and sends nothing until the chain shows its request
		// unused. A request stops under the first attention code it was
		// flagged with, so every known code allows it.
		CanManualRetry: func(task *model.Task) bool {
			return task != nil && task.FailureReason != nil &&
				(taskengine.RecoverableEngineFailure(*task.FailureReason) || storagecommit.AttentionCode(*task.FailureReason).Valid())
		},
		Subject: func(canonical json.RawMessage) (taskengine.Subject, error) {
			var input storagepipeline.CommitRequestInput
			if err := json.Unmarshal(canonical, &input); err != nil {
				return taskengine.Subject{}, fmt.Errorf("decoding task subject: %w", err)
			}
			if input.RequestID == "" {
				return taskengine.Subject{}, errors.New("task subject request ID is required")
			}
			return taskengine.Subject{Type: model.TaskSubjectStorageCommitRequest, Key: input.RequestID}, nil
		},
	}
	return taskengine.NewFuncHandler(definition, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.runCommit(ctx, execution, true)
	}, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.runCommit(ctx, execution, false)
	},
	)
}

// commitRun is one claim of a request's task.
type commitRun struct {
	execution taskengine.Execution
	request   *storagecommit.Request
	binding   *model.StorageDataSet
	advancer  storagecommit.Advancer
	mayExec   bool
}

func (r commitRun) taskID() int64 { return r.execution.ID() }

func (h *Handler) runCommit(ctx context.Context, execution taskengine.Execution, mayExecute bool) (result taskengine.Result) {
	input, err := taskengine.DecodeInput[storagepipeline.CommitRequestInput](execution)
	if err != nil {
		return decodeFailure(string(execution.Type()), err)
	}
	request, err := h.deps.Repositories.Contents.GetCommitRequest(ctx, input.RequestID)
	if errors.Is(err, repository.ErrNotFound) {
		return taskengine.Cancel("Storage registration no longer exists", nil)
	}
	if err != nil {
		return retryTask(err, "commit_request_load_failed")
	}
	if request.TaskID != nil && *request.TaskID == execution.ID() && request.FirstSentAt != nil {
		defer func() { result = result.WithWorkStartedAt(*request.FirstSentAt) }()
	}
	switch {
	case request.Status == storagecommit.RequestStatusConfirmed:
		return taskengine.Complete("Storage registered", nil)
	case request.Status == storagecommit.RequestStatusAbandoned:
		return taskengine.Complete("Storage registration was given up", nil)
	case request.TaskID == nil || *request.TaskID != execution.ID():
		return taskengine.Cancel("Storage registration is handled by another task", nil)
	}
	binding, err := h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, request.StorageDataSetID)
	if err != nil || binding == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return retryTask(err, "dataset_load_failed")
	}
	run := commitRun{
		execution: execution, request: request, binding: binding, mayExec: mayExecute,
		advancer: storagecommit.Advancer{Nonces: h.deps.CommitNonces},
	}
	//exhaustive:enforce
	switch request.Status {
	case storagecommit.RequestStatusCollecting:
		return h.runCollectingCommit(ctx, run)
	case storagecommit.RequestStatusReady:
		return h.runReadyCommit(ctx, run)
	case storagecommit.RequestStatusSubmitted:
		return h.runSubmittedCommit(ctx, run)
	case storagecommit.RequestStatusConfirmed, storagecommit.RequestStatusAbandoned:
		return taskengine.Complete("Storage registration is settled", nil)
	default:
		return taskengine.Fail(fmt.Errorf("storage registration has unknown status %q", request.Status), "commit_status_unknown", nil)
	}
}

// runCollectingCommit seals the next batch only when the data set can submit it.
func (h *Handler) runCollectingCommit(ctx context.Context, run commitRun) taskengine.Result {
	members, err := h.deps.Repositories.Contents.ListCommitRequestMembers(ctx, run.request.RequestID)
	if err != nil {
		return retryTask(err, "commit_members_load_failed")
	}
	if len(members) == 0 {
		return taskengine.Complete("No stored data is waiting to be registered", h.abandonCommitSettlement(run, "no storage copies are left to register"))
	}
	maxPieces := h.commitMaxPieces(run.binding)
	now := time.Now()
	oldest := now
	if members[0].CommitReadyAt != nil {
		oldest = *members[0].CommitReadyAt
	}
	input := storagecommit.SealInput{
		Members: len(members), OldestJoinedAt: oldest, Now: now,
		MaxPieces: maxPieces, MaxWait: h.deps.CommitMaxWait,
		Draining:        run.binding.Status == model.StorageDataSetStatusDraining,
		ManualRequested: run.request.SealRequestedAt != nil,
	}
	seal, wait := storagecommit.ShouldSeal(input)
	waitReason, message := "collecting", "Waiting for more stored data to register together"
	if !seal {
		input.CachePressure, err = h.commitCachePressure(ctx, run.request.RequestID)
		if err != nil {
			return retryTask(err, "cache_pressure_load_failed")
		}
		seal, wait = storagecommit.ShouldSeal(input)
	}
	if seal {
		queue, err := h.deps.Repositories.Contents.CommitQueueState(ctx, run.request.StorageDataSetID, now)
		if err != nil {
			return retryTask(err, "commit_queue_load_failed")
		}
		seal = queue.Submitted < storagecommit.MaxSubmittedRequestsPerDataSet && queue.ReadyHead == ""
		if !seal {
			waitReason, message = storagecommit.CommitQueueWaitReason, "Collecting more data; waiting for earlier batches"
			wait = input.MaxWait - now.Sub(oldest)
		}
		h.deps.Logger.DebugContext(ctx, "commit batch scheduling",
			"request_id", run.request.RequestID, "storage_data_set_id", run.request.StorageDataSetID,
			"members", len(members), "selected_members", min(len(members), maxPieces),
			"cache_pressure", input.CachePressure, "submitted_requests", queue.Submitted,
			"ready_head", queue.ReadyHead, "seal", seal)
	}
	if !seal {
		if wait <= 0 || wait > commitCollectionPollInterval {
			wait = commitCollectionPollInterval
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, wait, waitReason, message, nil)
	}
	if !run.mayExec {
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage registration is ready", nil)
	}

	target, err := h.deps.Resolver.OpenReadyDataSet(ctx, run.binding)
	if err != nil {
		return h.commitTargetUnavailable(err)
	}
	signed := members[:min(len(members), maxPieces)]
	for {
		extraHex, sealErr := h.signCommitMembers(ctx, target, signed)
		if sealErr == nil {
			err = h.sealCommit(ctx, run, signed, extraHex)
			break
		}
		if !errors.Is(sealErr, storage.ErrInvalidArgument) || len(signed) == 1 {
			return retryTask(sealErr, "commit_sign_failed")
		}
		// A request that does not fit one add-pieces message is signed again
		// with fewer pieces; the rest join the next request.
		signed = signed[:len(signed)/2]
	}
	if errors.Is(err, repository.ErrConflict) {
		return taskengine.Suspend(model.TaskResumeModeRecover, 0, "collecting", "Stored data waiting to register changed", nil)
	}
	if err != nil {
		return retryTask(err, "commit_seal_failed")
	}
	run.request, err = h.deps.Repositories.Contents.GetCommitRequest(ctx, run.request.RequestID)
	if err != nil {
		return retryTask(err, "commit_request_load_failed")
	}
	return h.runReadyCommit(ctx, run)
}

func (h *Handler) signCommitMembers(ctx context.Context, target synapse.DataSetTarget, members []repository.CommitRequestMember) (string, error) {
	pieces := make([]storage.PieceInput, len(members))
	addPieces := make([]pdp.AddPieceInput, len(members))
	for i, member := range members {
		pieceCID, err := cid.Parse(member.PieceCID)
		if err != nil {
			return "", fmt.Errorf("%w: storage copy %d has no valid piece identity", storage.ErrInvalidArgument, member.ID)
		}
		pieces[i] = storage.PieceInput{PieceCID: pieceCID}
		addPieces[i] = pdp.AddPieceInput{PieceCID: pieceCID}
	}
	extraData, err := target.PresignForCommit(ctx, pieces)
	if err != nil {
		return "", err
	}
	if _, err := storagecommit.ExtraDataNonce(extraData); err != nil {
		return "", fmt.Errorf("validating signed storage registration: %w", err)
	}
	size, err := pdp.EstimateAddPiecesMessageSize(addPieces, extraData)
	if err != nil {
		return "", err
	}
	if size > pdp.MaxAddPiecesMessageSize {
		return "", fmt.Errorf("%w: storage registration of %d pieces needs %d bytes", storage.ErrInvalidArgument, len(members), size)
	}
	return hex.EncodeToString(extraData), nil
}

func (h *Handler) sealCommit(ctx context.Context, run commitRun, members []repository.CommitRequestMember, extraHex string) error {
	sealMembers := make([]repository.SealMember, len(members))
	for i, member := range members {
		sealMembers[i] = repository.SealMember{CopyID: member.ID, ContentID: member.ContentID, PieceCID: member.PieceCID}
	}
	return run.execution.WriteCheckpointWith(ctx, commitCheckpoint{RequestID: run.request.RequestID}, func(ctx context.Context, repos *repository.Repositories) error {
		spilled, err := repos.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{
			RequestID: run.request.RequestID, TaskID: run.taskID(), Members: sealMembers, ExtraDataHex: extraHex,
		})
		if err != nil {
			return err
		}
		if len(spilled) == 0 {
			return nil
		}
		requestID, err := storagecommit.NewRequestID()
		if err != nil {
			return err
		}
		taskID, err := h.enqueueCommitTask(ctx, repos, requestID, time.Time{})
		if err != nil {
			return err
		}
		return repos.Contents.CreateCollectingCommitRequest(ctx, repository.CreateCommitRequestInput{
			RequestID: requestID, TaskID: taskID, StorageDataSetID: run.request.StorageDataSetID, CopyIDs: spilled,
		})
	})
}

// runReadyCommit sends a signed request once every member is transferred and
// its data set has room.
func (h *Handler) runReadyCommit(ctx context.Context, run commitRun) taskengine.Result {
	request := run.request
	transferring, stalled, err := h.commitMemberProgress(ctx, run)
	if err != nil {
		return retryTask(err, "commit_members_load_failed")
	}
	if len(stalled) > 0 {
		return h.giveUpUnacceptedCommit(ctx, run)
	}
	if transferring {
		return waitForCommitMembers(nil)
	}
	if request.RetryAt != nil && request.RetryAt.After(time.Now()) {
		return taskengine.Suspend(model.TaskResumeModeExecute, time.Until(*request.RetryAt), storagecommit.ProviderRejectedWaitReason,
			"Storage provider refused the registration; trying again later", nil)
	}
	// Requests leave in order and a data set holds few in flight. Settling one
	// wakes the next, so a request that is not next waits without touching the
	// chain or the provider.
	queue, err := h.deps.Repositories.Contents.CommitQueueState(ctx, request.StorageDataSetID, time.Now())
	if err != nil {
		return retryTask(err, "commit_queue_load_failed")
	}
	if queue.Submitted >= storagecommit.MaxSubmittedRequestsPerDataSet || queue.ReadyHead != request.RequestID {
		return taskengine.ResourceWait(commitQueueMessage)
	}
	target, commit, result, ok := h.loadCommit(ctx, run)
	if !ok {
		return result
	}
	if !run.mayExec {
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage registration is ready", nil)
	}
	prepared := run.advancer.Prepare(ctx, commit)
	//exhaustive:enforce
	switch prepared.Outcome {
	case storagecommit.PrepareRetry:
		return taskengine.SuspendWithError(model.TaskResumeModeExecute, prepared.RetryAfter, "dataset",
			"Checking the storage service before registering", synapse.SummarizedError(prepared.Cause), nil)
	case storagecommit.PrepareConfirmed:
		return h.confirmCommit(run, target, commit, storagecommit.Confirmation{FirstPieceID: prepared.Proof.FirstPieceID})
	case storagecommit.PrepareConflict:
		return h.resignCommit(run, "its nonce was used by another request; its pieces are signed again")
	case storagecommit.PrepareAbandon:
		return h.abandonCommit(run, prepared.Cause)
	case storagecommit.PrepareSend:
	default:
		return taskengine.Fail(fmt.Errorf("unknown storage registration check %d", prepared.Outcome), "commit_prepare_invalid", nil)
	}

	var sent storagecommit.SendResult
	now := time.Now().UTC()
	attempted, err := run.execution.WithCheckpointedEffect(ctx, taskengine.ResourceProviderMutation,
		commitCheckpoint{RequestID: request.RequestID, Sends: 1, SentAt: now},
		func(ctx context.Context, repos *repository.Repositories) error {
			if err := repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{
				RequestID: request.RequestID, TaskID: run.taskID(), Now: now,
			}); err != nil {
				return err
			}
			if request.FirstSentAt != nil {
				return repos.Tasks.MarkWorkStarted(ctx, run.taskID(), run.execution.ClaimGeneration(), *request.FirstSentAt)
			}
			return nil
		},
		func(ctx context.Context) error {
			sent = run.advancer.Send(ctx, commit, h.recordCommitEvidence(ctx, run, 1))
			return nil
		})
	if !attempted {
		switch {
		case errors.Is(err, taskengine.ErrResourceBusy):
			return taskengine.ResourceWait("Waiting for other storage operations to finish")
		case errors.Is(err, storagecommit.ErrNotEligible):
			return taskengine.ResourceWait(commitQueueMessage)
		default:
			return retryTask(err, "commit_submit_not_started")
		}
	}
	return h.settleFirstSend(ctx, run, target, commit, sent)
}

// settleFirstSend records what the period's first send left behind.
func (h *Handler) settleFirstSend(
	ctx context.Context,
	run commitRun,
	target synapse.DataSetTarget,
	commit storagecommit.Commit,
	sent storagecommit.SendResult,
) taskengine.Result {
	request := run.request
	//exhaustive:enforce
	switch sent.Kind {
	case storagecommit.SendAccepted:
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation",
			"Waiting for storage registration", h.submissionSettlement(run, 1, sent.Submission))
	case storagecommit.SendRefused, storagecommit.SendNotSent:
		refused := sent.Kind == storagecommit.SendRefused
		refusals := request.Refusals
		if refused {
			refusals++
		}
		retryAfter := storagecommit.RefusedRetryDelay(max(refusals, 1))
		if !refused {
			retryAfter = storagePollInterval
		}
		message := synapse.ErrorSummary(sent.Err)
		dropped := h.droppedCommitMembers(ctx, target, commit)
		h.deps.Logger.Warn("storage provider refused the registration",
			"task_id", run.taskID(), "commit_request_id", request.RequestID, "storage_data_set_id", request.StorageDataSetID,
			"pieces", request.PieceCount, "retry_after", retryAfter, "error", message)
		return taskengine.SuspendWithError(model.TaskResumeModeExecute, retryAfter, storagecommit.ProviderRejectedWaitReason,
			"Storage provider refused the registration; trying again later", synapse.SummarizedError(sent.Err),
			func(ctx context.Context, repos *repository.Repositories) error {
				if err := repos.Contents.ReturnCommitRequestToReady(ctx, repository.ReturnCommitRequestInput{
					RequestID: request.RequestID, TaskID: run.taskID(), Refused: refused,
					SubmitError: message, RetryAt: time.Now().Add(retryAfter),
				}); err != nil {
					return err
				}
				return h.returnDroppedMembers(ctx, repos, run, dropped)
			})
	case storagecommit.SendUnknown:
		return h.unknownSend(run, 1, sent)
	default:
		return taskengine.Fail(fmt.Errorf("unknown storage registration send result %d", sent.Kind), "commit_send_invalid", nil)
	}
}

// runSubmittedCommit resolves a request that may be on chain.
func (h *Handler) runSubmittedCommit(ctx context.Context, run commitRun) taskengine.Result {
	request := run.request
	if request.AttentionCode != nil {
		if _, err := storagecommit.ParseAttentionCode(*request.AttentionCode); err != nil {
			// A code this version does not know may mean a later version found
			// something it must not resend past; recovery stops until one that
			// understands it, or an operator, resolves the request.
			return taskengine.Fail(fmt.Errorf("storage registration requires attention: %w", err), "commit_attention_unknown", nil)
		}
	}
	pieces, result, ok := h.loadCommitPieces(ctx, run)
	if !ok {
		return result
	}
	target, err := h.deps.Resolver.OpenReadyDataSet(ctx, run.binding)
	if err != nil {
		// A send may still land while the data set cannot be reached; past the
		// attention threshold the request is flagged and recovery keeps trying.
		var settlement taskengine.Settlement
		if request.AttentionAt == nil && request.SubmittedAt != nil && time.Since(*request.SubmittedAt) >= storagecommit.DefaultAttentionAfter {
			settlement = h.attentionSettlement(run, storagecommit.AttentionDataSetUnavailable)
		}
		return taskengine.SuspendWithError(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation",
			"Checking storage registration", synapse.SummarizedError(err), settlement)
	}
	commit := storagecommit.Commit{Request: *request, Pieces: pieces, Target: target}
	observation, err := run.advancer.Observe(ctx, commit)
	if err != nil {
		return taskengine.SuspendWithError(model.TaskResumeModeRecover, storagecommit.ChainRetryDelay, "provider_confirmation",
			"Checking storage registration", synapse.SummarizedError(err), nil)
	}
	noted := h.observationSettlement(run, observation)
	transferring, stalled := false, []int64(nil)
	if observation.Kind == storagecommit.ObservePending || observation.Kind == storagecommit.ObserveResendDue {
		if transferring, stalled, err = h.commitMemberProgress(ctx, run); err != nil {
			return retryTask(err, "commit_members_load_failed")
		}
	}
	//exhaustive:enforce
	switch observation.Kind {
	case storagecommit.ObservePending:
		delay := observation.RetryAfter
		if delay <= 0 {
			delay = storagePollInterval
		}
		settlement := h.retransferStalledMembers(run, stalled, noted)
		if observation.Cause != nil {
			return taskengine.SuspendWithError(model.TaskResumeModeRecover, delay, "provider_confirmation",
				"Waiting for storage registration", synapse.SummarizedError(observation.Cause), settlement)
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, delay, "provider_confirmation", "Waiting for storage registration", settlement)
	case storagecommit.ObserveConfirmed:
		return h.confirmCommit(run, target, commit, *observation.Confirmation)
	case storagecommit.ObserveStop:
		return taskengine.Fail(commitAttentionError(request), string(observation.Attention), noted)
	case storagecommit.ObserveAbandon:
		return h.abandonCommit(run, observation.Cause)
	case storagecommit.ObserveResendDue:
		// A member the provider dropped cannot be added until it is
		// transferred again; sending before that is refused for nothing.
		if transferring || len(stalled) > 0 {
			return waitForCommitMembers(h.retransferStalledMembers(run, stalled, noted))
		}
		if !run.mayExec {
			return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Sending storage registration again", noted)
		}
	default:
		return taskengine.Fail(fmt.Errorf("unknown storage registration observation %d", observation.Kind), "commit_observation_invalid", nil)
	}

	sends := request.Sends + 1
	now := time.Now().UTC()
	var sent storagecommit.SendResult
	attempted, err := run.execution.WithCheckpointedEffect(ctx, taskengine.ResourceProviderMutation,
		commitCheckpoint{RequestID: request.RequestID, Sends: sends, SentAt: now},
		func(ctx context.Context, repos *repository.Repositories) error {
			if noted != nil {
				if err := noted(ctx, repos); err != nil {
					return err
				}
			}
			if err := repos.Contents.RecordCommitResend(ctx, repository.CommitSendInput{
				RequestID: request.RequestID, TaskID: run.taskID(), Sends: sends, Now: now,
			}); err != nil {
				return err
			}
			if request.FirstSentAt != nil {
				return repos.Tasks.MarkWorkStarted(ctx, run.taskID(), run.execution.ClaimGeneration(), *request.FirstSentAt)
			}
			return nil
		},
		func(ctx context.Context) error {
			sent = run.advancer.Send(ctx, commit, h.recordCommitEvidence(ctx, run, sends))
			return nil
		})
	if !attempted {
		if errors.Is(err, taskengine.ErrResourceBusy) {
			return taskengine.ResourceWait("Waiting for other storage operations to finish")
		}
		return taskengine.SuspendWithError(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation",
			"Checking storage registration", synapse.SummarizedError(err), nil)
	}
	//exhaustive:enforce
	switch sent.Kind {
	case storagecommit.SendAccepted:
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation",
			"Waiting for storage registration", h.submissionSettlement(run, sends, sent.Submission))
	case storagecommit.SendRefused, storagecommit.SendNotSent:
		// Refusing a resend proves only that this send produced nothing; an
		// earlier one can still land. A provider that dropped a piece cannot
		// add it, so that member is transferred again under the same request.
		dropped := h.droppedCommitMembers(ctx, target, commit)
		message := synapse.ErrorSummary(sent.Err)
		h.deps.Logger.Warn("storage provider refused to resend the registration",
			"task_id", run.taskID(), "commit_request_id", request.RequestID, "dropped_pieces", len(dropped), "error", message)
		return taskengine.SuspendWithError(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation",
			"Checking storage registration", synapse.SummarizedError(sent.Err),
			func(ctx context.Context, repos *repository.Repositories) error {
				if err := repos.Contents.RecordCommitSubmitFailure(ctx, repository.CommitSubmitFailureInput{
					CommitSendInput: repository.CommitSendInput{RequestID: request.RequestID, TaskID: run.taskID(), Sends: sends},
					Message:         message,
				}); err != nil {
					return err
				}
				return h.returnDroppedMembers(ctx, repos, run, dropped)
			})
	case storagecommit.SendUnknown:
		return h.unknownSend(run, sends, sent)
	default:
		return taskengine.Fail(fmt.Errorf("unknown storage registration send result %d", sent.Kind), "commit_send_invalid", nil)
	}
}

// unknownSend waits on a send whose outcome cannot be told. Recovery reads the
// nonce before anything is sent again.
func (h *Handler) unknownSend(run commitRun, sends int, sent storagecommit.SendResult) taskengine.Result {
	var settlement taskengine.Settlement
	if sent.Submission != nil {
		settlement = h.submissionSettlement(run, sends, sent.Submission)
	} else if sent.Err != nil {
		message := synapse.ErrorSummary(sent.Err)
		settlement = func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.RecordCommitSubmitFailure(ctx, repository.CommitSubmitFailureInput{
				CommitSendInput: repository.CommitSendInput{RequestID: run.request.RequestID, TaskID: run.taskID(), Sends: sends},
				Message:         message,
			})
		}
	}
	if sent.Err != nil {
		h.deps.Logger.Warn("storage registration submission failed",
			"task_id", run.taskID(), "commit_request_id", run.request.RequestID,
			"storage_data_set_id", run.request.StorageDataSetID, "error", synapse.ErrorSummary(sent.Err))
		return taskengine.SuspendWithError(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation",
			"Checking storage registration", synapse.SummarizedError(sent.Err), settlement)
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage registration", settlement)
}

// recordCommitEvidence keeps the provider's receipt as soon as the SDK
// announces it, under the task's claim.
func (h *Handler) recordCommitEvidence(ctx context.Context, run commitRun, sends int) func(storage.CommitSubmission) {
	return func(submission storage.CommitSubmission) {
		if submission.TransactionID == "" || submission.StatusURL == "" {
			return
		}
		evidenceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		err := run.execution.WriteCheckpointWith(evidenceCtx, commitCheckpoint{
			RequestID: run.request.RequestID, Sends: sends, SentAt: time.Now().UTC(), TransactionID: submission.TransactionID,
		}, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.RecordCommitSubmission(ctx, repository.CommitSubmissionInput{
				CommitSendInput: repository.CommitSendInput{RequestID: run.request.RequestID, TaskID: run.taskID(), Sends: sends},
				TransactionID:   submission.TransactionID, StatusURL: submission.StatusURL,
			})
		})
		if err != nil {
			h.deps.Logger.Warn("storage registration receipt could not be recorded",
				"task_id", run.taskID(), "commit_request_id", run.request.RequestID, "error", synapse.ErrorSummary(err))
		}
	}
}

func (h *Handler) submissionSettlement(run commitRun, sends int, submission *storage.CommitSubmission) taskengine.Settlement {
	if submission == nil || submission.TransactionID == "" || submission.StatusURL == "" {
		return nil
	}
	transactionID, statusURL := submission.TransactionID, submission.StatusURL
	return func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Contents.RecordCommitSubmission(ctx, repository.CommitSubmissionInput{
			CommitSendInput: repository.CommitSendInput{RequestID: run.request.RequestID, TaskID: run.taskID(), Sends: sends},
			TransactionID:   transactionID, StatusURL: statusURL,
		})
	}
}

// observationSettlement records the evidence an observation retired and the
// attention it raised.
func (h *Handler) observationSettlement(run commitRun, observation storagecommit.Observation) taskengine.Settlement {
	if !observation.DropEvidence && observation.Attention == "" {
		return nil
	}
	return func(ctx context.Context, repos *repository.Repositories) error {
		if observation.DropEvidence && run.request.TransactionID != nil {
			if err := repos.Contents.DropCommitEvidence(ctx, run.request.RequestID, run.taskID(),
				"the provider's receipt did not match this registration", time.Now()); err != nil {
				return err
			}
		}
		if observation.Attention != "" {
			return repos.Contents.MarkCommitRequestAttention(ctx, run.request.RequestID, run.taskID(), observation.Attention, time.Now())
		}
		return nil
	}
}

func (h *Handler) attentionSettlement(run commitRun, code storagecommit.AttentionCode) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Contents.MarkCommitRequestAttention(ctx, run.request.RequestID, run.taskID(), code, time.Now())
	}
}

// confirmCommit settles a request the chain proved and finishes every member's
// content.
func (h *Handler) confirmCommit(
	run commitRun,
	target synapse.DataSetTarget,
	commit storagecommit.Commit,
	confirmation storagecommit.Confirmation,
) taskengine.Result {
	retrievalURLs := make([]string, len(commit.Pieces))
	for i, pieceCID := range commit.Pieces {
		retrievalURLs[i] = target.PieceURL(pieceCID)
	}
	firstPieceID := idtypes.OnChainIDFromSDK(confirmation.FirstPieceID)
	return taskengine.Complete("Stored data registered", func(ctx context.Context, repos *repository.Repositories) error {
		members, err := repos.Contents.ConfirmCommitRequest(ctx, repository.ConfirmCommitRequestInput{
			RequestID: run.request.RequestID, TaskID: run.taskID(),
			ConfirmedTransactionID: confirmation.ConfirmedTransactionID,
			FirstPieceID:           firstPieceID, RetrievalURLs: retrievalURLs,
		})
		if err != nil {
			return err
		}
		for i := range members {
			if err := h.finishCommittedContent(ctx, repos, &members[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// finishCommittedContent runs what follows one member's commit: the content is
// readable now, independently of which versions point at it.
func (h *Handler) finishCommittedContent(ctx context.Context, repos *repository.Repositories, member *model.StorageCopy) error {
	if err := h.deps.Messenger.Notify(ctx, repos, storagepipeline.ContentCommitted{
		ContentID: member.ContentID, BucketID: member.BucketID,
		IngressCommitted: member.TransferMethod == model.StorageCopyTransferMethodIngress,
	}); err != nil {
		return err
	}
	if _, err := repos.Contents.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		ContentID: member.ContentID, BucketID: member.BucketID,
	}); err != nil {
		return err
	}
	_, refs, err := repos.Contents.FinalizeUploadIfTargetCopiesMet(ctx, repository.NewFinalizeUploadInput(member.ContentID))
	if err != nil {
		return err
	}
	ids := make([]int64, 0, len(refs))
	seen := make(map[int64]struct{}, len(refs))
	for _, ref := range refs {
		if ref.ContentID == nil {
			continue
		}
		if _, exists := seen[*ref.ContentID]; exists {
			continue
		}
		seen[*ref.ContentID] = struct{}{}
		ids = append(ids, *ref.ContentID)
	}
	if len(ids) == 0 {
		return nil
	}
	return h.deps.Messenger.Notify(ctx, repos, storagepipeline.ContentDurabilityReached{ContentIDs: ids})
}

// commitMemberRetransferDelay is how long a submitted request waits before
// transferring again a member whose transfer gave up.
const commitMemberRetransferDelay = 5 * time.Minute

// commitMemberProgress reports whether a member of a sealed request is still
// being transferred, and which members' transfers gave up. Members transfer
// again after the provider drops their pieces.
func (h *Handler) commitMemberProgress(ctx context.Context, run commitRun) (transferring bool, stalled []int64, err error) {
	members, err := h.deps.Repositories.Contents.ListCommitRequestMembers(ctx, run.request.RequestID)
	if err != nil {
		return false, nil, err
	}
	for _, member := range members {
		switch {
		case member.Status == model.StorageCopyStatusCommitting:
		case member.Status == model.StorageCopyStatusPending && member.ActiveTaskID == nil:
			stalled = append(stalled, member.ID)
		default:
			transferring = true
		}
	}
	return transferring, stalled, nil
}

// waitForCommitMembers holds a request, which is only ever sent whole, until
// every member is transferred. settlement records what the caller observed
// meanwhile.
func waitForCommitMembers(settlement taskengine.Settlement) taskengine.Result {
	return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "commit_members",
		"Waiting for stored data to finish transferring", settlement)
}

// retransferStalledMembers schedules another transfer for each member whose
// transfer gave up. A submitted request may still land, so its members stay
// with it until the chain decides; then runs first.
func (h *Handler) retransferStalledMembers(run commitRun, stalled []int64, then taskengine.Settlement) taskengine.Settlement {
	if len(stalled) == 0 {
		return then
	}
	return func(ctx context.Context, repos *repository.Repositories) error {
		if then != nil {
			if err := then(ctx, repos); err != nil {
				return err
			}
		}
		at := time.Now().Add(commitMemberRetransferDelay)
		for _, copyID := range stalled {
			copyRow, err := repos.Contents.GetUploadCopyByID(ctx, copyID)
			if err != nil {
				return err
			}
			if copyRow == nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID != nil ||
				copyRow.CommitRequestID == nil || *copyRow.CommitRequestID != run.request.RequestID {
				continue
			}
			h.deps.Logger.Warn("storage registration member could not be transferred again; trying later",
				"task_id", run.taskID(), "commit_request_id", run.request.RequestID, "copy_id", copyID, "retry_after", commitMemberRetransferDelay)
			if err := h.deps.Messenger.Handover(ctx, repos, storagepipeline.StartCopyTransfer{CopyID: copyID, AvailableAt: at}); err != nil {
				return err
			}
		}
		return nil
	}
}

// giveUpUnacceptedCommit settles a request the provider never accepted once a
// member can no longer be transferred. The chain decides: pieces already added
// confirm it. Otherwise no send of it can land, so the other members are
// signed again without the member that gave up, which fails as a lone copy
// would.
func (h *Handler) giveUpUnacceptedCommit(ctx context.Context, run commitRun) taskengine.Result {
	target, commit, result, ok := h.loadCommit(ctx, run)
	if !ok {
		return result
	}
	proof, err := run.advancer.Prove(ctx, commit)
	if err != nil {
		return taskengine.SuspendWithError(model.TaskResumeModeRecover, storagecommit.ChainRetryDelay, "provider_confirmation",
			"Checking storage registration", synapse.SummarizedError(err), nil)
	}
	if proof.Outcome == storagecommit.ProofLanded {
		return h.confirmCommit(run, target, commit, storagecommit.Confirmation{FirstPieceID: proof.FirstPieceID})
	}
	return h.resignCommit(run, "a member could not be transferred again; the other pieces are signed again")
}

// resignCommit gives up a request the provider never accepted and that can
// never be sent as signed. Its transferred members are signed again with a new
// nonce; a member whose transfer gave up fails as a lone copy would.
func (h *Handler) resignCommit(run commitRun, reason string) taskengine.Result {
	h.deps.Logger.Warn("storage registration is given up and its pieces signed again",
		"task_id", run.taskID(), "commit_request_id", run.request.RequestID, "storage_data_set_id", run.request.StorageDataSetID,
		"reason", reason)
	return taskengine.Complete("Storage registration is signed again", func(ctx context.Context, repos *repository.Repositories) error {
		members, err := repos.Contents.AbandonCommitRequest(ctx, repository.AbandonCommitRequestInput{
			RequestID: run.request.RequestID, TaskID: run.taskID(), Reason: reason,
		})
		if err != nil {
			return err
		}
		for i := range members {
			member := &members[i]
			switch {
			case member.Status == model.StorageCopyStatusCommitting || member.Status == model.StorageCopyStatusPieceReady:
				if err := h.queueCommit(ctx, repos, member.ID); err != nil {
					return err
				}
			case member.Status == model.StorageCopyStatusPending && member.ActiveTaskID == nil:
				message := reason
				if member.LastError != nil && *member.LastError != "" {
					message = *member.LastError
				}
				if err := h.deps.Messenger.Handover(ctx, repos, storagepipeline.FailCopy{CopyID: member.ID, Message: message}); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// abandonCommit gives up a request its data set can never take and fails its
// members. A member still being transferred settles first.
func (h *Handler) abandonCommit(run commitRun, cause error) taskengine.Result {
	message := "the storage service no longer accepts new data"
	if cause != nil {
		message = synapse.ErrorSummary(cause)
	}
	return taskengine.Complete("Storage service no longer accepts this registration", h.abandonCommitSettlement(run, message))
}

func (h *Handler) abandonCommitSettlement(run commitRun, reason string) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		members, err := repos.Contents.AbandonCommitRequest(ctx, repository.AbandonCommitRequestInput{
			RequestID: run.request.RequestID, TaskID: run.taskID(), Reason: reason,
		})
		if err != nil {
			return err
		}
		for i := range members {
			member := &members[i]
			if member.Status == model.StorageCopyStatusCommitted || member.Status == model.StorageCopyStatusFailed {
				continue
			}
			if member.ActiveTaskID != nil {
				// Its own transfer task is still running and fails it there.
				continue
			}
			if err := h.deps.Messenger.Handover(ctx, repos, storagepipeline.FailCopy{CopyID: member.ID, Message: reason}); err != nil {
				return err
			}
		}
		return nil
	}
}

// droppedCommitMembers returns the members whose pieces the provider positively
// no longer holds, as Curio does with an upload that joins no data set within
// hours.
func (h *Handler) droppedCommitMembers(ctx context.Context, target synapse.DataSetTarget, commit storagecommit.Commit) []int64 {
	if h.deps.ParkedPieces == nil {
		return nil
	}
	members, err := h.deps.Repositories.Contents.ListCommitRequestMembers(ctx, commit.Request.RequestID)
	if err != nil {
		return nil
	}
	var dropped []int64
	for _, member := range members {
		if member.Status != model.StorageCopyStatusCommitting {
			continue
		}
		pieceCID, err := cid.Parse(member.PieceCID)
		if err != nil {
			continue
		}
		state, err := h.deps.ParkedPieces.FindParkedPiece(ctx, target.ServiceURL(), pieceCID)
		if err == nil && state == synapse.ParkedPieceMissing {
			dropped = append(dropped, member.ID)
		}
	}
	return dropped
}

// returnDroppedMembers sends dropped members back to transfer. They keep
// their positions, so the request is sent whole once they are transferred.
func (h *Handler) returnDroppedMembers(ctx context.Context, repos *repository.Repositories, run commitRun, copyIDs []int64) error {
	if len(copyIDs) == 0 {
		return nil
	}
	if err := repos.Contents.ReturnCommitMembersToTransfer(ctx, run.request.RequestID, run.taskID(), copyIDs, time.Now()); err != nil {
		return err
	}
	for _, copyID := range copyIDs {
		if err := h.deps.Messenger.Handover(ctx, repos, storagepipeline.StartCopyTransfer{CopyID: copyID}); err != nil {
			return err
		}
	}
	return nil
}

// loadCommit opens the request's data set and its signed pieces.
func (h *Handler) loadCommit(ctx context.Context, run commitRun) (synapse.DataSetTarget, storagecommit.Commit, taskengine.Result, bool) {
	pieces, result, ok := h.loadCommitPieces(ctx, run)
	if !ok {
		return nil, storagecommit.Commit{}, result, false
	}
	target, err := h.deps.Resolver.OpenReadyDataSet(ctx, run.binding)
	if err != nil {
		return nil, storagecommit.Commit{}, h.commitTargetUnavailable(err), false
	}
	return target, storagecommit.Commit{Request: *run.request, Pieces: pieces, Target: target}, taskengine.Result{}, true
}

// loadCommitPieces returns the request's signed piece CIDs in position order.
func (h *Handler) loadCommitPieces(ctx context.Context, run commitRun) ([]cid.Cid, taskengine.Result, bool) {
	pieces, err := h.deps.Repositories.Contents.ListCommitRequestPieces(ctx, run.request.RequestID)
	if err != nil {
		return nil, retryTask(err, "commit_pieces_load_failed"), false
	}
	if len(pieces) != run.request.PieceCount {
		return nil, taskengine.Fail(errors.New("storage registration pieces are incomplete"), "commit_pieces_invalid", nil), false
	}
	cids := make([]cid.Cid, len(pieces))
	for i, piece := range pieces {
		parsed, err := cid.Parse(piece.PieceCID)
		if err != nil {
			return nil, taskengine.Fail(err, "commit_pieces_invalid", nil), false
		}
		cids[i] = parsed
	}
	return cids, taskengine.Result{}, true
}

func (h *Handler) commitTargetUnavailable(err error) taskengine.Result {
	return taskengine.SuspendWithError(model.TaskResumeModeRecover, storagePollInterval, "provider",
		"Waiting for storage provider", synapse.SummarizedError(err), nil)
}

// commitMaxPieces is how many pieces one request to the data set carries.
func (h *Handler) commitMaxPieces(binding *model.StorageDataSet) int {
	if binding == nil || binding.DataSetID == nil {
		return h.deps.CommitMaxPieces
	}
	return storagecommit.MaxPieces(*binding.DataSetID, h.deps.LegacyPieceStorageIDLimit, h.deps.CommitMaxPieces)
}

// queueCommit hands a transferred copy to registration: a signed member wakes
// its request, and any other transferred copy joins a collecting request of its
// data set, starting one when none exists.
func (h *Handler) queueCommit(ctx context.Context, repos *repository.Repositories, copyID int64) error {
	if h.deps.Scheduler == nil {
		return errors.New("task service is unavailable")
	}
	copyRow, err := repos.Contents.GetUploadCopyByID(ctx, copyID)
	if err != nil {
		return err
	}
	if copyRow == nil {
		return repository.ErrNotFound
	}
	if copyRow.CommitRequestID != nil {
		return repos.Contents.WakeCommitRequestTask(ctx, *copyRow.CommitRequestID)
	}
	if copyRow.Status != model.StorageCopyStatusPieceReady {
		return nil
	}
	binding, err := repos.Contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
	if err != nil {
		return err
	}
	maxPieces := h.commitMaxPieces(binding)
	requestID, members, err := repos.Contents.JoinCollectingCommitRequest(ctx, repository.JoinCommitRequestInput{
		CopyID: copyRow.ID, StorageDataSetID: copyRow.StorageDataSetID,
	})
	if err == nil {
		if members >= maxPieces {
			return repos.Contents.WakeCollectingCommitTask(ctx, requestID)
		}
		if h.deps.CommitSealOnCachePressure && pressureActive(h.deps.Pressure) {
			return repos.Contents.WakeCollectingCommitTask(ctx, requestID)
		}
		return nil
	}
	if !errors.Is(err, repository.ErrNotFound) {
		return err
	}
	requestID, err = storagecommit.NewRequestID()
	if err != nil {
		return err
	}
	taskID, err := h.enqueueCommitTask(ctx, repos, requestID, time.Time{})
	if err != nil {
		return err
	}
	return repos.Contents.CreateCollectingCommitRequest(ctx, repository.CreateCommitRequestInput{
		RequestID: requestID, TaskID: taskID, StorageDataSetID: copyRow.StorageDataSetID, CopyIDs: []int64{copyRow.ID},
	})
}

// enqueueCommitTask creates the task that drives a request, runnable from
// availableAt (now when zero).
func (h *Handler) enqueueCommitTask(ctx context.Context, repos *repository.Repositories, requestID string, availableAt time.Time) (int64, error) {
	if h.deps.Scheduler == nil {
		return 0, errors.New("task service is unavailable")
	}
	taskRow, _, err := h.deps.Scheduler.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type: model.TaskTypeStorageCommit, IdempotencyKey: storagepipeline.CommitKey(requestID),
		Input:       storagepipeline.CommitRequestInput{RequestID: requestID},
		SubjectType: model.TaskSubjectStorageCommitRequest, SubjectKey: requestID,
		AvailableAt: availableAt,
	})
	if err != nil {
		return 0, err
	}
	return taskRow.ID, nil
}

// commitAttentionError names why a registration stopped, including the
// provider's reply when the submission failed there.
func commitAttentionError(request *storagecommit.Request) error {
	if request != nil && request.SubmitError != nil && *request.SubmitError != "" {
		return fmt.Errorf("storage registration requires attention: %s", *request.SubmitError)
	}
	return errors.New("storage registration requires attention")
}
