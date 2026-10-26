package removal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storageremoval"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/task/binding"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type ReceiptChecker interface {
	TransactionReceipt(context.Context, common.Hash) (*ethtypes.Receipt, error)
}

type Dependencies struct {
	Repositories   *repository.Repositories
	Storage        synapse.StorageClient
	Receipts       ReceiptChecker
	RemovalChain   synapse.RemovalChainReader
	Epochs         synapse.ChainEpochReader
	MaxPieces      int
	MaxWait        time.Duration
	ReceiptTimeout time.Duration
	Logger         *slog.Logger
}

type Handler struct {
	*taskengine.FuncHandler
	deps Dependencies
}

type checkpoint struct {
	storagepipeline.ExecutionIdentity
	RequestID   string    `json:"request_id"`
	SendNo      int       `json:"send_no"`
	AttemptedAt time.Time `json:"attempted_at"`
	NextSendAt  time.Time `json:"next_send_at"`
	PieceLookup bool      `json:"piece_lookup,omitempty"`
}

type run struct {
	execution taskengine.Execution
	input     storageremoval.Input
	request   *storageremoval.Request
	members   []model.StorageCleanupCopy
	binding   *model.StorageDataSet
	sends     []storageremoval.Send
	accepted  *storageremoval.Send
	exposed   *storageremoval.Send
	state     synapse.CleanupPiecesState
	preflight synapse.RemovalPreflight
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.StorageRemoval == nil || deps.MaxPieces < 1 || deps.MaxWait < 0 {
		return nil, errors.New("removal handler requires repositories and valid batch limits")
	}
	if deps.ReceiptTimeout <= 0 {
		deps.ReceiptTimeout = 15 * time.Second
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	h := &Handler{deps: deps}
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageRemoval, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		MaxConcurrency: 4, AllowRetry: true,
		Codec: taskengine.StrictJSONCodec(func(input *storageremoval.Input) error { return storageremoval.ValidateInput(*input) }),
		Identity: func(raw json.RawMessage) (taskengine.Identity, error) {
			var input storageremoval.Input
			if err := json.Unmarshal(raw, &input); err != nil {
				return taskengine.Identity{}, err
			}
			return taskengine.Identity{SubjectType: model.TaskSubjectStorageRemovalRequest, SubjectKey: input.RequestID, Generation: new(int64(0))}, nil
		},
		Policy: taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.BackoffPolicy{InitialDelay: time.Minute, Multiplier: 2, MaximumDelay: 30 * time.Minute, Jitter: 0.2}, ResendBackoff: &taskengine.BackoffPolicy{InitialDelay: 5 * time.Minute, Multiplier: 2, MaximumDelay: 30 * time.Minute}, ObservationInterval: 30 * time.Second},
	}
	definition.InspectRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
		var input storageremoval.Input
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return err
		}
		request, err := repos.StorageRemoval.GetRemovalRequest(ctx, input.RequestID)
		if err != nil {
			return err
		}
		if request.TaskID == nil || *request.TaskID != source.ID || request.Status.Terminal() {
			return repository.ErrConflict
		}
		return nil
	}
	definition.BindSuccessor = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
		var input storageremoval.Input
		if err := json.Unmarshal(old.Input, &input); err != nil {
			return err
		}
		return repos.StorageRemoval.TransferRemovalTaskOwner(ctx, input.RequestID, old.ID, next.ID)
	}
	definition.OnEngineFailure = func(row *model.Task, _ string) taskengine.Settlement {
		return func(ctx context.Context, repos *repository.Repositories) error {
			var input storageremoval.Input
			if err := json.Unmarshal(row.Input, &input); err != nil {
				return nil
			}
			request, err := repos.StorageRemoval.GetRemovalRequest(ctx, input.RequestID)
			if err != nil {
				return err
			}
			if request.Status != storageremoval.RequestStatusCollecting {
				return nil
			}
			return repos.StorageRemoval.AbandonRemovalRequest(ctx, repository.RemovalRequestInput{RequestID: input.RequestID, TaskID: row.ID, Message: "Removal batch could not start"})
		}
	}
	h.FuncHandler = taskengine.NewFuncHandler(definition, h.execute, h.execute)
	return h, nil
}

func observe(err error, settlement taskengine.Settlement) taskengine.Result {
	return taskengine.WaitForObservation(model.TaskResumeModeRecover, "remote_removal", "Waiting for remote removal", settlement).WithDiagnostic(err)
}

func (h *Handler) execute(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[storageremoval.Input](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_input", nil)
	}
	request, err := h.deps.Repositories.StorageRemoval.GetRemovalRequest(ctx, input.RequestID)
	if err != nil {
		return observe(err, nil)
	}
	if request.Status.Terminal() {
		return taskengine.Complete("Removal batch finished", nil)
	}
	if request.TaskID == nil || *request.TaskID != execution.ID() {
		return taskengine.Cancel("Removal batch was superseded", nil)
	}
	if !request.Status.Valid() {
		return taskengine.Fail(errors.New("unsupported removal batch state"), "removal_state_unsupported", nil)
	}
	if request.AttentionCode != nil {
		code, err := storageremoval.ParseAttentionCode(*request.AttentionCode)
		if err != nil {
			return taskengine.Fail(err, "removal_attention_unknown", nil)
		}
		if code.IsStop() {
			return taskengine.Fail(errors.New("removal needs attention"), string(code), nil)
		}
	}
	members, err := h.deps.Repositories.StorageRemoval.ListRemovalMembers(ctx, input.RequestID)
	if err != nil {
		return observe(err, nil)
	}
	r := run{execution: execution, input: input, request: request, members: members}
	if request.Status == storageremoval.RequestStatusCollecting {
		seal, _ := storagecommit.ShouldSeal(storagecommit.SealInput{Members: len(members), OldestJoinedAt: request.CreatedAt, Now: time.Now(), MaxPieces: h.deps.MaxPieces, MaxWait: h.deps.MaxWait})
		if !seal && len(members) > 0 {
			return taskengine.WaitUntil(model.TaskResumeModeRecover, request.CreatedAt.Add(h.deps.MaxWait), "collecting", "Collecting pieces for removal", nil)
		}
		return observe(nil, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.SealRemovalRequest(ctx, repository.SealRemovalInput{RequestID: input.RequestID, TaskID: execution.ID(), MaxPieces: h.deps.MaxPieces})
		})
	}
	if h.deps.Storage == nil || h.deps.RemovalChain == nil {
		return taskengine.Fail(errors.New("remote removal service is unavailable"), "dependency_unavailable", nil)
	}
	r.binding, err = h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, request.StorageDataSetID)
	if err != nil {
		return observe(err, nil)
	}
	if err = h.checkIdentity(ctx, &r); err != nil {
		if storagepipeline.IsExecutionIdentityError(err) {
			return h.stop(r, storageremoval.AttentionIdentityMismatch, err)
		}
		return observe(err, nil)
	}
	r.sends, err = h.deps.Repositories.StorageRemoval.ListRemovalSends(ctx, input.RequestID)
	if err != nil {
		return observe(err, nil)
	}
	for i := range r.sends {
		if r.sends[i].Exposed() {
			r.exposed = &r.sends[i]
		}
		if r.sends[i].PossiblyAccepted() {
			r.accepted = &r.sends[i]
		}
	}
	pieceIDs := make([]sdktypes.BigInt, 0, len(members))
	for _, m := range members {
		pieceIDs = append(pieceIDs, m.PieceID.SDK())
	}
	r.state, err = h.deps.Storage.DeletionStates(ctx, r.binding.DataSetID.SDK(), pieceIDs)
	if err != nil {
		return observe(err, nil)
	}
	for _, m := range members {
		s, ok := r.state.Pieces[m.PieceID.String()]
		if !ok || s != synapse.CleanupPiecePresent && s != synapse.CleanupPieceScheduled && s != synapse.CleanupPieceRemoved && s != synapse.CleanupPieceDataSetCleanup {
			return observe(errors.New("remote removal state is incomplete"), nil)
		}
	}
	removed := []int64{}
	for _, m := range members {
		if m.Status != model.StorageCleanupCopyStatusRemoved && r.state.Pieces[m.PieceID.String()] == synapse.CleanupPieceRemoved {
			removed = append(removed, m.ID)
		}
	}
	if len(removed) > 0 {
		return observe(nil, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.MarkMembersRemoved(ctx, repository.RemovalMembersInput{RequestID: input.RequestID, TaskID: execution.ID(), CopyIDs: removed, Detach: r.accepted == nil && request.Status == storageremoval.RequestStatusReady})
		})
	}
	if request.Status == storageremoval.RequestStatusQueued {
		return h.queued(ctx, r)
	}
	allQueued := true
	for _, m := range members {
		if m.Status != model.StorageCleanupCopyStatusRemoved && r.state.Pieces[m.PieceID.String()] == synapse.CleanupPiecePresent {
			allQueued = false
		}
	}
	if allQueued {
		return h.markQueued(r)
	}
	cp, _, err := taskengine.DecodeCheckpoint[checkpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if request.Status == storageremoval.RequestStatusReady && r.accepted == nil && cp.PieceLookup {
		var ids []int64
		var pieces []sdktypes.BigInt
		for _, member := range members {
			if r.state.Pieces[member.PieceID.String()] == synapse.CleanupPiecePresent {
				ids = append(ids, member.ID)
				pieces = append(pieces, member.PieceID.SDK())
			}
		}
		lookupErr := errors.New("provider could not find a piece")
		for _, send := range r.sends {
			if send.SendNo == cp.SendNo && send.RefusedAt != nil && send.LastError != nil {
				lookupErr = errors.New(*send.LastError)
				break
			}
		}
		return h.refusedMembers(ctx, r, ids, pieces, lookupErr, cp)
	}
	if request.Status == storageremoval.RequestStatusSubmitted {
		if result, stop := h.checkReceipt(ctx, r); stop {
			return result
		}
	}
	if !storageremoval.AutoResendSupported(r.state.VerifierVersion) {
		return taskengine.Fail(fmt.Errorf("unsupported removal verifier version %q", r.state.VerifierVersion), "removal_verifier_unsupported", nil)
	}
	r.preflight, err = h.deps.RemovalChain.Preflight(ctx, r.binding.DataSetID.SDK())
	if err != nil {
		if errors.Is(err, synapse.ErrServicePaidByAnother) {
			return h.stop(r, storageremoval.AttentionIdentityMismatch, err)
		}
		return observe(err, nil)
	}
	if r.preflight.DataSetMissing {
		return observe(fmt.Errorf("storage service record for data set %s is missing while pieces remain on chain", r.binding.DataSetID), nil)
	}
	if r.preflight.EndEpoch != 0 {
		return h.markQueued(r)
	}
	if r.preflight.PayerUnderfunded {
		return h.stop(r, storageremoval.AttentionPayerUnderfunded, errors.New(r.preflight.Reason))
	}
	if len(r.sends) > 0 && request.Status == storageremoval.RequestStatusSubmitted {
		latest := r.sends[len(r.sends)-1]
		attemptedAt := latest.AttemptedAt
		if attemptedAt == nil {
			attemptedAt = request.SealedAt
		}
		if attemptedAt != nil {
			deadline, err := execution.ResendAt(*attemptedAt, len(r.sends))
			if err != nil {
				return taskengine.Fail(err, "removal_policy_invalid", nil)
			}
			if time.Now().Before(deadline) {
				return h.confirmationWait(ctx, r)
			}
		}
	}
	row, err := h.deps.Repositories.Tasks.GetByID(ctx, execution.ID())
	if err != nil {
		return observe(err, nil)
	}
	var runtime model.TaskRuntime
	if err := json.Unmarshal(row.Runtime, &runtime); err != nil {
		return taskengine.Fail(err, "invalid_runtime", nil)
	}
	if runtime.OperationKey == "removal:"+input.RequestID && runtime.LastAdmittedAttempt >= row.RetryCount+1 {
		return h.retrySend(r, errors.New("sending removal again"), nil)
	}
	if execution.Mode() != model.TaskResumeModeExecute {
		return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Remote removal is ready", nil)
	}
	return h.send(ctx, r)
}

func (h *Handler) checkIdentity(ctx context.Context, r *run) error {
	if r.binding == nil || r.binding.DataSetID == nil || r.binding.ClientDataSetID == nil {
		return storagepipeline.ErrExecutionIdentityMissing
	}
	identity, err := binding.RecordedIdentity(ctx, h.deps.Repositories, r.binding)
	if err != nil {
		return err
	}
	if err = r.input.Match(identity); err != nil {
		return err
	}
	if err = identity.Match(binding.CurrentIdentity(h.deps.Storage)); err != nil {
		return err
	}
	cp, ok, err := taskengine.DecodeCheckpoint[checkpoint](r.execution)
	if err != nil {
		return err
	}
	if ok && cp.RequestID != "" {
		if cp.RequestID != r.input.RequestID {
			return storagepipeline.ErrExecutionIdentityChanged
		}
		if err = cp.Match(identity); err != nil {
			return err
		}
	}
	for _, m := range r.members {
		if m.StorageDataSetID != r.binding.ID || !m.ProviderID.Equal(r.binding.ProviderID) || m.DataSetID == nil || !m.DataSetID.Equal(*r.binding.DataSetID) || m.ClientDataSetID == nil || !m.ClientDataSetID.Equal(*r.binding.ClientDataSetID) {
			return storagepipeline.ErrExecutionIdentityChanged
		}
	}
	return nil
}

func (h *Handler) stop(r run, code storageremoval.AttentionCode, err error) taskengine.Result {
	return taskengine.Fail(err, string(code), func(ctx context.Context, repos *repository.Repositories) error {
		return repos.StorageRemoval.SetRemovalAttention(ctx, repository.RemovalAttentionInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), Code: code, Message: errtext.Format(err)})
	})
}

func (h *Handler) markQueued(r run) taskengine.Result {
	return observe(nil, func(ctx context.Context, repos *repository.Repositories) error {
		input := repository.RemovalQueuedInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), QueuedAt: time.Now().UTC(), QueuedBlock: r.state.BlockNumber}
		if r.request.Status == storageremoval.RequestStatusReady {
			return repos.StorageRemoval.MarkQueuedWithoutSend(ctx, input)
		}
		return repos.StorageRemoval.MarkRemovalQueued(ctx, input)
	})
}

func (h *Handler) checkReceipt(ctx context.Context, r run) (taskengine.Result, bool) {
	if r.accepted == nil || r.accepted.TxHash == nil || r.accepted.ReceiptStatus != nil || h.deps.Receipts == nil {
		return taskengine.Result{}, false
	}
	if !common.IsHexHash(*r.accepted.TxHash) {
		return observe(errors.New("removal transaction hash is invalid"), nil), true
	}
	requestCtx, cancel := context.WithTimeout(ctx, h.deps.ReceiptTimeout)
	receipt, err := h.deps.Receipts.TransactionReceipt(requestCtx, common.HexToHash(*r.accepted.TxHash))
	cancel()
	if errors.Is(err, ethereum.NotFound) {
		return taskengine.Result{}, false
	}
	if err != nil {
		return observe(err, nil), true
	}
	if receipt == nil || receipt.TxHash != common.HexToHash(*r.accepted.TxHash) || receipt.BlockNumber == nil || receipt.BlockNumber.Uint64() > r.state.BlockNumber {
		return observe(errors.New("removal receipt is incomplete or newer than the observed state"), nil), true
	}
	pieceIDs := make([]sdktypes.BigInt, 0, len(r.members))
	for _, member := range r.members {
		pieceIDs = append(pieceIDs, member.PieceID.SDK())
	}
	r.state, err = h.deps.Storage.DeletionStates(ctx, r.binding.DataSetID.SDK(), pieceIDs)
	if err != nil {
		return observe(err, nil), true
	}
	if r.state.BlockNumber < receipt.BlockNumber.Uint64() {
		return observe(errors.New("removal state is older than its receipt"), nil), true
	}
	requestCtx, cancel = context.WithTimeout(ctx, h.deps.ReceiptTimeout)
	checked, err := h.deps.Receipts.TransactionReceipt(requestCtx, common.HexToHash(*r.accepted.TxHash))
	cancel()
	if err != nil || checked == nil || checked.TxHash != receipt.TxHash || checked.BlockNumber == nil || checked.BlockNumber.Cmp(receipt.BlockNumber) != 0 || checked.BlockHash != receipt.BlockHash || checked.Status != receipt.Status {
		return observe(err, nil), true
	}
	status := int64(receipt.Status)
	evidence := repository.RemovalSendEvidenceInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), SendNo: r.accepted.SendNo, ConfirmedTxHash: receipt.TxHash.Hex(), ReceiptStatus: &status}
	settlement := func(ctx context.Context, repos *repository.Repositories) error {
		return repos.StorageRemoval.RecordSendReceipt(ctx, evidence)
	}
	if receipt.Status != ethtypes.ReceiptStatusFailed {
		return observe(nil, settlement), true
	}
	pieces := make([]storageremoval.ReceiptPiece, 0, len(r.accepted.Pieces))
	for _, piece := range r.accepted.Pieces {
		state, ok := r.state.Pieces[piece.PieceID.String()]
		if !ok || state != synapse.CleanupPiecePresent && state != synapse.CleanupPieceScheduled && state != synapse.CleanupPieceRemoved && state != synapse.CleanupPieceDataSetCleanup {
			return observe(errors.New("removal receipt member is missing"), nil), true
		}
		pieces = append(pieces, storageremoval.ReceiptPiece{Removed: state == synapse.CleanupPieceRemoved, Queued: state == synapse.CleanupPieceScheduled || state == synapse.CleanupPieceDataSetCleanup, BlockNumber: r.state.BlockNumber})
	}
	class := storageremoval.ClassifyFailedReceipt(pieces, receipt.BlockNumber.Uint64(), true)
	if class == storageremoval.FailedReceiptInsufficient {
		return observe(nil, nil), true
	}
	if class == storageremoval.FailedReceiptCollision {
		return observe(nil, settlement), true
	}
	return taskengine.Fail(errors.New("removal transaction failed; retry to send again"), "transaction_reverted", func(ctx context.Context, repos *repository.Repositories) error {
		if err := settlement(ctx, repos); err != nil {
			return err
		}
		return repos.StorageRemoval.MarkRemovalReverted(ctx, repository.RemovalRequestInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), Message: "Removal transaction failed; retry to send again"})
	}), true
}

func (h *Handler) confirmationWait(ctx context.Context, r run) taskengine.Result {
	var first time.Time
	for _, send := range r.sends {
		if send.AttemptedAt != nil && (first.IsZero() || send.AttemptedAt.Before(first)) {
			first = *send.AttemptedAt
		}
	}
	if first.IsZero() && r.request.SealedAt != nil {
		first = *r.request.SealedAt
	}
	row, err := h.deps.Repositories.Tasks.GetByID(ctx, r.execution.ID())
	if err != nil {
		return observe(err, nil)
	}
	if row != nil && row.WorkStartedAt != nil {
		first = storageremoval.ConfirmationTimeoutStart(first, *row.WorkStartedAt)
	}
	if !first.IsZero() && time.Since(first) >= storagecommit.DefaultAttentionAfter {
		return observe(nil, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.SetRemovalAttention(ctx, repository.RemovalAttentionInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), Code: storageremoval.AttentionConfirmationTimeout, Message: "Removal is still unconfirmed"})
		})
	}
	return observe(nil, nil)
}

func (h *Handler) queued(ctx context.Context, r run) taskengine.Result {
	allRemoved := true
	for _, m := range r.members {
		if m.Status != model.StorageCleanupCopyStatusRemoved {
			allRemoved = false
		}
	}
	if allRemoved {
		return taskengine.Complete("Pieces removed", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.SettleRemovalRequest(ctx, repository.RemovalRequestInput{RequestID: r.input.RequestID, TaskID: r.execution.ID()})
		})
	}
	if h.deps.Epochs == nil {
		return observe(errors.New("chain epoch reader is unavailable"), nil)
	}
	preflight, err := h.deps.RemovalChain.Preflight(ctx, r.binding.DataSetID.SDK())
	if err != nil {
		return observe(err, nil)
	}
	config, err := h.deps.RemovalChain.ProvingConfig(ctx)
	if err != nil {
		return observe(err, nil)
	}
	current, err := h.deps.Epochs.CurrentEpoch(ctx)
	if err != nil {
		return observe(err, nil)
	}
	var code storageremoval.AttentionCode
	if preflight.EndEpoch != 0 {
		if current > preflight.EndEpoch+2*config.MaxProvingPeriod {
			code = storageremoval.AttentionCleanupStalled
		}
	} else if r.request.QueuedAt != nil && time.Since(*r.request.QueuedAt) >= storageremoval.RemovalStalledAfter(time.Duration(config.MaxProvingPeriod)*30*time.Second, time.Duration(config.ChallengeWindow)*30*time.Second) {
		code = storageremoval.AttentionRemovalStalled
	}
	if code != "" {
		return observe(nil, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.SetRemovalAttention(ctx, repository.RemovalAttentionInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), Code: code, Message: "Remote removal is taking longer than expected"})
		})
	}
	if preflight.EndEpoch != 0 && r.request.AttentionCode != nil && *r.request.AttentionCode == string(storageremoval.AttentionRemovalStalled) {
		return observe(nil, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.ClearRemovalAttention(ctx, repository.RemovalRequestInput{RequestID: r.input.RequestID, TaskID: r.execution.ID()})
		})
	}
	return observe(nil, nil)
}

func (h *Handler) send(ctx context.Context, r run) taskengine.Result {
	var ids []int64
	var pieceIDs []sdktypes.BigInt
	for _, m := range r.members {
		if r.state.Pieces[m.PieceID.String()] == synapse.CleanupPiecePresent && m.Status != model.StorageCleanupCopyStatusRemoved {
			ids = append(ids, m.ID)
			pieceIDs = append(pieceIDs, m.PieceID.SDK())
		}
	}
	if len(ids) == 0 {
		return h.markQueued(r)
	}
	if r.exposed != nil {
		previous := make([]int64, 0, len(r.exposed.Pieces))
		for _, piece := range r.exposed.Pieces {
			previous = append(previous, piece.CleanupCopyID)
		}
		if _, err := storageremoval.NextSendSet(previous, ids); err != nil {
			return taskengine.Fail(err, "removal_authorization_changed", nil)
		}
	}
	providerID := r.binding.ProviderID.SDK()
	target, err := h.deps.Storage.OpenCleanupContext(ctx, r.binding.DataSetID.SDK(), storage.NewDataSetContextOptions{ProviderID: &providerID})
	if err != nil {
		return observe(err, nil)
	}
	sendNo := len(r.sends) + 1
	now := time.Now().UTC().Truncate(time.Microsecond)
	next, err := r.execution.ResendAt(now, sendNo)
	if err != nil {
		return taskengine.Fail(err, "removal_policy_invalid", nil)
	}
	cp := checkpoint{ExecutionIdentity: r.input.ExecutionIdentity, RequestID: r.input.RequestID, SendNo: sendNo, AttemptedAt: now, NextSendAt: next}
	var result *sdktypes.WriteResult
	var sendErr error
	attempted, err := r.execution.WithCheckpointedEffect(ctx, "removal:"+r.input.RequestID, cp, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.StorageRemoval.AdmitRemovalSend(ctx, repository.AdmitRemovalSendInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), SendNo: sendNo, CopyIDs: ids, AttemptedAt: now, QueueLength: r.state.QueueLength, SnapshotBlock: r.state.BlockNumber})
	}, func(ctx context.Context) error {
		result, sendErr = target.DeletePiecesByID(ctx, pieceIDs)
		return sendErr
	})
	if !attempted {
		if errors.Is(err, repository.ErrNoRemovalRoom) {
			return taskengine.Wait(model.TaskResumeModeExecute, "removal_queue_full", "Waiting for room in the removal queue", nil)
		}
		return observe(err, nil)
	}
	evidence := repository.RemovalSendEvidenceInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), SendNo: sendNo, Message: errtext.Format(sendErr)}
	if result != nil && result.Hash != (common.Hash{}) {
		evidence.TxHash = result.Hash.Hex()
		settlement := func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.RecordSendSubmission(ctx, evidence)
		}
		if err := r.execution.RecordEffectEvidence(ctx, cp, settlement); err != nil {
			return observe(errors.Join(sendErr, err), settlement).WithContentionSettlement(settlement)
		}
		return observe(sendErr, nil)
	}
	rejection := synapse.ClassifyRemovalRejection(sendErr)
	if rejection != synapse.RemovalNotRejected {
		cp.PieceLookup = rejection == synapse.RemovalRejectedPieceNotFound && r.accepted == nil
		settlement := func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.RecordSendRefused(ctx, evidence)
		}
		if err := r.execution.RecordEffectEvidence(ctx, cp, settlement); err != nil {
			return observe(errors.Join(sendErr, err), settlement).WithContentionSettlement(settlement)
		}
		if rejection == synapse.RemovalRejectedPieceNotFound && r.accepted == nil {
			return h.refusedMembers(ctx, r, ids, pieceIDs, sendErr, cp)
		}
		return h.retrySend(r, sendErr, nil)
	}
	// Once the signed request may have left the SDK, transport errors do not
	// establish that the provider lacks its authorization.
	if sendErr != nil && (errors.Is(sendErr, storage.ErrInvalidArgument) || synapse.IsDataSetWriteBlocked(sendErr)) {
		settlement := func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageRemoval.RecordSendUnsent(ctx, evidence)
		}
		return h.retrySend(r, sendErr, settlement)
	}
	if sendErr == nil {
		sendErr = errors.New("provider did not return a removal transaction")
	}
	return observe(sendErr, nil)
}

func (h *Handler) retrySend(r run, err error, settlement taskengine.Settlement) taskengine.Result {
	return taskengine.RetryInMode(err, "removal_provider_unavailable", model.TaskResumeModeExecute, nil).WithRetrySettlements(func(ctx context.Context, repos *repository.Repositories, _ time.Time) error {
		if settlement != nil {
			return settlement(ctx, repos)
		}
		return nil
	}, func(ctx context.Context, repos *repository.Repositories) error {
		if settlement != nil {
			if err := settlement(ctx, repos); err != nil {
				return err
			}
		}
		sends, readErr := repos.StorageRemoval.ListRemovalSends(ctx, r.input.RequestID)
		if readErr != nil {
			return readErr
		}
		code := storageremoval.AttentionProviderUnreachable
		if slices.ContainsFunc(sends, storageremoval.Send.InFlight) {
			code = storageremoval.AttentionConfirmationTimeout
		}
		return repos.StorageRemoval.SetRemovalAttention(ctx, repository.RemovalAttentionInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), Code: code, Message: errtext.Format(err)})
	})
}

func (h *Handler) refusedMembers(ctx context.Context, r run, ids []int64, pieces []sdktypes.BigInt, sendErr error, cp checkpoint) taskengine.Result {
	provider, err := h.deps.Storage.OpenProviderTarget(ctx, r.binding.ProviderID.SDK(), storage.NewProviderContextOptions{})
	if err != nil || provider == nil {
		return observe(errors.Join(sendErr, err), nil)
	}
	known, err := h.deps.Storage.ProviderPiecesKnown(ctx, provider.ServiceURL(), r.binding.DataSetID.SDK(), pieces)
	diagnostics := []error{sendErr, err}
	for _, pieceID := range slices.Sorted(maps.Keys(known.Errors)) {
		if pieceErr := known.Errors[pieceID]; pieceErr != nil {
			diagnostics = append(diagnostics, fmt.Errorf("checking provider piece %s: %w", pieceID, pieceErr))
		}
	}
	sendErr = errors.Join(diagnostics...)
	if err != nil {
		return observe(sendErr, nil)
	}
	var failed, detach []int64
	if len(known.Known) == 0 {
		if len(ids) > 1 {
			detach = ids[len(ids)/2:]
		}
	} else {
		for _, m := range r.members {
			if slices.ContainsFunc(known.Unknown, func(id sdktypes.BigInt) bool { return id.String() == m.PieceID.String() }) {
				failed = append(failed, m.ID)
			}
		}
	}
	if len(known.Known) > 0 && len(failed) == 0 && len(detach) == 0 && len(known.Errors) > 0 {
		return observe(sendErr, nil)
	}
	settlement := func(ctx context.Context, repos *repository.Repositories) error {
		if len(failed) > 0 {
			if err := repos.StorageRemoval.MarkMemberFailed(ctx, repository.RemovalMembersInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), CopyIDs: failed, Message: errtext.Format(sendErr)}); err != nil {
				return err
			}
		}
		if len(detach) > 0 {
			if err := repos.StorageRemoval.DetachMembers(ctx, repository.RemovalMembersInput{RequestID: r.input.RequestID, TaskID: r.execution.ID(), CopyIDs: detach}); err != nil {
				return err
			}
		}
		cp.PieceLookup = false
		raw, err := json.Marshal(cp)
		if err != nil {
			return err
		}
		return repos.Tasks.WriteCheckpoint(ctx, r.execution.ID(), r.execution.ClaimGeneration(), raw)
	}
	return h.retrySend(r, sendErr, settlement)
}
