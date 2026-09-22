package storagecommit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
)

const (
	DefaultRequestTimeout = 15 * time.Second
	DefaultAttentionAfter = 15 * time.Minute
	evidenceWriteTimeout  = 10 * time.Second
)

type Advancer struct {
	Store          Store
	RequestTimeout time.Duration
	AttentionAfter time.Duration
	Now            func() time.Time
}

type AdvanceInput struct {
	Copy                model.StorageCopy
	Binding             model.StorageDataSet
	Target              synapse.DataSetTarget
	Pieces              []storage.PieceInput
	RequireEligibleCopy bool
	OwnerTerminal       bool
}

// AdvanceUnavailable records an attempted commit whose provider context could
// not be reconstructed. It never submits or confirms provider work.
func (a *Advancer) AdvanceUnavailable(
	ctx context.Context,
	copyRow model.StorageCopy,
	binding model.StorageDataSet,
) (AdvanceResult, error) {
	if a == nil || a.Store == nil || copyRow.ID <= 0 || copyRow.ContentID <= 0 || copyRow.CopyIndex < 0 ||
		copyRow.StorageDataSetID != binding.ID ||
		copyRow.CommitAttemptID == nil || *copyRow.CommitAttemptID == "" || copyRow.CommitAttemptedAt == nil {
		return AdvanceResult{}, errors.New("invalid unavailable storage commit input")
	}
	attemptID := *copyRow.CommitAttemptID
	if context.Cause(ctx) != nil {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	if result, ok := existingAttentionResult(
		copyRow, attemptID, AttentionDataSetUnavailable, true,
	); ok {
		return result, nil
	}
	if a.now().Sub(*copyRow.CommitAttemptedAt) < a.attentionAfter() {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	return a.attention(ctx, CopyIdentity{
		StorageCopyID:    copyRow.ID,
		ContentID:        copyRow.ContentID,
		CopyIndex:        copyRow.CopyIndex,
		StorageDataSetID: binding.ID,
	}, attemptID, AttentionDataSetUnavailable, true)
}

// ReleaseTerminalReservation releases an unattempted reservation whose owner
// is terminal without requiring a provider context.
func (a *Advancer) ReleaseTerminalReservation(
	ctx context.Context,
	copyRow model.StorageCopy,
	binding model.StorageDataSet,
) (AdvanceResult, error) {
	if a == nil || a.Store == nil || copyRow.ID <= 0 || copyRow.ContentID <= 0 || copyRow.CopyIndex < 0 ||
		copyRow.StorageDataSetID != binding.ID ||
		copyRow.CommitAttemptID == nil || *copyRow.CommitAttemptID == "" || copyRow.CommitAttemptedAt != nil {
		return AdvanceResult{}, errors.New("invalid terminal storage commit reservation")
	}
	return a.release(ctx, CopyIdentity{
		StorageCopyID:    copyRow.ID,
		ContentID:        copyRow.ContentID,
		CopyIndex:        copyRow.CopyIndex,
		StorageDataSetID: binding.ID,
	}, copyRow, ReleaseOwnerTerminal, true, true, false)
}

func (a *Advancer) Advance(ctx context.Context, input AdvanceInput) (AdvanceResult, error) {
	if err := a.validateInput(input); err != nil {
		return AdvanceResult{}, err
	}
	identity := copyIdentity(input, false)
	eligibleIdentity := copyIdentity(input, input.RequireEligibleCopy)
	copyRow := input.Copy
	if copyRow.CommitAttemptID != nil && *copyRow.CommitAttemptID != "" {
		if input.OwnerTerminal && copyRow.CommitAttemptedAt == nil {
			return a.release(ctx, identity, copyRow, ReleaseOwnerTerminal, true, true, false)
		}
		if copyRow.CommitAttemptedAt != nil {
			return a.observe(ctx, input, copyRow)
		}
		return a.submitReserved(ctx, input, copyRow)
	}
	if input.OwnerTerminal {
		evidenceCtx, cancel := evidenceContext(ctx)
		err := a.Store.ReleaseCommitReservation(evidenceCtx, ReservationReleaseInput{
			Copy:           identity,
			ClearReadyAt:   true,
			ClearExtraData: true,
			Now:            a.now(),
		})
		cancel()
		if err != nil {
			return AdvanceResult{}, err
		}
		return AdvanceResult{State: AdvanceReleased, ReleaseReason: ReleaseOwnerTerminal}, nil
	}
	attemptID, err := newAttemptID()
	if err != nil {
		return AdvanceResult{}, err
	}
	reservation, err := a.Store.ReserveCommitAttempt(ctx, ReserveInput{
		Copy:      eligibleIdentity,
		AttemptID: attemptID,
		Now:       a.now(),
	})
	if err != nil {
		return AdvanceResult{}, err
	}
	if reservation.State == ReservationWaiting {
		return AdvanceResult{State: AdvanceWaitingCapacity, AttentionHeld: reservation.AttentionHeld}, nil
	}
	return a.submitReserved(ctx, input, reservation.Copy)
}

func (a *Advancer) submitReserved(
	ctx context.Context,
	input AdvanceInput,
	copyRow model.StorageCopy,
) (AdvanceResult, error) {
	if copyRow.CommitAttemptID == nil || *copyRow.CommitAttemptID == "" {
		return AdvanceResult{}, errors.New("reserved storage commit has no attempt token")
	}
	identity := copyIdentity(input, false)
	eligibleIdentity := copyIdentity(input, input.RequireEligibleCopy)
	attemptID := *copyRow.CommitAttemptID
	if err := context.Cause(ctx); err != nil {
		return a.release(ctx, identity, copyRow, ReleaseBeforeSubmitCanceled, false, false, false)
	}
	extraHex, err := a.commitExtraData(ctx, input.Target, copyRow, input.Pieces)
	if err != nil {
		if dataSetRefusesWrites(err) {
			return a.releaseUnavailable(ctx, identity, copyRow, err, false)
		}
		if _, releaseErr := a.release(ctx, identity, copyRow, ReleaseBeforeSubmitCanceled, false, false, false); releaseErr != nil {
			return AdvanceResult{}, releaseErr
		}
		return AdvanceResult{}, err
	}
	attempt, err := a.Store.MarkCommitAttempted(ctx, AttemptInput{
		Copy:         eligibleIdentity,
		AttemptID:    attemptID,
		ExtraDataHex: extraHex,
		Now:          a.now(),
	})
	if err != nil {
		_, releaseErr := a.release(ctx, identity, copyRow, ReleaseBeforeSubmitCanceled, false, false, false)
		if releaseErr != nil {
			return AdvanceResult{}, errors.Join(err, fmt.Errorf("releasing unattempted storage commit reservation: %w", releaseErr))
		}
		return AdvanceResult{}, err
	}
	if !attempt.Entered {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	if err := context.Cause(ctx); err != nil {
		return a.release(ctx, identity, attempt.Copy, ReleaseBeforeSubmitCanceled, false, false, true)
	}
	extraData, err := decodeCommitExtraData(attempt.Copy.CommitExtraDataHex)
	if err != nil {
		if resetErr := a.reset(ctx, identity, attemptID, err); resetErr != nil {
			return AdvanceResult{}, resetErr
		}
		return AdvanceResult{State: AdvanceRejected, AttemptID: attemptID}, nil
	}
	var callbackObserved atomic.Bool
	var callbackEvidenceErr atomic.Pointer[commitEvidenceError]
	submission, submitErr := input.Target.SubmitCommit(ctx, storage.CommitRequest{
		Pieces:    input.Pieces,
		ExtraData: extraData,
		OnSubmitted: func(submission storage.CommitSubmission) {
			callbackObserved.Store(true)
			evidenceCtx, cancel := evidenceContext(ctx)
			err := a.Store.RecordCommitSubmission(evidenceCtx, EvidenceInput{
				Copy:          identity,
				AttemptID:     attemptID,
				TransactionID: submission.TransactionID,
				StatusURL:     submission.StatusURL,
				Now:           a.now(),
			})
			cancel()
			if err != nil {
				callbackEvidenceErr.Store(&commitEvidenceError{err: err})
			}
		},
	})
	if submitErr != nil {
		// A write-blocked data set is refused while the SDK validates it, before
		// the provider is contacted, so the attempt is safe to release here.
		if dataSetRefusesWrites(submitErr) {
			return a.releaseUnavailable(ctx, identity, attempt.Copy, submitErr, true)
		}
		if context.Cause(ctx) != nil {
			return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
		}
		if callbackObserved.Load() {
			if evidenceErr := callbackEvidenceErr.Load(); evidenceErr != nil {
				return AdvanceResult{State: AdvancePending, AttemptID: attemptID},
					errors.Join(submitErr, fmt.Errorf("recording storage commit callback evidence: %w", evidenceErr.err))
			}
		}
		// Every remaining submission failure is ambiguous, so the attempt fence
		// stays and the next pass observes it. The error still travels back so the
		// caller can record why, which parking alone would discard.
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, submitErr
	}
	if submission == nil {
		if evidenceErr := callbackEvidenceErr.Load(); evidenceErr != nil {
			return AdvanceResult{State: AdvancePending, AttemptID: attemptID},
				fmt.Errorf("recording storage commit callback evidence: %w", evidenceErr.err)
		}
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	evidenceCtx, cancel := evidenceContext(ctx)
	err = a.Store.RecordCommitSubmission(evidenceCtx, EvidenceInput{
		Copy:          identity,
		AttemptID:     attemptID,
		TransactionID: submission.TransactionID,
		StatusURL:     submission.StatusURL,
		Now:           a.now(),
	})
	cancel()
	if err != nil {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, err
	}
	return AdvanceResult{State: AdvanceSubmitted, AttemptID: attemptID}, nil
}

type commitEvidenceError struct {
	err error
}

func (a *Advancer) observe(
	ctx context.Context,
	input AdvanceInput,
	copyRow model.StorageCopy,
) (AdvanceResult, error) {
	identity := copyIdentity(input, false)
	attemptID := *copyRow.CommitAttemptID
	if context.Cause(ctx) != nil {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	if copyRow.CommitStatusURL != nil && *copyRow.CommitStatusURL != "" {
		requestCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
		status, err := input.Target.GetCommitStatus(requestCtx, *copyRow.CommitStatusURL)
		cancel()
		if err != nil {
			if context.Cause(ctx) != nil {
				return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
			}
			if errors.Is(err, storage.ErrDataSetUnavailable) || synapse.IsDataSetServiceEnded(err) {
				return a.pendingOrAttentionWithCode(
					ctx, identity, copyRow, attemptID, AttentionDataSetUnavailable,
				)
			}
			if errors.Is(err, storage.ErrInvalidArgument) || errors.Is(err, pdp.ErrInvalidStatus) {
				return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionSubmissionMismatch, false)
			}
			return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
		}
		return a.classifySDKStatus(ctx, input, identity, copyRow, attemptID, status)
	}
	if result, ok := existingAttentionResult(
		copyRow, attemptID, AttentionAttemptOnlyAmbiguous, false,
	); ok {
		return result, nil
	}
	requestCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	pieceStatus, err := input.Target.PieceStatus(requestCtx, input.Pieces[0].PieceCID)
	cancel()
	if err != nil && (errors.Is(err, storage.ErrDataSetUnavailable) || synapse.IsDataSetServiceEnded(err)) {
		return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionDataSetUnavailable, false)
	}
	code := AttentionAttemptOnlyAmbiguous
	if err == nil && pieceStatus != nil && pieceStatus.Exists {
		code = AttentionUnattributedPiece
	}
	return a.attentionForCopy(ctx, identity, copyRow, attemptID, code, false)
}

func (a *Advancer) classifySDKStatus(
	ctx context.Context,
	input AdvanceInput,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
	status *storage.CommitStatus,
) (AdvanceResult, error) {
	if status == nil {
		return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
	}
	ref, bound := input.Target.DataSetRef()
	if !bound || status.DataSet == nil || status.Kind != storage.CommitKindAddPieces ||
		copyRow.CommitTransactionID == nil || status.TransactionID != *copyRow.CommitTransactionID ||
		!status.DataSet.Equal(ref) {
		return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionSubmissionMismatch, false)
	}
	switch status.State {
	case storage.CommitStatePending:
		return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
	case storage.CommitStateConfirmed:
		if len(status.PieceIDs) != len(input.Pieces) {
			return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionSubmissionMismatch, false)
		}
		return AdvanceResult{
			State:     AdvanceConfirmed,
			AttemptID: attemptID,
			Confirmation: &storage.CommitResult{
				TransactionID:          status.TransactionID,
				ConfirmedTransactionID: confirmedTransactionID(status.TransactionID, status.ConfirmedTransactionID),
				DataSet:                *status.DataSet,
				PieceIDs:               status.PieceIDs,
			},
		}, nil
	case storage.CommitStateRejected:
		if err := a.reset(ctx, identity, attemptID, pdp.ErrTxRejected); err != nil {
			return AdvanceResult{}, err
		}
		return AdvanceResult{State: AdvanceRejected, AttemptID: attemptID}, nil
	default:
		return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionSubmissionMismatch, false)
	}
}

func confirmedTransactionID(transactionID, confirmedTransactionID string) string {
	if confirmedTransactionID != "" {
		return confirmedTransactionID
	}
	return transactionID
}

func (a *Advancer) pendingOrAttention(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
) (AdvanceResult, error) {
	return a.pendingOrAttentionWithCode(
		ctx, identity, copyRow, attemptID, AttentionConfirmationTimeout,
	)
}

func (a *Advancer) pendingOrAttentionWithCode(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
	code AttentionCode,
) (AdvanceResult, error) {
	if copyRow.CommitAttentionAt != nil || a.now().Sub(*copyRow.CommitAttemptedAt) >= a.attentionAfter() {
		return a.attentionForCopy(ctx, identity, copyRow, attemptID, code, true)
	}
	return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
}

func (a *Advancer) attentionForCopy(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
	code AttentionCode,
	keepObserving bool,
) (AdvanceResult, error) {
	if result, ok := existingAttentionResult(copyRow, attemptID, code, keepObserving); ok {
		return result, nil
	}
	return a.attention(ctx, identity, attemptID, code, keepObserving)
}

func existingAttentionResult(
	copyRow model.StorageCopy,
	attemptID string,
	fallbackCode AttentionCode,
	keepObserving bool,
) (AdvanceResult, bool) {
	if copyRow.CommitAttentionAt == nil {
		return AdvanceResult{}, false
	}
	code := fallbackCode
	if copyRow.CommitAttentionCode != nil && *copyRow.CommitAttentionCode != "" {
		code = AttentionCode(*copyRow.CommitAttentionCode)
	}
	return AdvanceResult{
		State:         AdvanceNeedsAttention,
		AttemptID:     attemptID,
		AttentionCode: code,
		Continue:      keepObserving && recoverableAttentionCode(code),
	}, true
}

func recoverableAttentionCode(code AttentionCode) bool {
	return code == AttentionDataSetUnavailable || code == AttentionConfirmationTimeout
}

func (a *Advancer) attention(
	ctx context.Context,
	identity CopyIdentity,
	attemptID string,
	code AttentionCode,
	keepObserving bool,
) (AdvanceResult, error) {
	if context.Cause(ctx) != nil {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	evidenceCtx, cancel := evidenceContext(ctx)
	err := a.Store.MarkCommitAttention(evidenceCtx, AttentionInput{
		Copy:      identity,
		AttemptID: attemptID,
		Code:      code,
		Now:       a.now(),
	})
	cancel()
	if err != nil {
		return AdvanceResult{}, err
	}
	return AdvanceResult{
		State:         AdvanceNeedsAttention,
		AttemptID:     attemptID,
		AttentionCode: code,
		Continue:      keepObserving && recoverableAttentionCode(code),
	}, nil
}

func (a *Advancer) reset(ctx context.Context, identity CopyIdentity, attemptID string, cause error) error {
	evidenceCtx, cancel := evidenceContext(ctx)
	err := a.Store.ResetCommitAttempt(evidenceCtx, ResetInput{
		Copy:      identity,
		AttemptID: attemptID,
		LastError: cause.Error(),
		Now:       a.now(),
	})
	cancel()
	return err
}

func (a *Advancer) release(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	reason ReleaseReason,
	clearReadyAt bool,
	clearExtraData bool,
	knownNotSubmitted bool,
) (AdvanceResult, error) {
	if copyRow.CommitAttemptID == nil || *copyRow.CommitAttemptID == "" {
		return AdvanceResult{State: AdvanceReleased, ReleaseReason: reason}, nil
	}
	evidenceCtx, cancel := evidenceContext(ctx)
	err := a.Store.ReleaseCommitAttempt(evidenceCtx, ReleaseInput{
		Copy:              identity,
		AttemptID:         *copyRow.CommitAttemptID,
		Reason:            reason,
		KnownNotSubmitted: knownNotSubmitted,
		ClearReadyAt:      clearReadyAt,
		ClearExtraData:    clearExtraData,
		Now:               a.now(),
	})
	cancel()
	if err != nil {
		return AdvanceResult{}, err
	}
	return AdvanceResult{
		State:         AdvanceReleased,
		ReleaseReason: reason,
		AttemptID:     *copyRow.CommitAttemptID,
	}, nil
}

// releaseUnavailable releases the attempt and keeps the provider error so the
// caller can record why the data set stopped accepting this commit.
func (a *Advancer) releaseUnavailable(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	cause error,
	knownNotSubmitted bool,
) (AdvanceResult, error) {
	result, err := a.release(ctx, identity, copyRow, ReleaseDataSetUnavailable, true, true, knownNotSubmitted)
	if err != nil {
		return result, err
	}
	result.Cause = cause
	return result, nil
}

// dataSetRefusesWrites reports whether err means the data set will not accept
// this commit, because its storage service ended or its PDP payment did. Both
// are decided before the provider is contacted.
func dataSetRefusesWrites(err error) bool {
	return errors.Is(err, storage.ErrDataSetUnavailable) ||
		synapse.IsDataSetServiceEnded(err) ||
		synapse.IsDataSetWriteBlocked(err)
}

func (a *Advancer) commitExtraData(
	ctx context.Context,
	target synapse.DataSetTarget,
	copyRow model.StorageCopy,
	pieces []storage.PieceInput,
) (string, error) {
	if copyRow.CommitExtraDataHex != nil && *copyRow.CommitExtraDataHex != "" {
		if _, err := hex.DecodeString(*copyRow.CommitExtraDataHex); err != nil {
			return "", fmt.Errorf("decoding stored commit extra data: %w", err)
		}
		return strings.ToLower(*copyRow.CommitExtraDataHex), nil
	}
	extraData, err := target.PresignForCommit(ctx, pieces)
	if err != nil {
		return "", err
	}
	return strings.ToLower(hex.EncodeToString(extraData)), nil
}

func (a *Advancer) validateInput(input AdvanceInput) error {
	if a == nil || a.Store == nil || input.Target == nil || input.Binding.ID <= 0 ||
		input.Binding.DataSetID == nil || input.Binding.DataSetID.IsZero() ||
		input.Copy.ID <= 0 || input.Copy.ContentID <= 0 || input.Copy.CopyIndex < 0 ||
		input.Copy.StorageDataSetID != input.Binding.ID ||
		len(input.Pieces) != 1 || !input.Pieces[0].PieceCID.Defined() {
		return errors.New("invalid storage commit advance input")
	}
	return nil
}

func copyIdentity(input AdvanceInput, requireEligibleCopy bool) CopyIdentity {
	return CopyIdentity{
		StorageCopyID:       input.Copy.ID,
		ContentID:           input.Copy.ContentID,
		CopyIndex:           input.Copy.CopyIndex,
		StorageDataSetID:    input.Binding.ID,
		RequireEligibleCopy: requireEligibleCopy,
	}
}

func decodeCommitExtraData(value *string) ([]byte, error) {
	if value == nil || *value == "" {
		return nil, errors.New("storage commit extra data is missing")
	}
	extraData, err := hex.DecodeString(*value)
	if err != nil {
		return nil, fmt.Errorf("decoding storage commit extra data: %w", err)
	}
	return extraData, nil
}

func evidenceContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), evidenceWriteTimeout)
}

func newAttemptID() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generating storage commit attempt ID: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func (a *Advancer) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func (a *Advancer) requestTimeout() time.Duration {
	if a.RequestTimeout > 0 {
		return a.RequestTimeout
	}
	return DefaultRequestTimeout
}

func (a *Advancer) attentionAfter() time.Duration {
	if a.AttentionAfter > 0 {
		return a.AttentionAfter
	}
	return DefaultAttentionAfter
}
