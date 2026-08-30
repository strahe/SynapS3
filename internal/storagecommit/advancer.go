package storagecommit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

const (
	DefaultRequestTimeout = 15 * time.Second
	DefaultAttentionAfter = 15 * time.Minute
	evidenceWriteTimeout  = 10 * time.Second
)

type AddPiecesStatusChecker interface {
	GetAddPiecesStatus(context.Context, synapse.AddPiecesStatusInput) (synapse.PDPStatusResult, error)
}

type Advancer struct {
	Store          Store
	StatusChecker  AddPiecesStatusChecker
	RequestTimeout time.Duration
	AttentionAfter time.Duration
	Now            func() time.Time
}

type AdvanceInput struct {
	Copy                model.StorageUploadCopy
	Binding             model.StorageDataSet
	Target              synapse.DataSetTarget
	Pieces              []storage.PieceInput
	RequireEligibleCopy bool
	OwnerTerminal       bool
}

func (a *Advancer) Advance(ctx context.Context, input AdvanceInput) (AdvanceResult, error) {
	if err := a.validateInput(input); err != nil {
		return AdvanceResult{}, err
	}
	identity := copyIdentity(input)
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
		Copy:      identity,
		AttemptID: attemptID,
		Now:       a.now(),
	})
	if err != nil {
		return AdvanceResult{}, err
	}
	if reservation.State == ReservationWaiting {
		return AdvanceResult{State: AdvanceWaitingCapacity}, nil
	}
	return a.submitReserved(ctx, input, reservation.Copy)
}

func (a *Advancer) submitReserved(
	ctx context.Context,
	input AdvanceInput,
	copyRow model.StorageUploadCopy,
) (AdvanceResult, error) {
	if copyRow.CommitAttemptID == nil || *copyRow.CommitAttemptID == "" {
		return AdvanceResult{}, errors.New("reserved storage commit has no attempt token")
	}
	identity := copyIdentity(input)
	attemptID := *copyRow.CommitAttemptID
	if err := context.Cause(ctx); err != nil {
		return a.release(ctx, identity, copyRow, ReleaseBeforeSubmitCanceled, false, false, false)
	}
	extraHex, err := a.commitExtraData(ctx, input.Target, copyRow, input.Pieces)
	if err != nil {
		if errors.Is(err, storage.ErrDataSetUnavailable) || synapse.IsDataSetServiceEnded(err) {
			return a.release(ctx, identity, copyRow, ReleaseDataSetUnavailable, true, true, false)
		}
		if _, releaseErr := a.release(ctx, identity, copyRow, ReleaseBeforeSubmitCanceled, false, false, false); releaseErr != nil {
			return AdvanceResult{}, releaseErr
		}
		return AdvanceResult{}, err
	}
	attempt, err := a.Store.MarkCommitAttempted(ctx, AttemptInput{
		Copy:         identity,
		AttemptID:    attemptID,
		ExtraDataHex: extraHex,
		Now:          a.now(),
	})
	if err != nil {
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
	submission, submitErr := input.Target.SubmitCommit(ctx, storage.CommitRequest{
		Pieces:    input.Pieces,
		ExtraData: extraData,
		OnSubmitted: func(transactionID string) {
			callbackObserved.Store(true)
			evidenceCtx, cancel := evidenceContext(ctx)
			_ = a.Store.RecordCommitTransaction(evidenceCtx, EvidenceInput{
				Copy:          identity,
				AttemptID:     attemptID,
				TransactionID: transactionID,
				Now:           a.now(),
			})
			cancel()
		},
	})
	if submitErr != nil {
		if errors.Is(submitErr, storage.ErrDataSetUnavailable) || synapse.IsDataSetServiceEnded(submitErr) {
			return a.attention(ctx, identity, attemptID, AttentionDataSetUnavailable, false)
		}
		if callbackObserved.Load() {
			return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
		}
		if definitelyNotSubmitted(submitErr) {
			if resetErr := a.reset(ctx, identity, attemptID, submitErr); resetErr != nil {
				return AdvanceResult{}, resetErr
			}
			return AdvanceResult{State: AdvanceRejected, AttemptID: attemptID}, nil
		}
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	if submission == nil {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	submissionJSON, err := EncodeSubmission(*submission)
	if err != nil {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	evidenceCtx, cancel := evidenceContext(ctx)
	err = a.Store.RecordCommitSubmission(evidenceCtx, EvidenceInput{
		Copy:           identity,
		AttemptID:      attemptID,
		TransactionID:  submission.TransactionID,
		SubmissionJSON: submissionJSON,
		Now:            a.now(),
	})
	cancel()
	if err != nil {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	return AdvanceResult{State: AdvanceSubmitted, AttemptID: attemptID}, nil
}

func (a *Advancer) observe(
	ctx context.Context,
	input AdvanceInput,
	copyRow model.StorageUploadCopy,
) (AdvanceResult, error) {
	identity := copyIdentity(input)
	attemptID := *copyRow.CommitAttemptID
	if copyRow.CommitSubmissionJSON != nil && *copyRow.CommitSubmissionJSON != "" {
		submission, err := DecodeSubmission(*copyRow.CommitSubmissionJSON)
		if err != nil {
			return a.attention(ctx, identity, attemptID, AttentionInvalidSubmission, false)
		}
		requestCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
		status, err := input.Target.GetCommitStatus(requestCtx, submission)
		cancel()
		if err != nil {
			if errors.Is(err, storage.ErrDataSetUnavailable) || synapse.IsDataSetServiceEnded(err) {
				return a.pendingOrAttentionWithCode(
					ctx, identity, copyRow, attemptID, AttentionDataSetUnavailable,
				)
			}
			if errors.Is(err, storage.ErrInvalidArgument) {
				return a.attention(ctx, identity, attemptID, AttentionSubmissionMismatch, false)
			}
			return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
		}
		return a.classifySDKStatus(ctx, identity, copyRow, attemptID, status)
	}
	if copyRow.CommitTransactionID != nil && *copyRow.CommitTransactionID != "" {
		return a.observeTransaction(ctx, input, copyRow)
	}
	if copyRow.CommitAttentionAt != nil {
		code := AttentionAttemptOnlyAmbiguous
		if copyRow.CommitAttentionCode != nil {
			if parsed, err := ParseAttentionCode(*copyRow.CommitAttentionCode); err == nil {
				code = parsed
			}
		}
		return AdvanceResult{
			State:         AdvanceNeedsAttention,
			AttemptID:     attemptID,
			AttentionCode: code,
		}, nil
	}
	requestCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	pieceStatus, err := input.Target.PieceStatus(requestCtx, input.Pieces[0].PieceCID)
	cancel()
	if err != nil && (errors.Is(err, storage.ErrDataSetUnavailable) || synapse.IsDataSetServiceEnded(err)) {
		return a.attention(ctx, identity, attemptID, AttentionDataSetUnavailable, false)
	}
	code := AttentionAttemptOnlyAmbiguous
	if err == nil && pieceStatus != nil && pieceStatus.Exists {
		code = AttentionUnattributedPiece
	}
	return a.attention(ctx, identity, attemptID, code, false)
}

func (a *Advancer) observeTransaction(
	ctx context.Context,
	input AdvanceInput,
	copyRow model.StorageUploadCopy,
) (AdvanceResult, error) {
	identity := copyIdentity(input)
	attemptID := *copyRow.CommitAttemptID
	transactionID := *copyRow.CommitTransactionID
	checker := a.StatusChecker
	if checker == nil {
		checker = synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{Timeout: a.requestTimeout()})
	}
	result, err := checker.GetAddPiecesStatus(ctx, synapse.AddPiecesStatusInput{
		ServiceURL:         input.Target.ServiceURL(),
		DataSetID:          input.Binding.DataSetID.String(),
		TransactionID:      transactionID,
		ExpectedPieceCount: len(input.Pieces),
	})
	if err != nil {
		if result.State == synapse.PDPStatusMismatch {
			return a.attention(ctx, identity, attemptID, AttentionSubmissionMismatch, false)
		}
		return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
	}
	switch result.State {
	case synapse.PDPStatusPending:
		return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
	case synapse.PDPStatusConfirmed:
		pieceIDs := make([]sdktypes.BigInt, 0, len(result.ConfirmedPieceIDs))
		for _, raw := range result.ConfirmedPieceIDs {
			pieceID, err := idtypes.ParseOnChainID("confirmed piece ID", raw)
			if err != nil {
				return a.attention(ctx, identity, attemptID, AttentionSubmissionMismatch, false)
			}
			pieceIDs = append(pieceIDs, pieceID.SDK())
		}
		ref, bound := input.Target.DataSetRef()
		if !bound {
			return a.attention(ctx, identity, attemptID, AttentionSubmissionMismatch, false)
		}
		return AdvanceResult{
			State:     AdvanceConfirmed,
			AttemptID: attemptID,
			Confirmation: &storage.CommitResult{
				TransactionID: transactionID,
				DataSet:       ref,
				PieceIDs:      pieceIDs,
			},
		}, nil
	case synapse.PDPStatusRejected:
		if err := a.reset(ctx, identity, attemptID, pdp.ErrTxRejected); err != nil {
			return AdvanceResult{}, err
		}
		return AdvanceResult{State: AdvanceRejected, AttemptID: attemptID}, nil
	case synapse.PDPStatusMismatch:
		return a.attention(ctx, identity, attemptID, AttentionSubmissionMismatch, false)
	default:
		return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
	}
}

func (a *Advancer) classifySDKStatus(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageUploadCopy,
	attemptID string,
	status *storage.CommitStatus,
) (AdvanceResult, error) {
	if status == nil {
		return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
	}
	switch status.State {
	case storage.CommitStatePending:
		return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
	case storage.CommitStateConfirmed:
		if status.DataSet == nil {
			return a.attention(ctx, identity, attemptID, AttentionSubmissionMismatch, false)
		}
		return AdvanceResult{
			State:     AdvanceConfirmed,
			AttemptID: attemptID,
			Confirmation: &storage.CommitResult{
				TransactionID:          status.TransactionID,
				ConfirmedTransactionID: status.ConfirmedTransactionID,
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
		return a.attention(ctx, identity, attemptID, AttentionSubmissionMismatch, false)
	}
}

func (a *Advancer) pendingOrAttention(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageUploadCopy,
	attemptID string,
) (AdvanceResult, error) {
	return a.pendingOrAttentionWithCode(
		ctx, identity, copyRow, attemptID, AttentionConfirmationTimeout,
	)
}

func (a *Advancer) pendingOrAttentionWithCode(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageUploadCopy,
	attemptID string,
	code AttentionCode,
) (AdvanceResult, error) {
	if copyRow.CommitAttentionAt != nil || a.now().Sub(*copyRow.CommitAttemptedAt) >= a.attentionAfter() {
		return a.attention(ctx, identity, attemptID, code, true)
	}
	return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
}

func (a *Advancer) attention(
	ctx context.Context,
	identity CopyIdentity,
	attemptID string,
	code AttentionCode,
	keepObserving bool,
) (AdvanceResult, error) {
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
		Continue:      keepObserving,
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
	copyRow model.StorageUploadCopy,
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

func (a *Advancer) commitExtraData(
	ctx context.Context,
	target synapse.DataSetTarget,
	copyRow model.StorageUploadCopy,
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
		input.Copy.ID <= 0 || input.Copy.UploadID <= 0 || input.Copy.CopyIndex < 0 ||
		input.Copy.StorageDataSetID == nil || *input.Copy.StorageDataSetID != input.Binding.ID ||
		len(input.Pieces) != 1 || !input.Pieces[0].PieceCID.Defined() {
		return errors.New("invalid storage commit advance input")
	}
	return nil
}

func copyIdentity(input AdvanceInput) CopyIdentity {
	return CopyIdentity{
		StorageUploadCopyID: input.Copy.ID,
		UploadID:            input.Copy.UploadID,
		CopyIndex:           input.Copy.CopyIndex,
		StorageDataSetID:    input.Binding.ID,
		RequireEligibleCopy: input.RequireEligibleCopy,
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

func definitelyNotSubmitted(err error) bool {
	if errors.Is(err, storage.ErrInvalidArgument) {
		return true
	}
	var httpErr *pdp.HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	if httpErr.StatusCode == http.StatusNotImplemented {
		return true
	}
	return httpErr.StatusCode >= http.StatusBadRequest &&
		httpErr.StatusCode < http.StatusInternalServerError &&
		httpErr.StatusCode != http.StatusRequestTimeout &&
		httpErr.StatusCode != http.StatusConflict
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
