package storagecommit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	// A provider keeps handling an add-pieces request for five minutes after
	// the client stops waiting, so a request whose outcome is unknown is sent
	// again only after that, doubling the wait per send up to 30 minutes.
	firstResendDelay = 5 * time.Minute
	maxResendDelay   = 30 * time.Minute
	// A refused submission is tried again after one minute, doubling per
	// consecutive refusal up to 30 minutes. From the fifth refusal on, the copy
	// gives up its place at the head of the data set's queue.
	rejectedRetryBaseDelay = time.Minute
	rejectedRetryMaxDelay  = 30 * time.Minute
	rejectionsKeepingHead  = 4
	// A chain read that failed, or a pre-submit check that could not complete,
	// is tried again after this.
	chainRetryDelay = time.Minute
)

type Advancer struct {
	Store Store
	// Nonces reads the chain record that settles an attempt the provider never
	// confirmed.
	Nonces         synapse.CommitNonceReader
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
	// Sent records the requests made for the unresolved attempt. Zero means
	// only the first one, sent at the attempt time.
	Sent SendHistory
	// Resend is set only in execute mode. It durably records next before effect
	// sends the attempt's signed request again. Without it an attempt whose
	// outcome is unknown is checked on chain but never sent.
	Resend func(ctx context.Context, next SendHistory, effect func(context.Context) error) (attempted bool, err error)
}

// SendHistory counts the requests sent for one attempt and when the latest
// went out.
type SendHistory struct {
	Sends      int
	LastSentAt time.Time
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
	return a.release(ctx, copyRow, ReleaseInput{
		Copy: CopyIdentity{
			StorageCopyID:    copyRow.ID,
			ContentID:        copyRow.ContentID,
			CopyIndex:        copyRow.CopyIndex,
			StorageDataSetID: binding.ID,
		},
		Reason: ReleaseOwnerTerminal, ClearReadyAt: true, ClearExtraData: true,
	})
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
			return a.release(ctx, copyRow, ReleaseInput{
				Copy: identity, Reason: ReleaseOwnerTerminal, ClearReadyAt: true, ClearExtraData: true,
			})
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
	beforeSubmit := ReleaseInput{Copy: identity, Reason: ReleaseBeforeSubmitCanceled}
	if err := context.Cause(ctx); err != nil {
		return a.release(ctx, copyRow, beforeSubmit)
	}
	request, requestErr := PrepareCommitRequest(ctx, a.Store, input.Target, copyRow, input.Pieces)
	if errors.Is(requestErr, ErrCommitRequestConflict) {
		if request.ExtraDataHex == "" {
			return AdvanceResult{}, requestErr
		}
	} else if requestErr != nil {
		if dataSetRefusesWrites(requestErr) {
			return a.releaseUnavailable(ctx, identity, copyRow, requestErr, false)
		}
		if _, releaseErr := a.release(ctx, copyRow, beforeSubmit); releaseErr != nil {
			return AdvanceResult{}, releaseErr
		}
		return AdvanceResult{}, requestErr
	}
	// Restored requests may already have an external effect. Establish their
	// fence before checking writability, and let recovery decide from the nonce;
	// conflicting requests stop it there before anything is sent.
	if request.MayHaveBeenSubmitted || requestErr != nil {
		attempt, err := a.Store.MarkCommitAttempted(ctx, AttemptInput{
			Copy: eligibleIdentity, AttemptID: attemptID, ExtraDataHex: request.ExtraDataHex, Now: a.now(),
		})
		if err != nil {
			return AdvanceResult{}, err
		}
		return a.observe(ctx, input, attempt.Copy)
	}
	extraHex := request.ExtraDataHex
	if request.HasHistory {
		requestCopy := copyRow
		requestCopy.CommitExtraDataHex = &extraHex
		proof, readErr := a.checkNonce(ctx, input, requestCopy)
		if readErr != nil || proof.outcome != nonceUnused {
			attempt, err := a.Store.MarkCommitAttempted(ctx, AttemptInput{
				Copy: eligibleIdentity, AttemptID: attemptID, ExtraDataHex: extraHex, Now: a.now(),
			})
			if err != nil {
				return AdvanceResult{}, err
			}
			if readErr != nil {
				return AdvanceResult{State: AdvancePending, AttemptID: attemptID, RetryAfter: chainRetryDelay}, readErr
			}
			if proof.outcome == nonceConflicts {
				return a.attentionForCopy(ctx, identity, attempt.Copy, attemptID, AttentionSubmissionMismatch, false)
			}
			return a.confirmedByNonce(attempt.Copy, attemptID, proof), nil
		}
	}
	// The SDK checks the data set again while submitting; checking first keeps
	// a data set that persistently refuses writes, or a chain that cannot be
	// read, from leaving an attempt whose outcome has to be recovered.
	if err := input.Target.CheckWritable(ctx); err != nil {
		if dataSetRefusesWrites(err) {
			return a.releaseUnavailable(ctx, identity, copyRow, err, false)
		}
		result, releaseErr := a.release(ctx, copyRow, beforeSubmit)
		if releaseErr != nil {
			return AdvanceResult{}, releaseErr
		}
		result.State, result.Cause, result.RetryAfter = AdvanceDeferred, err, chainRetryDelay
		return result, nil
	}
	attempt, err := a.Store.MarkCommitAttempted(ctx, AttemptInput{
		Copy:         eligibleIdentity,
		AttemptID:    attemptID,
		ExtraDataHex: extraHex,
		Now:          a.now(),
	})
	if err != nil {
		_, releaseErr := a.release(ctx, copyRow, beforeSubmit)
		if releaseErr != nil {
			return AdvanceResult{}, errors.Join(err, fmt.Errorf("releasing unattempted storage commit reservation: %w", releaseErr))
		}
		return AdvanceResult{}, err
	}
	if !attempt.Entered {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
	}
	if err := context.Cause(ctx); err != nil {
		beforeSubmit.KnownNotSubmitted = true
		return a.release(ctx, attempt.Copy, beforeSubmit)
	}
	extraData, err := decodeCommitExtraData(attempt.Copy.CommitExtraDataHex)
	if err != nil {
		if resetErr := a.reset(ctx, identity, attemptID, err); resetErr != nil {
			return AdvanceResult{}, resetErr
		}
		return AdvanceResult{State: AdvanceRejected, AttemptID: attemptID}, nil
	}
	sent := a.submit(ctx, input, identity, attemptID, extraData)
	return a.settleSubmit(ctx, identity, attempt.Copy, attemptID, sent, false)
}

// submitOutcome is what one SubmitCommit call left behind.
type submitOutcome struct {
	submission *storage.CommitSubmission
	err        error
	// observed reports that the SDK announced the submission, so the provider
	// accepted it whatever the call returned.
	observed bool
	// evidenceErr is set when the announced submission could not be recorded.
	evidenceErr error
}

func (a *Advancer) submit(
	ctx context.Context,
	input AdvanceInput,
	identity CopyIdentity,
	attemptID string,
	extraData []byte,
) submitOutcome {
	var observed atomic.Bool
	var evidenceErr atomic.Pointer[error]
	submission, err := input.Target.SubmitCommit(ctx, storage.CommitRequest{
		Pieces:    input.Pieces,
		ExtraData: extraData,
		OnSubmitted: func(submission storage.CommitSubmission) {
			observed.Store(true)
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
				evidenceErr.Store(&err)
			}
		},
	})
	outcome := submitOutcome{submission: submission, err: err, observed: observed.Load()}
	if recorded := evidenceErr.Load(); recorded != nil {
		outcome.evidenceErr = *recorded
	}
	return outcome
}

// settleSubmit records what a submission of the attempt's signed request
// produced, whether it was the first or a resend.
func (a *Advancer) settleSubmit(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
	sent submitOutcome,
	previousSubmission bool,
) (AdvanceResult, error) {
	pending := AdvanceResult{State: AdvancePending, AttemptID: attemptID}
	if submitErr := sent.err; submitErr != nil {
		if context.Cause(ctx) != nil {
			return pending, nil
		}
		if sent.observed {
			if sent.evidenceErr != nil {
				return pending, errors.Join(submitErr, fmt.Errorf("recording storage commit callback evidence: %w", sent.evidenceErr))
			}
			return pending, submitErr
		}
		// Refusal proves only this call did not submit. An earlier send of the
		// request can still land, including after an SDK writability refusal.
		if !previousSubmission {
			if dataSetRefusesWrites(submitErr) {
				return a.releaseUnavailable(ctx, identity, copyRow, submitErr, true)
			}
			switch synapse.ClassifyCommitRejection(submitErr) {
			case synapse.CommitRejectedDataSetUnavailable:
				return a.releaseUnavailable(ctx, identity, copyRow, submitErr, true)
			case synapse.CommitRejectedByProvider:
				return a.releaseRejected(ctx, identity, copyRow, submitErr)
			}
		}
		// Any other failure may have reached the chain, so the attempt fence
		// stays and recovery reads the nonce before anything is sent again. The
		// error still travels back so the caller can record why.
		evidenceCtx, cancel := evidenceContext(ctx)
		recordErr := a.Store.RecordCommitSubmitFailure(evidenceCtx, SubmitFailureInput{
			Copy:      identity,
			AttemptID: attemptID,
			Message:   synapse.ErrorSummary(submitErr),
			Now:       a.now(),
		})
		cancel()
		if recordErr != nil {
			return pending, errors.Join(submitErr, &SubmitFailureRecordError{Err: recordErr})
		}
		// A refused resend may mean the provider no longer holds the piece,
		// which only the caller can check and repair.
		if previousSubmission && synapse.ClassifyCommitRejection(submitErr) == synapse.CommitRejectedByProvider {
			return AdvanceResult{State: AdvanceResendRefused, AttemptID: attemptID, Cause: submitErr}, nil
		}
		return pending, submitErr
	}
	if sent.submission == nil {
		if sent.evidenceErr != nil {
			return pending, fmt.Errorf("recording storage commit callback evidence: %w", sent.evidenceErr)
		}
		return pending, nil
	}
	evidenceCtx, cancel := evidenceContext(ctx)
	err := a.Store.RecordCommitSubmission(evidenceCtx, EvidenceInput{
		Copy:          identity,
		AttemptID:     attemptID,
		TransactionID: sent.submission.TransactionID,
		StatusURL:     sent.submission.StatusURL,
		Now:           a.now(),
	})
	cancel()
	if err != nil {
		return pending, err
	}
	return AdvanceResult{State: AdvanceSubmitted, AttemptID: attemptID}, nil
}

// SubmitFailureRecordError reports that the provider's failed submission reply
// could not be retained, independently of the provider error itself.
type SubmitFailureRecordError struct {
	Err error
}

func (e *SubmitFailureRecordError) Error() string {
	return fmt.Sprintf("recording storage commit submit failure: %v", e.Err)
}

func (e *SubmitFailureRecordError) Unwrap() error { return e.Err }

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
	if copyRow.CommitStatusURL == nil || *copyRow.CommitStatusURL == "" {
		return a.recoverUnobserved(ctx, input, identity, copyRow, attemptID)
	}
	requestCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	status, err := input.Target.GetCommitStatus(requestCtx, *copyRow.CommitStatusURL)
	cancel()
	if err != nil {
		if context.Cause(ctx) != nil {
			return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
		}
		if errors.Is(err, storage.ErrInvalidArgument) || errors.Is(err, pdp.ErrInvalidStatus) {
			return a.rejectUnlessRegistered(ctx, input, identity, copyRow, attemptID, ErrCommitReceiptMismatch)
		}
		return a.settleByNonceOr(ctx, input, identity, copyRow, attemptID, func() (AdvanceResult, error) {
			if errors.Is(err, storage.ErrDataSetUnavailable) || synapse.IsDataSetServiceEnded(err) {
				return a.pendingOrAttentionWithCode(
					ctx, identity, copyRow, attemptID, AttentionDataSetUnavailable,
				)
			}
			return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
		})
	}
	return a.classifySDKStatus(ctx, input, identity, copyRow, attemptID, status)
}

func (a *Advancer) classifySDKStatus(
	ctx context.Context,
	input AdvanceInput,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
	status *storage.CommitStatus,
) (AdvanceResult, error) {
	// A receipt that does not describe this attempt proves nothing about it.
	mismatch := func() (AdvanceResult, error) {
		return a.rejectUnlessRegistered(ctx, input, identity, copyRow, attemptID, ErrCommitReceiptMismatch)
	}
	if status == nil {
		return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
	}
	ref, bound := input.Target.DataSetRef()
	if !bound || status.DataSet == nil || status.Kind != storage.CommitKindAddPieces ||
		copyRow.CommitTransactionID == nil || status.TransactionID != *copyRow.CommitTransactionID ||
		!status.DataSet.Equal(ref) {
		return mismatch()
	}
	switch status.State {
	case storage.CommitStatePending:
		// A provider still tracking its transaction is trusted until the attempt
		// is old enough to need attention; past that, the transaction it
		// reports may have been replaced by one the chain did include.
		if copyRow.CommitAttentionAt == nil && a.now().Sub(*copyRow.CommitAttemptedAt) < a.attentionAfter() {
			return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, nil
		}
		return a.settleByNonceOr(ctx, input, identity, copyRow, attemptID, func() (AdvanceResult, error) {
			return a.pendingOrAttention(ctx, identity, copyRow, attemptID)
		})
	case storage.CommitStateConfirmed:
		if len(status.PieceIDs) != len(input.Pieces) {
			return mismatch()
		}
		return AdvanceResult{
			State:        AdvanceConfirmed,
			AttemptID:    attemptID,
			ExtraDataHex: taskDeref(copyRow.CommitExtraDataHex),
			Confirmation: &storage.CommitResult{
				TransactionID:          status.TransactionID,
				ConfirmedTransactionID: confirmedTransactionID(status.TransactionID, status.ConfirmedTransactionID),
				DataSet:                *status.DataSet,
				PieceIDs:               status.PieceIDs,
			},
		}, nil
	case storage.CommitStateRejected:
		// The reported transaction failed, but another one carrying the same
		// signed request may still have been included.
		return a.rejectUnlessRegistered(ctx, input, identity, copyRow, attemptID, pdp.ErrTxRejected)
	default:
		return mismatch()
	}
}

// ErrCommitReceiptMismatch records an attempt rejected because the provider's
// receipt for it named another transaction, data set, kind, or piece count, or
// could not be read as a receipt at all.
var ErrCommitReceiptMismatch = errors.New("storage provider reported a registration that does not match this request")

// rejectUnlessRegistered rejects the attempt unless its own nonce proves the
// registration. Rejecting is safe even when the chain cannot be read: the copy
// keeps its request, so the next attempt can only land the same nonce.
func (a *Advancer) rejectUnlessRegistered(
	ctx context.Context,
	input AdvanceInput,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
	cause error,
) (AdvanceResult, error) {
	return a.settleByNonceOr(ctx, input, identity, copyRow, attemptID, func() (AdvanceResult, error) {
		if err := a.reset(ctx, identity, attemptID, cause); err != nil {
			return AdvanceResult{}, err
		}
		return AdvanceResult{State: AdvanceRejected, AttemptID: attemptID, Cause: cause}, nil
	})
}

// recoverUnobserved resolves an attempt the provider never acknowledged. The
// chain's nonce record decides: a consumed nonce proves where the pieces
// landed, and an unused one lets execute send the same signed request again,
// which FWSS can accept at most once.
func (a *Advancer) recoverUnobserved(
	ctx context.Context,
	input AdvanceInput,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
) (AdvanceResult, error) {
	waitForChain := AdvanceResult{State: AdvancePending, AttemptID: attemptID, RetryAfter: chainRetryDelay}
	proof, err := a.checkNonce(ctx, input, copyRow)
	if err != nil {
		return waitForChain, err
	}
	switch proof.outcome {
	case nonceProvesCommit:
		return a.confirmedByNonce(copyRow, attemptID, proof), nil
	case nonceConflicts:
		return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionSubmissionMismatch, false)
	}
	// The copy's other requests cannot disprove this attempt's own nonce, but
	// they decide whether sending it again could add the piece a second time.
	if _, err := selectCommitRequest(ctx, a.Store, copyRow); err != nil {
		if errors.Is(err, ErrCommitRequestConflict) {
			return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionSubmissionMismatch, false)
		}
		return waitForChain, err
	}
	// Like a provider still reporting pending, a request unregistered past the
	// attention threshold is flagged while recovery keeps sending it.
	if copyRow.CommitAttentionAt == nil && a.now().Sub(*copyRow.CommitAttemptedAt) >= a.attentionAfter() {
		if _, err := a.attention(ctx, identity, attemptID, AttentionConfirmationTimeout, true); err != nil {
			return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, err
		}
	}
	sent := input.Sent
	if sent.Sends < 1 || sent.LastSentAt.IsZero() {
		sent = SendHistory{Sends: 1, LastSentAt: *copyRow.CommitAttemptedAt}
	}
	if wait := sent.LastSentAt.Add(resendDelay(sent.Sends)).Sub(a.now()); wait > 0 {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID, RetryAfter: wait}, nil
	}
	if input.Resend == nil {
		return AdvanceResult{State: AdvanceResendDue, AttemptID: attemptID}, nil
	}
	return a.resend(ctx, input, identity, copyRow, attemptID, sent)
}

func (a *Advancer) resend(
	ctx context.Context,
	input AdvanceInput,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
	sent SendHistory,
) (AdvanceResult, error) {
	extraData, err := decodeCommitExtraData(copyRow.CommitExtraDataHex)
	if err != nil {
		return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionSubmissionMismatch, false)
	}
	var outcome submitOutcome
	next := SendHistory{Sends: sent.Sends + 1, LastSentAt: a.now()}
	attempted, err := input.Resend(ctx, next, func(ctx context.Context) error {
		outcome = a.submit(ctx, input, identity, attemptID, extraData)
		return nil
	})
	if !attempted {
		return AdvanceResult{State: AdvancePending, AttemptID: attemptID}, err
	}
	return a.settleSubmit(ctx, identity, copyRow, attemptID, outcome, true)
}

type nonceOutcome uint8

const (
	nonceUnused nonceOutcome = iota
	nonceProvesCommit
	// nonceConflicts means the nonce was consumed somewhere other than this
	// copy's piece in its data set, or the signed request cannot be read.
	nonceConflicts
)

type nonceProof struct {
	outcome      nonceOutcome
	confirmation storage.CommitResult
}

// checkNonce reads what the chain recorded for the attempt's signed request.
func (a *Advancer) checkNonce(ctx context.Context, input AdvanceInput, copyRow model.StorageCopy) (nonceProof, error) {
	if a.Nonces == nil {
		return nonceProof{}, errors.New("storage confirmation chain reader is unavailable")
	}
	extraData, err := decodeCommitExtraData(copyRow.CommitExtraDataHex)
	if err != nil {
		return nonceProof{outcome: nonceConflicts}, nil
	}
	nonce, err := ExtraDataNonce(extraData)
	if err != nil {
		return nonceProof{outcome: nonceConflicts}, nil
	}
	requestCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	defer cancel()
	state, err := a.Nonces.ClientNonce(requestCtx, nonce)
	if err != nil {
		return nonceProof{}, err
	}
	if !state.Consumed {
		return nonceProof{outcome: nonceUnused}, nil
	}
	ref, bound := input.Target.DataSetRef()
	if !bound || !state.DataSetID.Equal(ref.DataSetID()) {
		return nonceProof{outcome: nonceConflicts}, nil
	}
	pieceIDs, err := state.PieceIDs(len(input.Pieces))
	if err != nil {
		return nonceProof{outcome: nonceConflicts}, nil
	}
	for i, pieceID := range pieceIDs {
		pieceCID, err := a.Nonces.PieceCIDAt(requestCtx, state.DataSetID, pieceID)
		if err != nil {
			return nonceProof{}, err
		}
		if !pieceCID.Equals(input.Pieces[i].PieceCID) {
			return nonceProof{outcome: nonceConflicts}, nil
		}
	}
	return nonceProof{
		outcome: nonceProvesCommit,
		confirmation: storage.CommitResult{
			TransactionID: taskDeref(copyRow.CommitTransactionID),
			DataSet:       ref,
			PieceIDs:      pieceIDs,
		},
	}, nil
}

// settleByNonceOr lets the chain settle an attempt the provider could not
// confirm. When the chain shows nothing, or cannot be read, the provider's
// answer decides through fallback.
func (a *Advancer) settleByNonceOr(
	ctx context.Context,
	input AdvanceInput,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	attemptID string,
	fallback func() (AdvanceResult, error),
) (AdvanceResult, error) {
	proof, err := a.checkNonce(ctx, input, copyRow)
	if err != nil {
		return fallback()
	}
	switch proof.outcome {
	case nonceProvesCommit:
		return a.confirmedByNonce(copyRow, attemptID, proof), nil
	case nonceConflicts:
		return a.attentionForCopy(ctx, identity, copyRow, attemptID, AttentionSubmissionMismatch, false)
	default:
		return fallback()
	}
}

// confirmedByNonce settles an attempt on the chain's nonce record. A recorded
// transaction stays the attempt's submission, but the transaction that
// actually included the pieces may be another one, so none is confirmed.
func (a *Advancer) confirmedByNonce(copyRow model.StorageCopy, attemptID string, proof nonceProof) AdvanceResult {
	confirmation := proof.confirmation
	return AdvanceResult{
		State:         AdvanceConfirmed,
		AttemptID:     attemptID,
		Confirmation:  &confirmation,
		ExtraDataHex:  taskDeref(copyRow.CommitExtraDataHex),
		ProvenByNonce: true,
	}
}

func resendDelay(sends int) time.Duration {
	delay := firstResendDelay
	for i := 1; i < sends && delay < maxResendDelay; i++ {
		delay *= 2
	}
	return min(delay, maxResendDelay)
}

func rejectedRetryDelay(rejections int) time.Duration {
	delay := rejectedRetryBaseDelay
	for i := 1; i < rejections && delay < rejectedRetryMaxDelay; i++ {
		delay *= 2
	}
	return min(delay, rejectedRetryMaxDelay)
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

// reset rejects an attempt whose transaction failed. The copy keeps its signed
// request, so a later attempt can only ever land the same nonce.
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

// release resolves the copy's unresolved attempt with input; the attempt and
// time are filled in here.
func (a *Advancer) release(ctx context.Context, copyRow model.StorageCopy, input ReleaseInput) (AdvanceResult, error) {
	if copyRow.CommitAttemptID == nil || *copyRow.CommitAttemptID == "" {
		return AdvanceResult{State: AdvanceReleased, ReleaseReason: input.Reason}, nil
	}
	input.AttemptID = *copyRow.CommitAttemptID
	input.Now = a.now()
	evidenceCtx, cancel := evidenceContext(ctx)
	err := a.Store.ReleaseCommitAttempt(evidenceCtx, input)
	cancel()
	if err != nil {
		return AdvanceResult{}, err
	}
	return AdvanceResult{
		State:         AdvanceReleased,
		ReleaseReason: input.Reason,
		AttemptID:     input.AttemptID,
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
	result, err := a.release(ctx, copyRow, ReleaseInput{
		Copy: identity, Reason: ReleaseDataSetUnavailable, KnownNotSubmitted: knownNotSubmitted,
		ClearReadyAt: true, ClearExtraData: true,
	})
	if err != nil {
		return result, err
	}
	result.Cause = cause
	return result, nil
}

// releaseRejected releases an attempt the provider refused before sending a
// transaction. The copy keeps its signed request; consecutive refusals back
// off and eventually move the copy to the end of the queue.
func (a *Advancer) releaseRejected(
	ctx context.Context,
	identity CopyIdentity,
	copyRow model.StorageCopy,
	cause error,
) (AdvanceResult, error) {
	earlier, err := a.Store.CountConsecutiveCommitRejections(ctx, identity)
	if err != nil {
		return AdvanceResult{State: AdvancePending, AttemptID: taskDeref(copyRow.CommitAttemptID)}, errors.Join(cause, err)
	}
	rejections := earlier + 1
	result, err := a.release(ctx, copyRow, ReleaseInput{
		Copy: identity, Reason: ReleaseProviderRejected, KnownNotSubmitted: true,
		ClearReadyAt: rejections > rejectionsKeepingHead, SubmitError: synapse.ErrorSummary(cause),
	})
	if err != nil {
		return result, err
	}
	result.State, result.Cause, result.RetryAfter = AdvanceDeferred, cause, rejectedRetryDelay(rejections)
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

func taskDeref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
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
