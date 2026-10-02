package storagecommit_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/uptrace/bun"
)

func TestAdvancerPersistsFourSubmissionsBeforeConfirmationAndAdmitsFIFO(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 5)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
	dataSetRef, ok := target.DataSetRef()
	if !ok {
		t.Fatal("mock target has no data set ref")
	}

	var presignCalls, submissionCalls, confirmationCalls int
	submittedTransactions := make(map[string]string)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		presignCalls++
		return testutil.CommitExtraData(7), nil
	}
	target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		submissionCalls++
		tx := fmt.Sprintf("0x%064x", submissionCalls)
		submission := storage.CommitSubmission{
			Kind: storage.CommitKindAddPieces, TransactionID: tx,
			StatusURL:  fmt.Sprintf("https://provider.example/status/%d", submissionCalls),
			ProviderID: binding.ProviderID.SDK(), DataSet: &dataSetRef,
			PieceCIDs: []cid.Cid{pieceCID},
		}
		submittedTransactions[submission.StatusURL] = tx
		if request.OnSubmitted != nil {
			request.OnSubmitted(submission)
		}
		return &submission, nil
	}
	confirm := false
	target.GetCommitStatusFunc = func(_ context.Context, statusURL string) (*storage.CommitStatus, error) {
		confirmationCalls++
		tx := submittedTransactions[statusURL]
		if !confirm {
			return &storage.CommitStatus{Kind: storage.CommitKindAddPieces, State: storage.CommitStatePending, TransactionID: tx, DataSet: &dataSetRef}, nil
		}
		return &storage.CommitStatus{
			Kind: storage.CommitKindAddPieces, State: storage.CommitStateConfirmed, TransactionID: tx,
			DataSet:  &dataSetRef,
			PieceIDs: []sdktypes.BigInt{sdktypes.NewBigInt(5001)},
		}, nil
	}
	advancer := storagecommit.Advancer{Store: repos.Contents}

	for i := range 4 {
		result, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
			Copy: copies[i], Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
		})
		if err != nil || result.State != storagecommit.AdvanceSubmitted {
			t.Fatalf("submission %d = %#v err=%v", i, result, err)
		}
	}
	if confirmationCalls != 0 {
		t.Fatalf("confirmation calls = %d before any confirmation run, want 0", confirmationCalls)
	}
	for i := range 4 {
		persisted := loadAdvancerCopy(t, repos, copies[i].ID)
		if persisted.CommitAttemptID == nil || persisted.CommitAttemptedAt == nil ||
			persisted.CommitTransactionID == nil || persisted.CommitStatusURL == nil {
			t.Fatalf("copy %d submission evidence was not fully persisted: %#v", i, persisted)
		}
	}

	fifth, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[4], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || fifth.State != storagecommit.AdvanceWaitingCapacity {
		t.Fatalf("fifth advance = %#v err=%v, want waiting capacity", fifth, err)
	}
	if presignCalls != 4 || submissionCalls != 4 || confirmationCalls != 0 {
		t.Fatalf("SDK calls after capacity wait = presign %d submit %d confirm %d, want 4/4/0", presignCalls, submissionCalls, confirmationCalls)
	}
	fifthPersisted := loadAdvancerCopy(t, repos, copies[4].ID)
	if fifthPersisted.CommitAttemptID != nil || fifthPersisted.CommitReadyAt == nil {
		t.Fatalf("fifth copy = %#v, want durable FIFO wait without attempt", fifthPersisted)
	}

	first := loadAdvancerCopy(t, repos, copies[0].ID)
	confirm = true
	settled, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *first, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || settled.State != storagecommit.AdvanceConfirmed || settled.Confirmation == nil {
		t.Fatalf("settle first = %#v err=%v", settled, err)
	}
	if settled.Confirmation.ConfirmedTransactionID != settled.Confirmation.TransactionID {
		t.Fatalf("confirmed transaction = %q, want fallback %q", settled.Confirmation.ConfirmedTransactionID, settled.Confirmation.TransactionID)
	}
	pieceID := idtypes.OnChainIDFromSDK(settled.Confirmation.PieceIDs[0])
	settlement := repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[0].ID, ContentID: copies[0].ContentID, CopyIndex: 0,
		PieceCID: pieceCID.String(), PieceID: &pieceID, RetrievalURL: target.PieceURL(pieceCID),
		CommitExtraDataHex: *first.CommitExtraDataHex, CommitTransactionID: settled.Confirmation.TransactionID,
		CommitAttemptID: settled.AttemptID, CommitConfirmedTransactionID: settled.Confirmation.ConfirmedTransactionID,
	}
	if err := repos.Contents.MarkUploadCopyCommitted(t.Context(), settlement); err != nil {
		t.Fatalf("settle first copy: %v", err)
	}
	if err := repos.Contents.MarkUploadCopyCommitted(t.Context(), settlement); err != nil {
		t.Fatalf("replay first copy settlement: %v", err)
	}
	conflictingSettlement := settlement
	conflictingSettlement.CommitConfirmedTransactionID = "0xconflicting-confirmation"
	if err := repos.Contents.MarkUploadCopyCommitted(t.Context(), conflictingSettlement); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("conflicting first copy settlement = %v, want ErrConflict", err)
	}

	fifthPersisted = loadAdvancerCopy(t, repos, copies[4].ID)
	fifth, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *fifthPersisted, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || fifth.State != storagecommit.AdvanceSubmitted {
		t.Fatalf("fifth advance after settlement = %#v err=%v, want submitted", fifth, err)
	}
	if submissionCalls != 5 {
		t.Fatalf("submission calls = %d, want 5", submissionCalls)
	}
}

func TestAdvancerRejectsMismatchedCommitStatus(t *testing.T) {
	for _, mismatch := range []string{"transaction", "kind", "data set", "piece count", "rejected transaction"} {
		t.Run(mismatch, func(t *testing.T) {
			db := testutil.NewTestDB(t)
			repos := repository.NewRepositories(db)
			binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
			identity := advancerCopyIdentity(copies[0])
			if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
				Copy: identity, AttemptID: "status-mismatch",
			}); err != nil {
				t.Fatalf("reserve: %v", err)
			}
			if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
				Copy: identity, AttemptID: "status-mismatch", ExtraDataHex: testutil.CommitExtraDataHex(7),
			}); err != nil {
				t.Fatalf("mark attempted: %v", err)
			}
			const statusURL = "https://provider.example/status/commit"
			if err := repos.Contents.RecordCommitSubmission(t.Context(), storagecommit.EvidenceInput{
				Copy: identity, AttemptID: "status-mismatch", TransactionID: "0xexpected", StatusURL: statusURL,
			}); err != nil {
				t.Fatalf("record status URL: %v", err)
			}
			target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
			target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
			ref, ok := target.DataSetRef()
			if !ok {
				t.Fatal("mock target has no data set ref")
			}
			status := &storage.CommitStatus{
				Kind: storage.CommitKindAddPieces, State: storage.CommitStateConfirmed,
				TransactionID: "0xexpected", DataSet: &ref,
				PieceIDs: []sdktypes.BigInt{sdktypes.NewBigInt(5001)},
			}
			switch mismatch {
			case "transaction":
				status.TransactionID = "0xother"
			case "kind":
				status.Kind = storage.CommitKindCreateAndAdd
			case "data set":
				other, err := storage.NewDataSetRef(binding.ProviderID.SDK(), sdktypes.NewBigInt(9999), sdktypes.NewBigInt(9001))
				if err != nil {
					t.Fatal(err)
				}
				status.DataSet = &other
			case "piece count":
				status.PieceIDs = nil
			case "rejected transaction":
				status.State = storage.CommitStateRejected
				status.TransactionID = "0xother"
			}
			target.GetCommitStatusFunc = func(_ context.Context, gotURL string) (*storage.CommitStatus, error) {
				if gotURL != statusURL {
					t.Fatalf("status URL = %q, want %q", gotURL, statusURL)
				}
				return status, nil
			}
			result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
				Copy: *loadAdvancerCopy(t, repos, copies[0].ID), Binding: *binding, Target: target,
				Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
			})
			if err != nil || result.State != storagecommit.AdvanceNeedsAttention ||
				result.AttentionCode != storagecommit.AttentionSubmissionMismatch || result.Confirmation != nil {
				t.Fatalf("mismatched status = %#v err=%v", result, err)
			}
		})
	}
}

func TestCommitEvidenceIsAtomicAndIdempotent(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	_, copies, _ := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "atomic-evidence",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "atomic-evidence", ExtraDataHex: testutil.CommitExtraDataHex(7),
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	evidence := storagecommit.EvidenceInput{
		Copy: identity, AttemptID: "atomic-evidence",
		TransactionID: "0xsubmitted", StatusURL: "https://provider.example/status/submitted",
	}
	withoutURL := evidence
	withoutURL.StatusURL = ""
	if err := repos.Contents.RecordCommitSubmission(t.Context(), withoutURL); err == nil {
		t.Fatal("accepted submission evidence without status URL")
	}
	before := loadAdvancerCopy(t, repos, copies[0].ID)
	if before.CommitTransactionID != nil || before.CommitStatusURL != nil {
		t.Fatalf("invalid evidence partially persisted: %#v", before)
	}
	if err := repos.Contents.RecordCommitSubmission(t.Context(), evidence); err != nil {
		t.Fatalf("record evidence: %v", err)
	}
	if err := repos.Contents.RecordCommitSubmission(t.Context(), evidence); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	conflict := evidence
	conflict.StatusURL = "https://provider.example/status/other"
	if err := repos.Contents.RecordCommitSubmission(t.Context(), conflict); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("conflicting URL error = %v, want conflict", err)
	}
	conflict = evidence
	conflict.TransactionID = "0xother"
	if err := repos.Contents.RecordCommitSubmission(t.Context(), conflict); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("conflicting transaction error = %v, want conflict", err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	if copyRow.CommitTransactionID == nil || *copyRow.CommitTransactionID != evidence.TransactionID ||
		copyRow.CommitStatusURL == nil || *copyRow.CommitStatusURL != evidence.StatusURL {
		t.Fatalf("conflicting writes changed evidence: %#v", copyRow)
	}
}

func TestAdvancerOwnerTerminalReleasesUnattemptedReservationWithoutSDK(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, _ := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "owner-terminal-attempt",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	result, err := (&storagecommit.Advancer{Store: repos.Contents}).ReleaseTerminalReservation(
		t.Context(), *copyRow, *binding,
	)
	if err != nil || result.State != storagecommit.AdvanceReleased || result.ReleaseReason != storagecommit.ReleaseOwnerTerminal {
		t.Fatalf("advance = %#v err=%v", result, err)
	}
	copyRow = loadAdvancerCopy(t, repos, copies[0].ID)
	if copyRow.CommitAttemptID != nil || copyRow.CommitReadyAt != nil || copyRow.CommitAttemptedAt != nil {
		t.Fatalf("released copy retains reservation: %#v", copyRow)
	}
}

func TestAdvancerOwnerTerminalClearsFIFOReadinessWithoutReservation(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 5)
	for i := range 4 {
		if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
			Copy: advancerCopyIdentity(copies[i]), AttemptID: fmt.Sprintf("capacity-%d", i),
		}); err != nil {
			t.Fatalf("reserve capacity %d: %v", i, err)
		}
	}
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	advancer := storagecommit.Advancer{Store: repos.Contents}
	waiting, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[4], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || waiting.State != storagecommit.AdvanceWaitingCapacity {
		t.Fatalf("capacity wait = %#v err=%v", waiting, err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[4].ID)
	if copyRow.CommitAttemptID != nil || copyRow.CommitReadyAt == nil {
		t.Fatalf("waiting copy = %#v, want FIFO readiness without reservation", copyRow)
	}

	released, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target, OwnerTerminal: true,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || released.State != storagecommit.AdvanceReleased || released.ReleaseReason != storagecommit.ReleaseOwnerTerminal {
		t.Fatalf("owner-terminal advance = %#v err=%v", released, err)
	}
	copyRow = loadAdvancerCopy(t, repos, copies[4].ID)
	if copyRow.CommitReadyAt != nil || copyRow.CommitAttemptID != nil {
		t.Fatalf("owner-terminal copy retains FIFO readiness: %#v", copyRow)
	}
}

func TestAdvancerDataSetUnavailableSeparatesReservationFromAttempt(t *testing.T) {
	t.Run("before submit releases reservation", func(t *testing.T) {
		db := testutil.NewTestDB(t)
		repos := repository.NewRepositories(db)
		binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
		target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
		target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
			return nil, storage.ErrDataSetUnavailable
		}
		target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
			t.Fatal("preflight failure entered SubmitCommit")
			return nil, nil
		}

		result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
			Copy: copies[0], Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
		})
		if err != nil || result.State != storagecommit.AdvanceReleased ||
			result.ReleaseReason != storagecommit.ReleaseDataSetUnavailable {
			t.Fatalf("advance = %#v err=%v", result, err)
		}
		persisted := loadAdvancerCopy(t, repos, copies[0].ID)
		if persisted.CommitAttemptID != nil || persisted.CommitAttemptedAt != nil || persisted.CommitReadyAt != nil {
			t.Fatalf("preflight release retained commit state: %#v", persisted)
		}
	})

	t.Run("SDK pre-submit rejection releases attempted fence", func(t *testing.T) {
		db := testutil.NewTestDB(t)
		repos := repository.NewRepositories(db)
		binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
		target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
		target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
			return testutil.CommitExtraData(7), nil
		}
		target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
			return nil, storage.ErrDataSetUnavailable
		}

		result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
			Copy: copies[0], Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
		})
		if err != nil || result.State != storagecommit.AdvanceReleased ||
			result.ReleaseReason != storagecommit.ReleaseDataSetUnavailable {
			t.Fatalf("advance = %#v err=%v", result, err)
		}
		persisted := loadAdvancerCopy(t, repos, copies[0].ID)
		if persisted.CommitAttemptID != nil || persisted.CommitAttemptedAt != nil ||
			persisted.CommitAttentionAt != nil {
			t.Fatalf("pre-submit rejection retained commit state: %#v", persisted)
		}
	})
}

// A 5xx may come after the provider sent its transaction, so the attempt stays
// fenced. A 4xx is answered before any transaction: the attempt is released and
// the copy keeps its signed request, except when the data set itself is gone.
func TestAdvancerSubmitErrorFollowsProviderStatus(t *testing.T) {
	for _, tt := range []struct {
		status     int
		wantState  storagecommit.AdvanceState
		wantReason storagecommit.ReleaseReason
	}{
		{status: 500, wantState: storagecommit.AdvancePending},
		{status: 400, wantState: storagecommit.AdvanceDeferred, wantReason: storagecommit.ReleaseProviderRejected},
		{status: 429, wantState: storagecommit.AdvanceDeferred, wantReason: storagecommit.ReleaseProviderRejected},
		{status: 404, wantState: storagecommit.AdvanceReleased, wantReason: storagecommit.ReleaseDataSetUnavailable},
		{status: 409, wantState: storagecommit.AdvanceReleased, wantReason: storagecommit.ReleaseDataSetUnavailable},
	} {
		t.Run(fmt.Sprint(tt.status), func(t *testing.T) {
			db := testutil.NewTestDB(t)
			repos := repository.NewRepositories(db)
			binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
			target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
			target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
				return testutil.CommitExtraData(7), nil
			}
			submitErr := fmt.Errorf("add pieces: %w", &pdp.HTTPError{
				Method: "POST", URL: "https://provider.example/pdp/data-sets/1/pieces", StatusCode: tt.status, Body: "piece not found",
			})
			target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
				return nil, submitErr
			}

			result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
				Copy: copies[0], Binding: *binding, Target: target,
				Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
			})
			persisted := loadAdvancerCopy(t, repos, copies[0].ID)
			reply := fmt.Sprintf("provider returned HTTP %d: piece not found", tt.status)
			switch tt.wantState {
			case storagecommit.AdvancePending:
				if !errors.Is(err, submitErr) || result.State != tt.wantState {
					t.Fatalf("advance = %#v err=%v, want a fenced pending result carrying the submit error", result, err)
				}
				if persisted.Status != model.StorageCopyStatusCommitting || persisted.CommitAttemptID == nil ||
					persisted.CommitSubmitError == nil || *persisted.CommitSubmitError != reply {
					t.Fatalf("copy = %#v, want the attempt fenced with the provider's reply", persisted)
				}
				return
			case storagecommit.AdvanceDeferred:
				if err != nil || result.State != tt.wantState || result.ReleaseReason != tt.wantReason ||
					result.RetryAfter != time.Minute || !errors.Is(result.Cause, submitErr) {
					t.Fatalf("advance = %#v err=%v, want a deferred release", result, err)
				}
				if persisted.CommitReadyAt == nil || persisted.CommitExtraDataHex == nil || *persisted.CommitExtraDataHex != testutil.CommitExtraDataHex(7) {
					t.Fatalf("copy = %#v, want its queue place and signed request kept", persisted)
				}
			default:
				if err != nil || result.State != tt.wantState || result.ReleaseReason != tt.wantReason {
					t.Fatalf("advance = %#v err=%v, want a released unavailable data set", result, err)
				}
				if persisted.CommitExtraDataHex != nil {
					t.Fatalf("copy = %#v, want the request dropped with the data set", persisted)
				}
			}
			if persisted.Status != model.StorageCopyStatusPieceReady || persisted.CommitAttemptID != nil {
				t.Fatalf("copy = %#v, want the attempt released", persisted)
			}
			attempt := loadAdvancerAttempt(t, db, result.AttemptID)
			if attempt.Status != storagecommit.AttemptStatusReleased || attempt.ReleaseReason == nil ||
				*attempt.ReleaseReason != string(tt.wantReason) {
				t.Fatalf("attempt = %#v, want released for %s", attempt, tt.wantReason)
			}
			if tt.wantReason == storagecommit.ReleaseProviderRejected && (attempt.SubmitError == nil || *attempt.SubmitError != reply) {
				t.Fatalf("submit error = %v, want the provider's reply", attempt.SubmitError)
			}
		})
	}
}

// Each consecutive refusal doubles the wait. From the fifth on, the copy gives
// up its place at the head of the queue.
func TestAdvancerConsecutiveRejectionsBackOffThenYieldQueueHead(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	var signed int
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		signed++
		return testutil.CommitExtraData(7), nil
	}
	target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
		return nil, &pdp.HTTPError{StatusCode: 429}
	}
	advancer := storagecommit.Advancer{Store: repos.Contents, Nonces: &testutil.MockCommitNonces{}}
	for i, wantDelay := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute} {
		result, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
			Copy: *loadAdvancerCopy(t, repos, copies[0].ID), Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
		})
		if err != nil || result.State != storagecommit.AdvanceDeferred || result.RetryAfter != wantDelay {
			t.Fatalf("refusal %d = %#v err=%v, want a %s wait", i+1, result, err, wantDelay)
		}
		persisted := loadAdvancerCopy(t, repos, copies[0].ID)
		if keepsHead := i < 4; (persisted.CommitReadyAt != nil) != keepsHead {
			t.Fatalf("refusal %d left commit_ready_at = %v, want kept=%v", i+1, persisted.CommitReadyAt, keepsHead)
		}
	}
	if signed != 1 {
		t.Fatalf("requests signed = %d, want one for every refusal", signed)
	}
}

// A data set whose state cannot be read before submitting releases the
// reservation and waits, rather than leaving an attempt to recover.
func TestAdvancerWriteCheckFailureReleasesReservation(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return testutil.CommitExtraData(7), nil
	}
	readErr := errors.New("chain RPC unavailable")
	target.CheckWritableFunc = func(context.Context) error { return readErr }
	target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
		t.Fatal("a failed write check reached SubmitCommit")
		return nil, nil
	}

	result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceDeferred || result.ReleaseReason != storagecommit.ReleaseBeforeSubmitCanceled ||
		result.RetryAfter != time.Minute || !errors.Is(result.Cause, readErr) {
		t.Fatalf("advance = %#v err=%v, want a deferred reservation release", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.CommitAttemptID != nil || persisted.CommitAttemptedAt != nil || persisted.CommitReadyAt == nil {
		t.Fatalf("copy = %#v, want no attempt and its queue place kept", persisted)
	}
	if attempt := loadAdvancerAttempt(t, db, result.AttemptID); attempt.Status != storagecommit.AttemptStatusReleased || attempt.AttemptedAt != nil {
		t.Fatalf("attempt = %#v, want the unsent reservation released", attempt)
	}
}

func TestAdvancerProviderUnavailableSubmitKeepsFenceAndSignalsDependency(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return testutil.CommitExtraData(7), nil
	}
	providerErr := &synapse.ProviderUnavailableError{Cause: context.DeadlineExceeded}
	target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
		return nil, providerErr
	}

	result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if !synapse.IsProviderUnavailable(err) || result.State != storagecommit.AdvancePending {
		t.Fatalf("advance = %#v err=%v, want pending provider dependency", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.CommitAttemptID == nil || persisted.CommitAttemptedAt == nil {
		t.Fatalf("provider failure lost attempt fence: %#v", persisted)
	}
}

func TestAdvancerReleasesReservationWhenMarkAttemptedFails(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return testutil.CommitExtraData(7), nil
	}
	injected := errors.New("injected mark-attempted failure")
	store := &failingMarkAttemptedStore{Store: repos.Contents, err: injected}

	result, err := (&storagecommit.Advancer{Store: store}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if !errors.Is(err, injected) || result.State != "" {
		t.Fatalf("advance = %#v err=%v, want mark-attempted failure", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.CommitAttemptID != nil || persisted.CommitAttemptedAt != nil {
		t.Fatalf("failed mark-attempted retained reservation: %#v", persisted)
	}
}

func TestAdvancerSubmissionCallbackPreventsErrorBasedReset(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return testutil.CommitExtraData(7), nil
	}
	target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		request.OnSubmitted(storage.CommitSubmission{TransactionID: "0xcallback", StatusURL: "https://provider.example/status/callback"})
		return nil, storage.ErrInvalidArgument
	}

	result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if !errors.Is(err, storage.ErrInvalidArgument) || result.State != storagecommit.AdvancePending {
		t.Fatalf("advance = %#v err=%v, want pending callback evidence carrying the submit error", result, err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	if copyRow.CommitAttemptID == nil || copyRow.CommitAttemptedAt == nil ||
		copyRow.CommitTransactionID == nil || *copyRow.CommitTransactionID != "0xcallback" ||
		copyRow.CommitStatusURL == nil || *copyRow.CommitStatusURL != "https://provider.example/status/callback" {
		t.Fatalf("callback evidence was reset: %#v", copyRow)
	}
}

func TestAdvancerSurfacesDurableSubmissionEvidenceFailure(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
	dataSetRef, ok := target.DataSetRef()
	if !ok {
		t.Fatal("mock target has no data set ref")
	}
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return testutil.CommitExtraData(7), nil
	}
	target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		request.OnSubmitted(storage.CommitSubmission{TransactionID: "0xevidence", StatusURL: "https://provider.example/status/evidence"})
		return &storage.CommitSubmission{
			Kind: storage.CommitKindAddPieces, TransactionID: "0xevidence",
			StatusURL:  "https://provider.example/status/evidence",
			ProviderID: binding.ProviderID.SDK(), DataSet: &dataSetRef,
			PieceCIDs: []cid.Cid{pieceCID},
		}, nil
	}
	injected := errors.New("injected evidence write failure")
	store := &failingCommitEvidenceStore{Store: repos.Contents, err: injected}

	result, err := (&storagecommit.Advancer{Store: store}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if !errors.Is(err, injected) || result.State != storagecommit.AdvancePending {
		t.Fatalf("advance = %#v err=%v, want pending with evidence error", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.CommitAttemptID == nil || persisted.CommitAttemptedAt == nil ||
		persisted.CommitTransactionID != nil || persisted.CommitStatusURL != nil {
		t.Fatalf("failed evidence write lost attempt fence or invented evidence: %#v", persisted)
	}
}

func TestAdvancerUnavailableConfirmationWaitsThenRecoversFromAttention(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
	dataSetRef, ok := target.DataSetRef()
	if !ok {
		t.Fatal("mock target has no data set ref")
	}
	startedAt := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return testutil.CommitExtraData(7), nil
	}
	target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		const tx = "0xunavailable"
		request.OnSubmitted(storage.CommitSubmission{TransactionID: tx, StatusURL: "https://provider.example/status/unavailable"})
		return &storage.CommitSubmission{
			Kind: storage.CommitKindAddPieces, TransactionID: tx,
			StatusURL:  "https://provider.example/status/unavailable",
			ProviderID: binding.ProviderID.SDK(), DataSet: &dataSetRef,
			PieceCIDs: []cid.Cid{pieceCID},
		}, nil
	}
	confirmed := false
	target.GetCommitStatusFunc = func(context.Context, string) (*storage.CommitStatus, error) {
		if !confirmed {
			return nil, storage.ErrDataSetUnavailable
		}
		return &storage.CommitStatus{
			Kind: storage.CommitKindAddPieces, State: storage.CommitStateConfirmed, TransactionID: "0xunavailable",
			ConfirmedTransactionID: "0xconfirmed", DataSet: &dataSetRef,
			PieceIDs: []sdktypes.BigInt{sdktypes.NewBigInt(5001)},
		}, nil
	}
	advancer := storagecommit.Advancer{Store: repos.Contents, Now: func() time.Time { return startedAt }}
	result, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceSubmitted {
		t.Fatalf("submit = %#v err=%v", result, err)
	}

	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	advancer.Now = func() time.Time { return startedAt.Add(14 * time.Minute) }
	result, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvancePending {
		t.Fatalf("pre-threshold observation = %#v err=%v", result, err)
	}

	copyRow = loadAdvancerCopy(t, repos, copies[0].ID)
	advancer.Now = func() time.Time { return startedAt.Add(16 * time.Minute) }
	result, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention || !result.Continue ||
		result.AttentionCode != storagecommit.AttentionDataSetUnavailable {
		t.Fatalf("post-threshold observation = %#v err=%v", result, err)
	}

	confirmed = true
	copyRow = loadAdvancerCopy(t, repos, copies[0].ID)
	result, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceConfirmed || result.Confirmation == nil {
		t.Fatalf("recovered observation = %#v err=%v", result, err)
	}
	pieceID := idtypes.OnChainIDFromSDK(result.Confirmation.PieceIDs[0])
	if err := repos.Contents.MarkUploadCopyCommitted(t.Context(), repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[0].ID, ContentID: copies[0].ContentID, CopyIndex: copies[0].CopyIndex,
		PieceCID: pieceCID.String(), PieceID: &pieceID, RetrievalURL: target.PieceURL(pieceCID),
		CommitExtraDataHex: *copyRow.CommitExtraDataHex, CommitTransactionID: result.Confirmation.TransactionID,
		CommitAttemptID: result.AttemptID, CommitConfirmedTransactionID: result.Confirmation.ConfirmedTransactionID,
	}); err != nil {
		t.Fatalf("settle recovered confirmation: %v", err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.CommitAttentionCode != nil || persisted.CommitAttentionAt != nil || persisted.CommitAttemptID != nil {
		t.Fatalf("confirmed settlement retained attention: %#v", persisted)
	}
}

func TestAdvancerUnavailableContextMakesAttemptVisibleAfterThreshold(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, _ := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	startedAt := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "unavailable-context", Now: startedAt,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "unavailable-context", ExtraDataHex: testutil.CommitExtraDataHex(7), Now: startedAt,
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	advancer := storagecommit.Advancer{
		Store: repos.Contents,
		Now:   func() time.Time { return startedAt.Add(14 * time.Minute) },
	}

	result, err := advancer.AdvanceUnavailable(t.Context(), *copyRow, *binding)
	if err != nil || result.State != storagecommit.AdvancePending {
		t.Fatalf("pre-threshold unavailable advance = %#v err=%v", result, err)
	}
	advancer.Now = func() time.Time { return startedAt.Add(16 * time.Minute) }
	result, err = advancer.AdvanceUnavailable(t.Context(), *copyRow, *binding)
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention || !result.Continue ||
		result.AttentionCode != storagecommit.AttentionDataSetUnavailable {
		t.Fatalf("post-threshold unavailable advance = %#v err=%v", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.CommitAttentionAt == nil || persisted.CommitAttentionCode == nil ||
		*persisted.CommitAttentionCode != string(storagecommit.AttentionDataSetUnavailable) {
		t.Fatalf("unavailable attempted commit is not operator-visible: %#v", persisted)
	}
}

func TestCommitAttemptIdempotencyRejectsChangedExtraData(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	_, copies, _ := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "immutable-extra-data",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "immutable-extra-data", ExtraDataHex: testutil.CommitExtraDataHex(7),
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "immutable-extra-data", ExtraDataHex: "beef",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("changed extra data error = %v, want ErrConflict", err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.CommitExtraDataHex == nil || *persisted.CommitExtraDataHex != testutil.CommitExtraDataHex(7) {
		t.Fatalf("persisted extra data = %v, want immutable abcd", persisted.CommitExtraDataHex)
	}
}

func TestAdvancerUnavailableContextPreservesCancellationAndUnknownAttention(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, _ := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	startedAt := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "unavailable-canceled", Now: startedAt,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "unavailable-canceled", ExtraDataHex: testutil.CommitExtraDataHex(7), Now: startedAt,
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	store := &attentionCountingStore{Store: repos.Contents}
	advancer := storagecommit.Advancer{
		Store: store,
		Now:   func() time.Time { return startedAt.Add(16 * time.Minute) },
	}
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := advancer.AdvanceUnavailable(canceledCtx, *copyRow, *binding)
	if err != nil || result.State != storagecommit.AdvancePending || store.calls != 0 {
		t.Fatalf("canceled unavailable advance = %#v err=%v attentionWrites=%d", result, err, store.calls)
	}

	attentionAt := startedAt.Add(15 * time.Minute)
	if _, err := db.NewUpdate().
		Model((*storagecommit.Attempt)(nil)).
		Set("attention_code = ?", "future_attention_code").
		Set("attention_at = ?", attentionAt).
		Where("attempt_id = ?", "unavailable-canceled").
		Exec(t.Context()); err != nil {
		t.Fatalf("set future attention: %v", err)
	}
	copyRow = loadAdvancerCopy(t, repos, copies[0].ID)
	result, err = advancer.AdvanceUnavailable(t.Context(), *copyRow, *binding)
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention || result.Continue ||
		result.AttentionCode != storagecommit.AttentionCode("future_attention_code") || store.calls != 0 {
		t.Fatalf("future unavailable attention = %#v err=%v attentionWrites=%d", result, err, store.calls)
	}
}

func TestAdvancerFullSubmissionInvalidStatusKeepsStableAttentionAndRecovers(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
	dataSetRef, ok := target.DataSetRef()
	if !ok {
		t.Fatal("mock target has no data set ref")
	}
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return testutil.CommitExtraData(7), nil
	}
	target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		const tx = "0x0000000000000000000000000000000000000000000000000000000000000101"
		request.OnSubmitted(storage.CommitSubmission{TransactionID: tx, StatusURL: "https://provider.example/status/invalid"})
		return &storage.CommitSubmission{
			Kind: storage.CommitKindAddPieces, TransactionID: tx,
			StatusURL:  "https://provider.example/status/invalid",
			ProviderID: binding.ProviderID.SDK(), DataSet: &dataSetRef,
			PieceCIDs: []cid.Cid{pieceCID},
		}, nil
	}
	mode := "invalid"
	statusCalls := 0
	target.GetCommitStatusFunc = func(_ context.Context, statusURL string) (*storage.CommitStatus, error) {
		statusCalls++
		switch mode {
		case "invalid":
			return nil, fmt.Errorf("provider status identity: %w", pdp.ErrInvalidStatus)
		case "pending":
			return &storage.CommitStatus{Kind: storage.CommitKindAddPieces, State: storage.CommitStatePending, TransactionID: "0x0000000000000000000000000000000000000000000000000000000000000101", DataSet: &dataSetRef}, nil
		case "confirmed":
			return &storage.CommitStatus{
				Kind: storage.CommitKindAddPieces, State: storage.CommitStateConfirmed, TransactionID: "0x0000000000000000000000000000000000000000000000000000000000000101",
				ConfirmedTransactionID: "0xconfirmed", DataSet: &dataSetRef,
				PieceIDs: []sdktypes.BigInt{sdktypes.NewBigInt(5001)},
			}, nil
		case "rejected":
			return &storage.CommitStatus{Kind: storage.CommitKindAddPieces, State: storage.CommitStateRejected, TransactionID: "0x0000000000000000000000000000000000000000000000000000000000000101", DataSet: &dataSetRef}, nil
		default:
			t.Fatalf("unexpected status mode %q", mode)
			return nil, nil
		}
	}
	advancer := storagecommit.Advancer{Store: repos.Contents}
	result, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceSubmitted {
		t.Fatalf("submit = %#v err=%v", result, err)
	}

	store := &attentionCountingStore{Store: repos.Contents}
	advancer.Store = store
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	result, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention || result.Continue ||
		result.AttentionCode != storagecommit.AttentionSubmissionMismatch || store.calls != 1 {
		t.Fatalf("invalid status advance = %#v err=%v attentionWrites=%d", result, err, store.calls)
	}

	mode = "pending"
	copyRow = loadAdvancerCopy(t, repos, copies[0].ID)
	result, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention || result.Continue ||
		result.AttentionCode != storagecommit.AttentionSubmissionMismatch || store.calls != 1 {
		t.Fatalf("repeated attention advance = %#v err=%v attentionWrites=%d", result, err, store.calls)
	}

	if _, err := db.NewUpdate().
		Model((*storagecommit.Attempt)(nil)).
		Set("attention_code = ?", "future_attention_code").
		Where("content_id = ? AND storage_data_set_id = ? AND resolved_at IS NULL", copies[0].ContentID, copies[0].StorageDataSetID).
		Exec(t.Context()); err != nil {
		t.Fatalf("set future attention: %v", err)
	}
	copyRow = loadAdvancerCopy(t, repos, copies[0].ID)
	result, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention || result.Continue ||
		result.AttentionCode != storagecommit.AttentionCode("future_attention_code") || store.calls != 1 {
		t.Fatalf("future attention advance = %#v err=%v attentionWrites=%d", result, err, store.calls)
	}

	mode = "confirmed"
	copyRow = loadAdvancerCopy(t, repos, copies[0].ID)
	result, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceConfirmed || result.Confirmation == nil ||
		statusCalls != 4 || store.calls != 1 {
		t.Fatalf("confirmed recovery = %#v err=%v statusCalls=%d attentionWrites=%d", result, err, statusCalls, store.calls)
	}

	mode = "rejected"
	copyRow = loadAdvancerCopy(t, repos, copies[0].ID)
	result, err = advancer.Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceRejected || statusCalls != 5 || store.calls != 1 {
		t.Fatalf("rejected recovery = %#v err=%v statusCalls=%d attentionWrites=%d", result, err, statusCalls, store.calls)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.Status != model.StorageCopyStatusPieceReady || persisted.CommitAttemptID != nil ||
		persisted.CommitAttentionCode != nil || persisted.CommitAttentionAt != nil {
		t.Fatalf("copy after rejected recovery = %#v, want reset piece-ready copy", persisted)
	}
}

// nonceCase is one copy whose attempt went out signed with nonce 7 and was
// never acknowledged by the provider.
type nonceCase struct {
	db          *bun.DB
	repos       *repository.Repositories
	binding     *model.StorageDataSet
	copyRow     model.StorageCopy
	pieceCID    cid.Cid
	target      *testutil.MockStorageTarget
	nonces      *testutil.MockCommitNonces
	attemptedAt time.Time
}

const unacknowledgedNonce = 7

func seedUnacknowledgedAttempt(t *testing.T) nonceCase {
	t.Helper()
	return seedUnacknowledgedAttemptWithDB(t, testutil.NewTestDB(t))
}

func seedUnacknowledgedAttemptWithDB(t *testing.T, db *bun.DB) nonceCase {
	t.Helper()
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	attemptedAt := time.Date(2026, time.September, 30, 18, 0, 0, 0, time.UTC)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "unacknowledged", Now: attemptedAt,
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "unacknowledged", ExtraDataHex: testutil.CommitExtraDataHex(unacknowledgedNonce), Now: attemptedAt,
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
	return nonceCase{
		db: db, repos: repos, binding: binding, copyRow: copies[0], pieceCID: pieceCID,
		target: target, nonces: &testutil.MockCommitNonces{}, attemptedAt: attemptedAt,
	}
}

func (c nonceCase) advance(t *testing.T, at time.Time, input storagecommit.AdvanceInput) (storagecommit.AdvanceResult, error) {
	t.Helper()
	input.Copy = *loadAdvancerCopy(t, c.repos, c.copyRow.ID)
	input.Binding, input.Target = *c.binding, c.target
	input.Pieces = []storage.PieceInput{{PieceCID: c.pieceCID}}
	advancer := storagecommit.Advancer{Store: c.repos.Contents, Nonces: c.nonces, Now: func() time.Time { return at }}
	return advancer.Advance(t.Context(), input)
}

// A consumed nonce proves where the pieces landed even though no provider
// reported the transaction, and the copy is committed without one.
func TestAdvancerConfirmsUnacknowledgedAttemptByNonce(t *testing.T) {
	c := seedUnacknowledgedAttempt(t)
	pieceID := sdktypes.NewBigInt(41)
	c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), pieceID, c.pieceCID)

	result, err := c.advance(t, c.attemptedAt.Add(time.Minute), storagecommit.AdvanceInput{})
	if err != nil || result.State != storagecommit.AdvanceConfirmed || !result.ProvenByNonce || result.Confirmation == nil ||
		result.Confirmation.TransactionID != "" || result.Confirmation.ConfirmedTransactionID != "" ||
		len(result.Confirmation.PieceIDs) != 1 || !result.Confirmation.PieceIDs[0].Equal(pieceID) {
		t.Fatalf("advance = %#v err=%v, want a confirmation proven by the nonce", result, err)
	}
	committedPiece := idtypes.OnChainIDFromSDK(pieceID)
	settlement := repository.MarkUploadCopyCommittedInput{
		StorageCopyID: c.copyRow.ID, ContentID: c.copyRow.ContentID, CopyIndex: c.copyRow.CopyIndex,
		PieceCID: c.pieceCID.String(), PieceID: &committedPiece, RetrievalURL: c.target.PieceURL(c.pieceCID),
		CommitExtraDataHex: testutil.CommitExtraDataHex(unacknowledgedNonce), CommitAttemptID: result.AttemptID,
		ProvenByNonce: true,
	}
	for range 2 {
		if err := c.repos.Contents.MarkUploadCopyCommitted(t.Context(), settlement); err != nil {
			t.Fatalf("settle nonce confirmation: %v", err)
		}
	}
	persisted := loadAdvancerCopy(t, c.repos, c.copyRow.ID)
	if persisted.Status != model.StorageCopyStatusCommitted || persisted.PieceID == nil || !persisted.PieceID.Equal(committedPiece) {
		t.Fatalf("copy = %#v, want it committed at the nonce's piece", persisted)
	}
	attempt := loadAdvancerAttempt(t, c.db, result.AttemptID)
	if attempt.Status != storagecommit.AttemptStatusConfirmed || attempt.TransactionID != nil || attempt.ConfirmedTransactionID != nil {
		t.Fatalf("attempt = %#v, want confirmed without a transaction", attempt)
	}
}

// A nonce consumed anywhere but this copy's piece in its data set, or another
// request this copy signed that landed, stops the copy for an operator instead
// of sending anything.
func TestAdvancerStopsWhenNonceEvidenceDisagrees(t *testing.T) {
	for _, tt := range []struct {
		name    string
		consume func(c nonceCase)
	}{
		{name: "another data set", consume: func(c nonceCase) {
			c.nonces.Consume(unacknowledgedNonce, sdktypes.NewBigInt(9999), sdktypes.NewBigInt(41), c.pieceCID)
		}},
		{name: "another piece", consume: func(c nonceCase) {
			other, err := cid.Parse("bafkqaaa")
			if err != nil {
				panic(err)
			}
			c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), other)
		}},
		{name: "earlier request landed", consume: func(c nonceCase) {
			now := c.attemptedAt.Add(-time.Hour)
			earlier := testutil.CommitExtraDataHex(6)
			reason := string(storagecommit.ReleaseManualDuplicateAck)
			if _, err := c.db.NewInsert().Model(&storagecommit.Attempt{
				AttemptID: "released-by-hand", ContentID: c.copyRow.ContentID, StorageDataSetID: c.binding.ID,
				Status: storagecommit.AttemptStatusReleased, ExtraDataHex: &earlier, ReleaseReason: &reason,
				AttemptedAt: &now, ResolvedAt: &now, CreatedAt: now, UpdatedAt: now,
			}).Exec(context.Background()); err != nil {
				panic(err)
			}
			c.nonces.Consume(6, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(40), c.pieceCID)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := seedUnacknowledgedAttempt(t)
			tt.consume(c)
			var resent bool
			result, err := c.advance(t, c.attemptedAt.Add(time.Hour), storagecommit.AdvanceInput{
				Resend: func(context.Context, storagecommit.SendHistory, func(context.Context) error) (bool, error) {
					resent = true
					return false, nil
				},
			})
			if err != nil || result.State != storagecommit.AdvanceNeedsAttention || result.Continue ||
				result.AttentionCode != storagecommit.AttentionSubmissionMismatch || resent {
				t.Fatalf("advance = %#v err=%v resent=%v, want a stopped mismatch", result, err, resent)
			}
		})
	}
}

// While the chain shows the nonce unused, recovery waits for the provider to
// give up on the request, then lets execute send the same signed request.
func TestAdvancerResendsUnusedNonceWithTheSameRequest(t *testing.T) {
	c := seedUnacknowledgedAttempt(t)
	result, err := c.advance(t, c.attemptedAt.Add(4*time.Minute), storagecommit.AdvanceInput{})
	if err != nil || result.State != storagecommit.AdvancePending || result.RetryAfter != time.Minute {
		t.Fatalf("early recovery = %#v err=%v, want a one-minute wait", result, err)
	}
	result, err = c.advance(t, c.attemptedAt.Add(5*time.Minute), storagecommit.AdvanceInput{})
	if err != nil || result.State != storagecommit.AdvanceResendDue {
		t.Fatalf("due recovery = %#v err=%v, want resend due", result, err)
	}
	// After a second send the wait doubles from that send.
	sent := storagecommit.SendHistory{Sends: 2, LastSentAt: c.attemptedAt.Add(5 * time.Minute)}
	result, err = c.advance(t, c.attemptedAt.Add(14*time.Minute), storagecommit.AdvanceInput{Sent: sent})
	if err != nil || result.State != storagecommit.AdvancePending || result.RetryAfter != time.Minute {
		t.Fatalf("recovery after a resend = %#v err=%v, want a one-minute wait", result, err)
	}

	// An earlier version flagged the attempt; the first receipt answers it.
	if err := c.repos.Contents.MarkCommitAttention(t.Context(), storagecommit.AttentionInput{
		Copy: advancerCopyIdentity(c.copyRow), AttemptID: "unacknowledged", Code: storagecommit.AttentionAttemptOnlyAmbiguous,
	}); err != nil {
		t.Fatalf("flag attempt: %v", err)
	}
	ref, _ := c.target.DataSetRef()
	var submitted []byte
	c.target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		submitted = request.ExtraData
		submission := storage.CommitSubmission{
			Kind: storage.CommitKindAddPieces, TransactionID: "0xresent", StatusURL: "https://provider.example/status/resent",
			DataSet: &ref, PieceCIDs: []cid.Cid{c.pieceCID},
		}
		request.OnSubmitted(submission)
		return &submission, nil
	}
	var recorded storagecommit.SendHistory
	result, err = c.advance(t, c.attemptedAt.Add(15*time.Minute), storagecommit.AdvanceInput{
		Sent: sent,
		Resend: func(ctx context.Context, next storagecommit.SendHistory, effect func(context.Context) error) (bool, error) {
			recorded = next
			return true, effect(ctx)
		},
	})
	if err != nil || result.State != storagecommit.AdvanceSubmitted {
		t.Fatalf("resend = %#v err=%v, want submitted", result, err)
	}
	if string(submitted) != string(testutil.CommitExtraData(unacknowledgedNonce)) {
		t.Fatal("resend did not carry the attempt's signed request")
	}
	if recorded.Sends != 3 || !recorded.LastSentAt.Equal(c.attemptedAt.Add(15*time.Minute)) {
		t.Fatalf("recorded send = %#v, want the third send at the resend time", recorded)
	}
	persisted := loadAdvancerCopy(t, c.repos, c.copyRow.ID)
	if persisted.CommitTransactionID == nil || *persisted.CommitTransactionID != "0xresent" ||
		persisted.CommitAttentionAt != nil || persisted.CommitAttentionCode != nil {
		t.Fatalf("copy = %#v, want the receipt recorded and the old flag cleared", persisted)
	}
}

// A chain that cannot be read leaves the attempt fenced and sends nothing.
func TestAdvancerUnreadableNonceWaitsWithoutSending(t *testing.T) {
	c := seedUnacknowledgedAttempt(t)
	readErr := errors.New("chain RPC unavailable")
	c.nonces.Err = readErr
	var resent bool
	result, err := c.advance(t, c.attemptedAt.Add(time.Hour), storagecommit.AdvanceInput{
		Resend: func(context.Context, storagecommit.SendHistory, func(context.Context) error) (bool, error) {
			resent = true
			return false, nil
		},
	})
	if !errors.Is(err, readErr) || result.State != storagecommit.AdvancePending || result.RetryAfter != time.Minute || resent {
		t.Fatalf("advance = %#v err=%v resent=%v, want a fenced wait", result, err, resent)
	}
}

// A stopped confirmation keeps its task visible and counted: neither a single
// nor a bulk dismissal can hide it. Retry checks the chain again, and a
// confirmation found there clears it.
func TestStoppedCommitAttentionStaysVisibleUntilRetried(t *testing.T) {
	c := seedUnacknowledgedAttempt(t)
	repos := c.repos
	taskRow, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
		Type: model.TaskTypeStorageCommit, IdempotencyKey: "stopped-attention", InputVersion: 1,
		Input: []byte(`{}`), InputHash: "stopped-attention", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})
	if err != nil || !created {
		t.Fatalf("enqueue commit task = %#v created=%v err=%v", taskRow, created, err)
	}
	generation, err := repos.Contents.NextCopyWorkGeneration(t.Context(), c.copyRow.ID)
	if err != nil {
		t.Fatalf("next copy generation: %v", err)
	}
	if err := repos.Contents.BindCopyTask(t.Context(), c.copyRow.ID, generation, taskRow.ID); err != nil {
		t.Fatalf("bind commit task: %v", err)
	}
	if err := repos.Contents.MarkCommitAttention(t.Context(), storagecommit.AttentionInput{
		Copy: advancerCopyIdentity(c.copyRow), AttemptID: "unacknowledged", Code: storagecommit.AttentionAttemptOnlyAmbiguous,
	}); err != nil {
		t.Fatalf("mark commit attention: %v", err)
	}
	claimed, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("claim commit task = %#v err=%v", claimed, err)
	}
	reason := string(storagecommit.AttentionAttemptOnlyAmbiguous)
	message := "storage registration requires attention"
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: &reason, LastError: &message,
	}); err != nil {
		t.Fatalf("fail commit task: %v", err)
	}

	if err := repos.Tasks.AcknowledgeFailed(t.Context(), claimed.ID, time.Hour); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("dismiss stopped confirmation task err = %v, want conflict", err)
	}
	backlog := repository.TaskAcknowledgeFilter{Type: model.TaskTypeStorageCommit, FailedBefore: time.Now().Add(time.Minute)}
	if count, err := repos.Tasks.CountFailedMatching(t.Context(), backlog); err != nil || count != 0 {
		t.Fatalf("bulk dismissal preview = %d err=%v, want the stopped confirmation left out", count, err)
	}
	if count, err := repos.Tasks.AcknowledgeFailedMatching(t.Context(), backlog, time.Hour); err != nil || count != 0 {
		t.Fatalf("bulk dismissal = %d err=%v, want the stopped confirmation left out", count, err)
	}
	records, err := repos.Contents.ListCommitAttentionForTasks(t.Context(), []int64{claimed.ID})
	if err != nil || len(records) != 1 || records[0].AttemptID != "unacknowledged" ||
		records[0].TaskID == nil || *records[0].TaskID != claimed.ID {
		t.Fatalf("task confirmations = %#v err=%v, want the stopped attempt", records, err)
	}
	counts, err := repos.Contents.CountStoppedCommitAttentionByDataSet(t.Context())
	if err != nil || len(counts) != 1 || counts[c.binding.ID] != 1 {
		t.Fatalf("stopped confirmations = %v err=%v, want one on the copy's data set", counts, err)
	}

	pieceID := sdktypes.NewBigInt(41)
	c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), pieceID, c.pieceCID)
	result, err := c.advance(t, time.Now(), storagecommit.AdvanceInput{})
	if err != nil || result.State != storagecommit.AdvanceConfirmed || !result.ProvenByNonce {
		t.Fatalf("retried recovery = %#v err=%v, want a nonce confirmation", result, err)
	}
	committedPiece := idtypes.OnChainIDFromSDK(pieceID)
	if err := repos.Contents.MarkUploadCopyCommitted(t.Context(), repository.MarkUploadCopyCommittedInput{
		StorageCopyID: c.copyRow.ID, ContentID: c.copyRow.ContentID, CopyIndex: c.copyRow.CopyIndex,
		PieceCID: c.pieceCID.String(), PieceID: &committedPiece, RetrievalURL: c.target.PieceURL(c.pieceCID),
		CommitExtraDataHex: testutil.CommitExtraDataHex(unacknowledgedNonce), CommitAttemptID: result.AttemptID,
		ProvenByNonce: true,
	}); err != nil {
		t.Fatalf("settle retried confirmation: %v", err)
	}
	counts, err = repos.Contents.CountStoppedCommitAttentionByDataSet(t.Context())
	if err != nil || len(counts) != 0 {
		t.Fatalf("stopped confirmations after the retry = %v err=%v, want none", counts, err)
	}
}

func TestAdvancerCanceledObservationDoesNotWriteAttention(t *testing.T) {
	c := seedUnacknowledgedAttempt(t)
	c.nonces.Consume(unacknowledgedNonce, sdktypes.NewBigInt(9999), sdktypes.NewBigInt(41), c.pieceCID)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	advancer := storagecommit.Advancer{Store: c.repos.Contents, Nonces: c.nonces}
	result, err := advancer.Advance(ctx, storagecommit.AdvanceInput{
		Copy: *loadAdvancerCopy(t, c.repos, c.copyRow.ID), Binding: *c.binding, Target: c.target,
		Pieces: []storage.PieceInput{{PieceCID: c.pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvancePending || c.nonces.Reads() != 0 {
		t.Fatalf("advance = %#v err=%v reads=%d, want pending cancellation", result, err, c.nonces.Reads())
	}
	persisted := loadAdvancerCopy(t, c.repos, c.copyRow.ID)
	if persisted.CommitAttentionAt != nil || persisted.CommitAttentionCode != nil {
		t.Fatalf("canceled observation wrote attention: %#v", persisted)
	}
}

type failingCommitEvidenceStore struct {
	storagecommit.Store
	err error
}

type failingMarkAttemptedStore struct {
	storagecommit.Store
	err error
}

type attentionCountingStore struct {
	storagecommit.Store
	calls int
}

func (s *attentionCountingStore) MarkCommitAttention(ctx context.Context, input storagecommit.AttentionInput) error {
	s.calls++
	return s.Store.MarkCommitAttention(ctx, input)
}

func (s *failingMarkAttemptedStore) MarkCommitAttempted(
	context.Context,
	storagecommit.AttemptInput,
) (storagecommit.AttemptResult, error) {
	return storagecommit.AttemptResult{}, s.err
}

func (s *failingCommitEvidenceStore) RecordCommitSubmission(context.Context, storagecommit.EvidenceInput) error {
	return s.err
}

func TestCommitCapacityWakesOnlyTheQueueHead(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	_, copies, pieceCID := seedAdvancerCopies(t, db, 7)
	taskIDs := make([]int64, len(copies))
	for i, copyRow := range copies {
		task, _, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
			Type: model.TaskTypeStorageCommit, IdempotencyKey: fmt.Sprintf("queue-%d", copyRow.ID),
			InputVersion: 1, Input: []byte(`{}`), InputHash: "queue", AvailableAt: time.Now().Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("enqueue commit task %d: %v", i, err)
		}
		taskIDs[i] = task.ID
		if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
			Set("active_task_id = ?", task.ID).Where("id = ?", copyRow.ID).Exec(t.Context()); err != nil {
			t.Fatalf("bind commit task %d: %v", i, err)
		}
	}
	reserve := func(i int) storagecommit.ReservationState {
		t.Helper()
		result, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
			Copy: advancerCopyIdentity(copies[i]), AttemptID: fmt.Sprintf("queue-attempt-%d", i),
		})
		if err != nil {
			t.Fatalf("reserve copy %d: %v", i, err)
		}
		return result.State
	}
	release := func(i int) {
		t.Helper()
		if err := repos.Contents.ReleaseCommitAttempt(t.Context(), storagecommit.ReleaseInput{
			Copy: advancerCopyIdentity(copies[i]), AttemptID: fmt.Sprintf("queue-attempt-%d", i),
			Reason: storagecommit.ReleaseBeforeSubmitCanceled, ClearReadyAt: true, ClearExtraData: true,
		}); err != nil {
			t.Fatalf("release copy %d: %v", i, err)
		}
	}
	runnable := func(step string, want ...int) {
		t.Helper()
		var got []int
		for i, id := range taskIDs {
			task, err := repos.Tasks.GetByID(t.Context(), id)
			if err != nil {
				t.Fatalf("load commit task %d: %v", i, err)
			}
			if !task.AvailableAt.After(time.Now()) {
				got = append(got, i)
			}
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: runnable queue tasks = %v, want %v", step, got, want)
		}
	}

	for i := range 7 {
		want := storagecommit.ReservationAcquired
		if i >= storagecommit.MaxActiveAttemptsPerDataSet {
			want = storagecommit.ReservationWaiting
		}
		if state := reserve(i); state != want {
			t.Fatalf("reserve copy %d = %s, want %s", i, state, want)
		}
	}
	runnable("while every slot is taken")

	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: advancerCopyIdentity(copies[0]), AttemptID: "queue-attempt-0", ExtraDataHex: testutil.CommitExtraDataHex(7),
	}); err != nil {
		t.Fatalf("mark copy 0 attempted: %v", err)
	}
	pieceID := idtypes.OnChainIDFromSDK(sdktypes.NewBigInt(5001))
	confirmation := repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[0].ID, ContentID: copies[0].ContentID, CopyIndex: 0,
		PieceCID: pieceCID.String(), PieceID: &pieceID, RetrievalURL: "https://provider.example/piece",
		CommitExtraDataHex: testutil.CommitExtraDataHex(7), CommitTransactionID: "0x01", CommitAttemptID: "queue-attempt-0",
		CommitConfirmedTransactionID: "0x01",
	}
	if err := repos.Contents.MarkUploadCopyCommitted(t.Context(), confirmation); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("confirm copy 0 without submission evidence: %v, want conflict", err)
	}
	if err := repos.Contents.RecordCommitSubmission(t.Context(), storagecommit.EvidenceInput{
		Copy: advancerCopyIdentity(copies[0]), AttemptID: "queue-attempt-0",
		TransactionID: "0x01", StatusURL: "https://provider.example/status/queue-attempt-0",
	}); err != nil {
		t.Fatalf("record copy 0 submission: %v", err)
	}
	if err := repos.Contents.MarkUploadCopyCommitted(t.Context(), confirmation); err != nil {
		t.Fatalf("confirm copy 0: %v", err)
	}
	runnable("after a confirmation frees one slot", 4)

	if state := reserve(4); state != storagecommit.ReservationAcquired {
		t.Fatalf("woken head reservation = %s", state)
	}
	runnable("after the head takes the last slot", 4)

	release(1)
	release(2)
	runnable("after two releases", 4, 5)
	if state := reserve(5); state != storagecommit.ReservationAcquired {
		t.Fatalf("second head reservation = %s", state)
	}
	runnable("after the head reserves with a slot left", 4, 5, 6)
}

func seedAdvancerCopies(t *testing.T, db *bun.DB, count int) (*model.StorageDataSet, []model.StorageCopy, cid.Cid) {
	t.Helper()
	pieceCID := advancerTestCID(t)
	bucket := &model.Bucket{Name: "storage-commit-advancer", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if _, err := db.NewInsert().Model(bucket).Exec(t.Context()); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	testutil.OpenBucketReplicaSlots(t, db, bucket.ID, bucket.DefaultCopies)
	providerID := idtypes.OnChainIDFromSDK(sdktypes.NewBigInt(101))
	dataSetID := idtypes.OnChainIDFromSDK(sdktypes.NewBigInt(1001))
	clientDataSetID := idtypes.OnChainIDFromSDK(sdktypes.NewBigInt(9001))
	binding := &model.StorageDataSet{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, Generation: 1, IsCurrent: true,
		DataSetID: &dataSetID, ClientDataSetID: &clientDataSetID, Status: model.StorageDataSetStatusReady,
	}
	if _, err := db.NewInsert().Model(binding).Exec(t.Context()); err != nil {
		t.Fatalf("insert data set: %v", err)
	}
	copies := make([]model.StorageCopy, 0, count)
	for i := range count {
		piece := pieceCID.String()
		upload := &model.StorageContent{
			BucketID:    bucket.ID,
			ContentSize: 1, Checksum: testutil.StorageChecksum(fmt.Sprintf("checksum-%d", i)), PieceCID: &piece, RequestedCopies: 1,
		}
		if _, err := db.NewInsert().Model(upload).Exec(t.Context()); err != nil {
			t.Fatalf("insert upload %d: %v", i, err)
		}
		copyRow := model.StorageCopy{
			ContentID: upload.ID, BucketID: bucket.ID, ContentSize: upload.ContentSize,
			CopyIndex: 0, ProviderID: providerID,
			TransferMethod: model.StorageCopyTransferMethodPeerPull,
			Status:         model.StorageCopyStatusPieceReady, StorageDataSetID: binding.ID,
		}
		if _, err := db.NewInsert().Model(&copyRow).Exec(t.Context()); err != nil {
			t.Fatalf("insert copy %d: %v", i, err)
		}
		copies = append(copies, copyRow)
	}
	return binding, copies, pieceCID
}

func loadAdvancerCopy(t *testing.T, repos *repository.Repositories, copyID int64) *model.StorageCopy {
	t.Helper()
	copyRow, err := repos.Contents.GetUploadCopyByID(t.Context(), copyID)
	if err != nil {
		t.Fatalf("load copy %d: %v", copyID, err)
	}
	return copyRow
}

func loadAdvancerAttempt(t *testing.T, db *bun.DB, attemptID string) *storagecommit.Attempt {
	t.Helper()
	attempt := new(storagecommit.Attempt)
	if err := db.NewSelect().Model(attempt).Where("attempt_id = ?", attemptID).Scan(t.Context()); err != nil {
		t.Fatalf("load attempt %s: %v", attemptID, err)
	}
	return attempt
}

func advancerCopyIdentity(copyRow model.StorageCopy) storagecommit.CopyIdentity {
	return storagecommit.CopyIdentity{
		StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID,
		CopyIndex: copyRow.CopyIndex, StorageDataSetID: copyRow.StorageDataSetID,
	}
}

func advancerTestCID(t *testing.T) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("durable-commit-test"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("create multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

func TestAdvancerWriteBlockedDataSetReleasesAttemptWithCause(t *testing.T) {
	writeBlocked := func() error {
		return fmt.Errorf("storage.DataSetContext.SubmitCommit: %w", &storage.DataSetPDPPaymentTerminatedError{
			DataSetID: sdktypes.NewBigInt(1001), PDPEndEpoch: sdktypes.Epoch(42),
		})
	}

	t.Run("releases the attempt and names the cause", func(t *testing.T) {
		db := testutil.NewTestDB(t)
		repos := repository.NewRepositories(db)
		binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
		target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
		target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
			return testutil.CommitExtraData(7), nil
		}
		target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
			return nil, writeBlocked()
		}

		result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
			Copy: copies[0], Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
		})
		if err != nil || result.State != storagecommit.AdvanceReleased ||
			result.ReleaseReason != storagecommit.ReleaseDataSetUnavailable {
			t.Fatalf("advance = %#v err=%v, want a released write-blocked commit", result, err)
		}
		if _, ok := errors.AsType[*storage.DataSetPDPPaymentTerminatedError](result.Cause); !ok {
			t.Fatalf("release cause = %v, want the payment terminated error", result.Cause)
		}
		persisted := loadAdvancerCopy(t, repos, copies[0].ID)
		if persisted.Status != model.StorageCopyStatusPieceReady || persisted.CommitAttemptID != nil ||
			persisted.CommitAttemptedAt != nil || persisted.CommitReadyAt != nil ||
			persisted.CommitExtraDataHex != nil {
			t.Fatalf("write-blocked release retained commit state: %#v", persisted)
		}
	})

	t.Run("keeps the fence when a transaction was already recorded", func(t *testing.T) {
		db := testutil.NewTestDB(t)
		repos := repository.NewRepositories(db)
		binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
		target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
		target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
			return testutil.CommitExtraData(7), nil
		}
		target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
			request.OnSubmitted(storage.CommitSubmission{TransactionID: "0xwriteblocked", StatusURL: "https://provider.example/status/writeblocked"})
			return nil, writeBlocked()
		}

		result, _ := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
			Copy: copies[0], Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
		})
		if result.State == storagecommit.AdvanceReleased {
			t.Fatalf("advance = %#v, want the recorded transaction to refuse the release", result)
		}
		persisted := loadAdvancerCopy(t, repos, copies[0].ID)
		if persisted.Status != model.StorageCopyStatusCommitting || persisted.CommitAttemptID == nil ||
			persisted.CommitAttemptedAt == nil || persisted.CommitTransactionID == nil ||
			*persisted.CommitTransactionID != "0xwriteblocked" {
			t.Fatalf("write-blocked release dropped submitted evidence: %#v", persisted)
		}
	})
}
