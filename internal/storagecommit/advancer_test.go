package storagecommit_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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
		return []byte{0xab, 0xcd}, nil
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
				Copy: identity, AttemptID: "status-mismatch", ExtraDataHex: "abcd",
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
		Copy: identity, AttemptID: "atomic-evidence", ExtraDataHex: "abcd",
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
			return []byte{0xab}, nil
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

func TestAdvancerAmbiguousSubmitErrorRetainsAttemptFence(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return []byte{0xab}, nil
	}
	target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
		return nil, storage.ErrInvalidArgument
	}

	result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if !errors.Is(err, storage.ErrInvalidArgument) || result.State != storagecommit.AdvancePending {
		t.Fatalf("advance = %#v err=%v, want a fenced pending result carrying the submit error", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.Status != model.StorageCopyStatusCommitting ||
		persisted.CommitAttemptID == nil || persisted.CommitAttemptedAt == nil {
		t.Fatalf("ambiguous submit error lost attempt fence: %#v", persisted)
	}
}

func TestAdvancerProviderUnavailableSubmitKeepsFenceAndSignalsDependency(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		return []byte{0xab}, nil
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
		return []byte{0xab}, nil
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
		return []byte{0xab, 0xcd}, nil
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
		return []byte{0xab}, nil
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
		return []byte{0xab}, nil
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
		Copy: identity, AttemptID: "unavailable-context", ExtraDataHex: "abcd", Now: startedAt,
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
		Copy: identity, AttemptID: "immutable-extra-data", ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "immutable-extra-data", ExtraDataHex: "beef",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("changed extra data error = %v, want ErrConflict", err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.CommitExtraDataHex == nil || *persisted.CommitExtraDataHex != "abcd" {
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
		Copy: identity, AttemptID: "unavailable-canceled", ExtraDataHex: "abcd", Now: startedAt,
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
		return []byte{0xab}, nil
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

func TestAdvancerAttemptOnlyUsesPieceStatusAsDiagnosticEvidence(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "attempt-only",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "attempt-only", ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	pieceStatusCalls := 0
	target.PieceStatusFunc = func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
		pieceStatusCalls++
		return &storage.PieceStatus{Exists: true}, nil
	}

	result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention ||
		result.AttentionCode != storagecommit.AttentionUnattributedPiece || result.Continue {
		t.Fatalf("advance = %#v err=%v", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.PieceID != nil || persisted.Status != model.StorageCopyStatusCommitting ||
		persisted.CommitAttentionAt == nil || persisted.CommitAttentionCode == nil ||
		*persisted.CommitAttentionCode != string(storagecommit.AttentionUnattributedPiece) {
		t.Fatalf("attempt-only evidence was incorrectly adopted: %#v", persisted)
	}
	result, err = (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *persisted, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention || pieceStatusCalls != 1 {
		t.Fatalf("second advance = %#v err=%v pieceStatusCalls=%d", result, err, pieceStatusCalls)
	}
}

func seedCommitAttentionTask(t *testing.T, db *bun.DB) (*repository.Repositories, model.StorageCopy, *model.Task, string) {
	t.Helper()
	repos := repository.NewRepositories(db)
	_, copies, _ := seedAdvancerCopies(t, db, 1)
	copyRow := copies[0]
	taskRow, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
		Type: model.TaskTypeStorageCommit, IdempotencyKey: "release-attention", InputVersion: 1,
		Input: []byte(`{}`), InputHash: "release-attention", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})
	if err != nil || !created {
		t.Fatalf("enqueue commit task = %#v created=%v err=%v", taskRow, created, err)
	}
	generation, err := repos.Contents.NextCopyWorkGeneration(t.Context(), copyRow.ID)
	if err != nil {
		t.Fatalf("next copy generation: %v", err)
	}
	if err := repos.Contents.BindCopyTask(t.Context(), copyRow.ID, generation, taskRow.ID); err != nil {
		t.Fatalf("bind commit task: %v", err)
	}
	identity := advancerCopyIdentity(copyRow)
	const attemptID = "release-attention-attempt"
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: attemptID,
	}); err != nil {
		t.Fatalf("reserve commit attempt: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: attemptID, ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark commit attempted: %v", err)
	}
	if err := repos.Contents.MarkCommitAttention(t.Context(), storagecommit.AttentionInput{
		Copy: identity, AttemptID: attemptID, Code: storagecommit.AttentionAttemptOnlyAmbiguous,
	}); err != nil {
		t.Fatalf("mark commit attention: %v", err)
	}
	claimed, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("claim commit task = %#v err=%v", claimed, err)
	}
	return repos, copyRow, claimed, attemptID
}

func TestReleaseCommitAttentionResumesFencedFailedTask(t *testing.T) {
	repos, copyRow, claimed, attemptID := seedCommitAttentionTask(t, testutil.NewTestDB(t))
	reason := string(storagecommit.AttentionAttemptOnlyAmbiguous)
	message := "storage registration requires attention"
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: &reason, LastError: &message,
	}); err != nil {
		t.Fatalf("fail commit task: %v", err)
	}

	if err := repos.Contents.ReleaseCommitAttention(t.Context(), storagecommit.ManualReleaseInput{
		CopyID: copyRow.ID, ExpectedAttemptID: attemptID, AcknowledgePossibleDuplicate: true,
	}); err != nil {
		t.Fatalf("release commit attention: %v", err)
	}
	resumed, err := repos.Tasks.GetByID(t.Context(), claimed.ID)
	if err != nil || resumed == nil || resumed.Status != model.TaskStatusPending ||
		resumed.ResumeMode != model.TaskResumeModeRecover || resumed.RetryCount != 0 {
		t.Fatalf("resumed task = %#v err=%v", resumed, err)
	}
	persisted := loadAdvancerCopy(t, repos, copyRow.ID)
	if persisted.Status != model.StorageCopyStatusPieceReady || persisted.CommitAttemptID != nil ||
		persisted.ActiveTaskID == nil || *persisted.ActiveTaskID != claimed.ID {
		t.Fatalf("released copy = %#v, want piece-ready copy fenced to resumed task", persisted)
	}
}

func TestReleaseCommitAttentionFencesRunningTask(t *testing.T) {
	repos, copyRow, claimed, attemptID := seedCommitAttentionTask(t, testutil.NewTestDB(t))
	if err := repos.Contents.ReleaseCommitAttention(t.Context(), storagecommit.ManualReleaseInput{
		CopyID: copyRow.ID, ExpectedAttemptID: attemptID, AcknowledgePossibleDuplicate: true,
	}); err != nil {
		t.Fatalf("release commit attention: %v", err)
	}
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
	}); !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("old task settlement = %v, want lost lease", err)
	}
	resumed, err := repos.Tasks.GetByID(t.Context(), claimed.ID)
	if err != nil || resumed == nil || resumed.Status != model.TaskStatusPending ||
		resumed.ResumeMode != model.TaskResumeModeRecover || resumed.LeaseUntil != nil || resumed.ClaimedAt != nil {
		t.Fatalf("resumed task = %#v err=%v", resumed, err)
	}
	persisted := loadAdvancerCopy(t, repos, copyRow.ID)
	if persisted.Status != model.StorageCopyStatusPieceReady || persisted.CommitAttemptID != nil ||
		persisted.ActiveTaskID == nil || *persisted.ActiveTaskID != claimed.ID {
		t.Fatalf("released copy = %#v", persisted)
	}
	next, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || next == nil || next.ID != claimed.ID || next.ClaimGeneration <= claimed.ClaimGeneration ||
		next.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("next claim = %#v err=%v", next, err)
	}
}

type commitReleaseTaskLockSignal struct {
	once    sync.Once
	started chan struct{}
}

type commitReleaseContextKey struct{}

func (h *commitReleaseTaskLockSignal) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	query := strings.ToLower(event.Query)
	if ctx.Value(commitReleaseContextKey{}) != nil && strings.Contains(query, "update") &&
		strings.Contains(query, "tasks") && strings.Contains(query, "updated_at = updated_at") {
		h.once.Do(func() { close(h.started) })
	}
	return ctx
}

func (*commitReleaseTaskLockSignal) AfterQuery(context.Context, *bun.QueryEvent) {}

func TestPostgresReleaseCommitAttentionLocksTaskBeforeStorage(t *testing.T) {
	db := testutil.NewTestPostgresDB(t)
	repos, copyRow, claimed, attemptID := seedCommitAttentionTask(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin settlement: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := repository.NewRepositories(tx).Tasks.ValidateClaim(ctx, claimed.ID, claimed.ClaimGeneration); err != nil {
		t.Fatalf("lock settling task: %v", err)
	}
	signal := &commitReleaseTaskLockSignal{started: make(chan struct{})}
	db.AddQueryHook(signal)
	released := make(chan error, 1)
	go func() {
		releaseCtx := context.WithValue(ctx, commitReleaseContextKey{}, true)
		released <- repos.Contents.ReleaseCommitAttention(releaseCtx, storagecommit.ManualReleaseInput{
			CopyID: copyRow.ID, ExpectedAttemptID: attemptID, AcknowledgePossibleDuplicate: true,
		})
	}()
	select {
	case <-signal.started:
	case <-ctx.Done():
		t.Fatalf("release did not try to lock the task first: %v", ctx.Err())
	}
	if _, err := tx.NewRaw("UPDATE storage_contents SET updated_at = updated_at WHERE id = ?", copyRow.ContentID).Exec(ctx); err != nil {
		t.Fatalf("settlement storage lock: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit settlement: %v", err)
	}
	select {
	case err := <-released:
		if err != nil {
			t.Fatalf("release after settlement lock: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("release deadlocked with settlement: %v", ctx.Err())
	}
}

func TestAdvancerCanceledObservationDoesNotWriteAttention(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Contents.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "canceled-observation",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "canceled-observation", ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	ctx, cancel := context.WithCancel(t.Context())
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PieceStatusFunc = func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
		cancel()
		return nil, context.Canceled
	}

	result, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(ctx, storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvancePending {
		t.Fatalf("advance = %#v err=%v, want pending cancellation", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
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
		Copy: advancerCopyIdentity(copies[0]), AttemptID: "queue-attempt-0", ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark copy 0 attempted: %v", err)
	}
	pieceID := idtypes.OnChainIDFromSDK(sdktypes.NewBigInt(5001))
	confirmation := repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[0].ID, ContentID: copies[0].ContentID, CopyIndex: 0,
		PieceCID: pieceCID.String(), PieceID: &pieceID, RetrievalURL: "https://provider.example/piece",
		CommitExtraDataHex: "abcd", CommitTransactionID: "0x01", CommitAttemptID: "queue-attempt-0",
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
			return []byte{0xab}, nil
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
			return []byte{0xab}, nil
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
