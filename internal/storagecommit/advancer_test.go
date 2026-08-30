package storagecommit_test

import (
	"context"
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
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		presignCalls++
		return []byte{0xab, 0xcd}, nil
	}
	target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
		submissionCalls++
		tx := fmt.Sprintf("0x%064x", submissionCalls)
		if request.OnSubmitted != nil {
			request.OnSubmitted(tx)
		}
		return &storage.CommitSubmission{
			Kind: storage.CommitKindAddPieces, TransactionID: tx,
			StatusURL:  fmt.Sprintf("https://provider.example/status/%d", submissionCalls),
			ProviderID: binding.ProviderID.SDK(), DataSet: &dataSetRef,
			PieceCIDs: []cid.Cid{pieceCID},
		}, nil
	}
	confirm := false
	target.GetCommitStatusFunc = func(_ context.Context, submission storage.CommitSubmission) (*storage.CommitStatus, error) {
		confirmationCalls++
		if !confirm {
			return &storage.CommitStatus{State: storage.CommitStatePending, TransactionID: submission.TransactionID}, nil
		}
		return &storage.CommitStatus{
			State: storage.CommitStateConfirmed, TransactionID: submission.TransactionID,
			ConfirmedTransactionID: "0xconfirmed", DataSet: &dataSetRef,
			PieceIDs: []sdktypes.BigInt{sdktypes.NewBigInt(5001)},
		}, nil
	}
	advancer := storagecommit.Advancer{Store: repos.Uploads}

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
			persisted.CommitTransactionID == nil || persisted.CommitSubmissionJSON == nil {
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
	pieceID := idtypes.OnChainIDFromSDK(settled.Confirmation.PieceIDs[0])
	if err := repos.Uploads.MarkUploadCopyCommitted(t.Context(), repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copies[0].ID, UploadID: copies[0].UploadID, CopyIndex: 0,
		PieceCID: pieceCID.String(), PieceID: &pieceID, RetrievalURL: target.PieceURL(pieceCID),
		CommitExtraDataHex: *first.CommitExtraDataHex, CommitTransactionID: settled.Confirmation.TransactionID,
		CommitAttemptID: settled.AttemptID, CommitConfirmedTransactionID: settled.Confirmation.ConfirmedTransactionID,
	}); err != nil {
		t.Fatalf("settle first copy: %v", err)
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

func TestAdvancerOwnerTerminalReleasesUnattemptedReservationWithoutSDK(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "owner-terminal-attempt",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
		t.Fatal("owner-terminal release entered the SDK")
		return nil, nil
	}

	result, err := (&storagecommit.Advancer{Store: repos.Uploads}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target, OwnerTerminal: true,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
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
		if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
			Copy: advancerCopyIdentity(copies[i]), AttemptID: fmt.Sprintf("capacity-%d", i),
		}); err != nil {
			t.Fatalf("reserve capacity %d: %v", i, err)
		}
	}
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	advancer := storagecommit.Advancer{Store: repos.Uploads}
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

		result, err := (&storagecommit.Advancer{Store: repos.Uploads}).Advance(t.Context(), storagecommit.AdvanceInput{
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

	t.Run("after SDK entry requires attention", func(t *testing.T) {
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

		result, err := (&storagecommit.Advancer{Store: repos.Uploads}).Advance(t.Context(), storagecommit.AdvanceInput{
			Copy: copies[0], Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
		})
		if err != nil || result.State != storagecommit.AdvanceNeedsAttention ||
			result.AttentionCode != storagecommit.AttentionDataSetUnavailable || result.Continue {
			t.Fatalf("advance = %#v err=%v", result, err)
		}
		persisted := loadAdvancerCopy(t, repos, copies[0].ID)
		if persisted.CommitAttemptID == nil || persisted.CommitAttemptedAt == nil ||
			persisted.CommitAttentionAt == nil {
			t.Fatalf("attempted unavailable commit was released: %#v", persisted)
		}
	})
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
		request.OnSubmitted("0xcallback")
		return nil, storage.ErrInvalidArgument
	}

	result, err := (&storagecommit.Advancer{Store: repos.Uploads}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvancePending {
		t.Fatalf("advance = %#v err=%v, want pending callback evidence", result, err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	if copyRow.CommitAttemptID == nil || copyRow.CommitAttemptedAt == nil ||
		copyRow.CommitTransactionID == nil || *copyRow.CommitTransactionID != "0xcallback" {
		t.Fatalf("callback evidence was reset: %#v", copyRow)
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
		request.OnSubmitted(tx)
		return &storage.CommitSubmission{
			Kind: storage.CommitKindAddPieces, TransactionID: tx,
			StatusURL:  "https://provider.example/status/unavailable",
			ProviderID: binding.ProviderID.SDK(), DataSet: &dataSetRef,
			PieceCIDs: []cid.Cid{pieceCID},
		}, nil
	}
	confirmed := false
	target.GetCommitStatusFunc = func(context.Context, storage.CommitSubmission) (*storage.CommitStatus, error) {
		if !confirmed {
			return nil, storage.ErrDataSetUnavailable
		}
		return &storage.CommitStatus{
			State: storage.CommitStateConfirmed, TransactionID: "0xunavailable",
			ConfirmedTransactionID: "0xconfirmed", DataSet: &dataSetRef,
			PieceIDs: []sdktypes.BigInt{sdktypes.NewBigInt(5001)},
		}, nil
	}
	advancer := storagecommit.Advancer{Store: repos.Uploads, Now: func() time.Time { return startedAt }}
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
	if err := repos.Uploads.MarkUploadCopyCommitted(t.Context(), repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copies[0].ID, UploadID: copies[0].UploadID, CopyIndex: copies[0].CopyIndex,
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

func TestAdvancerAttemptOnlyUsesPieceStatusAsDiagnosticEvidence(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "attempt-only",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Uploads.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
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

	result, err := (&storagecommit.Advancer{Store: repos.Uploads}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *copyRow, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention ||
		result.AttentionCode != storagecommit.AttentionUnattributedPiece || result.Continue {
		t.Fatalf("advance = %#v err=%v", result, err)
	}
	persisted := loadAdvancerCopy(t, repos, copies[0].ID)
	if persisted.PieceID != nil || persisted.Status != model.StorageUploadCopyStatusCommitting ||
		persisted.CommitAttentionAt == nil || persisted.CommitAttentionCode == nil ||
		*persisted.CommitAttentionCode != string(storagecommit.AttentionUnattributedPiece) {
		t.Fatalf("attempt-only evidence was incorrectly adopted: %#v", persisted)
	}
	result, err = (&storagecommit.Advancer{Store: repos.Uploads}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: *persisted, Binding: *binding, Target: target,
		Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	if err != nil || result.State != storagecommit.AdvanceNeedsAttention || pieceStatusCalls != 1 {
		t.Fatalf("second advance = %#v err=%v pieceStatusCalls=%d", result, err, pieceStatusCalls)
	}
}

func TestAdvancerTxOnlyEvidenceConfirmsWithoutPieceStatus(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	identity := advancerCopyIdentity(copies[0])
	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "tx-only",
	}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := repos.Uploads.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
		Copy: identity, AttemptID: "tx-only", ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("mark attempted: %v", err)
	}
	if err := repos.Uploads.RecordCommitTransaction(t.Context(), storagecommit.EvidenceInput{
		Copy: identity, AttemptID: "tx-only", TransactionID: "0xtxonly",
	}); err != nil {
		t.Fatalf("record transaction: %v", err)
	}
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
	target.PieceStatusFunc = func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
		t.Fatal("tx-only recovery used PieceStatus")
		return nil, nil
	}
	checkerCalls := 0
	checker := commitStatusCheckerFunc(func(context.Context, synapse.AddPiecesStatusInput) (synapse.PDPStatusResult, error) {
		checkerCalls++
		return synapse.PDPStatusResult{
			State: synapse.PDPStatusConfirmed, ConfirmedPieceIDs: []string{"5001"},
		}, nil
	})
	result, err := (&storagecommit.Advancer{Store: repos.Uploads, StatusChecker: checker}).Advance(
		t.Context(), storagecommit.AdvanceInput{
			Copy: *copyRow, Binding: *binding, Target: target,
			Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
		})
	if err != nil || result.State != storagecommit.AdvanceConfirmed || result.Confirmation == nil || checkerCalls != 1 {
		t.Fatalf("advance = %#v err=%v checkerCalls=%d", result, err, checkerCalls)
	}
}

type commitStatusCheckerFunc func(context.Context, synapse.AddPiecesStatusInput) (synapse.PDPStatusResult, error)

func (f commitStatusCheckerFunc) GetAddPiecesStatus(
	ctx context.Context,
	input synapse.AddPiecesStatusInput,
) (synapse.PDPStatusResult, error) {
	return f(ctx, input)
}

func seedAdvancerCopies(t *testing.T, db *bun.DB, count int) (*model.StorageDataSet, []model.StorageUploadCopy, cid.Cid) {
	t.Helper()
	pieceCID := advancerTestCID(t)
	bucket := &model.Bucket{Name: "storage-commit-advancer", Status: model.BucketStatusActive}
	if _, err := db.NewInsert().Model(bucket).Exec(t.Context()); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
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
	copies := make([]model.StorageUploadCopy, 0, count)
	for i := range count {
		piece := pieceCID.String()
		upload := &model.StorageUpload{
			BucketID: bucket.ID, ContentSize: 1, Checksum: fmt.Sprintf("checksum-%d", i),
			Status: model.StorageUploadStatusRunning, PieceCID: &piece, RequestedCopies: 1,
		}
		if _, err := db.NewInsert().Model(upload).Exec(t.Context()); err != nil {
			t.Fatalf("insert upload %d: %v", i, err)
		}
		copyRow := model.StorageUploadCopy{
			UploadID: upload.ID, CopyIndex: 0, ProviderID: &providerID,
			TransferMethod: model.StorageCopyTransferMethodPeerPull,
			Status:         model.StorageUploadCopyStatusPieceReady, StorageDataSetID: &binding.ID,
		}
		if _, err := db.NewInsert().Model(&copyRow).Exec(t.Context()); err != nil {
			t.Fatalf("insert copy %d: %v", i, err)
		}
		copies = append(copies, copyRow)
	}
	return binding, copies, pieceCID
}

func loadAdvancerCopy(t *testing.T, repos *repository.Repositories, copyID int64) *model.StorageUploadCopy {
	t.Helper()
	copyRow, err := repos.Uploads.GetUploadCopyByID(t.Context(), copyID)
	if err != nil {
		t.Fatalf("load copy %d: %v", copyID, err)
	}
	return copyRow
}

func advancerCopyIdentity(copyRow model.StorageUploadCopy) storagecommit.CopyIdentity {
	return storagecommit.CopyIdentity{
		StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
		CopyIndex: copyRow.CopyIndex, StorageDataSetID: *copyRow.StorageDataSetID,
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
