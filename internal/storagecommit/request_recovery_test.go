package storagecommit_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func checkpointResend(ctx context.Context, _ storagecommit.SendHistory, effect func(context.Context) error) (bool, error) {
	return true, effect(ctx)
}

func TestAdvancerResendRefusalPreservesEarlierSubmission(t *testing.T) {
	for _, refusal := range []error{
		&pdp.HTTPError{StatusCode: 400}, &pdp.HTTPError{StatusCode: 404},
		&pdp.HTTPError{StatusCode: 409}, &pdp.HTTPError{StatusCode: 429},
		storage.ErrDataSetUnavailable, context.DeadlineExceeded,
	} {
		t.Run(fmt.Sprint(refusal), func(t *testing.T) {
			c := seedUnacknowledgedAttempt(t)
			var sends int
			c.target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
				t.Fatal("resend signed a new request")
				return nil, nil
			}
			c.target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
				sends++
				if string(request.ExtraData) != string(testutil.CommitExtraData(unacknowledgedNonce)) {
					t.Fatal("resend changed the request")
				}
				// The first send lands after the nonce read but before this refusal.
				c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), c.pieceCID)
				return nil, refusal
			}
			at := c.attemptedAt.Add(6 * time.Minute)
			result, err := c.advance(t, at, storagecommit.AdvanceInput{Resend: checkpointResend})
			// Only a refusal the provider may have answered for a dropped piece is
			// handed back for the caller to check.
			refused := synapse.ClassifyCommitRejection(refusal) == synapse.CommitRejectedByProvider
			switch {
			case refused && (result.State != storagecommit.AdvanceResendRefused || err != nil || !errors.Is(result.Cause, refusal)),
				!refused && (result.State != storagecommit.AdvancePending || !errors.Is(err, refusal)),
				sends != 1:
				t.Fatalf("refused resend = %#v err=%v sends=%d", result, err, sends)
			}
			copyRow := loadAdvancerCopy(t, c.repos, c.copyRow.ID)
			attempt := loadAdvancerAttempt(t, c.db, "unacknowledged")
			if copyRow.CommitExtraDataHex == nil || *copyRow.CommitExtraDataHex != testutil.CommitExtraDataHex(unacknowledgedNonce) ||
				attempt.Status != storagecommit.AttemptStatusAttempted || attempt.ResolvedAt != nil || attempt.SubmitError == nil {
				t.Fatalf("refusal lost the fence or signed request: copy=%#v attempt=%#v", copyRow, attempt)
			}
			result, err = c.advance(t, at.Add(time.Minute), storagecommit.AdvanceInput{Resend: checkpointResend})
			if err != nil || result.State != storagecommit.AdvanceConfirmed || !result.ProvenByNonce || sends != 1 {
				t.Fatalf("late first submission = %#v err=%v sends=%d", result, err, sends)
			}
		})
	}
}

func TestAdvancerRestoresReleasedRequestBeforeSubmitting(t *testing.T) {
	for _, scenario := range []string{"registered", "registered after refusal", "unused", "chain unavailable", "another data set", "another piece", "multiple requests", "malformed hex", "malformed authorization", "unknown reason", "normalized history"} {
		t.Run(scenario, func(t *testing.T) {
			c := seedUnacknowledgedAttempt(t)
			releaseLegacyRequest(t, c)
			if scenario == "registered after refusal" {
				if _, err := c.db.NewUpdate().Model((*storagecommit.Attempt)(nil)).Set("release_reason = ?", storagecommit.ReleaseProviderRejected).
					Where("attempt_id = ?", "unacknowledged").Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			var signatures, sends, writeChecks int
			c.target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
				signatures++
				return testutil.CommitExtraData(8), nil
			}
			c.target.CheckWritableFunc = func(context.Context) error {
				writeChecks++
				return storage.ErrDataSetUnavailable
			}
			c.target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
				sends++
				if string(request.ExtraData) != string(testutil.CommitExtraData(unacknowledgedNonce)) {
					t.Fatal("restored request changed")
				}
				return nil, &pdp.HTTPError{StatusCode: 404}
			}
			want := storagecommit.AdvanceNeedsAttention
			switch scenario {
			case "registered", "registered after refusal", "normalized history":
				want = storagecommit.AdvanceConfirmed
				c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), c.pieceCID)
				if scenario == "normalized history" {
					insertReleasedRequest(t, c, "same-request-refused", strings.ToUpper(testutil.CommitExtraDataHex(unacknowledgedNonce)), storagecommit.ReleaseProviderRejected)
				}
			case "unused":
				want = storagecommit.AdvancePending
			case "chain unavailable":
				want = storagecommit.AdvancePending
				c.nonces.Err = errors.New("chain unavailable")
			case "another data set":
				c.nonces.Consume(unacknowledgedNonce, sdktypes.NewBigInt(9999), sdktypes.NewBigInt(41), c.pieceCID)
			case "another piece":
				other, err := cid.Parse("bafkqaaa")
				if err != nil {
					t.Fatal(err)
				}
				c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), other)
			case "multiple requests":
				c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), c.pieceCID)
				insertReleasedRequest(t, c, "another-request", testutil.CommitExtraDataHex(6), storagecommit.ReleaseManualDuplicateAck)
			case "malformed hex", "malformed authorization":
				value := "zz"
				if scenario == "malformed authorization" {
					value = "ab"
				}
				if _, err := c.db.NewUpdate().Model((*storagecommit.Attempt)(nil)).Set("extra_data_hex = ?", value).
					Where("attempt_id = ?", "unacknowledged").Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "unknown reason":
				if _, err := c.db.NewUpdate().Model((*storagecommit.Attempt)(nil)).Set("release_reason = ?", "future_reason").
					Where("attempt_id = ?", "unacknowledged").Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			at := c.attemptedAt.Add(time.Hour)
			result, err := c.advance(t, at, storagecommit.AdvanceInput{})
			if result.State != want || (err != nil && scenario != "chain unavailable") || signatures != 0 || sends != 0 || writeChecks != 0 {
				t.Fatalf("restore %s = %#v err=%v signed=%d sent=%d writes=%d", scenario, result, err, signatures, sends, writeChecks)
			}
			old := loadAdvancerAttempt(t, c.db, "unacknowledged")
			if old.Status != storagecommit.AttemptStatusReleased || old.ResolvedAt == nil || result.AttemptID == old.AttemptID {
				t.Fatalf("restoration rewrote history: %#v result=%#v", old, result)
			}
			switch want {
			case storagecommit.AdvanceConfirmed:
				settleNonceResult(t, c, result)
			case storagecommit.AdvanceNeedsAttention:
				for range 2 {
					replayed, err := c.advance(t, at.Add(time.Hour), storagecommit.AdvanceInput{Resend: checkpointResend})
					if err != nil || replayed.State != want || replayed.AttemptID != result.AttemptID || replayed.Continue {
						t.Fatalf("conflict replay = %#v err=%v", replayed, err)
					}
				}
			case storagecommit.AdvancePending:
				if scenario == "unused" {
					replayed, err := c.advance(t, at.Add(6*time.Minute), storagecommit.AdvanceInput{Resend: checkpointResend})
					if replayed.State != storagecommit.AdvancePending || err == nil || sends != 1 {
						t.Fatalf("restored resend = %#v err=%v sends=%d", replayed, err, sends)
					}
				}
				c.nonces.Err = nil
				c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), c.pieceCID)
				replayed, err := c.advance(t, at.Add(7*time.Minute), storagecommit.AdvanceInput{})
				if err != nil || replayed.State != storagecommit.AdvanceConfirmed {
					t.Fatalf("late historical registration = %#v err=%v", replayed, err)
				}
				settleNonceResult(t, c, replayed)
			}
			if signatures != 0 {
				t.Fatal("recovery signed a new nonce")
			}
		})
	}
}

func TestAdvancerInvalidNewRequestReleasesReservation(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
	target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
	target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xab}, nil }
	target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
		t.Fatal("invalid new request reached the provider")
		return nil, nil
	}
	_, err := (&storagecommit.Advancer{Store: repos.Contents}).Advance(t.Context(), storagecommit.AdvanceInput{
		Copy: copies[0], Binding: *binding, Target: target, Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
	})
	copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
	if err == nil || copyRow.CommitAttemptID != nil || copyRow.CommitExtraDataHex != nil || copyRow.Status != model.StorageCopyStatusPieceReady {
		t.Fatalf("invalid local request held capacity: copy=%#v err=%v", copyRow, err)
	}
}

func TestAdvancerUnavailableRefusalsReuseRequestWithoutHoldingCapacity(t *testing.T) {
	for _, code := range []int{404, 409} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			db := testutil.NewTestDB(t)
			repos := repository.NewRepositories(db)
			binding, copies, pieceCID := seedAdvancerCopies(t, db, 1)
			target := testutil.NewMockDataSetTarget(binding.ProviderID.SDK(), binding.DataSetID.SDK(), nil)
			var signatures, sends int
			target.PresignForCommitFunc = func(context.Context, []storage.PieceInput) ([]byte, error) {
				signatures++
				return testutil.CommitExtraData(7), nil
			}
			target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
				sends++
				return nil, &pdp.HTTPError{StatusCode: code}
			}
			advancer := storagecommit.Advancer{Store: repos.Contents, Nonces: &testutil.MockCommitNonces{}}
			for range 2 {
				result, err := advancer.Advance(t.Context(), storagecommit.AdvanceInput{
					Copy: *loadAdvancerCopy(t, repos, copies[0].ID), Binding: *binding,
					Target: target, Pieces: []storage.PieceInput{{PieceCID: pieceCID}},
				})
				if err != nil || result.State != storagecommit.AdvanceReleased {
					t.Fatalf("known refusal held the attempt: %#v err=%v", result, err)
				}
			}
			copyRow := loadAdvancerCopy(t, repos, copies[0].ID)
			if signatures != 1 || sends != 2 || copyRow.CommitAttemptID != nil {
				t.Fatalf("known refusals signed=%d sent=%d copy=%#v", signatures, sends, copyRow)
			}
		})
	}
}

func TestAdvancerObservedSubmissionCanBeConfirmedByNonce(t *testing.T) {
	for _, scenario := range []string{"rejected", "overdue pending", "mismatched transaction"} {
		t.Run(scenario, func(t *testing.T) {
			c := seedUnacknowledgedAttempt(t)
			if err := c.repos.Contents.RecordCommitSubmission(t.Context(), storagecommit.EvidenceInput{
				Copy: advancerCopyIdentity(c.copyRow), AttemptID: "unacknowledged", TransactionID: "0xsubmitted",
				StatusURL: "https://provider.example/status/original", Now: c.attemptedAt,
			}); err != nil {
				t.Fatal(err)
			}
			c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), c.pieceCID)
			c.target.GetCommitStatusFunc = func(context.Context, string) (*storage.CommitStatus, error) {
				ref, _ := c.target.DataSetRef()
				status := &storage.CommitStatus{Kind: storage.CommitKindAddPieces, State: storage.CommitStateRejected, TransactionID: "0xsubmitted", DataSet: &ref}
				switch scenario {
				case "overdue pending":
					status.State = storage.CommitStatePending
				case "mismatched transaction":
					status.TransactionID = "0xother"
				}
				return status, nil
			}
			result, err := c.advance(t, c.attemptedAt.Add(time.Hour), storagecommit.AdvanceInput{})
			if err != nil || result.State != storagecommit.AdvanceConfirmed || result.Confirmation.TransactionID != "0xsubmitted" {
				t.Fatalf("nonce confirmation = %#v err=%v", result, err)
			}
			settleNonceResult(t, c, result)
			attempt := loadAdvancerAttempt(t, c.db, "unacknowledged")
			if attempt.TransactionID == nil || *attempt.TransactionID != "0xsubmitted" || attempt.ConfirmedTransactionID != nil {
				t.Fatalf("nonce confirmation changed transaction evidence: %#v", attempt)
			}
		})
	}
}

// Another request in the copy's history cannot disprove the attempt's own
// evidence; it only keeps recovery from sending the attempt's request again.
func TestAdvancerConflictingRequestsSettleOnlyByOwnEvidence(t *testing.T) {
	for _, scenario := range []string{"provider confirmed", "own nonce registered", "unregistered"} {
		t.Run(scenario, func(t *testing.T) {
			c := seedUnacknowledgedAttempt(t)
			insertReleasedRequest(t, c, "earlier-request", testutil.CommitExtraDataHex(6), storagecommit.ReleaseManualDuplicateAck)
			sends := 0
			c.target.SubmitCommitFunc = func(context.Context, storage.CommitRequest) (*storage.CommitSubmission, error) {
				sends++
				return nil, errors.New("conflicting history sent a request")
			}
			switch scenario {
			case "provider confirmed":
				if err := c.repos.Contents.RecordCommitSubmission(t.Context(), storagecommit.EvidenceInput{
					Copy: advancerCopyIdentity(c.copyRow), AttemptID: "unacknowledged", TransactionID: "0xsubmitted",
					StatusURL: "https://provider.example/status/original", Now: c.attemptedAt,
				}); err != nil {
					t.Fatal(err)
				}
				c.target.GetCommitStatusFunc = func(context.Context, string) (*storage.CommitStatus, error) {
					ref, _ := c.target.DataSetRef()
					return &storage.CommitStatus{
						Kind: storage.CommitKindAddPieces, State: storage.CommitStateConfirmed, TransactionID: "0xsubmitted",
						DataSet: &ref, PieceIDs: []sdktypes.BigInt{sdktypes.NewBigInt(41)},
					}, nil
				}
			case "own nonce registered":
				c.nonces.Consume(unacknowledgedNonce, c.binding.DataSetID.SDK(), sdktypes.NewBigInt(41), c.pieceCID)
			}
			result, err := c.advance(t, c.attemptedAt.Add(time.Hour), storagecommit.AdvanceInput{Resend: checkpointResend})
			switch {
			case err != nil || sends != 0:
				t.Fatalf("advance = %#v err=%v sends=%d", result, err, sends)
			case scenario == "unregistered":
				if result.State != storagecommit.AdvanceNeedsAttention || result.Continue ||
					result.AttentionCode != storagecommit.AttentionSubmissionMismatch {
					t.Fatalf("unregistered conflict = %#v, want a stopped mismatch", result)
				}
			case result.State != storagecommit.AdvanceConfirmed || result.ExtraDataHex != testutil.CommitExtraDataHex(unacknowledgedNonce):
				t.Fatalf("%s = %#v, want the attempt confirmed by its own request", scenario, result)
			case scenario == "own nonce registered":
				settleNonceResult(t, c, result)
			}
		})
	}
}

func releaseLegacyRequest(t *testing.T, c nonceCase) {
	t.Helper()
	if _, err := c.db.NewUpdate().Model((*storagecommit.Attempt)(nil)).
		Set("status = ?", storagecommit.AttemptStatusReleased).
		Set("release_reason = ?", storagecommit.ReleaseManualDuplicateAck).
		Set("resolved_at = ?", c.attemptedAt.Add(time.Minute)).Where("attempt_id = ?", "unacknowledged").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.NewUpdate().Model((*model.StorageCopy)(nil)).
		Set("status = ?", model.StorageCopyStatusPieceReady).Set("commit_extra_data_hex = NULL").
		Set("commit_ready_at = NULL").Where("id = ?", c.copyRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func insertReleasedRequest(t *testing.T, c nonceCase, attemptID, extra string, reason storagecommit.ReleaseReason) {
	t.Helper()
	releasedAt := c.attemptedAt.Add(time.Minute)
	reasonText := string(reason)
	if _, err := c.db.NewInsert().Model(&storagecommit.Attempt{
		AttemptID: attemptID, ContentID: c.copyRow.ContentID, StorageDataSetID: c.binding.ID,
		Status: storagecommit.AttemptStatusReleased, ExtraDataHex: &extra, ReleaseReason: &reasonText,
		AttemptedAt: &c.attemptedAt, ResolvedAt: &releasedAt,
	}).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func settleNonceResult(t *testing.T, c nonceCase, result storagecommit.AdvanceResult) {
	t.Helper()
	if !result.ProvenByNonce || result.ExtraDataHex != testutil.CommitExtraDataHex(unacknowledgedNonce) ||
		result.Confirmation == nil || result.Confirmation.ConfirmedTransactionID != "" {
		t.Fatalf("wrong nonce confirmation evidence: %#v", result)
	}
	pieceID := idtypes.OnChainIDFromSDK(result.Confirmation.PieceIDs[0])
	input := repository.MarkUploadCopyCommittedInput{
		StorageCopyID: c.copyRow.ID, ContentID: c.copyRow.ContentID, CopyIndex: c.copyRow.CopyIndex,
		PieceCID: c.pieceCID.String(), PieceID: &pieceID, RetrievalURL: c.target.PieceURL(c.pieceCID),
		CommitExtraDataHex: result.ExtraDataHex, CommitAttemptID: result.AttemptID,
		CommitTransactionID: result.Confirmation.TransactionID, ProvenByNonce: true,
	}
	for range 2 {
		if err := c.repos.Contents.MarkUploadCopyCommitted(t.Context(), input); err != nil {
			t.Fatal(err)
		}
	}
	copyRow := loadAdvancerCopy(t, c.repos, c.copyRow.ID)
	if copyRow.Status != model.StorageCopyStatusCommitted || copyRow.ConfirmedAttemptID == nil || *copyRow.ConfirmedAttemptID != result.AttemptID {
		t.Fatalf("copy did not project its proving attempt: %#v", copyRow)
	}
}
