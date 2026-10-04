package storagecommit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

const testNonce = 7

type commitCase struct {
	target  *testutil.MockStorageTarget
	nonces  *testutil.MockCommitNonces
	pieces  []cid.Cid
	now     time.Time
	request storagecommit.Request
}

func newCommitCase(t *testing.T, pieces int) *commitCase {
	t.Helper()
	dataSetID := sdktypes.NewBigInt(801)
	target := testutil.NewMockDataSetTarget(sdktypes.NewBigInt(701), dataSetID, nil)
	target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
	extra := testutil.CommitExtraDataHex(testNonce)
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	c := &commitCase{
		target: target, nonces: &testutil.MockCommitNonces{}, now: now,
		request: storagecommit.Request{
			RequestID: "request", StorageDataSetID: 1, Status: storagecommit.RequestStatusSubmitted,
			PieceCount: pieces, ExtraDataHex: &extra, Sends: 1,
		},
	}
	for i := range pieces {
		c.pieces = append(c.pieces, testPieceCID(t, byte(i)))
	}
	c.sentAt(now.Add(-time.Minute))
	return c
}

func (c *commitCase) sentAt(at time.Time) {
	c.request.FirstSentAt, c.request.SubmittedAt, c.request.LastSentAt = &at, &at, &at
}

func (c *commitCase) commit() storagecommit.Commit {
	return storagecommit.Commit{Request: c.request, Pieces: c.pieces, Target: c.target}
}

func (c *commitCase) advancer() *storagecommit.Advancer {
	return &storagecommit.Advancer{Nonces: c.nonces, Now: func() time.Time { return c.now }}
}

// land records the request's nonce as having added pieces at firstPieceID.
func (c *commitCase) land(firstPieceID uint64, pieces []cid.Cid) {
	c.nonces.ConsumeRequest(testutil.CommitExtraData(testNonce), sdktypes.NewBigInt(801), sdktypes.NewBigInt(firstPieceID), pieces)
}

func (c *commitCase) receipt(state storage.CommitState, pieceIDs ...uint64) {
	tx, statusURL := "0xtx", "https://provider.example/status/0xtx"
	c.request.TransactionID, c.request.StatusURL = &tx, &statusURL
	ref, _ := c.target.DataSetRef()
	ids := make([]sdktypes.BigInt, len(pieceIDs))
	for i, id := range pieceIDs {
		ids[i] = sdktypes.NewBigInt(id)
	}
	c.target.GetCommitStatusFunc = func(context.Context, string) (*storage.CommitStatus, error) {
		return &storage.CommitStatus{
			Kind: storage.CommitKindAddPieces, State: state, TransactionID: tx,
			DataSet: &ref, PieceIDs: ids,
		}, nil
	}
}

func testPieceCID(t *testing.T, seed byte) cid.Cid {
	t.Helper()
	digest, err := multihash.Sum([]byte{seed, 0xa5}, multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, digest)
}

func TestProveRequiresEveryPositionToHoldItsPiece(t *testing.T) {
	c := newCommitCase(t, 2)
	ctx := t.Context()
	proof, err := c.advancer().Prove(ctx, c.commit())
	if err != nil || proof.Outcome != storagecommit.ProofUnused {
		t.Fatalf("unused nonce = %#v, %v", proof, err)
	}
	c.land(40, c.pieces)
	proof, err = c.advancer().Prove(ctx, c.commit())
	if err != nil || proof.Outcome != storagecommit.ProofLanded || proof.FirstPieceID.String() != "40" {
		t.Fatalf("landed request = %#v, %v", proof, err)
	}
	// The count and the IDs agree, but one position holds another piece.
	c.land(40, []cid.Cid{c.pieces[0], testPieceCID(t, 9)})
	if proof, err := c.advancer().Prove(ctx, c.commit()); err != nil || proof.Outcome != storagecommit.ProofConflict {
		t.Fatalf("wrong piece at one position = %#v, %v, want conflict", proof, err)
	}
	c.nonces.Err = errors.New("rpc unavailable")
	if _, err := c.advancer().Prove(ctx, c.commit()); err == nil {
		t.Fatal("an unreadable chain proved something")
	}
}

func TestObserveConfirmsOnlyWhatTheChainProves(t *testing.T) {
	ctx := t.Context()
	c := newCommitCase(t, 2)
	c.receipt(storage.CommitStateConfirmed, 40, 41)
	// The provider reports the transaction before this node's chain view does.
	observation, err := c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObservePending {
		t.Fatalf("receipt ahead of the chain = %#v, %v, want pending", observation, err)
	}
	c.land(40, c.pieces)
	observation, err = c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObserveConfirmed ||
		observation.Confirmation.FirstPieceID.String() != "40" || observation.Confirmation.ConfirmedTransactionID != "0xtx" {
		t.Fatalf("proven receipt = %#v, %v", observation, err)
	}

	c = newCommitCase(t, 2)
	c.receipt(storage.CommitStateConfirmed, 40, 41)
	c.land(40, []cid.Cid{c.pieces[1], c.pieces[0]})
	if observation, err := c.advancer().Observe(ctx, c.commit()); err != nil ||
		observation.Kind != storagecommit.ObserveStop || observation.Attention != storagecommit.AttentionSubmissionMismatch {
		t.Fatalf("receipt for other pieces = %#v, %v, want a stop", observation, err)
	}

	// Piece IDs that are not consecutive describe no single add-pieces call.
	c = newCommitCase(t, 2)
	c.receipt(storage.CommitStateConfirmed, 40, 42)
	c.sentAt(c.now.Add(-10 * time.Minute))
	observation, err = c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObserveResendDue || !observation.DropEvidence {
		t.Fatalf("non-consecutive receipt = %#v, %v, want its evidence dropped and a resend", observation, err)
	}
}

func TestObserveResendsAnUnknownOutcomeUnchangedOnItsPace(t *testing.T) {
	ctx := t.Context()
	c := newCommitCase(t, 1)
	observation, err := c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObservePending || observation.RetryAfter != 4*time.Minute {
		t.Fatalf("recent send = %#v, %v, want to wait out the first resend delay", observation, err)
	}
	c.sentAt(c.now.Add(-6 * time.Minute))
	observation, err = c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObserveResendDue || observation.Attention != "" {
		t.Fatalf("due send = %#v, %v", observation, err)
	}
	c.sentAt(c.now.Add(-20 * time.Minute))
	observation, err = c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObserveResendDue || observation.Attention != storagecommit.AttentionConfirmationTimeout {
		t.Fatalf("long-unregistered request = %#v, %v, want resend with attention", observation, err)
	}
	c.target.CheckWritableFunc = func(context.Context) error { return storage.ErrDataSetUnavailable }
	observation, err = c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObserveAbandon {
		t.Fatalf("ended data set = %#v, %v, want abandon", observation, err)
	}
}

func TestObserveResendsOnceAnUnreadableProviderNeedsAttention(t *testing.T) {
	ctx := t.Context()
	c := newCommitCase(t, 1)
	c.receipt(storage.CommitStatePending)
	c.target.GetCommitStatusFunc = func(context.Context, string) (*storage.CommitStatus, error) {
		return nil, &pdp.HTTPError{StatusCode: 503, Body: "unavailable"}
	}
	observation, err := c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObservePending || observation.Attention != "" {
		t.Fatalf("recent submission = %#v, %v, want to wait for the provider", observation, err)
	}
	c.sentAt(c.now.Add(-20 * time.Minute))
	observation, err = c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObservePending || observation.Attention != storagecommit.AttentionConfirmationTimeout {
		t.Fatalf("long-unanswered submission = %#v, %v, want it flagged", observation, err)
	}
	// Once flagged, the provider's silence settles nothing: an unused nonce
	// is sent again, keeping the submission in case it still lands.
	flagged := c.now.Add(-time.Minute)
	c.request.AttentionAt = &flagged
	observation, err = c.advancer().Observe(ctx, c.commit())
	if err != nil || observation.Kind != storagecommit.ObserveResendDue || observation.DropEvidence {
		t.Fatalf("flagged unanswered submission = %#v, %v, want a resend", observation, err)
	}
	c.nonces.Err = errors.New("rpc unavailable")
	if observation, err := c.advancer().Observe(ctx, c.commit()); err != nil || observation.Kind != storagecommit.ObservePending {
		t.Fatalf("unreadable chain = %#v, %v, want no resend", observation, err)
	}
	c.nonces.Err = nil
	c.land(40, c.pieces)
	if observation, err := c.advancer().Observe(ctx, c.commit()); err != nil || observation.Kind != storagecommit.ObserveConfirmed {
		t.Fatalf("landed request = %#v, %v, want confirmed", observation, err)
	}
}

func TestPrepareReadsTheChainBeforeSending(t *testing.T) {
	ctx := t.Context()
	c := newCommitCase(t, 1)
	c.request.Status = storagecommit.RequestStatusReady
	if prepared := c.advancer().Prepare(ctx, c.commit()); prepared.Outcome != storagecommit.PrepareSend {
		t.Fatalf("writable data set = %#v", prepared)
	}
	c.target.CheckWritableFunc = func(context.Context) error { return errors.New("rpc timeout") }
	if prepared := c.advancer().Prepare(ctx, c.commit()); prepared.Outcome != storagecommit.PrepareRetry {
		t.Fatalf("unreadable data set = %#v, want retry", prepared)
	}
	c.target.CheckWritableFunc = func(context.Context) error { return storage.ErrDataSetUnavailable }
	if prepared := c.advancer().Prepare(ctx, c.commit()); prepared.Outcome != storagecommit.PrepareAbandon {
		t.Fatalf("ended data set = %#v, want abandon", prepared)
	}
	c.land(40, c.pieces)
	if prepared := c.advancer().Prepare(ctx, c.commit()); prepared.Outcome != storagecommit.PrepareConfirmed {
		t.Fatalf("already landed request = %#v, want confirmed", prepared)
	}
}

func TestSendTellsRefusalsFromUnknownOutcomes(t *testing.T) {
	ctx := t.Context()
	c := newCommitCase(t, 2)
	for _, tt := range []struct {
		name   string
		submit func(storage.CommitRequest) (*storage.CommitSubmission, error)
		want   storagecommit.SendKind
	}{
		{"accepted", func(storage.CommitRequest) (*storage.CommitSubmission, error) {
			return &storage.CommitSubmission{TransactionID: "0xtx", StatusURL: "https://provider.example/status"}, nil
		}, storagecommit.SendAccepted},
		{"refused", func(storage.CommitRequest) (*storage.CommitSubmission, error) {
			return nil, &pdp.HTTPError{StatusCode: 400, Body: "piece not found"}
		}, storagecommit.SendRefused},
		{"data set unknown to provider", func(storage.CommitRequest) (*storage.CommitSubmission, error) {
			return nil, &pdp.HTTPError{StatusCode: 404}
		}, storagecommit.SendRefused},
		{"refused by the SDK before sending", func(storage.CommitRequest) (*storage.CommitSubmission, error) {
			return nil, storage.ErrDataSetUnavailable
		}, storagecommit.SendNotSent},
		{"announced then failed", func(request storage.CommitRequest) (*storage.CommitSubmission, error) {
			request.OnSubmitted(storage.CommitSubmission{TransactionID: "0xtx", StatusURL: "https://provider.example/status"})
			return nil, &pdp.HTTPError{StatusCode: 400}
		}, storagecommit.SendUnknown},
		{"server error", func(storage.CommitRequest) (*storage.CommitSubmission, error) {
			return nil, &pdp.HTTPError{StatusCode: 502}
		}, storagecommit.SendUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var sentPieces int
			c.target.SubmitCommitFunc = func(_ context.Context, request storage.CommitRequest) (*storage.CommitSubmission, error) {
				sentPieces = len(request.Pieces)
				return tt.submit(request)
			}
			if result := c.advancer().Send(ctx, c.commit(), nil); result.Kind != tt.want {
				t.Fatalf("send = %#v, want kind %d", result, tt.want)
			}
			if sentPieces != 2 {
				t.Fatalf("sent %d pieces, want the whole request", sentPieces)
			}
		})
	}
}

func TestShouldSealHonorsCollectionWindow(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := storagecommit.SealInput{
		Members: 1, OldestJoinedAt: now.Add(-10 * time.Minute), Now: now, MaxPieces: 4, MaxWait: 30 * time.Minute,
	}
	for _, tt := range []struct {
		name string
		edit func(*storagecommit.SealInput)
		seal bool
		wait time.Duration
	}{
		{"one member", func(*storagecommit.SealInput) {}, false, 20 * time.Minute},
		{"more members", func(in *storagecommit.SealInput) { in.Members = 3 }, false, 20 * time.Minute},
		{"later poll", func(in *storagecommit.SealInput) { in.Now = now.Add(5 * time.Minute) }, false, 15 * time.Minute},
		{"just before deadline", func(in *storagecommit.SealInput) { in.Now = in.OldestJoinedAt.Add(in.MaxWait - time.Nanosecond) }, false, time.Nanosecond},
		{"at deadline", func(in *storagecommit.SealInput) { in.Now = in.OldestJoinedAt.Add(in.MaxWait) }, true, 0},
		{"past deadline", func(in *storagecommit.SealInput) { in.Now = in.OldestJoinedAt.Add(in.MaxWait + time.Second) }, true, 0},
		{"full", func(in *storagecommit.SealInput) { in.Members = 4 }, true, 0},
		{"draining", func(in *storagecommit.SealInput) { in.Draining = true }, true, 0},
		{"cache pressure", func(in *storagecommit.SealInput) { in.CachePressure = true }, true, 0},
		{"manual request", func(in *storagecommit.SealInput) { in.ManualRequested = true }, true, 0},
		{"empty under pressure and manual request", func(in *storagecommit.SealInput) { in.Members = 0; in.CachePressure = true; in.ManualRequested = true }, false, 0},
		{"zero wait", func(in *storagecommit.SealInput) { in.MaxWait = 0 }, true, 0},
		{"zero wait after clock moved back", func(in *storagecommit.SealInput) { in.MaxWait, in.Now = 0, in.OldestJoinedAt.Add(-time.Second) }, true, 0},
		{"empty", func(in *storagecommit.SealInput) { in.Members = 0 }, false, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := base
			tt.edit(&in)
			if seal, wait := storagecommit.ShouldSeal(in); seal != tt.seal || wait != tt.wait {
				t.Fatalf("ShouldSeal = %t, %s, want %t, %s", seal, wait, tt.seal, tt.wait)
			}
		})
	}
}

func TestMaxPiecesKeepsTheLegacyLimit(t *testing.T) {
	if got := storagecommit.MaxPieces(idtypes.NewOnChainID(100), 32331, 120); got != pdp.MaxLegacyAddPiecesBatchSize {
		t.Fatalf("legacy data set limit = %d, want %d", got, pdp.MaxLegacyAddPiecesBatchSize)
	}
	if got := storagecommit.MaxPieces(idtypes.NewOnChainID(40000), 32331, 120); got != 120 {
		t.Fatalf("compact data set limit = %d, want the configured 120", got)
	}
	if got := storagecommit.MaxPieces(idtypes.NewOnChainID(100), 0, 120); got != 120 {
		t.Fatalf("unknown network limit = %d, want the configured 120", got)
	}
}
