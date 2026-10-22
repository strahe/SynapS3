//go:build systemtest

package systemtest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestMemoryFilecoinLifecycleAndProviderIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	filecoin := NewMemoryFilecoin()
	targets, err := filecoin.SelectUploadTargets(ctx, storage.SelectUploadContextsOptions{Copies: 3})
	if err != nil {
		t.Fatalf("SelectUploadTargets: %v", err)
	}
	contexts := make([]synapse.DataSetTarget, 0, len(targets))
	for _, target := range targets {
		providerTarget, ok := target.(synapse.ProviderTarget)
		if !ok {
			t.Fatalf("selected target %T is not a ProviderTarget", target)
		}
		created, err := providerTarget.CreateDataSet(ctx, nil)
		if err != nil {
			t.Fatalf("CreateDataSet provider %s: %v", providerTarget.ProviderID().String(), err)
		}
		if _, bound := providerTarget.DataSetRef(); bound {
			t.Fatal("ProviderTarget became bound after CreateDataSet")
		}
		providerID := created.DataSet.ProviderID()
		dataSetTarget, err := filecoin.OpenDataSetTarget(ctx, created.DataSet.DataSetID(), storage.NewDataSetContextOptions{ProviderID: &providerID})
		if err != nil {
			t.Fatalf("OpenDataSetTarget: %v", err)
		}
		contexts = append(contexts, dataSetTarget)
	}

	content := bytes.Repeat([]byte("synaps3-system-test"), 128)
	identity, err := piece.CalculateFromBytes(content)
	if err != nil {
		t.Fatal(err)
	}
	sessionID, err := contexts[0].CreateStoreSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := contexts[0].FinalizeStoreSession(ctx, contexts[0].ServiceURL(), sessionID, identity.CIDv2); !errors.Is(err, synapse.ErrStoreSessionIncomplete) {
		t.Fatalf("FinalizeStoreSession before upload = %v", err)
	}
	stored, err := contexts[0].UploadStoreSession(ctx, contexts[0].ServiceURL(), sessionID, bytes.NewReader(content), &storage.StoreOptions{PieceCID: identity.CIDv2})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if state, err := filecoin.FindParkedPiece(ctx, contexts[0].ServiceURL(), stored.PieceCID); err != nil || state != synapse.ParkedPieceMissing {
		t.Fatalf("piece before finalization = %s, %v", state, err)
	}
	if err := contexts[0].FinalizeStoreSession(ctx, contexts[0].ServiceURL(), sessionID, stored.PieceCID); err != nil {
		t.Fatal(err)
	}
	if err := contexts[0].FinalizeStoreSession(ctx, contexts[0].ServiceURL(), sessionID, stored.PieceCID); !errors.Is(err, synapse.ErrStoreSessionMissing) {
		t.Fatalf("FinalizeStoreSession after publication = %v", err)
	}
	for i, target := range contexts {
		state, err := filecoin.FindParkedPiece(ctx, target.ServiceURL(), stored.PieceCID)
		want := synapse.ParkedPieceMissing
		if i == 0 {
			want = synapse.ParkedPieceReady
		}
		if err != nil || state != want {
			t.Fatalf("provider %s local piece before Pull = %s, %v; want %s", target.ProviderID(), state, err, want)
		}
		if i == 0 {
			status, err := target.PieceStatus(ctx, stored.PieceCID)
			if err == nil {
				t.Fatalf("piece registered before Commit = %#v, %v", status, err)
			}
		}
	}
	if _, err := filecoin.Download(ctx, stored.PieceCID, nil); !errors.Is(err, errInvalidFilecoinSequence) {
		t.Fatalf("Download before Commit error = %v, want invalid sequence", err)
	}
	piece := storage.PieceInput{PieceCID: stored.PieceCID}
	extra, err := contexts[1].PresignForCommit(ctx, []storage.PieceInput{piece})
	if err != nil {
		t.Fatalf("secondary PresignForCommit: %v", err)
	}
	if _, err := contexts[1].SubmitCommit(ctx, storage.CommitRequest{Pieces: []storage.PieceInput{piece}, ExtraData: extra}); !errors.Is(err, errInvalidFilecoinSequence) {
		t.Fatalf("secondary SubmitCommit before Pull error = %v, want invalid sequence", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	pullAuthorizations := make([][]byte, len(contexts)-1)
	for i, uploadContext := range contexts[1:] {
		pullAuthorizations[i], err = uploadContext.PresignForCommit(ctx, []storage.PieceInput{piece})
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			_, pullErr := uploadContext.SubmitPull(ctx, storage.PullRequest{
				Pieces: []cid.Cid{stored.PieceCID}, ExtraData: pullAuthorizations[i],
				From: contexts[0].PieceURL,
			})
			errCh <- pullErr
		})
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("Pull: %v", err)
		}
	}
	for _, uploadContext := range contexts {
		if state, err := filecoin.FindParkedPiece(ctx, uploadContext.ServiceURL(), stored.PieceCID); err != nil || state != synapse.ParkedPieceReady {
			t.Fatalf("provider-local piece after Pull = %s, %v", state, err)
		}
		extra, err := uploadContext.PresignForCommit(ctx, []storage.PieceInput{piece})
		if err != nil {
			t.Fatalf("PresignForCommit provider %s: %v", uploadContext.ProviderID().String(), err)
		}
		submission, err := uploadContext.SubmitCommit(ctx, storage.CommitRequest{Pieces: []storage.PieceInput{piece}, ExtraData: extra})
		if err != nil {
			t.Fatalf("SubmitCommit provider %s: %v", uploadContext.ProviderID().String(), err)
		}
		status, err := uploadContext.GetCommitStatus(ctx, submission.StatusURL)
		if err != nil || status == nil || status.State != storage.CommitStateConfirmed || len(status.PieceIDs) != 1 {
			t.Fatalf("GetCommitStatus provider %s: status=%#v err=%v", uploadContext.ProviderID().String(), status, err)
		}
	}

	for i, authorization := range pullAuthorizations {
		nonce, err := storagecommit.ExtraDataNonce(authorization)
		if err != nil {
			t.Fatal(err)
		}
		state, err := filecoin.ClientNonce(ctx, nonce)
		if err != nil || state.Consumed {
			t.Fatalf("Pull authorization after Commit = %#v, %v, want unconsumed", state, err)
		}
		// B does not revoke A or prevent another registration of the same CID.
		if _, err := contexts[i+1].SubmitCommit(ctx, storage.CommitRequest{Pieces: []storage.PieceInput{piece}, ExtraData: authorization}); err != nil {
			t.Fatalf("residual Pull authorization: %v", err)
		}
	}

	download, err := filecoin.Download(ctx, stored.PieceCID, nil)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer func() { _ = download.Close() }()
	got, err := io.ReadAll(download)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("downloaded content differs from stored content")
	}
	for _, target := range contexts {
		ref, _ := target.DataSetRef()
		dataSetID := ref.DataSetID()
		var ids []sdktypes.BigInt
		for key := range filecoin.dataSets[dataSetID.String()].pieces {
			id, err := sdktypes.ParseBigInt(key)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		cleanup, err := filecoin.OpenCleanupContext(ctx, dataSetID, storage.NewDataSetContextOptions{})
		if err != nil {
			t.Fatal(err)
		}
		submission, err := cleanup.DeletePiecesByID(ctx, append(ids, ids[0]))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := filecoin.TransactionReceipt(ctx, submission.Hash); err != nil {
			t.Fatalf("removal receipt: %v", err)
		}
		if _, err := cleanup.DeletePiecesByID(ctx, ids); err == nil {
			t.Fatal("duplicate queued removal succeeded")
		}
		queued, err := filecoin.DeletionStates(ctx, dataSetID, ids)
		if err != nil || queued.QueueLength != len(ids) {
			t.Fatalf("queued removal = %#v, err=%v", queued, err)
		}
		for _, status := range queued.Pieces {
			if status != synapse.CleanupPieceScheduled {
				t.Fatalf("pending deletion state = %s", status)
			}
		}
		removed, err := filecoin.DeletionStates(ctx, dataSetID, ids)
		if err != nil || removed.QueueLength != 0 || removed.BlockNumber <= queued.BlockNumber {
			t.Fatalf("completed removal = %#v, err=%v", removed, err)
		}
		for _, status := range removed.Pieces {
			if status != synapse.CleanupPieceRemoved {
				t.Fatalf("completed deletion state = %s", status)
			}
		}
		if _, err := cleanup.DeletePiecesByID(ctx, ids); err == nil {
			t.Fatal("duplicate completed removal succeeded")
		}
	}
	if filecoin.removalFees != uint64(len(contexts)) {
		t.Fatalf("removal fees = %d, want %d", filecoin.removalFees, len(contexts))
	}
	if _, err := filecoin.Download(ctx, stored.PieceCID, nil); !errors.Is(err, errInvalidFilecoinSequence) {
		t.Fatalf("download after removal = %v", err)
	}
}

func TestMemoryFilecoinRemovalBatchRejectsAtomicallyAndRetainsCleanup(t *testing.T) {
	t.Parallel()
	filecoin := NewMemoryFilecoin()
	dataSetID, providerID := sdktypes.NewBigInt(11), sdktypes.NewBigInt(101)
	identity, err := piece.CalculateFromBytes(bytes.Repeat([]byte("removal"), 128))
	if err != nil {
		t.Fatal(err)
	}
	dataSet := &memoryDataSet{
		id: dataSetID, clientID: sdktypes.NewBigInt(51), provider: providerID,
		pieces: map[string]cid.Cid{"7": identity.CIDv2, "8": identity.CIDv2},
	}
	filecoin.dataSets[dataSetID.String()] = dataSet
	target, err := filecoin.OpenCleanupContext(t.Context(), dataSetID, storage.NewDataSetContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.DeletePiecesByID(t.Context(), []sdktypes.BigInt{sdktypes.NewBigInt(7), sdktypes.NewBigInt(99)}); err == nil {
		t.Fatal("batch with an absent member succeeded")
	}
	if len(dataSet.removals) != 0 || len(dataSet.pieces) != 2 || filecoin.removalFees != 0 {
		t.Fatalf("failed batch changed state: queue=%v pieces=%v fees=%d", dataSet.removals, dataSet.pieces, filecoin.removalFees)
	}
	dataSet.cleanup = true
	for range 2 {
		state, err := filecoin.DeletionStates(t.Context(), dataSetID, []sdktypes.BigInt{sdktypes.NewBigInt(7)})
		if err != nil || state.Pieces["7"] != synapse.CleanupPieceDataSetCleanup || len(dataSet.pieces) != 2 {
			t.Fatalf("cleanup phase = %#v, err=%v", state, err)
		}
	}
	delete(filecoin.dataSets, dataSetID.String())
	state, err := filecoin.DeletionStates(t.Context(), dataSetID, []sdktypes.BigInt{sdktypes.NewBigInt(7)})
	if err != nil || state.Pieces["7"] != synapse.CleanupPieceRemoved {
		t.Fatalf("removed data set = %#v, err=%v", state, err)
	}
}

func TestMemoryFilecoinFindMatchingDataSetRequiresExactMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	filecoin := NewMemoryFilecoin()
	providerID := sdktypes.NewBigInt(101)
	target, err := filecoin.OpenProviderTarget(ctx, providerID, storage.NewProviderContextOptions{
		DataSetMetadata: map[string]string{"bucket": "bucket-a", "original": ""},
	})
	if err != nil {
		t.Fatalf("OpenProviderTarget: %v", err)
	}
	if _, err := target.CreateDataSet(ctx, nil); err != nil {
		t.Fatalf("CreateDataSet: %v", err)
	}

	matched, err := filecoin.FindMatchingDataSet(
		ctx,
		providerID,
		map[string]string{"bucket": "bucket-a", "different": ""},
		false,
	)
	if err != nil {
		t.Fatalf("FindMatchingDataSet: %v", err)
	}
	if matched != nil {
		t.Fatalf("FindMatchingDataSet returned data set %s for different metadata", matched.DataSetID().String())
	}
}

func TestMemoryFilecoinRejectsInvalidSequenceAndCancellation(t *testing.T) {
	t.Parallel()
	filecoin := NewMemoryFilecoin()
	ctx := context.Background()
	targets, err := filecoin.SelectUploadTargets(ctx, storage.SelectUploadContextsOptions{Copies: 1})
	if err != nil {
		t.Fatalf("SelectUploadTargets: %v", err)
	}
	if _, bound := targets[0].DataSetRef(); bound {
		t.Fatal("new provider target unexpectedly has a data set")
	}
	if _, err := filecoin.OpenDataSetTarget(ctx, sdktypes.NewBigInt(999999), storage.NewDataSetContextOptions{}); err == nil {
		t.Fatal("OpenDataSetTarget unknown data set succeeded")
	}
	unknown := cid.MustParse("bafkreibm6jg3ux5qumh4jxcq3xjgbpfs2jsl2w7jtjsq3btqfnhtzgdrmq")
	if _, err := filecoin.Download(ctx, unknown, nil); err == nil {
		t.Fatal("Download unknown CID succeeded")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := filecoin.SelectUploadTargets(cancelled, storage.SelectUploadContextsOptions{Copies: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("SelectUploadTargets cancelled error = %v, want context.Canceled", err)
	}
}

func TestMemoryFilecoinAuthorizationBindsTargetAndOrderedPieces(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	filecoin := NewMemoryFilecoin()
	selected, err := filecoin.SelectUploadTargets(ctx, storage.SelectUploadContextsOptions{Copies: 2})
	if err != nil {
		t.Fatal(err)
	}
	var targets []synapse.DataSetTarget
	for _, provider := range selected {
		created, err := provider.(synapse.ProviderTarget).CreateDataSet(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		providerID := created.DataSet.ProviderID()
		target, err := filecoin.OpenDataSetTarget(ctx, created.DataSet.DataSetID(), storage.NewDataSetContextOptions{ProviderID: &providerID})
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target)
	}
	var inputs []storage.PieceInput
	for _, data := range []string{"first piece", "second piece"} {
		payload := bytes.Repeat([]byte(data), 128)
		identity, err := piece.CalculateFromBytes(payload)
		if err != nil {
			t.Fatal(err)
		}
		sessionID, err := targets[0].CreateStoreSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := targets[0].UploadStoreSession(ctx, targets[0].ServiceURL(), sessionID, bytes.NewReader(payload), &storage.StoreOptions{PieceCID: identity.CIDv2})
		if err != nil {
			t.Fatal(err)
		}
		if err := targets[0].FinalizeStoreSession(ctx, targets[0].ServiceURL(), sessionID, stored.PieceCID); err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, storage.PieceInput{PieceCID: stored.PieceCID})
	}
	extra, err := targets[1].PresignForCommit(ctx, inputs)
	if err != nil {
		t.Fatal(err)
	}
	ordered := []cid.Cid{inputs[0].PieceCID, inputs[1].PieceCID}
	reversed := []cid.Cid{inputs[1].PieceCID, inputs[0].PieceCID}
	for _, mismatch := range []struct {
		name   string
		target synapse.DataSetTarget
		pieces []cid.Cid
	}{
		{"target", targets[0], ordered},
		{"order", targets[1], reversed},
		{"membership", targets[1], ordered[:1]},
	} {
		t.Run(mismatch.name, func(t *testing.T) {
			if _, err := mismatch.target.SubmitPull(ctx, storage.PullRequest{Pieces: mismatch.pieces, ExtraData: extra, From: targets[0].PieceURL}); !errors.Is(err, errInvalidFilecoinSequence) {
				t.Fatalf("mismatched Pull = %v", err)
			}
			pieces := make([]storage.PieceInput, len(mismatch.pieces))
			for i, pieceCID := range mismatch.pieces {
				pieces[i] = storage.PieceInput{PieceCID: pieceCID}
			}
			if _, err := mismatch.target.SubmitCommit(ctx, storage.CommitRequest{Pieces: pieces, ExtraData: extra}); !errors.Is(err, errInvalidFilecoinSequence) {
				t.Fatalf("mismatched Commit = %v", err)
			}
		})
	}
	if _, err := targets[1].SubmitPull(ctx, storage.PullRequest{Pieces: ordered, ExtraData: extra, From: targets[0].PieceURL}); err != nil {
		t.Fatal(err)
	}
	if _, err := targets[1].SubmitCommit(ctx, storage.CommitRequest{Pieces: inputs, ExtraData: extra}); err != nil {
		t.Fatal(err)
	}
}
