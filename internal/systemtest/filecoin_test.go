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
	"github.com/strahe/synaps3/internal/synapse"
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
	stored, err := contexts[0].Store(ctx, bytes.NewReader(content), nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
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
	for _, uploadContext := range contexts[1:] {
		wg.Go(func() {
			_, pullErr := uploadContext.Pull(ctx, storage.PullRequest{
				Pieces: []cid.Cid{stored.PieceCID},
				From:   contexts[0].PieceURL,
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
