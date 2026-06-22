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
	"github.com/strahe/synapse-go/storage"
)

func TestMemoryFilecoinLifecycleAndProviderIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	filecoin := NewMemoryFilecoin()
	contexts, err := filecoin.CreateContexts(ctx, &storage.CreateContextsOptions{Copies: 3})
	if err != nil {
		t.Fatalf("CreateContexts: %v", err)
	}
	for _, uploadContext := range contexts {
		if _, err := uploadContext.CreateDataSet(ctx, nil); err != nil {
			t.Fatalf("CreateDataSet provider %s: %v", uploadContext.ProviderID().String(), err)
		}
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
	if _, err := contexts[1].Commit(ctx, storage.CommitRequest{Pieces: []storage.PieceInput{piece}, ExtraData: extra}); !errors.Is(err, errInvalidFilecoinSequence) {
		t.Fatalf("secondary Commit before Pull error = %v, want invalid sequence", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for _, uploadContext := range contexts[1:] {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, pullErr := uploadContext.Pull(ctx, storage.PullRequest{
				Pieces: []cid.Cid{stored.PieceCID},
				From:   contexts[0].PieceURL,
			})
			errCh <- pullErr
		}()
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
		if _, err := uploadContext.Commit(ctx, storage.CommitRequest{Pieces: []storage.PieceInput{piece}, ExtraData: extra}); err != nil {
			t.Fatalf("Commit provider %s: %v", uploadContext.ProviderID().String(), err)
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

func TestMemoryFilecoinRejectsInvalidSequenceAndCancellation(t *testing.T) {
	t.Parallel()
	filecoin := NewMemoryFilecoin()
	ctx := context.Background()
	contexts, err := filecoin.CreateContexts(ctx, &storage.CreateContextsOptions{Copies: 1})
	if err != nil {
		t.Fatalf("CreateContexts: %v", err)
	}
	if _, err := contexts[0].Store(ctx, bytes.NewReader([]byte("out-of-order")), nil); !errors.Is(err, errInvalidFilecoinSequence) {
		t.Fatalf("Store before CreateDataSet error = %v, want invalid sequence", err)
	}
	unknown := cid.MustParse("bafkreibm6jg3ux5qumh4jxcq3xjgbpfs2jsl2w7jtjsq3btqfnhtzgdrmq")
	if _, err := filecoin.Download(ctx, unknown, nil); err == nil {
		t.Fatal("Download unknown CID succeeded")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := filecoin.CreateContexts(cancelled, &storage.CreateContextsOptions{Copies: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateContexts cancelled error = %v, want context.Canceled", err)
	}
}
