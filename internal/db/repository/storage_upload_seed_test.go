package repository_test

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
)

type storageUploadCopySeed struct {
	CopyIndex      int
	ProviderID     *types.OnChainID
	DataSetID      *types.OnChainID
	PieceID        *types.OnChainID
	TransferMethod model.StorageCopyTransferMethod
	RetrievalURL   *string
	IsNewDataSet   bool
}

func seedCommittedUploadCopies(t *testing.T, repos *repository.Repositories, bucketID int64, uploadID int64, pieceCID string, copies []storageUploadCopySeed) {
	t.Helper()
	ctx := context.Background()
	copyInputs := make([]repository.UploadCopyBindingInput, 0, len(copies))
	for i, copySeed := range copies {
		copyIndex := copySeed.CopyIndex
		if i > 0 && copyIndex == 0 {
			copyIndex = i
		}
		providerID := types.OnChainID{}
		if copySeed.ProviderID != nil {
			providerID = *copySeed.ProviderID
		}
		createdByUploadID := int64(0)
		if copySeed.IsNewDataSet {
			createdByUploadID = uploadID
		}
		binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID:          bucketID,
			ProviderID:        providerID,
			CopyIndex:         copyIndex,
			CreatedByUploadID: createdByUploadID,
		})
		if err != nil {
			t.Fatalf("EnsureDataSetBinding: %v", err)
		}
		if copySeed.DataSetID != nil {
			if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
				ID:        binding.ID,
				UploadID:  uploadID,
				DataSetID: *copySeed.DataSetID,
			}); err != nil {
				t.Fatalf("MarkDataSetReady: %v", err)
			}
		}
		transferMethod := copySeed.TransferMethod
		if transferMethod == "" {
			transferMethod = model.StorageCopyTransferMethodPeerPull
			if i == 0 {
				transferMethod = model.StorageCopyTransferMethodIngress
			}
		}
		copyInputs = append(copyInputs, repository.UploadCopyBindingInput{
			StorageDataSetID: binding.ID,
			CopyIndex:        copyIndex,
			TransferMethod:   transferMethod,
			ProviderID:       providerID,
		})
		copies[i].CopyIndex = copyIndex
		copies[i].TransferMethod = transferMethod
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, uploadID, copyInputs); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	for _, copySeed := range copies {
		retrievalURL := ""
		if copySeed.RetrievalURL != nil {
			retrievalURL = *copySeed.RetrievalURL
		}
		if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
			UploadID:     uploadID,
			CopyIndex:    copySeed.CopyIndex,
			PieceCID:     pieceCID,
			PieceID:      copySeed.PieceID,
			RetrievalURL: retrievalURL,
		}); err != nil {
			t.Fatalf("MarkUploadCopyCommitted: %v", err)
		}
	}
}

func bindReadableUploadForContent(t *testing.T, repos *repository.Repositories, uploadID int64, bucketID int64, size int64, checksum string) []repository.ObjectVersionRef {
	t.Helper()
	refs, err := repos.Uploads.BindReadableUploadForContent(context.Background(), repository.BindReadableUploadInput{
		UploadID:    uploadID,
		BucketID:    bucketID,
		ContentSize: size,
		Checksum:    checksum,
	})
	if err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}
	return refs
}

func finalizeUploadForTest(t *testing.T, repos *repository.Repositories, uploadID int64) []repository.ObjectVersionRef {
	t.Helper()
	finalized, refs, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(context.Background(), repository.FinalizeUploadInput{UploadID: uploadID})
	if err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet: %v", err)
	}
	if !finalized {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet finalized = false, want true")
	}
	return refs
}
