package repository_test

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
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

func seedCommittedUploadCopies(t *testing.T, db *bun.DB, repos *repository.Repositories, bucketID int64, contentID int64, pieceCID string, copies []storageUploadCopySeed) {
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
		createdByContentID := int64(0)
		if copySeed.IsNewDataSet {
			createdByContentID = contentID
		}
		binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID:           bucketID,
			ProviderID:         providerID,
			CopyIndex:          copyIndex,
			CreatedByContentID: createdByContentID,
		})
		if err != nil {
			t.Fatalf("EnsureDataSetBinding: %v", err)
		}
		if copySeed.DataSetID != nil {
			if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
				ID:        binding.ID,
				ContentID: contentID,
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
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, contentID, copyInputs); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	for _, copySeed := range copies {
		retrievalURL := ""
		if copySeed.RetrievalURL != nil {
			retrievalURL = *copySeed.RetrievalURL
		}
		testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
			ContentID:    contentID,
			CopyIndex:    copySeed.CopyIndex,
			PieceCID:     pieceCID,
			PieceID:      copySeed.PieceID,
			RetrievalURL: retrievalURL,
		})
	}
}
