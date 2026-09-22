package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/uptrace/bun"
)

// CommitStorageCopy drives one copy to committed the way production does: a
// commit attempt is recorded in the ledger first, and the copy then projects
// that confirmed row. A committed copy without confirmed evidence is refused by
// the schema, so tests cannot shortcut this.
func CommitStorageCopy(
	t *testing.T,
	db bun.IDB,
	repos *repository.Repositories,
	input repository.MarkUploadCopyCommittedInput,
) {
	t.Helper()
	ctx := context.Background()
	if input.StorageCopyID == 0 {
		copyRow := new(model.StorageCopy)
		if err := db.NewSelect().
			Model(copyRow).
			Where("content_id = ? AND copy_index = ?", input.ContentID, input.CopyIndex).
			Scan(ctx); err != nil {
			t.Fatalf("loading copy for content %d slot %d: %v", input.ContentID, input.CopyIndex, err)
		}
		input.StorageCopyID = copyRow.ID
	}
	copyRow := new(model.StorageCopy)
	if err := db.NewSelect().Model(copyRow).Where("id = ?", input.StorageCopyID).Scan(ctx); err != nil {
		t.Fatalf("loading copy %d: %v", input.StorageCopyID, err)
	}
	if input.CommitExtraDataHex == "" {
		input.CommitExtraDataHex = "abcd"
	}
	if input.CommitTransactionID == "" {
		input.CommitTransactionID = fmt.Sprintf("tx-%d", input.StorageCopyID)
	}
	if input.CommitConfirmedTransactionID == "" {
		input.CommitConfirmedTransactionID = input.CommitTransactionID
	}
	if input.CommitAttemptID == "" {
		input.CommitAttemptID = fmt.Sprintf("attempt-%d-%d", input.ContentID, input.StorageCopyID)
	}
	now := time.Now()
	statusURL := "https://provider.example/status/" + input.CommitAttemptID
	attempt := &storagecommit.Attempt{
		AttemptID: input.CommitAttemptID, ContentID: copyRow.ContentID,
		StorageDataSetID: copyRow.StorageDataSetID, Status: storagecommit.AttemptStatusAttempted,
		ExtraDataHex: &input.CommitExtraDataHex, TransactionID: &input.CommitTransactionID,
		StatusURL:   &statusURL,
		AttemptedAt: &now, CreatedAt: now, UpdatedAt: now,
	}
	if _, err := db.NewInsert().Model(attempt).Exec(ctx); err != nil {
		t.Fatalf("seeding commit attempt for copy %d: %v", input.StorageCopyID, err)
	}
	// Confirmation lands on a copy the coordinator already moved to committing.
	if _, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("status = ?", model.StorageCopyStatusCommitting).
		Set("commit_extra_data_hex = ?", input.CommitExtraDataHex).
		Set("updated_at = ?", now).
		Where("id = ?", input.StorageCopyID).
		Exec(ctx); err != nil {
		t.Fatalf("moving copy %d to committing: %v", input.StorageCopyID, err)
	}
	if err := repos.Contents.MarkUploadCopyCommitted(ctx, input); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
}
