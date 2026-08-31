package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	synaps3db "github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite"
)

func TestSettleClaimedTerminalReplacementItemDistinguishesReadableCommittedCopy(t *testing.T) {
	for _, tc := range []struct {
		name            string
		targetStatus    model.StorageDataSetStatus
		wantStatus      storagereplacement.ItemStatus
		wantItemsCopied int
	}{
		{
			name:            "readable",
			targetStatus:    model.StorageDataSetStatusReady,
			wantStatus:      storagereplacement.ItemStatusCopied,
			wantItemsCopied: 1,
		},
		{
			name:            "unavailable",
			targetStatus:    model.StorageDataSetStatusUnavailable,
			wantStatus:      storagereplacement.ItemStatusFailed,
			wantItemsCopied: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := terminalReplacementCommitTestDB(t)
			ctx := t.Context()
			now := time.Now()
			bucket := &model.Bucket{Name: "terminal-commit-" + tc.name, Status: model.BucketStatusActive}
			if _, err := db.NewInsert().Model(bucket).Exec(ctx); err != nil {
				t.Fatalf("insert bucket: %v", err)
			}
			sourceDataSetID := types.NewOnChainID(1001)
			targetDataSetID := types.NewOnChainID(2002)
			source := &model.StorageDataSet{
				BucketID: bucket.ID, ProviderID: types.NewOnChainID(101), CopyIndex: 0,
				Generation: 1, IsCurrent: false, DataSetID: &sourceDataSetID,
				Status: model.StorageDataSetStatusDraining,
			}
			target := &model.StorageDataSet{
				BucketID: bucket.ID, ProviderID: types.NewOnChainID(202), CopyIndex: 0,
				Generation: 2, IsCurrent: true, DataSetID: &targetDataSetID,
				Status: tc.targetStatus,
			}
			if _, err := db.NewInsert().Model(source).Exec(ctx); err != nil {
				t.Fatalf("insert source data set: %v", err)
			}
			if _, err := db.NewInsert().Model(target).Exec(ctx); err != nil {
				t.Fatalf("insert target data set: %v", err)
			}
			pieceCID := "bafkqaaa"
			upload := &model.StorageUpload{
				BucketID: bucket.ID, ContentSize: 1, Checksum: tc.name,
				Status: model.StorageUploadStatusComplete, PieceCID: &pieceCID, RequestedCopies: 1,
			}
			if _, err := db.NewInsert().Model(upload).Exec(ctx); err != nil {
				t.Fatalf("insert upload: %v", err)
			}
			providerID := target.ProviderID
			pieceID := types.NewOnChainID(3003)
			retrievalURL := "https://provider.example/piece"
			copyRow := &model.StorageUploadCopy{
				UploadID: upload.ID, CopyIndex: target.CopyIndex, ProviderID: &providerID,
				PieceID: &pieceID, TransferMethod: model.StorageCopyTransferMethodPeerPull,
				Status: model.StorageUploadCopyStatusCommitted, RetrievalURL: &retrievalURL,
				StorageDataSetID: &target.ID,
			}
			if _, err := db.NewInsert().Model(copyRow).Exec(ctx); err != nil {
				t.Fatalf("insert target copy: %v", err)
			}
			replacement := &storagereplacement.Replacement{
				BucketID: bucket.ID, CopyIndex: source.CopyIndex,
				SourceDataSetID: source.ID, TargetDataSetID: target.ID,
				SelectionMode:   storagereplacement.SelectionModeManual,
				ClientRequestID: "terminal-commit-" + tc.name,
				Status:          storagereplacement.StatusFailed, ItemsTotal: 1,
				ConfirmedAt: now, CreatedAt: now, UpdatedAt: now,
			}
			if _, err := db.NewInsert().Model(replacement).Exec(ctx); err != nil {
				t.Fatalf("insert replacement: %v", err)
			}
			claimedAt := now.Add(-time.Second)
			leaseUntil := now.Add(time.Minute)
			maxRetries := 5
			item := &storagereplacement.Item{
				ReplacementID: replacement.ID, UploadID: upload.ID, TargetCopyID: &copyRow.ID,
				Status: storagereplacement.ItemStatusRunning, ScheduledAt: claimedAt,
				MaxRetries: &maxRetries, ClaimedAt: &claimedAt, LeaseUntil: &leaseUntil,
				CreatedAt: claimedAt, UpdatedAt: claimedAt,
			}
			if _, err := db.NewInsert().Model(item).Exec(ctx); err != nil {
				t.Fatalf("insert replacement item: %v", err)
			}

			var settled bool
			err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
				var err error
				settled, err = settleClaimedTerminalReplacementItem(
					ctx,
					tx,
					item,
					storagereplacement.StatusFailed,
					now,
				)
				return err
			})
			if err != nil || !settled {
				t.Fatalf("settle terminal item = %v err=%v, want settled", settled, err)
			}
			persistedItem := new(storagereplacement.Item)
			if err := db.NewSelect().Model(persistedItem).Where("id = ?", item.ID).Scan(ctx); err != nil {
				t.Fatalf("load replacement item: %v", err)
			}
			if persistedItem.Status != tc.wantStatus || persistedItem.ClaimedAt != nil || persistedItem.LeaseUntil != nil {
				t.Fatalf("replacement item = %#v, want %s without a claim", persistedItem, tc.wantStatus)
			}
			persistedReplacement := new(storagereplacement.Replacement)
			if err := db.NewSelect().Model(persistedReplacement).Where("id = ?", replacement.ID).Scan(ctx); err != nil {
				t.Fatalf("load replacement: %v", err)
			}
			if persistedReplacement.ItemsCopied != tc.wantItemsCopied {
				t.Fatalf("items copied = %d, want %d", persistedReplacement.ItemsCopied, tc.wantItemsCopied)
			}
			persistedCopy := new(model.StorageUploadCopy)
			if err := db.NewSelect().Model(persistedCopy).Where("id = ?", copyRow.ID).Scan(ctx); err != nil {
				t.Fatalf("load target copy: %v", err)
			}
			if persistedCopy.Status != model.StorageUploadCopyStatusCommitted {
				t.Fatalf("target copy status = %s, want committed", persistedCopy.Status)
			}
		})
	}
}

func terminalReplacementCommitTestDB(t *testing.T) *bun.DB {
	t.Helper()
	sqldb, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })
	if err := synaps3db.RunMigrations(t.Context(), db); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return db
}
