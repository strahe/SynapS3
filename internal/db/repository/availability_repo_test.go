package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/availability"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestAvailabilityRepoReplacesProviderSnapshotsAndSummarizes(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)

	err := repos.Availability.ReplaceProviderSnapshots(ctx, []availability.ProviderSnapshot{
		{
			ProviderID:    onChainID(t, "101"),
			Status:        availability.StatusAvailable,
			ReasonCodes:   []availability.ReasonCode{},
			Active:        true,
			HealthStatus:  "reachable",
			LastCheckedAt: checkedAt,
			Evidence:      map[string]any{"service_url": "https://provider-101.test"},
		},
		{
			ProviderID:    onChainID(t, "202"),
			Status:        availability.StatusDegraded,
			ReasonCodes:   []availability.ReasonCode{availability.ReasonProviderHTTPUnreachable},
			Active:        true,
			HealthStatus:  "unreachable",
			LastCheckedAt: checkedAt,
			Evidence:      map[string]any{"service_url": "https://provider-202.test"},
		},
	})
	if err != nil {
		t.Fatalf("ReplaceProviderSnapshots: %v", err)
	}

	page, err := repos.Availability.ListProviderSnapshots(ctx, availability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderSnapshots: %v", err)
	}
	if page.Total != 2 || page.Summary.Total != 2 || page.Summary.Available != 1 || page.Summary.Degraded != 1 {
		t.Fatalf("provider page summary = total:%d summary:%+v, want two providers split available/degraded", page.Total, page.Summary)
	}

	page, err = repos.Availability.ListProviderSnapshots(ctx, availability.ListOptions{
		Status: availability.StatusDegraded,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListProviderSnapshots filtered: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ProviderID.String() != "202" {
		t.Fatalf("filtered provider page = total:%d items:%+v, want provider 202", page.Total, page.Items)
	}

	if err := repos.Availability.ReplaceProviderSnapshots(ctx, []availability.ProviderSnapshot{
		{
			ProviderID:    onChainID(t, "101"),
			Status:        availability.StatusUnavailable,
			ReasonCodes:   []availability.ReasonCode{availability.ReasonProviderInactive},
			Active:        false,
			HealthStatus:  "reachable",
			LastCheckedAt: checkedAt.Add(time.Minute),
			Evidence:      map[string]any{},
		},
	}); err != nil {
		t.Fatalf("ReplaceProviderSnapshots prune: %v", err)
	}

	page, err = repos.Availability.ListProviderSnapshots(ctx, availability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderSnapshots after prune: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ProviderID.String() != "101" {
		t.Fatalf("provider page after prune = total:%d items:%+v, want only provider 101", page.Total, page.Items)
	}
}

func TestAvailabilityRepoProviderOrderAndPaginatedSummary(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)

	if err := repos.Availability.ReplaceProviderSnapshots(ctx, []availability.ProviderSnapshot{
		{ProviderID: onChainID(t, "10"), Status: availability.StatusDegraded, ReasonCodes: []availability.ReasonCode{}, Active: true, HealthStatus: "reachable", LastCheckedAt: checkedAt},
		{ProviderID: onChainID(t, "2"), Status: availability.StatusAvailable, ReasonCodes: []availability.ReasonCode{}, Active: true, HealthStatus: "reachable", LastCheckedAt: checkedAt},
		{ProviderID: onChainID(t, "101"), Status: availability.StatusUnknown, ReasonCodes: []availability.ReasonCode{}, Active: true, HealthStatus: "unknown", LastCheckedAt: checkedAt},
	}); err != nil {
		t.Fatalf("ReplaceProviderSnapshots: %v", err)
	}

	page, err := repos.Availability.ListProviderSnapshots(ctx, availability.ListOptions{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("ListProviderSnapshots: %v", err)
	}
	if page.Total != 3 || page.Summary.Total != 3 || page.Summary.Available != 1 || page.Summary.Degraded != 1 || page.Summary.Unknown != 1 {
		t.Fatalf("paginated provider summary = total:%d summary:%+v, want aggregate over all rows", page.Total, page.Summary)
	}
	if len(page.Items) != 2 || page.Items[0].ProviderID.String() != "10" || page.Items[1].ProviderID.String() != "101" {
		t.Fatalf("provider page items = %+v, want numeric order page [10,101]", page.Items)
	}
}

func TestAvailabilityRepoReplacesDataSetSnapshotsAndFilters(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucketA := seedBucket(t, db, "alpha")
	bucketB := seedBucket(t, db, "beta")
	localA := seedStorageDataSet(t, db, bucketA.ID, "101", "1001", model.StorageDataSetStatusReady)
	localB := seedStorageDataSet(t, db, bucketB.ID, "202", "2002", model.StorageDataSetStatusReady)
	checkedAt := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)

	activePieces := int64(7)
	err := repos.Availability.ReplaceDataSetSnapshots(ctx, []availability.DataSetSnapshot{
		{
			LocalDataSetID:   localA.ID,
			BucketID:         bucketA.ID,
			BucketName:       bucketA.Name,
			ProviderID:       localA.ProviderID,
			ChainDataSetID:   localA.DataSetID,
			ClientDataSetID:  localA.ClientDataSetID,
			LocalStatus:      localA.Status,
			Status:           availability.StatusAvailable,
			ReasonCodes:      []availability.ReasonCode{},
			ActivePieceCount: &activePieces,
			LastCheckedAt:    checkedAt,
			Evidence:         map[string]any{"active_piece_count": activePieces},
		},
		{
			LocalDataSetID: localB.ID,
			BucketID:       bucketB.ID,
			BucketName:     bucketB.Name,
			ProviderID:     localB.ProviderID,
			ChainDataSetID: localB.DataSetID,
			LocalStatus:    localB.Status,
			Status:         availability.StatusUnavailable,
			ReasonCodes:    []availability.ReasonCode{availability.ReasonChainDataSetMissing},
			LastCheckedAt:  checkedAt,
			Evidence:       map[string]any{},
		},
	})
	if err != nil {
		t.Fatalf("ReplaceDataSetSnapshots: %v", err)
	}

	page, err := repos.Availability.ListDataSetSnapshots(ctx, availability.ListOptions{
		BucketID: bucketA.ID,
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("ListDataSetSnapshots filtered by bucket: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].LocalDataSetID != localA.ID {
		t.Fatalf("bucket-filtered data set page = total:%d items:%+v, want local data set A", page.Total, page.Items)
	}
	if page.Items[0].ActivePieceCount == nil || *page.Items[0].ActivePieceCount != 7 {
		t.Fatalf("active_piece_count = %#v, want 7", page.Items[0].ActivePieceCount)
	}

	page, err = repos.Availability.ListDataSetSnapshots(ctx, availability.ListOptions{Limit: 1, Offset: 1})
	if err != nil {
		t.Fatalf("ListDataSetSnapshots paginated: %v", err)
	}
	if page.Total != 2 || page.Summary.Total != 2 || page.Summary.Available != 1 || page.Summary.Unavailable != 1 || len(page.Items) != 1 {
		t.Fatalf("paginated data set page = total:%d summary:%+v items:%+v, want aggregate over filtered rows", page.Total, page.Summary, page.Items)
	}

	byLocalID, err := repos.Availability.GetDataSetSnapshotsByLocalIDs(ctx, []int64{localA.ID, localB.ID, 999})
	if err != nil {
		t.Fatalf("GetDataSetSnapshotsByLocalIDs: %v", err)
	}
	snapshotB, ok := byLocalID[localB.ID]
	if len(byLocalID) != 2 || !ok || snapshotB.ActivePieceCount != nil {
		t.Fatalf("snapshots by local id = %+v, want two rows and nil active_piece_count for local B", byLocalID)
	}

	if err := repos.Availability.ReplaceDataSetSnapshots(ctx, []availability.DataSetSnapshot{
		{
			LocalDataSetID: localA.ID,
			BucketID:       bucketA.ID,
			BucketName:     bucketA.Name,
			ProviderID:     localA.ProviderID,
			ChainDataSetID: localA.DataSetID,
			LocalStatus:    localA.Status,
			Status:         availability.StatusDegraded,
			ReasonCodes:    []availability.ReasonCode{availability.ReasonChainDataSetUnmanaged},
			LastCheckedAt:  checkedAt.Add(time.Minute),
			Evidence:       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("ReplaceDataSetSnapshots prune: %v", err)
	}

	page, err = repos.Availability.ListDataSetSnapshots(ctx, availability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDataSetSnapshots after prune: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].LocalDataSetID != localA.ID {
		t.Fatalf("data set page after prune = total:%d items:%+v, want only local data set A", page.Total, page.Items)
	}
}

func seedStorageDataSet(t *testing.T, db *bun.DB, bucketID int64, providerID string, dataSetID string, status model.StorageDataSetStatus) *model.StorageDataSet {
	t.Helper()
	row := &model.StorageDataSet{
		BucketID:   bucketID,
		ProviderID: onChainID(t, providerID),
		CopyIndex:  int(bucketID),
		DataSetID:  onChainIDPtr(t, dataSetID),
		Status:     status,
	}
	if _, err := db.NewInsert().Model(row).Exec(context.Background()); err != nil {
		t.Fatalf("seeding storage data set: %v", err)
	}
	return row
}
