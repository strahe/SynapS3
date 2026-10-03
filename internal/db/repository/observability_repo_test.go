package repository_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestObservabilityRepoReplacesProviderStatesAndSummarizes(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)

	err := repos.Observability.ReplaceProviderStates(ctx, checkedAt, []observability.ProviderState{
		{
			ProviderID:    onChainID(t, "101"),
			Status:        observability.StatusAvailable,
			ReasonCodes:   []observability.ReasonCode{},
			Active:        boolPtr(true),
			HasPDP:        boolPtr(true),
			ServiceURL:    stringPtr("https://provider-101.test"),
			HealthStatus:  stringPtr("reachable"),
			LastCheckedAt: checkedAt,
			Evidence:      map[string]any{"service_url": "https://provider-101.test"},
		},
		{
			ProviderID:    onChainID(t, "202"),
			Status:        observability.StatusDegraded,
			ReasonCodes:   []observability.ReasonCode{observability.ReasonProviderHTTPUnreachable},
			Active:        boolPtr(true),
			HasPDP:        boolPtr(true),
			ServiceURL:    stringPtr("https://provider-202.test"),
			HealthStatus:  stringPtr("unreachable"),
			LastCheckedAt: checkedAt,
			Evidence:      map[string]any{"service_url": "https://provider-202.test"},
		},
	})
	if err != nil {
		t.Fatalf("ReplaceProviderStates: %v", err)
	}

	page, err := repos.Observability.ListProviderStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderStates: %v", err)
	}
	if page.Total != 2 || page.Summary.Total != 2 || page.Summary.Available != 1 || page.Summary.Degraded != 1 {
		t.Fatalf("provider page summary = total:%d summary:%+v, want two providers split available/degraded", page.Total, page.Summary)
	}
	if page.LastCheckedAt == nil || !page.LastCheckedAt.Equal(checkedAt) {
		t.Fatalf("provider collection last checked = %v, want %s", page.LastCheckedAt, checkedAt)
	}

	page, err = repos.Observability.ListProviderStates(ctx, observability.ListOptions{
		Status: observability.StatusDegraded,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListProviderStates filtered: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ProviderID.String() != "202" {
		t.Fatalf("filtered provider page = total:%d items:%+v, want provider 202", page.Total, page.Items)
	}

	if err := repos.Observability.ReplaceProviderStates(ctx, checkedAt.Add(time.Minute), []observability.ProviderState{
		{
			ProviderID:    onChainID(t, "101"),
			Status:        observability.StatusUnavailable,
			ReasonCodes:   []observability.ReasonCode{observability.ReasonProviderInactive},
			Active:        boolPtr(false),
			HasPDP:        boolPtr(true),
			HealthStatus:  stringPtr("reachable"),
			LastCheckedAt: checkedAt.Add(time.Minute),
			Evidence:      map[string]any{},
		},
	}); err != nil {
		t.Fatalf("ReplaceProviderStates prune: %v", err)
	}

	page, err = repos.Observability.ListProviderStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderStates after prune: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ProviderID.String() != "101" {
		t.Fatalf("provider page after prune = total:%d items:%+v, want only provider 101", page.Total, page.Items)
	}

	// Status values are not checked by the schema, so an unknown one is refused
	// before a snapshot claims its collection time.
	unknown := observability.ProviderState{
		ProviderID: onChainID(t, "101"), Status: observability.Status("terminated"),
		ReasonCodes: []observability.ReasonCode{}, LastCheckedAt: checkedAt.Add(2 * time.Minute), Evidence: map[string]any{},
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, checkedAt.Add(2*time.Minute), []observability.ProviderState{unknown}); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("ReplaceProviderStates with unknown status error = %v, want ErrInvalidInput", err)
	}
	if err := repos.Observability.UpsertProviderObservation(ctx, checkedAt.Add(2*time.Minute), unknown); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("UpsertProviderObservation with unknown status error = %v, want ErrInvalidInput", err)
	}
	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt.Add(2*time.Minute), []observability.DataSetState{{
		LocalDataSetID: 1, Status: observability.Status("terminated"),
	}}); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("ReplaceDataSetStates with unknown status error = %v, want ErrInvalidInput", err)
	}
	page, err = repos.Observability.ListProviderStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderStates after refused writes: %v", err)
	}
	if page.Total != 1 || page.Items[0].Status != observability.StatusUnavailable ||
		page.LastCheckedAt == nil || !page.LastCheckedAt.Equal(checkedAt.Add(time.Minute)) {
		t.Fatalf("provider page after refused writes = total:%d items:%+v checked:%v, want the previous snapshot", page.Total, page.Items, page.LastCheckedAt)
	}
}

func TestOverviewStorageStatesUsesLocalDependenciesWithoutFilteringGlobalObservations(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "overview-local")
	current := seedStorageDataSet(t, db, bucket.ID, "101", "1001", model.StorageDataSetStatusReady)
	retiredBucket := seedBucket(t, db, "overview-retired")
	retired := seedStorageDataSet(t, db, retiredBucket.ID, "202", "2002", model.StorageDataSetStatusReady)
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("is_current = ?", false).Set("status = ?", model.StorageDataSetStatusRetired).
		Where("id = ?", retired.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkedAt := time.Now().UTC()
	if err := repos.Observability.ReplaceProviderStates(t.Context(), checkedAt, []observability.ProviderState{
		{ProviderID: current.ProviderID, Status: observability.StatusAvailable},
		{ProviderID: retired.ProviderID, Status: observability.StatusUnavailable},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.ReplaceDataSetStates(t.Context(), checkedAt, []observability.DataSetState{
		{
			LocalDataSetID: current.ID, BucketID: bucket.ID, CopyIndex: current.CopyIndex,
			ProviderID: current.ProviderID, Status: observability.StatusAvailable,
		},
		{
			LocalDataSetID: retired.ID, BucketID: retiredBucket.ID, CopyIndex: retired.CopyIndex,
			ProviderID: retired.ProviderID, Status: observability.StatusUnavailable,
		},
	}); err != nil {
		t.Fatal(err)
	}
	dataSets, providers, states, providerCheckedAt, dataSetCheckedAt, err := repos.Observability.OverviewStorageStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(dataSets) != 1 || dataSets[0].ID != current.ID || len(providers) != 1 || providers[0].ProviderID.String() != current.ProviderID.String() || len(states) != 1 || states[0].LocalDataSetID != current.ID {
		t.Fatalf("scoped states = data sets:%#v providers:%#v observations:%#v", dataSets, providers, states)
	}
	if providerCheckedAt == nil || dataSetCheckedAt == nil {
		t.Fatal("scoped overview lost collection freshness")
	}
	global, err := repos.Observability.ListProviderStates(t.Context(), observability.ListOptions{})
	if err != nil || global.Summary.Total != 2 {
		t.Fatalf("global provider observations = %#v, err=%v", global, err)
	}
}

func TestOverviewStorageStatesIncludesBothSidesOfUnfinishedReplacement(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "overview-replacement")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "301"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	markSourceReady(t, repos, source.ID)
	replacement, created, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "302"), ClientRequestID: "overview-replacement",
	})
	if err != nil || !created {
		t.Fatalf("replacement = %#v, created=%v, err=%v", replacement, created, err)
	}
	sets, _, _, _, _, err := repos.Observability.OverviewStorageStates(t.Context())
	if err != nil || len(sets) != 2 {
		t.Fatalf("unfinished replacement data sets = %#v, err=%v", sets, err)
	}
	seen := map[int64]bool{}
	for _, row := range sets {
		seen[row.ID] = true
	}
	if !seen[source.ID] || !seen[replacement.TargetDataSetID] {
		t.Fatalf("replacement source and target missing: %#v", sets)
	}
	if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", storagereplacement.StatusCompleted).Where("id = ?", replacement.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	sets, _, _, _, _, err = repos.Observability.OverviewStorageStates(t.Context())
	if err != nil || len(sets) != 1 || sets[0].ID != source.ID {
		t.Fatalf("completed replacement without readable old copy = %#v, err=%v", sets, err)
	}
}

func TestOverviewStorageStatesKeepsReadableOlderGenerationOnlyWhileReferenced(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "overview-old-readable")
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 128,
		Checksum: testutil.StorageChecksum("overview-old-readable"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID,
		Key: "old.bin", ContentID: &content.ID, Size: 128, ETag: "old", ContentType: "application/octet-stream",
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	old, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "401"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	dataSetID := onChainID(t, "1401")
	clientID := onChainID(t, "2401")
	if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: old.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: old.ID, CopyIndex: 0, ProviderID: old.ProviderID,
		TransferMethod: model.StorageCopyTransferMethodIngress,
	}}); err != nil {
		t.Fatal(err)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 1 {
		t.Fatalf("copies = %#v, err=%v", copies, err)
	}
	pieceID := onChainID(t, "51")
	testutil.CommitStorageCopy(t, db, repos, testutil.CommitCopyInput{
		StorageCopyID: copies[0].ID, ContentID: content.ID, CopyIndex: 0,
		PieceCID: "bafk2bzacecoverviewold", PieceID: &pieceID, RetrievalURL: "https://old.example/piece",
	})
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("is_current = ?", false).
		Where("id = ?", old.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	sets, _, _, _, _, err := repos.Observability.OverviewStorageStates(t.Context())
	if err != nil || len(sets) != 1 || sets[0].ID != old.ID {
		t.Fatalf("readable old data set = %#v, err=%v", sets, err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Set("status = ?", model.StorageDataSetStatusRetired).
		Where("id = ?", old.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	sets, _, _, _, _, err = repos.Observability.OverviewStorageStates(t.Context())
	if err != nil || len(sets) != 0 {
		t.Fatalf("retired old data set = %#v, err=%v", sets, err)
	}
}

func TestObservabilityRepoProviderOrderAndPaginatedSummary(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)

	if err := repos.Observability.ReplaceProviderStates(ctx, checkedAt, []observability.ProviderState{
		{ProviderID: onChainID(t, "10"), Status: observability.StatusDegraded, ReasonCodes: []observability.ReasonCode{}, Active: boolPtr(true), HealthStatus: stringPtr("reachable"), LastCheckedAt: checkedAt},
		{ProviderID: onChainID(t, "2"), Status: observability.StatusAvailable, ReasonCodes: []observability.ReasonCode{}, Active: boolPtr(true), HealthStatus: stringPtr("reachable"), LastCheckedAt: checkedAt},
		{ProviderID: onChainID(t, "101"), Status: observability.StatusUnknown, ReasonCodes: []observability.ReasonCode{}, Active: boolPtr(true), HealthStatus: stringPtr("unknown"), LastCheckedAt: checkedAt},
	}); err != nil {
		t.Fatalf("ReplaceProviderStates: %v", err)
	}

	page, err := repos.Observability.ListProviderStates(ctx, observability.ListOptions{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("ListProviderStates: %v", err)
	}
	if page.Total != 3 || page.Summary.Total != 3 || page.Summary.Available != 1 || page.Summary.Degraded != 1 || page.Summary.Unknown != 1 {
		t.Fatalf("paginated provider summary = total:%d summary:%+v, want aggregate over all rows", page.Total, page.Summary)
	}
	if len(page.Items) != 2 || page.Items[0].ProviderID.String() != "10" || page.Items[1].ProviderID.String() != "101" {
		t.Fatalf("provider page items = %+v, want numeric order page [10,101]", page.Items)
	}
}

// A status written by a newer version is not one this binary can place in a
// bucket, so the summary counts it as unknown instead of leaving it out.
func TestObservabilityRepoSummaryCountsUnrecognizedStatusAsUnknown(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)
	unrecognized := observability.Status("terminated")

	bucket := seedBucket(t, db, "unrecognized-status")
	local := seedStorageDataSet(t, db, bucket.ID, "101", "1001", model.StorageDataSetStatusReady)
	if err := repos.Observability.ReplaceProviderStates(ctx, checkedAt, []observability.ProviderState{
		{ProviderID: onChainID(t, "101"), Status: observability.StatusAvailable, ReasonCodes: []observability.ReasonCode{}, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
		{ProviderID: onChainID(t, "202"), Status: observability.StatusAvailable, ReasonCodes: []observability.ReasonCode{}, LastCheckedAt: checkedAt, Evidence: map[string]any{}},
	}); err != nil {
		t.Fatalf("ReplaceProviderStates: %v", err)
	}
	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, dataSetSnapshot(local, observability.StatusAvailable)); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}
	if _, err := db.NewUpdate().Model((*observability.ProviderState)(nil)).
		Set("status = ?", unrecognized).Where("provider_id = ?", "202").Exec(ctx); err != nil {
		t.Fatalf("store unrecognized provider status: %v", err)
	}
	if _, err := db.NewUpdate().Model((*observability.DataSetState)(nil)).
		Set("status = ?", unrecognized).Where("local_data_set_id = ?", local.ID).Exec(ctx); err != nil {
		t.Fatalf("store unrecognized data set status: %v", err)
	}

	providers, err := repos.Observability.ListProviderStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderStates: %v", err)
	}
	if got := providers.Summary; got.Total != 2 || got.Available != 1 || got.Unknown != 1 {
		t.Fatalf("provider summary = %+v, want the unrecognized status counted as unknown", got)
	}
	dataSets, err := repos.Observability.ListDataSetStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDataSetStates: %v", err)
	}
	if got := dataSets.Summary; got.Total != 1 || got.Available != 0 || got.Unknown != 1 {
		t.Fatalf("data set summary = %+v, want the unrecognized status counted as unknown", got)
	}
}

func TestObservabilityRepoRecordsCollectionStateForEmptyRefresh(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	checkedAt := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)

	if err := repos.Observability.ReplaceProviderStates(ctx, checkedAt, nil); err != nil {
		t.Fatalf("ReplaceProviderStates empty: %v", err)
	}
	providers, err := repos.Observability.ListProviderStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderStates: %v", err)
	}
	if providers.Total != 0 || providers.LastCheckedAt == nil || !providers.LastCheckedAt.Equal(checkedAt) {
		t.Fatalf("provider empty page = total:%d last:%v, want empty page with collection timestamp %s", providers.Total, providers.LastCheckedAt, checkedAt)
	}

	dataSetCheckedAt := checkedAt.Add(time.Minute)
	if err := repos.Observability.ReplaceDataSetStates(ctx, dataSetCheckedAt, nil); err != nil {
		t.Fatalf("ReplaceDataSetStates empty: %v", err)
	}
	dataSets, err := repos.Observability.ListDataSetStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDataSetStates: %v", err)
	}
	if dataSets.Total != 0 || dataSets.LastCheckedAt == nil || !dataSets.LastCheckedAt.Equal(dataSetCheckedAt) {
		t.Fatalf("data set empty page = total:%d last:%v, want empty page with collection timestamp %s", dataSets.Total, dataSets.LastCheckedAt, dataSetCheckedAt)
	}
}

func TestObservabilityRepoKeepsNewerCompleteSnapshots(t *testing.T) {
	assertNewerCompleteSnapshotsWin(t, testDB(t))
}

// assertNewerCompleteSnapshotsWin checks that an older complete read, whether
// empty or not, and a replay at the stored microsecond leave the newer
// snapshot and its collection time in place.
func assertNewerCompleteSnapshotsWin(t *testing.T, db *bun.DB) {
	t.Helper()
	ctx := t.Context()
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "snapshot-order")
	local := seedStorageDataSet(t, db, bucket.ID, "101", "1001", model.StorageDataSetStatusReady)
	provider := onChainID(t, "101")
	newer := time.Date(2026, 9, 26, 10, 0, 0, 123456789, time.UTC)
	stored := newer.Truncate(time.Microsecond)
	dataSet := func(status observability.Status) []observability.DataSetState {
		return dataSetSnapshot(local, status)
	}
	providers := func(status observability.Status) []observability.ProviderState {
		return []observability.ProviderState{{ProviderID: provider, Status: status}}
	}
	if err := repos.Observability.ReplaceDataSetStates(ctx, newer, dataSet(observability.StatusAvailable)); err != nil {
		t.Fatalf("ReplaceDataSetStates newer: %v", err)
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, newer, providers(observability.StatusAvailable)); err != nil {
		t.Fatalf("ReplaceProviderStates newer: %v", err)
	}
	for _, late := range []time.Time{newer.Add(-time.Minute), newer.Add(-300 * time.Nanosecond)} {
		for _, states := range [][]observability.DataSetState{nil, dataSet(observability.StatusUnavailable)} {
			if err := repos.Observability.ReplaceDataSetStates(ctx, late, states); err != nil {
				t.Fatalf("ReplaceDataSetStates at %s: %v", late, err)
			}
		}
		for _, states := range [][]observability.ProviderState{nil, providers(observability.StatusUnavailable)} {
			if err := repos.Observability.ReplaceProviderStates(ctx, late, states); err != nil {
				t.Fatalf("ReplaceProviderStates at %s: %v", late, err)
			}
		}
	}

	dataSets, err := repos.Observability.ListDataSetStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDataSetStates: %v", err)
	}
	if len(dataSets.Items) != 1 || dataSets.Items[0].Status != observability.StatusAvailable ||
		dataSets.LastCheckedAt == nil || !dataSets.LastCheckedAt.Equal(stored) {
		t.Fatalf("data sets = %#v, want the newer snapshot at %s", dataSets, stored)
	}
	providerPage, err := repos.Observability.ListProviderStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderStates: %v", err)
	}
	if len(providerPage.Items) != 1 || providerPage.Items[0].Status != observability.StatusAvailable ||
		providerPage.LastCheckedAt == nil || !providerPage.LastCheckedAt.Equal(stored) {
		t.Fatalf("providers = %#v, want the newer snapshot at %s", providerPage, stored)
	}
}

func TestObservabilityRepoReplacesObservationsStoredAheadOfTheClock(t *testing.T) {
	assertObservationsAheadOfTheClockAreReplaced(t, testDB(t))
}

// assertObservationsAheadOfTheClockAreReplaced checks that observations written
// while the clock ran ahead do not block those taken after it moved back.
func assertObservationsAheadOfTheClockAreReplaced(t *testing.T, db *bun.DB) {
	t.Helper()
	ctx := t.Context()
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "clock-moved-back")
	local := seedStorageDataSet(t, db, bucket.ID, "101", "1001", model.StorageDataSetStatusReady)
	kept, dropped := onChainID(t, "101"), onChainID(t, "102")
	ahead := time.Now().UTC().Add(time.Hour)
	now := time.Now().UTC().Truncate(time.Microsecond)

	if err := repos.Observability.ReplaceDataSetStates(ctx, ahead, dataSetSnapshot(local, observability.StatusAvailable)); err != nil {
		t.Fatalf("ReplaceDataSetStates ahead: %v", err)
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, ahead, []observability.ProviderState{
		{ProviderID: kept, Status: observability.StatusAvailable}, {ProviderID: dropped, Status: observability.StatusAvailable},
	}); err != nil {
		t.Fatalf("ReplaceProviderStates ahead: %v", err)
	}
	if _, err := repos.Observability.RecordApprovedProviders(ctx, ahead, nil); err != nil {
		t.Fatalf("RecordApprovedProviders ahead: %v", err)
	}

	if err := repos.Observability.ReplaceDataSetStates(ctx, now, dataSetSnapshot(local, observability.StatusUnavailable)); err != nil {
		t.Fatalf("ReplaceDataSetStates now: %v", err)
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, now, []observability.ProviderState{
		{ProviderID: kept, Status: observability.StatusUnavailable},
	}); err != nil {
		t.Fatalf("ReplaceProviderStates now: %v", err)
	}
	if checkedAt, err := repos.Observability.RecordApprovedProviders(ctx, now, nil); err != nil || !checkedAt.Equal(now) {
		t.Fatalf("approved providers checked at %s, err=%v, want %s", checkedAt, err, now)
	}

	dataSets, err := repos.Observability.ListDataSetStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDataSetStates: %v", err)
	}
	if len(dataSets.Items) != 1 || dataSets.Items[0].Status != observability.StatusUnavailable ||
		dataSets.LastCheckedAt == nil || !dataSets.LastCheckedAt.Equal(now) {
		t.Fatalf("data sets = %#v, want the snapshot at %s", dataSets, now)
	}
	providerPage, err := repos.Observability.ListProviderStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListProviderStates: %v", err)
	}
	if len(providerPage.Items) != 1 || providerPage.Items[0].ProviderID.String() != kept.String() ||
		providerPage.Items[0].Status != observability.StatusUnavailable ||
		providerPage.LastCheckedAt == nil || !providerPage.LastCheckedAt.Equal(now) {
		t.Fatalf("providers = %#v, want only provider %s from the snapshot at %s", providerPage, kept, now)
	}
}

func dataSetSnapshot(local *model.StorageDataSet, status observability.Status) []observability.DataSetState {
	return []observability.DataSetState{{
		LocalDataSetID: local.ID, BucketID: local.BucketID, CopyIndex: local.CopyIndex,
		ProviderID: local.ProviderID, ChainDataSetID: local.DataSetID,
		Status: status, ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
	}}
}

func TestObservabilityRepoReplacesDataSetStatesAndFilters(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucketA := seedBucket(t, db, "alpha")
	bucketB := seedBucket(t, db, "beta")
	localA := seedStorageDataSet(t, db, bucketA.ID, "101", "1001", model.StorageDataSetStatusReady)
	localB := seedStorageDataSet(t, db, bucketB.ID, "202", "2002", model.StorageDataSetStatusReady)
	checkedAt := time.Date(2026, 5, 17, 10, 0, 0, 0, time.UTC)

	activePieces := int64(7)
	err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{
		{
			LocalDataSetID:   localA.ID,
			BucketID:         bucketA.ID,
			BucketName:       bucketA.Name,
			CopyIndex:        localA.CopyIndex,
			ProviderID:       localA.ProviderID,
			ChainDataSetID:   localA.DataSetID,
			ClientDataSetID:  localA.ClientDataSetID,
			LocalStatus:      localA.Status,
			Status:           observability.StatusAvailable,
			ReasonCodes:      []observability.ReasonCode{},
			ActivePieceCount: &activePieces,
			LastCheckedAt:    checkedAt,
			Evidence:         map[string]any{"active_piece_count": activePieces},
		},
		{
			LocalDataSetID: localB.ID,
			BucketID:       bucketB.ID,
			BucketName:     bucketB.Name,
			CopyIndex:      localB.CopyIndex,
			ProviderID:     localB.ProviderID,
			ChainDataSetID: localB.DataSetID,
			LocalStatus:    localB.Status,
			Status:         observability.StatusUnavailable,
			ReasonCodes:    []observability.ReasonCode{observability.ReasonChainDataSetMissing},
			LastCheckedAt:  checkedAt,
			Evidence:       map[string]any{},
		},
	})
	if err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}

	page, err := repos.Observability.ListDataSetStates(ctx, observability.ListOptions{
		BucketID: bucketA.ID,
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("ListDataSetStates filtered by bucket: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].LocalDataSetID != localA.ID {
		t.Fatalf("bucket-filtered data set page = total:%d items:%+v, want local data set A", page.Total, page.Items)
	}
	if page.Items[0].ActivePieceCount == nil || *page.Items[0].ActivePieceCount != 7 {
		t.Fatalf("active_piece_count = %#v, want 7", page.Items[0].ActivePieceCount)
	}
	if page.Items[0].CopyIndex != localA.CopyIndex {
		t.Fatalf("copy_index = %d, want %d", page.Items[0].CopyIndex, localA.CopyIndex)
	}
	if page.LastCheckedAt == nil || !page.LastCheckedAt.Equal(checkedAt) {
		t.Fatalf("data set collection last checked = %v, want %s", page.LastCheckedAt, checkedAt)
	}

	page, err = repos.Observability.ListDataSetStates(ctx, observability.ListOptions{Limit: 1, Offset: 1})
	if err != nil {
		t.Fatalf("ListDataSetStates paginated: %v", err)
	}
	if page.Total != 2 || page.Summary.Total != 2 || page.Summary.Available != 1 || page.Summary.Unavailable != 1 || len(page.Items) != 1 {
		t.Fatalf("paginated data set page = total:%d summary:%+v items:%+v, want aggregate over filtered rows", page.Total, page.Summary, page.Items)
	}

	byLocalID, err := repos.Observability.GetDataSetStatesByLocalIDs(ctx, []int64{localA.ID, localB.ID, 999})
	if err != nil {
		t.Fatalf("GetDataSetStatesByLocalIDs: %v", err)
	}
	stateB, ok := byLocalID[localB.ID]
	if len(byLocalID) != 2 || !ok || stateB.ActivePieceCount != nil {
		t.Fatalf("states by local id = %+v, want two rows and nil active_piece_count for local B", byLocalID)
	}

	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt.Add(time.Minute), []observability.DataSetState{
		{
			LocalDataSetID: localA.ID,
			BucketID:       bucketA.ID,
			BucketName:     bucketA.Name,
			CopyIndex:      localA.CopyIndex,
			ProviderID:     localA.ProviderID,
			ChainDataSetID: localA.DataSetID,
			LocalStatus:    localA.Status,
			Status:         observability.StatusDegraded,
			ReasonCodes:    []observability.ReasonCode{observability.ReasonChainDataSetUnmanaged},
			LastCheckedAt:  checkedAt.Add(time.Minute),
			Evidence:       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("ReplaceDataSetStates prune: %v", err)
	}

	page, err = repos.Observability.ListDataSetStates(ctx, observability.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDataSetStates after prune: %v", err)
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
		Generation: 1,
		IsCurrent:  true,
		DataSetID:  onChainIDPtr(t, dataSetID),
		Status:     status,
	}
	if _, err := db.NewInsert().Model(row).Exec(context.Background()); err != nil {
		t.Fatalf("seeding storage data set: %v", err)
	}
	return row
}

func boolPtr(value bool) *bool {
	return &value
}

func stringPtr(value string) *string {
	return &value
}
