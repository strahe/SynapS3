package repository_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

// TestStorageCleanupBlocksReuseUntilContentIsFinalized walks content through
// its last delete: cleanup starts only once no live version remains, new
// versions cannot name the content until it is finalized, and finalizing waits
// for cached bytes before it deletes the current-state rows and leaves the
// ledgers. A replacement item naming the content does not hold it back; the
// item is cancelled once the replacement reaches it.
func TestStorageCleanupBlocksReuseUntilContentIsFinalized(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "cleanup-finalize")
	contentID := seedContent(t, repos, bucket.ID, "cleanup-finalize", 10)
	source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByContentID: contentID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	versions := make([]*model.ObjectVersion, 2)
	for i := range versions {
		versions[i] = newObjectVersion(bucket.ID, fmt.Sprintf("file-%d.txt", i), model.NewVersionID(), 10)
		versions[i].ContentID = &contentID
		if _, err := createVersion(t, repos, versions[i]); err != nil {
			t.Fatalf("create version %d: %v", i, err)
		}
	}
	deleteVersion := func(version *model.ObjectVersion) *repository.StorageCleanupReservation {
		t.Helper()
		result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID: bucket.ID, Key: version.Key, VersionID: version.VersionID,
		})
		if err != nil {
			t.Fatalf("DeleteObjectVersionPermanently(%s): %v", version.Key, err)
		}
		return result.StorageCleanup
	}
	if cleanup := deleteVersion(versions[0]); cleanup != nil {
		t.Fatalf("cleanup reserved while another version is live: %#v", cleanup)
	}
	// Nothing was ever stored remotely, and the last delete still starts cleanup.
	cleanup := deleteVersion(versions[1])
	if cleanup == nil || cleanup.ContentID != contentID || cleanup.TaskID != nil {
		t.Fatalf("last delete cleanup = %#v", cleanup)
	}
	taskRow, created, err := repos.Tasks.Enqueue(ctx, &model.Task{
		Type: model.TaskTypeStorageCleanup, IdempotencyKey: "cleanup-finalize", InputVersion: 1,
		Input: []byte(`{}`), InputHash: "cleanup-finalize", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})
	if err != nil || !created {
		t.Fatalf("Enqueue = %#v, created=%v, err=%v", taskRow, created, err)
	}
	if err := repos.StorageCleanup.BindTask(ctx, contentID, cleanup.Generation, taskRow.ID); err != nil {
		t.Fatalf("BindTask: %v", err)
	}
	reuse := func() error {
		version := newObjectVersion(bucket.ID, "reuse.txt", model.NewVersionID(), 10)
		version.ContentID = &contentID
		_, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
		return err
	}
	if err := reuse(); !errors.Is(err, repository.ErrContentCleanupInProgress) {
		t.Fatalf("reuse during cleanup = %v, want ErrContentCleanupInProgress", err)
	}

	markSourceReady(t, repos, source.ID)
	replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "cleanup-finalize",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	item := &storagereplacement.Item{
		ReplacementID: replacement.ID, ContentID: contentID, TargetDataSetID: replacement.TargetDataSetID,
		Status: storagereplacement.ItemStatusAttention,
	}
	if _, err := db.NewInsert().Model(item).Exec(ctx); err != nil {
		t.Fatalf("insert replacement item: %v", err)
	}
	pull := func(attemptID string, resolvedAt *time.Time) *storagepull.Attempt {
		return &storagepull.Attempt{
			AttemptID: attemptID, ContentID: contentID, StorageDataSetID: source.ID,
			Status:           storagepull.AttemptStatusAttempted,
			SourceProviderID: onChainID(t, "303"), SourceDataSetID: onChainID(t, "3003"), SourcePieceID: onChainID(t, "7"),
			SourcePieceCID: "source-piece", SourceRetrievalURL: "https://source.example/piece",
			AttemptedAt: time.Now(), ResolvedAt: resolvedAt,
		}
	}
	earlier := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	openPull, resolvedPull := pull("open-pull", nil), pull("resolved-pull", &earlier)
	if _, err := db.NewInsert().Model(openPull).Exec(ctx); err != nil {
		t.Fatalf("insert open pull: %v", err)
	}
	if _, err := db.NewInsert().Model(resolvedPull).Exec(ctx); err != nil {
		t.Fatalf("insert resolved pull: %v", err)
	}
	now := time.Now()
	if _, err := db.NewInsert().Model(&model.ObjectCache{ContentID: contentID, InCache: true, CreatedAt: now, UpdatedAt: now}).
		On("CONFLICT (content_id) DO UPDATE").Set("in_cache = EXCLUDED.in_cache").Exec(ctx); err != nil {
		t.Fatalf("mark content cached: %v", err)
	}
	finalize := func(taskID int64) error {
		return repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
			return txRepos.StorageCleanup.FinalizeContent(ctx, contentID, cleanup.Generation, taskID)
		})
	}
	if err := finalize(taskRow.ID); !errors.Is(err, repository.ErrContentCleanupNotReady) {
		t.Fatalf("finalize with cached bytes = %v, want ErrContentCleanupNotReady", err)
	}
	if released, err := repos.Objects.ReleaseContentCacheIfUnreferenced(ctx, contentID, func() error { return nil }); err != nil || !released {
		t.Fatalf("release cache = %t, %v", released, err)
	}
	if err := finalize(taskRow.ID + 1); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("finalize by another task = %v, want ErrConflict", err)
	}
	if err := finalize(taskRow.ID); err != nil {
		t.Fatalf("FinalizeContent: %v", err)
	}

	for _, rows := range []struct {
		table, column string
		want          int
	}{
		{"storage_contents", "id", 0},
		{"storage_copies", "content_id", 0},
		{"object_cache", "content_id", 0},
		{"storage_data_sets", "created_by_content_id", 0},
		{"object_deletions", "content_id", 2},
		{"storage_replacement_items", "content_id", 1},
	} {
		var count int
		if err := db.NewRaw("SELECT count(*) FROM "+rows.table+" WHERE "+rows.column+" = ?", contentID).Scan(ctx, &count); err != nil {
			t.Fatalf("count %s: %v", rows.table, err)
		}
		if count != rows.want {
			t.Fatalf("%s rows naming the content = %d, want %d", rows.table, count, rows.want)
		}
	}
	// Nothing can commit a pull for finalized content, so an open one is
	// abandoned; a pull that already resolved keeps its evidence unchanged.
	pulls := map[string]storagepull.Attempt{}
	var pullRows []storagepull.Attempt
	if err := db.NewSelect().Model(&pullRows).Where("content_id = ?", contentID).Scan(ctx); err != nil {
		t.Fatalf("load pull attempts: %v", err)
	}
	for _, row := range pullRows {
		pulls[row.AttemptID] = row
	}
	if got := pulls["open-pull"]; got.Status != storagepull.AttemptStatusAbandoned || got.ResolvedAt == nil || got.LastError == nil {
		t.Fatalf("open pull after finalizing = %#v, want it abandoned", got)
	}
	if got := pulls["resolved-pull"]; got.Status != storagepull.AttemptStatusAttempted || got.ResolvedAt == nil || !got.ResolvedAt.Equal(earlier) {
		t.Fatalf("resolved pull after finalizing = %#v, want it unchanged", got)
	}
	// A write that resolved the content before it was finalized is refused the
	// same way, and a cache file left under its key is an orphan.
	if err := reuse(); !errors.Is(err, repository.ErrContentCleanupInProgress) {
		t.Fatalf("reuse after finalizing = %v, want ErrContentCleanupInProgress", err)
	}
	releases := 0
	released, err := repos.Objects.ReleaseContentCacheIfUnreferenced(ctx, contentID, func() error {
		releases++
		return nil
	})
	if err != nil || !released || releases != 1 {
		t.Fatalf("orphan cache release = %t, %v, calls=%d", released, err, releases)
	}

	// Retrying the replacement puts the item back to pending; the coordinator
	// then finds its content gone and cancels it instead of stalling.
	if _, err := db.NewUpdate().Model(item).Set("status = ?", storagereplacement.ItemStatusPending).WherePK().Exec(ctx); err != nil {
		t.Fatalf("reset replacement item: %v", err)
	}
	if _, err := repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: replacement.ID, ItemID: item.ID,
	}); !errors.Is(err, storagereplacement.ErrItemCancelled) {
		t.Fatalf("AcquireItem(finalized content) = %v, want ErrItemCancelled", err)
	}
	var itemStatus storagereplacement.ItemStatus
	if err := db.NewSelect().Model((*storagereplacement.Item)(nil)).Column("status").
		Where("id = ?", item.ID).Scan(ctx, &itemStatus); err != nil {
		t.Fatalf("load replacement item: %v", err)
	}
	if itemStatus != storagereplacement.ItemStatusCancelled {
		t.Fatalf("replacement item status = %s, want cancelled", itemStatus)
	}
}

func TestStorageCleanupReferenceChecksIgnoreOwnUnacceptedCopy(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "cleanup-unaccepted-copy")
	content, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 10,
		Checksum: testutil.StorageChecksum("cleanup-unaccepted-copy"), RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}
	providerID := onChainID(t, "701")
	dataSetID := onChainID(t, "801")
	pieceID := onChainID(t, "901")
	retrievalURL := "https://provider.example/piece"
	seedCommittedUploadCopies(t, db, repos, bucket.ID, content.ID, "piece-cid", []storageUploadCopySeed{{
		ProviderID: &providerID, DataSetID: &dataSetID, PieceID: &pieceID, RetrievalURL: &retrievalURL,
	}})
	committed, err := repos.Contents.GetUploadCopy(ctx, content.ID, 0)
	if err != nil || committed == nil {
		t.Fatalf("GetUploadCopy: %#v, %v", committed, err)
	}
	for copyIndex := 1; copyIndex < 3; copyIndex++ {
		failedProvider := onChainID(t, strconv.Itoa(701+copyIndex))
		binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: failedProvider, CopyIndex: copyIndex, CreatedByContentID: content.ID,
		})
		if err != nil {
			t.Fatalf("EnsureDataSetBinding(%d): %v", copyIndex, err)
		}
		if err := repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: binding.ID, CopyIndex: copyIndex,
			TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: failedProvider,
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings(%d): %v", copyIndex, err)
		}
		result, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
			Set("status = ?", model.StorageCopyStatusFailed).
			Where("content_id = ? AND copy_index = ?", content.ID, copyIndex).Exec(ctx)
		if err != nil {
			t.Fatalf("fail copy %d: %v", copyIndex, err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			t.Fatalf("failed copy %d rows = %d, want 1", copyIndex, rows)
		}
	}
	version := newObjectVersion(bucket.ID, "file.txt", model.NewVersionID(), 10)
	version.ContentID = &content.ID
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatalf("create version: %v", err)
	}
	if referenced, err := repos.StorageCleanup.UploadHasObjectReferences(ctx, content.ID); err != nil || !referenced {
		t.Fatalf("UploadHasObjectReferences = %t, %v, want true", referenced, err)
	}
	deleted, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: bucket.ID, Key: version.Key, VersionID: version.VersionID,
	})
	if err != nil || deleted.StorageCleanup == nil {
		t.Fatalf("DeleteObjectVersionPermanently: %#v, %v", deleted, err)
	}
	storedContent, err := repos.Contents.GetByID(ctx, content.ID)
	if err != nil || storedContent == nil || storedContent.AcceptedAt != nil {
		t.Fatalf("unaccepted content after delete = %#v, %v", storedContent, err)
	}
	if referenced, err := repos.StorageCleanup.UploadHasObjectReferences(ctx, content.ID); err != nil || referenced {
		t.Fatalf("UploadHasObjectReferences = %t, %v, want false", referenced, err)
	}
	checkCleanupReferences := func(want bool) {
		t.Helper()
		referenced, err := repos.StorageCleanup.CleanupHasObjectReferences(ctx, content.ID)
		if err != nil || referenced != want {
			t.Fatalf("CleanupHasObjectReferences = %t, %v, want %t", referenced, err, want)
		}
	}
	checkCleanupReferences(false)

	sharedContentID := seedContent(t, repos, bucket.ID, "cleanup-shared-copy", 10)
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, sharedContentID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: committed.StorageDataSetID, CopyIndex: 0,
		TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: providerID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings(shared): %v", err)
	}
	testutil.CommitStorageCopy(t, db, repos, testutil.CommitCopyInput{
		ContentID: sharedContentID, CopyIndex: 0, PieceCID: "piece-cid", PieceID: &pieceID,
		RetrievalURL: retrievalURL,
	})
	checkCleanupReferences(true)

	if _, err := db.NewUpdate().Model((*model.StorageContent)(nil)).
		Set("accepted_at = ?", time.Now()).Where("id = ?", sharedContentID).Exec(ctx); err != nil {
		t.Fatalf("accept shared content: %v", err)
	}
	sharedVersion := newObjectVersion(bucket.ID, "shared.txt", model.NewVersionID(), 10)
	sharedVersion.ContentID = &sharedContentID
	if _, err := createVersion(t, repos, sharedVersion); err != nil {
		t.Fatalf("create shared version: %v", err)
	}
	checkCleanupReferences(true)
}

func TestStorageCleanupSnapshotsEveryCommittedCopy(t *testing.T) {
	db, repos, version, secondCopy := seedCleanupCopyCommitBoundary(t)
	ctx := t.Context()
	requestID, taskID := collectCleanupBoundaryCopy(t, repos, secondCopy)
	if _, err := repos.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{
		RequestID: requestID, TaskID: taskID, ExtraDataHex: "abcd",
		Members: []repository.SealMember{{CopyID: secondCopy.ID, ContentID: *version.ContentID, PieceCID: "piece-cid"}},
	}); err != nil {
		t.Fatalf("SealCommitRequest: %v", err)
	}
	deleteVersion := func() (*repository.StorageCleanupReservation, error) {
		result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID: version.BucketID, Key: version.Key, VersionID: version.VersionID,
		})
		return result.StorageCleanup, err
	}
	// A signed member waits for its request: the request is sent whole or not
	// at all.
	if cleanup, err := deleteVersion(); !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) || cleanup != nil {
		t.Fatalf("delete with a signed registration = %#v, %v, want storage busy", cleanup, err)
	}
	if err := repos.Contents.BeginCommitSubmission(ctx, repository.BeginCommitSubmissionInput{RequestID: requestID, TaskID: taskID}); err != nil {
		t.Fatalf("BeginCommitSubmission: %v", err)
	}
	if cleanup, err := deleteVersion(); !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) || cleanup != nil {
		t.Fatalf("delete during submitted registration = %#v, %v, want storage busy", cleanup, err)
	}
	var cleanupRows int
	if err := db.NewRaw("SELECT count(*) FROM storage_cleanup_copies WHERE content_id = ?", *version.ContentID).Scan(ctx, &cleanupRows); err != nil {
		t.Fatalf("count cleanup copies before confirmation: %v", err)
	}
	if cleanupRows != 0 {
		t.Fatalf("cleanup copies before confirmation = %d, want 0", cleanupRows)
	}
	if err := repos.Contents.RecordCommitSubmission(ctx, repository.CommitSubmissionInput{
		CommitSendInput: repository.CommitSendInput{RequestID: requestID, TaskID: taskID, Sends: 1},
		TransactionID:   "tx-late-copy", StatusURL: "https://provider.example/status/late-copy",
	}); err != nil {
		t.Fatalf("RecordCommitSubmission: %v", err)
	}
	secondPieceID := onChainID(t, "902")
	members, err := repos.Contents.ConfirmCommitRequest(ctx, repository.ConfirmCommitRequestInput{
		RequestID: requestID, TaskID: taskID, ConfirmedTransactionID: "tx-late-copy",
		FirstPieceID: secondPieceID, RetrievalURLs: []string{"https://provider.example/second-piece"},
	})
	if err != nil || len(members) != 1 || members[0].ID != secondCopy.ID {
		t.Fatalf("ConfirmCommitRequest = %#v, %v", members, err)
	}
	cleanup, err := deleteVersion()
	if err != nil || cleanup == nil {
		t.Fatalf("delete after confirmation = %#v, %v", cleanup, err)
	}
	if err := db.NewRaw("SELECT count(*) FROM storage_cleanup_copies WHERE content_id = ?", *version.ContentID).Scan(ctx, &cleanupRows); err != nil {
		t.Fatalf("count cleanup copies after confirmation: %v", err)
	}
	if cleanupRows != 2 {
		t.Fatalf("cleanup copies after confirmation = %d, want both committed copies", cleanupRows)
	}
	var secondPieceSnapshots int
	if err := db.NewRaw("SELECT count(*) FROM storage_cleanup_copies WHERE content_id = ? AND storage_data_set_id = ? AND piece_id = ?",
		*version.ContentID, secondCopy.StorageDataSetID, secondPieceID).Scan(ctx, &secondPieceSnapshots); err != nil {
		t.Fatalf("count second copy cleanup snapshot: %v", err)
	}
	if secondPieceSnapshots != 1 {
		t.Fatalf("second copy cleanup snapshots = %d, want 1", secondPieceSnapshots)
	}
}

func TestStorageCleanupCancelsCollectingCommitBeforeSnapshot(t *testing.T) {
	db, repos, version, secondCopy := seedCleanupCopyCommitBoundary(t)
	ctx := t.Context()
	requestID, taskID := collectCleanupBoundaryCopy(t, repos, secondCopy)
	deleted, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: version.BucketID, Key: version.Key, VersionID: version.VersionID,
	})
	if err != nil || deleted.StorageCleanup == nil {
		t.Fatalf("delete with a collecting registration = %#v, %v", deleted, err)
	}
	stored, err := repos.Contents.GetUploadCopyByID(ctx, secondCopy.ID)
	if err != nil || stored == nil || stored.Status != model.StorageCopyStatusFailed || stored.CommitRequestID != nil {
		t.Fatalf("second copy after delete = %#v, %v, want failed outside any request", stored, err)
	}
	// The request lost its only copy, so it can never be signed.
	request, err := repos.Contents.GetCommitRequest(ctx, requestID)
	if err != nil || request.Status != storagecommit.RequestStatusAbandoned || request.TaskID != nil {
		t.Fatalf("collecting request after delete = %#v, %v, want abandoned", request, err)
	}
	if _, err := repos.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{
		RequestID: requestID, TaskID: taskID, ExtraDataHex: "abcd",
		Members: []repository.SealMember{{CopyID: secondCopy.ID, ContentID: *version.ContentID, PieceCID: "piece-cid"}},
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("SealCommitRequest after delete = %v, want conflict", err)
	}
	var cleanupRows int
	if err := db.NewRaw("SELECT count(*) FROM storage_cleanup_copies WHERE content_id = ?", *version.ContentID).Scan(ctx, &cleanupRows); err != nil {
		t.Fatalf("count cleanup copies: %v", err)
	}
	if cleanupRows != 1 {
		t.Fatalf("cleanup copies = %d, want only the committed copy", cleanupRows)
	}
}

// collectCleanupBoundaryCopy puts the transferred copy into a collecting
// request driven by a new task.
func collectCleanupBoundaryCopy(t *testing.T, repos *repository.Repositories, copyRow *model.StorageCopy) (string, int64) {
	t.Helper()
	requestID := "cleanup-boundary-request"
	taskRow, _, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
		Type: model.TaskTypeStorageCommit, IdempotencyKey: "cleanup-boundary-commit", InputVersion: 1,
		Input: []byte(`{"request_id":"cleanup-boundary-request"}`), InputHash: "cleanup-boundary-commit",
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("Enqueue commit task: %v", err)
	}
	if err := repos.Contents.CreateCollectingCommitRequest(t.Context(), repository.CreateCommitRequestInput{
		RequestID: requestID, TaskID: taskRow.ID, StorageDataSetID: copyRow.StorageDataSetID, CopyIDs: []int64{copyRow.ID},
	}); err != nil {
		t.Fatalf("CreateCollectingCommitRequest: %v", err)
	}
	return requestID, taskRow.ID
}

func seedCleanupCopyCommitBoundary(t *testing.T) (*bun.DB, *repository.Repositories, *model.ObjectVersion, *model.StorageCopy) {
	t.Helper()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "cleanup-commit-boundary")
	content, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 10,
		Checksum: testutil.StorageChecksum("cleanup-commit-boundary"), RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}
	version := newObjectVersion(bucket.ID, "file.txt", model.NewVersionID(), 10)
	version.ContentID = &content.ID
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatalf("create version: %v", err)
	}
	firstProviderID := onChainID(t, "701")
	firstDataSetID := onChainID(t, "801")
	firstPieceID := onChainID(t, "901")
	retrievalURL := "https://provider.example/first-piece"
	seedCommittedUploadCopies(t, db, repos, bucket.ID, content.ID, "piece-cid", []storageUploadCopySeed{{
		ProviderID: &firstProviderID, DataSetID: &firstDataSetID, PieceID: &firstPieceID, RetrievalURL: &retrievalURL,
	}})
	secondProviderID := onChainID(t, "702")
	binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: secondProviderID, CopyIndex: 1, CreatedByContentID: content.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding(second): %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: binding.ID, DataSetID: onChainID(t, "802"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady(second): %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 1,
		TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: secondProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings(second): %v", err)
	}
	secondCopy, err := repos.Contents.GetUploadCopy(ctx, content.ID, 1)
	if err != nil || secondCopy == nil {
		t.Fatalf("GetUploadCopy(second) = %#v, %v", secondCopy, err)
	}
	if err := repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		StorageCopyID: secondCopy.ID, ContentID: content.ID, CopyIndex: 1,
		PieceCID: "piece-cid", RequireEligibleCopy: true,
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady(second): %v", err)
	}
	return db, repos, version, secondCopy
}

func TestStorageCleanupCopyTransitionsAreGuardedAndIdempotent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "cleanup-transitions")
	contentID := seedContent(t, repos, bucket.ID, "cleanup-transitions", 10)
	providerID := onChainID(t, "701")
	binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: contentID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	insertCopy := func(piece int64) *model.StorageCleanupCopy {
		t.Helper()
		row := &model.StorageCleanupCopy{
			ContentID: contentID, BucketID: bucket.ID, CopyIndex: 0, ProviderID: providerID,
			StorageDataSetID: binding.ID, PieceID: onChainID(t, strconv.FormatInt(piece, 10)), PieceCID: "piece-cid",
			Checksum: "cleanup-transitions-checksum", Status: model.StorageCleanupCopyStatusPending,
		}
		if _, err := db.NewInsert().Model(row).Exec(t.Context()); err != nil {
			t.Fatalf("insert cleanup copy: %v", err)
		}
		return row
	}

	scheduled := insertCopy(801)
	if err := repos.StorageCleanup.MarkCopyDeleteScheduled(t.Context(), scheduled.ID, "0xtx"); err != nil {
		t.Fatalf("MarkCopyDeleteScheduled: %v", err)
	}
	first := new(model.StorageCleanupCopy)
	if err := db.NewSelect().Model(first).Where("id = ?", scheduled.ID).Scan(t.Context()); err != nil {
		t.Fatalf("load scheduled copy: %v", err)
	}
	if err := repos.StorageCleanup.MarkCopyDeleteScheduled(t.Context(), scheduled.ID, "0xtx"); err != nil {
		t.Fatalf("idempotent MarkCopyDeleteScheduled: %v", err)
	}
	replayed := new(model.StorageCleanupCopy)
	if err := db.NewSelect().Model(replayed).Where("id = ?", scheduled.ID).Scan(t.Context()); err != nil {
		t.Fatalf("reload scheduled copy: %v", err)
	}
	if first.ScheduledAt == nil || replayed.ScheduledAt == nil || !first.ScheduledAt.Equal(*replayed.ScheduledAt) {
		t.Fatalf("scheduled_at changed across replay: %v -> %v", first.ScheduledAt, replayed.ScheduledAt)
	}
	if err := repos.StorageCleanup.MarkCopyDeleteScheduled(t.Context(), scheduled.ID, "0xother"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("changed scheduling evidence = %v, want ErrConflict", err)
	}
	if err := repos.StorageCleanup.BeginFailedCopyRetry(t.Context(), scheduled.ID, "0xtx"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("retry live request = %v, want ErrConflict", err)
	}
	if err := repos.StorageCleanup.MarkCopyFailed(t.Context(), scheduled.ID, "confirmed rejected"); err != nil {
		t.Fatalf("mark rejected request failed: %v", err)
	}
	if err := repos.StorageCleanup.BeginFailedCopyRetry(t.Context(), scheduled.ID, "0xwrong"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("retry wrong transaction = %v, want ErrConflict", err)
	}
	if err := repos.StorageCleanup.BeginFailedCopyRetry(t.Context(), scheduled.ID, "0xtx"); err != nil {
		t.Fatalf("begin rejected transaction retry: %v", err)
	}
	retrying := new(model.StorageCleanupCopy)
	if err := db.NewSelect().Model(retrying).Where("id = ?", scheduled.ID).Scan(t.Context()); err != nil {
		t.Fatalf("reload retrying copy: %v", err)
	}
	if retrying.Status != model.StorageCleanupCopyStatusPending || retrying.DeleteTxHash != nil || retrying.LastError != nil || retrying.ScheduledAt != nil {
		t.Fatalf("retrying cleanup request = %#v", retrying)
	}
	if err := repos.StorageCleanup.MarkCopyDeleteScheduled(t.Context(), scheduled.ID, "0xretry"); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	replaced := new(model.StorageCleanupCopy)
	if err := db.NewSelect().Model(replaced).Where("id = ?", scheduled.ID).Scan(t.Context()); err != nil {
		t.Fatalf("reload retried copy: %v", err)
	}
	if replaced.Status != model.StorageCleanupCopyStatusDeleteScheduled || replaced.DeleteTxHash == nil || *replaced.DeleteTxHash != "0xretry" || replaced.ScheduledAt == nil || replaced.ScheduledAt.Before(*first.ScheduledAt) {
		t.Fatalf("rescheduled cleanup request = %#v", replaced)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), scheduled.ID); err != nil {
		t.Fatalf("MarkCopyRemoved(scheduled): %v", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), scheduled.ID); err != nil {
		t.Fatalf("idempotent MarkCopyRemoved: %v", err)
	}

	failed := insertCopy(802)
	if err := repos.StorageCleanup.MarkCopyFailed(t.Context(), failed.ID, "unknown outcome"); err != nil {
		t.Fatalf("MarkCopyFailed: %v", err)
	}
	if err := repos.StorageCleanup.BeginFailedCopyRetry(t.Context(), failed.ID, "old hash"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("retry hashless copy with wrong hash = %v, want ErrConflict", err)
	}
	if err := repos.StorageCleanup.BeginFailedCopyRetry(t.Context(), failed.ID, ""); err != nil {
		t.Fatalf("retry hashless copy: %v", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), failed.ID); err != nil {
		t.Fatalf("MarkCopyRemoved(failed): %v", err)
	}

	unsupported := insertCopy(803)
	if err := repos.StorageCleanup.MarkCopyUnsupported(t.Context(), unsupported.ID, "unsupported"); err != nil {
		t.Fatalf("MarkCopyUnsupported: %v", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), unsupported.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkCopyRemoved(unsupported) = %v, want ErrConflict", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), 999999); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("MarkCopyRemoved(missing) = %v, want ErrNotFound", err)
	}
}
