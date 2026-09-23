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
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

// TestStorageCleanupBlocksReuseUntilContentIsFinalized walks content through
// its last delete: cleanup starts only once no live version remains, new
// versions cannot name the content until it is finalized, and finalizing waits
// for cached bytes and blocking replacement items before it deletes the
// current-state rows and leaves the ledgers.
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
	if err := finalize(taskRow.ID); !errors.Is(err, repository.ErrContentCleanupNotReady) {
		t.Fatalf("finalize with a replacement item in attention = %v, want ErrContentCleanupNotReady", err)
	}
	if _, err := db.NewUpdate().Model(item).Set("status = ?", storagereplacement.ItemStatusCancelled).WherePK().Exec(ctx); err != nil {
		t.Fatalf("cancel replacement item: %v", err)
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
	testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
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
	db, repos, version, secondCopy, identity := seedCleanupCopyCommitBoundary(t)
	ctx := t.Context()
	attemptID := "late-copy-attempted"
	if result, err := repos.Contents.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
		Copy: identity, AttemptID: attemptID,
	}); err != nil || result.State != storagecommit.ReservationAcquired {
		t.Fatalf("ReserveCommitAttempt = %#v, %v", result, err)
	}
	if _, err := repos.Contents.MarkCommitAttempted(ctx, storagecommit.AttemptInput{
		Copy: identity, AttemptID: attemptID, ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("MarkCommitAttempted: %v", err)
	}
	deleteVersion := func() (*repository.StorageCleanupReservation, error) {
		result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID: version.BucketID, Key: version.Key, VersionID: version.VersionID,
		})
		return result.StorageCleanup, err
	}
	if cleanup, err := deleteVersion(); !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) || cleanup != nil {
		t.Fatalf("delete during attempted commit = %#v, %v, want storage busy", cleanup, err)
	}
	var cleanupRows int
	if err := db.NewRaw("SELECT count(*) FROM storage_cleanup_copies WHERE content_id = ?", *version.ContentID).Scan(ctx, &cleanupRows); err != nil {
		t.Fatalf("count cleanup copies before confirmation: %v", err)
	}
	if cleanupRows != 0 {
		t.Fatalf("cleanup copies before confirmation = %d, want 0", cleanupRows)
	}
	if err := repos.Contents.RecordCommitSubmission(ctx, storagecommit.EvidenceInput{
		Copy: identity, AttemptID: attemptID, TransactionID: "tx-late-copy",
		StatusURL: "https://provider.example/status/late-copy",
	}); err != nil {
		t.Fatalf("RecordCommitSubmission: %v", err)
	}
	secondPieceID := onChainID(t, "902")
	if err := repos.Contents.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: secondCopy.ID, ContentID: *version.ContentID, CopyIndex: secondCopy.CopyIndex,
		PieceCID: "piece-cid", PieceID: &secondPieceID, RetrievalURL: "https://provider.example/second-piece",
		CommitAttemptID: attemptID, CommitExtraDataHex: "abcd",
		CommitTransactionID: "tx-late-copy", CommitConfirmedTransactionID: "tx-late-copy",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
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

func TestStorageCleanupCancelsUnattemptedCommitBeforeSnapshot(t *testing.T) {
	db, repos, version, secondCopy, identity := seedCleanupCopyCommitBoundary(t)
	ctx := t.Context()
	attemptID := "late-copy-reserved"
	if result, err := repos.Contents.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
		Copy: identity, AttemptID: attemptID,
	}); err != nil || result.State != storagecommit.ReservationAcquired {
		t.Fatalf("ReserveCommitAttempt = %#v, %v", result, err)
	}
	deleted, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: version.BucketID, Key: version.Key, VersionID: version.VersionID,
	})
	if err != nil || deleted.StorageCleanup == nil {
		t.Fatalf("delete with reserved commit = %#v, %v", deleted, err)
	}
	stored, err := repos.Contents.GetUploadCopyByID(ctx, secondCopy.ID)
	if err != nil || stored == nil || stored.Status != model.StorageCopyStatusFailed {
		t.Fatalf("second copy after delete = %#v, %v, want failed", stored, err)
	}
	attempt := new(storagecommit.Attempt)
	if err := db.NewSelect().Model(attempt).Where("attempt_id = ?", attemptID).Scan(ctx); err != nil {
		t.Fatalf("load reserved attempt after delete: %v", err)
	}
	if attempt.Status != storagecommit.AttemptStatusReleased || attempt.ResolvedAt == nil {
		t.Fatalf("reserved attempt after delete = %#v, want released", attempt)
	}
	if _, err := repos.Contents.MarkCommitAttempted(ctx, storagecommit.AttemptInput{
		Copy: identity, AttemptID: attemptID, ExtraDataHex: "abcd",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkCommitAttempted after delete = %v, want conflict", err)
	}
	secondPieceID := onChainID(t, "902")
	if err := repos.Contents.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: secondCopy.ID, ContentID: *version.ContentID, CopyIndex: secondCopy.CopyIndex,
		PieceCID: "piece-cid", PieceID: &secondPieceID, RetrievalURL: "https://provider.example/second-piece",
		CommitAttemptID: attemptID, CommitExtraDataHex: "abcd",
		CommitTransactionID: "tx-late-copy", CommitConfirmedTransactionID: "tx-late-copy",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkUploadCopyCommitted after delete = %v, want conflict", err)
	}
	var cleanupRows int
	if err := db.NewRaw("SELECT count(*) FROM storage_cleanup_copies WHERE content_id = ?", *version.ContentID).Scan(ctx, &cleanupRows); err != nil {
		t.Fatalf("count cleanup copies: %v", err)
	}
	if cleanupRows != 1 {
		t.Fatalf("cleanup copies = %d, want only the committed copy", cleanupRows)
	}
}

func seedCleanupCopyCommitBoundary(t *testing.T) (*bun.DB, *repository.Repositories, *model.ObjectVersion, *model.StorageCopy, storagecommit.CopyIdentity) {
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
		ID: binding.ID, ContentID: content.ID, DataSetID: onChainID(t, "802"),
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
		PieceCID: "piece-cid", CommitExtraDataHex: "abcd", RequireEligibleCopy: true,
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady(second): %v", err)
	}
	identity := storagecommit.CopyIdentity{
		StorageCopyID: secondCopy.ID, ContentID: content.ID, CopyIndex: 1,
		StorageDataSetID: secondCopy.StorageDataSetID, RequireEligibleCopy: true,
	}
	return db, repos, version, secondCopy, identity
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
