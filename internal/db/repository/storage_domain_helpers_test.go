package repository_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestStartObjectUploadAttemptValidatesRequiredIdentity(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "upload-identity")
	valid := repository.EnsureContentInput{
		BucketID:        bucket.ID,
		ContentSize:     1,
		Checksum:        testutil.StorageChecksum("checksum-1"),
		RequestedCopies: 1,
	}
	tests := []struct {
		name   string
		mutate func(*repository.EnsureContentInput)
	}{
		{name: "bucket", mutate: func(input *repository.EnsureContentInput) { input.BucketID = 0 }},
		{name: "content size", mutate: func(input *repository.EnsureContentInput) { input.ContentSize = -1 }},
		{name: "empty checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = "" }},
		{name: "short checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = strings.Repeat("a", 63) }},
		{name: "uppercase checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = strings.Repeat("A", 64) }},
		{name: "prefixed checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = "sha256:" + strings.Repeat("a", 64) }},
		{name: "non hex checksum", mutate: func(input *repository.EnsureContentInput) { input.Checksum = strings.Repeat("a", 63) + "g" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := valid
			tt.mutate(&input)
			if _, err := repos.Contents.EnsureContent(t.Context(), input); !errors.Is(err, repository.ErrInvalidInput) {
				t.Fatalf("EnsureContent error = %v, want ErrInvalidInput", err)
			}
		})
	}
	count, err := db.NewSelect().Model((*model.StorageContent)(nil)).Count(t.Context())
	if err != nil {
		t.Fatalf("count storage uploads: %v", err)
	}
	if count != 0 {
		t.Fatalf("storage upload count = %d after invalid writes, want 0", count)
	}
}

func TestListCopiesDerivesNewDataSetFromCreatorContent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "copy-data-set-creator")
	creator, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 1,
		Checksum: testutil.StorageChecksum("creator-content"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent(creator): %v", err)
	}
	reused, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 2,
		Checksum: testutil.StorageChecksum("reused-content"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent(reused): %v", err)
	}
	binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
		CreatedByContentID: creator.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	for _, tc := range []struct {
		content        *model.StorageContent
		transferMethod model.StorageCopyTransferMethod
		wantNew        bool
	}{
		{content: creator, transferMethod: model.StorageCopyTransferMethodIngress, wantNew: true},
		{content: reused, transferMethod: model.StorageCopyTransferMethodPeerPull, wantNew: false},
	} {
		if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), tc.content.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: binding.ID,
			CopyIndex:        binding.CopyIndex,
			ProviderID:       binding.ProviderID,
			TransferMethod:   tc.transferMethod,
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings(%d): %v", tc.content.ID, err)
		}
		copies, err := repos.Contents.ListCopies(t.Context(), tc.content.ID)
		if err != nil {
			t.Fatalf("ListCopies(%d): %v", tc.content.ID, err)
		}
		if len(copies) != 1 || copies[0].IsNewDataSet != tc.wantNew {
			t.Fatalf("ListCopies(%d) = %+v, want one copy with is_new_data_set=%t", tc.content.ID, copies, tc.wantNew)
		}
	}
}

func TestAuthorizeReplacementPreservesCurrentSource(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-current-source")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID:   bucket.ID,
		ProviderID: onChainID(t, "101"),
		CopyIndex:  0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}

	replacement, created, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID:         bucket.ID,
		SourceDataSetID:  source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"),
		ClientRequestID:  "replacement-current-source",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if !created {
		t.Fatal("Authorize created = false, want true")
	}
	target, err := repos.Contents.GetDataSetBindingByID(t.Context(), replacement.TargetDataSetID)
	if err != nil {
		t.Fatalf("GetDataSetBindingByID(target): %v", err)
	}
	if target == nil {
		t.Fatal("replacement target is nil")
	}
	if target.IsCurrent {
		t.Fatal("replacement target is current before activation")
	}
	currentSource, err := repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
	if err != nil {
		t.Fatalf("GetDataSetBindingByID(source): %v", err)
	}
	if currentSource == nil || !currentSource.IsCurrent {
		t.Fatalf("replacement source = %#v, want current source", currentSource)
	}
}

func TestAttachReplacementTargetCopyRejectsMismatchedUpload(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-item-upload-identity")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	replacement, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "replacement-item-upload-identity",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	target, err := repos.Contents.GetDataSetBindingByID(t.Context(), replacement.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("GetDataSetBindingByID(target) = %#v, err=%v", target, err)
	}
	itemUpload := startCopyHealthUpload(t, repos, bucket.ID, "replacement-item-upload", 1, "replacement-item-checksum", 1)
	wrongUpload := startCopyHealthUpload(t, repos, bucket.ID, "replacement-wrong-upload", 1, "replacement-wrong-checksum", 1)
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), itemUpload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: target.ID, CopyIndex: target.CopyIndex,
		ProviderID: target.ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings(item upload): %v", err)
	}
	item := &storagereplacement.Item{
		ReplacementID:   replacement.ID,
		ContentID:       itemUpload.ID,
		TargetDataSetID: target.ID,
		Status:          storagereplacement.ItemStatusPending,
	}
	if _, err := db.NewInsert().Model(item).Exec(t.Context()); err != nil {
		t.Fatalf("insert replacement item: %v", err)
	}

	if _, err := repos.Replacements.AttachTargetCopy(t.Context(), repository.AttachReplacementTargetCopyInput{
		ReplacementID: replacement.ID,
		ItemID:        item.ID,
		ContentID:     wrongUpload.ID,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("AttachTargetCopy error = %v, want ErrConflict", err)
	}
	count, err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Where("content_id = ? AND storage_data_set_id = ?", wrongUpload.ID, target.ID).
		Count(t.Context())
	if err != nil {
		t.Fatalf("count rolled-back wrong target copies: %v", err)
	}
	if count != 0 {
		t.Fatalf("wrong upload target copy count = %d, want 0", count)
	}
}

func TestEnsureDataSetBindingDoesNotReuseProviderBeforeRetirement(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "provider-retirement-occupancy")
	providerID := onChainID(t, "101")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding(source): %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Contents.MarkDataSetDraining(t.Context(), source.ID, "replacement in progress"); err != nil {
		t.Fatalf("MarkDataSetDraining: %v", err)
	}

	if _, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	}); !errors.Is(err, repository.ErrAlreadyExists) {
		t.Fatalf("reuse draining provider error = %v, want ErrAlreadyExists", err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusRetired).
		Where("id = ?", source.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("retire source data set: %v", err)
	}

	reused, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("reuse retired provider: %v", err)
	}
	if reused.ID == source.ID || reused.Generation <= source.Generation {
		t.Fatalf("reused data set = %#v, want a newer generation than %#v", reused, source)
	}
}

func TestRetryReplacementRejectsUnknownFailureReason(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-unknown-reason")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	replacement, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "unknown-failure-reason",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", storagereplacement.StatusFailed).
		Set("failure_reason = ?", "future_failure_reason").
		Set("last_error = ?", "written by a newer binary").
		Where("id = ?", replacement.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("store future failure reason: %v", err)
	}
	if _, err := repos.Replacements.Retry(t.Context(), repository.RetryReplacementInput{
		ReplacementID: replacement.ID,
	}); !errors.Is(err, storagereplacement.ErrNotRetryable) {
		t.Fatalf("Retry error = %v, want ErrNotRetryable", err)
	}
}

// Retiring a rejected generation frees its provider without erasing anything.
// Deleting it would have to refuse whenever a copy was bound to it, which is
// exactly when the provider is most worth getting back.
func TestRetireRejectedDataSetKeepsTheRowAndItsCopies(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "retire-rejected-candidate")
	upload := startCopyHealthUpload(t, repos, bucket.ID, "retire-version", 1, "retire-checksum", 1)
	binding := ensureCopyHealthBinding(t, repos, bucket.ID, upload.ID, 0, "101")
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0, ProviderID: binding.ProviderID,
		TransferMethod: model.StorageCopyTransferMethodIngress,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
		Set("status = ?", model.StorageCopyStatusFailed).
		Where("content_id = ? AND storage_data_set_id = ?", upload.ID, binding.ID).
		Exec(ctx); err != nil {
		t.Fatalf("mark candidate copy failed: %v", err)
	}
	if err := repos.Contents.MarkDataSetFailed(ctx, binding.ID, "creation refused"); err != nil {
		t.Fatalf("MarkDataSetFailed: %v", err)
	}

	retired, err := repos.Contents.RetireRejectedDataSet(ctx, binding.ID)
	if err != nil || !retired {
		t.Fatalf("RetireRejectedDataSet = %t, err=%v", retired, err)
	}
	kept, err := repos.Contents.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || kept == nil || kept.Status != model.StorageDataSetStatusRetired {
		t.Fatalf("retired generation = %#v err=%v, want the row kept as retired", kept, err)
	}
	// The copy is what records that ingest was attempted here.
	copyRow, err := repos.Contents.GetUploadCopyForDataSet(ctx, upload.ID, binding.ID)
	if err != nil || copyRow == nil {
		t.Fatalf("retained copy = %#v err=%v", copyRow, err)
	}
	// And the provider is what retirement buys back.
	reused, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: binding.ProviderID, CopyIndex: 0,
	})
	if err != nil || reused == nil || reused.ID == binding.ID {
		t.Fatalf("rebinding the freed provider = %#v err=%v", reused, err)
	}
}

func TestStorageContentBindingRejectsCrossBucketIdentity(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	sourceBucket := seedBucket(t, db, "upload-source-bucket")
	targetBucket := seedBucket(t, db, "upload-target-bucket")
	version := newObjectVersion(targetBucket.ID, "cross-bucket.txt", model.NewVersionID(), 11)
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	content := startCopyHealthUpload(t, repos, sourceBucket.ID, "source-version", version.Size, "cross-bucket-checksum", 1)
	commitStorageHealthCopy(t, db, repos, sourceBucket.ID, content.ID, 0, "901", "1901", "2901", "https://provider.example/cross-bucket")

	if _, err := repos.Contents.BindReadableUploadForVersion(t.Context(), repository.BindReadableUploadForVersionInput{
		ContentID: content.ID, BucketID: targetBucket.ID, VersionID: version.VersionID,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("BindReadableUploadForVersion error = %v, want ErrConflict", err)
	}

	if _, err := db.NewRaw(`UPDATE object_versions
		SET content_id = ?
		WHERE version_id = ?`, content.ID, version.VersionID).Exec(t.Context()); err == nil {
		t.Fatal("direct cross-bucket object version binding succeeded")
	}
	if _, err := db.NewRaw(`INSERT INTO object_deletions
		(bucket_id, object_id, key, version_id, content_id, size, deleted_at)
		VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		targetBucket.ID, 1, "cross-bucket.txt", model.NewVersionID(), content.ID, 11).Exec(t.Context()); err == nil {
		t.Fatal("direct cross-bucket object deletion binding succeeded")
	}
}

func startCopyHealthUpload(
	t *testing.T,
	repos *repository.Repositories,
	bucketID int64,
	versionID string,
	size int64,
	checksum string,
	requestedCopies int,
) *model.StorageContent {
	t.Helper()
	origin, err := repos.Objects.GetVersionByID(t.Context(), versionID)
	if err != nil {
		t.Fatalf("GetVersionByID(origin): %v", err)
	}
	if origin == nil {
		origin = newObjectVersion(bucketID, "origin-"+versionID, versionID, size)
		origin.Checksum = checksum
		if _, err := createVersion(t, repos, origin); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(origin): %v", err)
		}
	}
	upload, err := repos.Contents.EnsureContent(context.Background(), repository.EnsureContentInput{
		BucketID: bucketID, ContentSize: size,
		Checksum: testutil.StorageChecksum(checksum), RequestedCopies: requestedCopies,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}
	return upload
}

func ensureCopyHealthBinding(
	t *testing.T,
	repos *repository.Repositories,
	bucketID, contentID int64,
	copyIndex int,
	providerID string,
) *model.StorageDataSet {
	t.Helper()
	binding, err := repos.Contents.EnsureDataSetBinding(context.Background(), repository.EnsureDataSetBindingInput{
		BucketID: bucketID, ProviderID: onChainID(t, providerID), CopyIndex: copyIndex,
		CreatedByContentID: contentID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	return binding
}

func commitStorageHealthCopy(
	t *testing.T,
	db *bun.DB,
	repos *repository.Repositories,
	bucketID, contentID int64,
	copyIndex int,
	providerID, dataSetID, pieceID, retrievalURL string,
) *model.StorageDataSet {
	t.Helper()
	binding := ensureCopyHealthBinding(t, repos, bucketID, contentID, copyIndex, providerID)
	if err := repos.Contents.MarkDataSetReady(context.Background(), repository.MarkDataSetReadyInput{
		ID: binding.ID, ContentID: contentID, DataSetID: onChainID(t, dataSetID),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(context.Background(), contentID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: copyIndex,
		TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, providerID),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
		ContentID: contentID, CopyIndex: copyIndex, PieceCID: "bafk2bzacestorhealth",
		PieceID: onChainIDPtr(t, pieceID), RetrievalURL: retrievalURL,
	})
	got, err := repos.Contents.GetDataSetBindingByID(context.Background(), binding.ID)
	if err != nil {
		t.Fatalf("GetDataSetBindingByID: %v", err)
	}
	return got
}

func bindStorageHealthVersion(
	t *testing.T,
	repos *repository.Repositories,
	bucketID, contentID int64,
	version *model.ObjectVersion,
) {
	t.Helper()
	if _, err := repos.Contents.BindReadableUploadForVersion(context.Background(), repository.BindReadableUploadForVersionInput{
		ContentID: contentID, BucketID: bucketID, VersionID: version.VersionID,
	}); err != nil {
		t.Fatalf("BindReadableUploadForVersion: %v", err)
	}
}

// A termination is a ledger row, so every path that asks "has the end of term
// been recorded?" has to read that row. The retirement gate blocks forever if it
// reads the replacement alone.
func TestRecordedTerminationEpochIsVisibleToTheRetirementGate(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "replacement-termination-evidence")
	source, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	replacement, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "termination-evidence",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", storagereplacement.StatusRetiring).
		Where("id = ?", replacement.ID).
		Exec(t.Context()); err != nil {
		t.Fatalf("move replacement to retiring: %v", err)
	}
	if err := repos.Replacements.RecordTerminationEpoch(t.Context(), repository.RecordTerminationEpochInput{
		ReplacementID: replacement.ID, TxHash: "0xterminate", Epoch: 84,
	}); err != nil {
		t.Fatalf("RecordTerminationEpoch: %v", err)
	}
	// Recording twice would pay for a second termination transaction.
	if err := repos.Replacements.RecordTerminationEpoch(t.Context(), repository.RecordTerminationEpochInput{
		ReplacementID: replacement.ID, TxHash: "0xterminate-again", Epoch: 85,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("second RecordTerminationEpoch error = %v, want ErrConflict", err)
	}

	stored, err := repos.Replacements.GetByID(t.Context(), replacement.ID)
	if err != nil || stored == nil || stored.TerminationEpoch == nil || *stored.TerminationEpoch != 84 {
		t.Fatalf("stored replacement = %#v err=%v, want termination epoch 84", stored, err)
	}
	observed := int64(90)
	gate, err := repos.Replacements.EvaluateRetirementGate(t.Context(), replacement.ID, &observed)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate: %v", err)
	}
	if gate.TerminationEpoch == nil || *gate.TerminationEpoch != 84 || !gate.EpochReached {
		t.Fatalf("retirement gate = %#v, want the recorded epoch 84 reached at 90", gate)
	}
	if slices.Contains(gate.Blockers, "termination_epoch") {
		t.Fatalf("retirement gate blockers = %v, want no epoch blocker", gate.Blockers)
	}
}

// A failed generation must give up the replica slot it was holding. While it
// kept is_current, bucket provisioning saw a current failed binding, suspended
// on it every minute, and nothing in the system could ever clear it.
func TestMarkDataSetFailedReleasesTheReplicaSlot(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "data-set-failure-releases-slot")
	binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if !binding.IsCurrent {
		t.Fatalf("new binding = %#v, want is_current", binding)
	}

	if err := repos.Contents.MarkDataSetFailed(t.Context(), binding.ID, "creation outcome unknown"); err != nil {
		t.Fatalf("MarkDataSetFailed: %v", err)
	}
	failed, err := repos.Contents.GetDataSetBindingByID(t.Context(), binding.ID)
	if err != nil || failed == nil {
		t.Fatalf("GetDataSetBindingByID: binding=%#v err=%v", failed, err)
	}
	if failed.Status != model.StorageDataSetStatusFailed || failed.IsCurrent {
		t.Fatalf("failed binding = status:%s is_current:%t, want failed and not current", failed.Status, failed.IsCurrent)
	}

	// The slot is free, so the next generation can take it.
	next, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding after failure: %v", err)
	}
	if !next.IsCurrent || next.Generation <= failed.Generation {
		t.Fatalf("next generation = %#v, want current and newer than %d", next, failed.Generation)
	}

	// And the database refuses to put the failed row back on the slot.
	if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
		Set("is_current = ?", true).
		Where("id = ?", failed.ID).
		Exec(t.Context()); err == nil {
		t.Fatal("restoring is_current on a failed data set was accepted")
	}
}

// Provisioning fails before any copy exists, so reclamation has to work with
// zero copies bound to the generation. A recorded data set identity is what
// keeps a generation: a rejected transaction hash is not, because it names a
// submission the chain refused rather than storage anyone is paying for.
func TestRetireRejectedDataSetFreesTheProviderWithoutADataSetIdentity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		transactionID *string
		dataSetID     *string
		wantDiscarded bool
	}{
		{name: "rejected submission", transactionID: new("0xcreate"), wantDiscarded: true},
		{name: "no submission at all", wantDiscarded: true},
		{name: "recorded a data set identity", dataSetID: new("9001"), wantDiscarded: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			bucket := seedBucket(t, db, "discard-"+strings.ReplaceAll(tc.name, " ", "-"))
			provider := onChainID(t, "101")
			binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: provider, CopyIndex: 0,
			})
			if err != nil {
				t.Fatalf("EnsureDataSetBinding: %v", err)
			}
			update := db.NewUpdate().Model((*model.StorageDataSet)(nil)).Where("id = ?", binding.ID)
			if tc.transactionID != nil {
				update = update.Set("create_transaction_id = ?", *tc.transactionID)
			}
			if tc.dataSetID != nil {
				update = update.Set("data_set_id = ?", *tc.dataSetID)
			}
			if tc.transactionID != nil || tc.dataSetID != nil {
				if _, err := update.Exec(t.Context()); err != nil {
					t.Fatalf("record creation evidence: %v", err)
				}
			}
			if err := repos.Contents.MarkDataSetFailed(t.Context(), binding.ID, "creation refused"); err != nil {
				t.Fatalf("MarkDataSetFailed: %v", err)
			}

			discarded, err := repos.Contents.RetireRejectedDataSet(t.Context(), binding.ID)
			if err != nil {
				t.Fatalf("RetireRejectedDataSet: %v", err)
			}
			if discarded != tc.wantDiscarded {
				t.Fatalf("discarded = %t, want %t", discarded, tc.wantDiscarded)
			}

			// The provider is reusable exactly when the row is gone.
			reused, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: provider, CopyIndex: 0,
			})
			if tc.wantDiscarded {
				if err != nil || reused == nil {
					t.Fatalf("rebinding the freed provider = %#v err=%v", reused, err)
				}
				return
			}
			if !errors.Is(err, repository.ErrAlreadyExists) {
				t.Fatalf("rebinding a retained provider error = %v, want ErrAlreadyExists", err)
			}
			kept, err := repos.Contents.GetDataSetBindingByID(t.Context(), binding.ID)
			if err != nil || kept == nil || kept.DataSetID == nil {
				t.Fatalf("retained generation = %#v err=%v, want its data set identity kept", kept, err)
			}
		})
	}
}

// A health snapshot must not keep a rejected generation holding its provider.
// The observability table cascades with the data set and is rebuilt on every
// refresh, so it is a snapshot rather than evidence, and the refresh runs far
// more often than a creation takes to fail.
func TestRetireRejectedDataSetIgnoresObservabilitySnapshots(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "discard-with-observation")
	provider := onChainID(t, "101")
	binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: provider, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Contents.MarkDataSetFailed(ctx, binding.ID, "creation outcome unknown"); err != nil {
		t.Fatalf("MarkDataSetFailed: %v", err)
	}
	// The periodic refresh snapshots every data set, including one that never
	// reached the chain, and it runs far more often than a failure takes.
	checkedAt := time.Now().UTC()
	if err := repos.Observability.ReplaceDataSetStates(ctx, checkedAt, []observability.DataSetState{{
		LocalDataSetID: binding.ID, BucketID: bucket.ID, BucketName: bucket.Name,
		CopyIndex: binding.CopyIndex, ProviderID: binding.ProviderID,
		Status: observability.StatusUnavailable, LocalStatus: model.StorageDataSetStatusFailed,
		ReasonCodes:   []observability.ReasonCode{observability.ReasonChainDataSetMissing},
		LastCheckedAt: checkedAt, Evidence: map[string]any{},
	}}); err != nil {
		t.Fatalf("ReplaceDataSetStates: %v", err)
	}

	discarded, err := repos.Contents.RetireRejectedDataSet(ctx, binding.ID)
	if err != nil || !discarded {
		t.Fatalf("RetireRejectedDataSet = %t err=%v, want the snapshot ignored", discarded, err)
	}
	if _, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: provider, CopyIndex: 0,
	}); err != nil {
		t.Fatalf("rebinding the freed provider: %v", err)
	}
}
