package repository_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

type replacementFixture struct {
	db      *bun.DB
	repos   *repository.Repositories
	bucket  *model.Bucket
	upload  *model.StorageUpload
	version *model.ObjectVersion
	source  *model.StorageDataSet
	request int
}

func newReplacementFixture(t *testing.T, name, versionID string) *replacementFixture {
	t.Helper()
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, name)

	version := newObjectVersion(bucket.ID, "file.txt", versionID, 10)
	version.Checksum = name + "-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload := startCopyHealthUpload(t, repos, bucket.ID, version.VersionID, version.Size, version.Checksum, 1)
	source := commitStorageHealthCopy(t, repos, bucket.ID, upload.ID, 0, "101", "1001", "2001", "https://source.example/piece")
	bindStorageHealthVersion(t, repos, bucket.ID, upload.ID, version)
	return &replacementFixture{db: db, repos: repos, bucket: bucket, upload: upload, version: version, source: source}
}

func (f *replacementFixture) authorize(t *testing.T, provider string) *storagereplacement.Replacement {
	t.Helper()
	f.request++
	row, _, err := f.repos.Replacements.Authorize(context.Background(), repository.AuthorizeReplacementInput{
		BucketID:         f.bucket.ID,
		SourceDataSetID:  f.source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, provider),
		ClientRequestID:  fmt.Sprintf("fixture-%d", f.request),
		MaxRetries:       5,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	return row
}

// readyTarget brings the approved target to the point where it can take over.
func (f *replacementFixture) readyTarget(t *testing.T, row *storagereplacement.Replacement, dataSetID string) *model.StorageDataSet {
	t.Helper()
	ctx := context.Background()
	if err := f.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        row.TargetDataSetID,
		UploadID:  f.upload.ID,
		DataSetID: onChainID(t, dataSetID),
	}); err != nil {
		t.Fatalf("MarkDataSetReady target: %v", err)
	}
	target, err := f.repos.Uploads.GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("GetDataSetBindingByID target = %#v err=%v", target, err)
	}
	return target
}

func TestStorageReplacementRepo_AuthorizeCreatesTargetGenerationWithoutMovingWrites(t *testing.T) {
	f := newReplacementFixture(t, "replacement-authorize", "01J000000000000000000RPL01")
	ctx := context.Background()

	row := f.authorize(t, "202")
	if row.Status != storagereplacement.StatusPreparingTarget || row.CopyIndex != 0 {
		t.Fatalf("replacement = %#v, want preparing_target on slot 0", row)
	}

	// Writes must keep going to the source until the target is actually usable.
	current, err := f.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, f.bucket.ID, 0)
	if err != nil || current == nil || current.ID != f.source.ID {
		t.Fatalf("current binding = %#v err=%v, want the source until activation", current, err)
	}
	target, err := f.repos.Uploads.GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("target binding = %#v err=%v", target, err)
	}
	if target.IsCurrent || target.Generation != f.source.Generation+1 || target.Status != model.StorageDataSetStatusPending {
		t.Fatalf("target = %#v, want a pending next generation that is not current", target)
	}

	task, err := f.repos.Tasks.GetByIdempotencyKey(ctx, storagereplacement.MigrateTaskKey(row.ID))
	if err != nil || task == nil {
		t.Fatalf("migration coordinator = %#v err=%v, want it queued with the confirmation", task, err)
	}
}

func TestStorageReplacementRepo_AuthorizeRejections(t *testing.T) {
	f := newReplacementFixture(t, "replacement-reject", "01J000000000000000000RPL02")
	ctx := context.Background()
	base := repository.AuthorizeReplacementInput{
		BucketID:         f.bucket.ID,
		SourceDataSetID:  f.source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"),
		MaxRetries:       5,
		ClientRequestID:  "reject-request",
	}

	t.Run("target is the source", func(t *testing.T) {
		input := base
		input.TargetProviderID = f.source.ProviderID
		if _, _, err := f.repos.Replacements.Authorize(ctx, input); !errors.Is(err, storagereplacement.ErrInvalidTarget) {
			t.Fatalf("error = %v, want ErrInvalidTarget", err)
		}
	})

	t.Run("target already serves the bucket", func(t *testing.T) {
		other := commitStorageHealthCopy(t, f.repos, f.bucket.ID, f.upload.ID, 1, "303", "3003", "3003", "https://other.example/piece")
		input := base
		input.TargetProviderID = other.ProviderID
		if _, _, err := f.repos.Replacements.Authorize(ctx, input); !errors.Is(err, storagereplacement.ErrTargetInUse) {
			t.Fatalf("error = %v, want ErrTargetInUse", err)
		}
	})

	// A provider whose earlier generation is draining still owns its data set for
	// this bucket, so preparing a second one would collide on the provider/data
	// set uniqueness inside the worker.
	t.Run("target still holds a draining generation", func(t *testing.T) {
		drainingProvider := commitStorageHealthCopy(t, f.repos, f.bucket.ID, f.upload.ID, 2, "404", "4004", "4004", "https://draining.example/piece")
		mustExec(t, f.db, `UPDATE storage_data_sets SET is_current = FALSE, status = ? WHERE id = ?`,
			model.StorageDataSetStatusDraining, drainingProvider.ID)
		input := base
		input.TargetProviderID = drainingProvider.ProviderID
		if _, _, err := f.repos.Replacements.Authorize(ctx, input); !errors.Is(err, storagereplacement.ErrTargetInUse) {
			t.Fatalf("error = %v, want ErrTargetInUse", err)
		}
	})

	t.Run("unknown data set", func(t *testing.T) {
		input := base
		input.SourceDataSetID = 9999
		if _, _, err := f.repos.Replacements.Authorize(ctx, input); !errors.Is(err, repository.ErrNotFound) {
			t.Fatalf("error = %v, want ErrNotFound", err)
		}
	})

	t.Run("unknown selection mode", func(t *testing.T) {
		input := base
		input.SelectionMode = "guess"
		if _, _, err := f.repos.Replacements.Authorize(ctx, input); !errors.Is(err, repository.ErrInvalidInput) {
			t.Fatalf("error = %v, want ErrInvalidInput", err)
		}
	})
}

// A later confirmation takes over in the same transaction, so a source never
// holds two live replacements.
func TestStorageReplacementRepo_AuthorizeSupersedesEarlierConfirmation(t *testing.T) {
	f := newReplacementFixture(t, "replacement-supersede", "01J000000000000000000RPL03")
	ctx := context.Background()

	first := f.authorize(t, "202")
	second := f.authorize(t, "303")
	if second.ID == first.ID {
		t.Fatal("second confirmation reused the first replacement")
	}

	got, err := f.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID first = %#v err=%v", got, err)
	}
	if got.Status != storagereplacement.StatusSuperseded {
		t.Fatalf("first replacement status = %s, want superseded", got.Status)
	}
	if got.SupersededByID == nil || *got.SupersededByID != second.ID {
		t.Fatalf("first.SupersededByID = %v, want %d", got.SupersededByID, second.ID)
	}
	active, err := f.repos.Replacements.GetActiveForDataSet(ctx, f.source.ID)
	if err != nil || active == nil || active.ID != second.ID {
		t.Fatalf("active replacement = %#v err=%v, want the newest confirmation", active, err)
	}

	abandoned, err := f.repos.Tasks.GetByIdempotencyKey(ctx, storagereplacement.AbandonedTargetTaskKey(first.ID))
	if err != nil || abandoned == nil {
		t.Fatalf("abandoned-target cleanup = %#v err=%v, want it queued with the later confirmation", abandoned, err)
	}
	claimed, err := f.repos.Tasks.ClaimReady(ctx, model.TaskTypeStorageCleanup, time.Minute)
	if err != nil || claimed == nil || claimed.ID != abandoned.ID {
		t.Fatalf("ClaimReady abandoned cleanup = %#v err=%v, want the leftover terminator", claimed, err)
	}
}

func TestStorageReplacementRepo_ActivateSwitchesTheSlotAtomically(t *testing.T) {
	f := newReplacementFixture(t, "replacement-activate", "01J000000000000000000RPL04")
	ctx := context.Background()
	row := f.authorize(t, "202")

	// A target that is not writable yet cannot take the slot.
	if err := f.repos.Replacements.Activate(ctx, row.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("activate before ready = %v, want conflict", err)
	}
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	current, err := f.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, f.bucket.ID, 0)
	if err != nil || current == nil || current.ID != row.TargetDataSetID {
		t.Fatalf("current binding = %#v err=%v, want the target", current, err)
	}
	source, err := f.repos.Uploads.GetDataSetBindingByID(ctx, f.source.ID)
	if err != nil || source == nil || source.IsCurrent || source.Status != model.StorageDataSetStatusDraining {
		t.Fatalf("source after activation = %#v err=%v, want draining and not current", source, err)
	}
	got, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || got == nil || got.Status != storagereplacement.StatusMigrating {
		t.Fatalf("replacement after activation = %#v err=%v, want migrating", got, err)
	}
	if err := f.repos.Replacements.Activate(ctx, row.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("second activate = %v, want conflict", err)
	}
}

// Recovery must not revive a generation an operator is actively replacing, but
// a replacement that has given up should not hold the slot hostage.
func TestStorageReplacementRepo_RecoveryYieldsToInProgressReplacementOnly(t *testing.T) {
	f := newReplacementFixture(t, "replacement-recovery", "01J000000000000000000RPL05")
	ctx := context.Background()
	row := f.authorize(t, "202")

	if err := f.repos.Uploads.MarkDataSetUnavailable(ctx, f.source.ID, "provider unreachable"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	recovered, err := f.repos.Uploads.RecoverDataSet(ctx, repository.MarkDataSetReadyInput{
		ID:        f.source.ID,
		UploadID:  f.upload.ID,
		DataSetID: onChainID(t, "1001"),
	})
	if err != nil {
		t.Fatalf("RecoverDataSet during replacement: %v", err)
	}
	if recovered {
		t.Fatal("recovery revived a generation with an in-progress replacement")
	}

	if err := f.repos.Replacements.MarkFailed(ctx, row.ID, nil, "target creation exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	recovered, err = f.repos.Uploads.RecoverDataSet(ctx, repository.MarkDataSetReadyInput{
		ID:        f.source.ID,
		UploadID:  f.upload.ID,
		DataSetID: onChainID(t, "1001"),
	})
	if err != nil {
		t.Fatalf("RecoverDataSet after failure: %v", err)
	}
	if !recovered {
		t.Fatal("recovery stayed blocked after the replacement terminally failed")
	}
}

func TestStorageReplacementRepo_SeedMigrationBatchIsBounded(t *testing.T) {
	f := newReplacementFixture(t, "replacement-seed", "01J000000000000000000RPL06")
	ctx := context.Background()

	// Extra stored content on the same source, so seeding has to page.
	for i, versionID := range []string{
		"01J000000000000000000RPL07",
		"01J000000000000000000RPL08",
		"01J000000000000000000RPL09",
	} {
		version := newObjectVersion(f.bucket.ID, "file.txt", versionID, 10)
		version.Checksum = versionID
		if _, err := f.repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent %d: %v", i, err)
		}
		upload := startCopyHealthUpload(t, f.repos, f.bucket.ID, version.VersionID, version.Size, version.Checksum, 1)
		if err := f.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: f.source.ID,
			CopyIndex:        0,
			TransferMethod:   model.StorageCopyTransferMethodIngress,
			ProviderID:       f.source.ProviderID,
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings %d: %v", i, err)
		}
		if err := f.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
			UploadID:     upload.ID,
			CopyIndex:    0,
			PieceCID:     "bafk2bzacestorhealth",
			PieceID:      onChainIDPtr(t, "700"+versionID[len(versionID)-1:]),
			RetrievalURL: "https://source.example/piece-" + versionID,
		}); err != nil {
			t.Fatalf("MarkUploadCopyCommitted %d: %v", i, err)
		}
		bindStorageHealthVersion(t, f.repos, f.bucket.ID, upload.ID, version)
	}

	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	total := 0
	passes := 0
	for {
		inserted, done, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 2)
		if err != nil {
			t.Fatalf("SeedMigrationBatch: %v", err)
		}
		if inserted > 2 {
			t.Fatalf("seeded %d items in one pass, want at most the batch limit", inserted)
		}
		total += inserted
		passes++
		if done {
			break
		}
		if passes > 10 {
			t.Fatal("seeding never completed, cursor is not advancing")
		}
	}
	if total != 4 {
		t.Fatalf("seeded %d items, want one per stored upload", total)
	}
	if passes < 2 {
		t.Fatalf("seeding finished in %d pass, want it to page", passes)
	}

	got, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || got == nil || !got.SeedingComplete || got.ItemsTotal != 4 {
		t.Fatalf("replacement after seeding = %#v err=%v, want complete with 4 items", got, err)
	}
	// Re-running is a no-op rather than a duplicate.
	inserted, done, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 2)
	if err != nil || inserted != 0 || !done {
		t.Fatalf("re-seed = (%d, %v, %v), want no new work", inserted, done, err)
	}
}

// Parked items are revisited only after fresh work runs out, so one unreachable
// piece of content cannot stall the whole replacement.
func TestStorageReplacementRepo_NextExecutableItemPrefersFreshWork(t *testing.T) {
	f := newReplacementFixture(t, "replacement-next-item", "01J000000000000000000RPL10")
	ctx := context.Background()
	later := newObjectVersion(f.bucket.ID, "other.txt", "01J000000000000000000RPL10B", 10)
	later.Checksum = "replacement-next-item-later"
	if _, err := f.repos.Objects.CreateVersionAndSetCurrent(ctx, later); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent later: %v", err)
	}
	laterUpload := startCopyHealthUpload(t, f.repos, f.bucket.ID, later.VersionID, later.Size, later.Checksum, 1)
	if err := f.repos.Uploads.CreateUploadCopiesForBindings(ctx, laterUpload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: f.source.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       f.source.ProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings later: %v", err)
	}
	if err := f.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     laterUpload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacepreferfresh",
		PieceID:      onChainIDPtr(t, "7010"),
		RetrievalURL: "https://source.example/piece-later",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted later: %v", err)
	}
	bindStorageHealthVersion(t, f.repos, f.bucket.ID, laterUpload.ID, later)

	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 10); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}

	first, err := f.repos.Replacements.NextExecutableItem(ctx, row.ID)
	if err != nil || first == nil {
		t.Fatalf("NextExecutableItem = %#v err=%v", first, err)
	}
	if err := f.repos.Replacements.MarkItemWaitingSource(ctx, first.ID, "no readable source"); err != nil {
		t.Fatalf("MarkItemWaitingSource: %v", err)
	}
	fresh, err := f.repos.Replacements.NextExecutableItem(ctx, row.ID)
	if err != nil || fresh == nil || fresh.ID == first.ID {
		t.Fatalf("NextExecutableItem after parking = %#v err=%v, want pending work ahead of the parked item", fresh, err)
	}
	if fresh.Status != storagereplacement.ItemStatusPending {
		t.Fatalf("fresh item status = %s, want pending", fresh.Status)
	}

	if err := f.repos.Replacements.MarkItemCopied(ctx, fresh.ID); err != nil {
		t.Fatalf("MarkItemCopied: %v", err)
	}
	parked, err := f.repos.Replacements.NextExecutableItem(ctx, row.ID)
	if err != nil || parked == nil || parked.ID != first.ID {
		t.Fatalf("NextExecutableItem after fresh work = %#v err=%v, want the parked item revisited", parked, err)
	}
	if parked.Status != storagereplacement.ItemStatusWaitingSource {
		t.Fatalf("parked item status = %s, want waiting_source", parked.Status)
	}
}

func TestStorageReplacementRepo_RetryOnlyResumesOperatorAttentionStates(t *testing.T) {
	f := newReplacementFixture(t, "replacement-retry", "01J000000000000000000RPL11")
	ctx := context.Background()
	row := f.authorize(t, "202")

	if _, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: row.ID, MaxRetries: 5}); !errors.Is(err, storagereplacement.ErrNotRetryable) {
		t.Fatalf("retry while preparing = %v, want ErrNotRetryable", err)
	}
	if err := f.repos.Replacements.MarkFailed(ctx, row.ID, nil, "creation exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	resumed, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: row.ID, MaxRetries: 5})
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	// The target never activated, so the retry resumes preparation.
	if resumed.Status != storagereplacement.StatusPreparingTarget || resumed.LastError != nil {
		t.Fatalf("resumed = %#v, want preparing_target with the error cleared", resumed)
	}

	superseded := f.authorize(t, "303")
	if _, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: row.ID, MaxRetries: 5}); !errors.Is(err, storagereplacement.ErrSuperseded) {
		t.Fatalf("retry after supersede = %v, want ErrSuperseded", err)
	}
	if superseded.ID == row.ID {
		t.Fatal("supersede reused the replacement row")
	}
}

func TestStorageCleanupRepo_SettlesReplacementItemBeforeDeletingUpload(t *testing.T) {
	f := newReplacementFixture(t, "replacement-provenance-cleanup", model.NewVersionID())
	ctx := context.Background()
	replacement := f.authorize(t, "202")
	orphan, err := f.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: f.bucket.ID, SourceVersionID: model.NewVersionID(), ContentSize: 10,
		Checksum: "orphaned-replacement-content", RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	item := &storagereplacement.Item{
		ReplacementID: replacement.ID, UploadID: orphan.ID,
		Status: storagereplacement.ItemStatusRunning, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if _, err := f.db.NewInsert().Model(item).Exec(ctx); err != nil {
		t.Fatalf("insert replacement item: %v", err)
	}
	if _, err := f.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("items_total = ?", 1).Where("id = ?", replacement.ID).Exec(ctx); err != nil {
		t.Fatalf("set replacement total: %v", err)
	}

	if err := f.repos.StorageCleanup.DeleteUploadProvenanceIfUnreferenced(ctx, orphan.ID); err != nil {
		t.Fatalf("DeleteUploadProvenanceIfUnreferenced: %v", err)
	}
	if got, err := f.repos.Uploads.GetByID(ctx, orphan.ID); err != nil || got != nil {
		t.Fatalf("upload after cleanup = %#v err=%v, want deleted", got, err)
	}
	itemCount, err := f.db.NewSelect().Model((*storagereplacement.Item)(nil)).
		Where("id = ?", item.ID).Count(ctx)
	if err != nil {
		t.Fatalf("count replacement item: %v", err)
	}
	if itemCount != 0 {
		t.Fatalf("replacement item count = %d, want deleted after settlement", itemCount)
	}
	if err := f.repos.Replacements.MarkItemCopied(ctx, item.ID); err != nil {
		t.Fatalf("late MarkItemCopied: %v", err)
	}
	got, err := f.repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || got == nil || got.ItemsCopied != 0 || got.ItemsTotal != 1 {
		t.Fatalf("replacement progress after late completion = %#v err=%v, want copied 0 of historical total 1", got, err)
	}
}

// The retirement gate is the last thing standing between a replacement and
// permanent data loss, so every predicate is checked independently and
// CompleteRetirement refuses on its own, whoever calls it.
func TestStorageReplacementRepo_RetirementGateBlocksEachUnsafeCondition(t *testing.T) {
	f := newReplacementFixture(t, "replacement-gate", "01J000000000000000000RPL12")
	ctx := context.Background()
	row := f.authorize(t, "202")
	target := f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 10); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}

	// Outstanding migration work blocks retirement.
	gate, err := f.repos.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate: %v", err)
	}
	if gate.Passed() || gate.WaitingItems != 1 {
		t.Fatalf("gate = %#v, want it blocked by one outstanding item", gate)
	}
	if err := f.repos.Replacements.CompleteRetirement(ctx, row.ID, 9999); !errors.Is(err, storagereplacement.ErrPrematureComplete) {
		t.Fatalf("CompleteRetirement with outstanding work = %v, want ErrPrematureComplete", err)
	}

	item, err := f.repos.Replacements.NextExecutableItem(ctx, row.ID)
	if err != nil || item == nil {
		t.Fatalf("NextExecutableItem: %#v err=%v", item, err)
	}
	if err := f.repos.Replacements.MarkItemCopied(ctx, item.ID); err != nil {
		t.Fatalf("MarkItemCopied: %v", err)
	}

	// The content is not actually on the new provider yet.
	gate, err = f.repos.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate: %v", err)
	}
	if gate.Passed() || gate.CoverageGaps != 1 {
		t.Fatalf("gate = %#v, want it blocked by a coverage gap", gate)
	}

	mustExec(t, f.db, `INSERT INTO storage_upload_copies (upload_id, copy_index, provider_id, piece_id, transfer_method, status, retrieval_url, storage_data_set_id, created_at, updated_at)
		VALUES (?, 0, '202', '3002', ?, ?, 'https://target.example/piece', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		f.upload.ID, model.StorageCopyTransferMethodPeerPull, model.StorageUploadCopyStatusCommitted, target.ID)

	// Everything is covered, but the service has not been terminated yet.
	gate, err = f.repos.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate: %v", err)
	}
	if !gate.Passed() {
		t.Fatalf("gate = %#v, want everything except the epoch satisfied", gate)
	}
	if err := f.repos.Replacements.CompleteRetirement(ctx, row.ID, 9999); !errors.Is(err, storagereplacement.ErrPrematureComplete) {
		t.Fatalf("CompleteRetirement before termination = %v, want ErrPrematureComplete", err)
	}

	if err := f.repos.Replacements.BeginRetirement(ctx, row.ID); err != nil {
		t.Fatalf("BeginRetirement: %v", err)
	}
	if err := f.repos.Replacements.RecordTerminationEpoch(ctx, repository.RecordTerminationEpochInput{
		ReplacementID: row.ID, TxHash: "0xterminate", Epoch: 5000,
	}); err != nil {
		t.Fatalf("RecordTerminationEpoch: %v", err)
	}

	// The chain has not reached the end of term.
	if err := f.repos.Replacements.CompleteRetirement(ctx, row.ID, 4999); !errors.Is(err, storagereplacement.ErrPrematureComplete) {
		t.Fatalf("CompleteRetirement before the epoch = %v, want ErrPrematureComplete", err)
	}

	// Another upload still writing to the source blocks retirement even now.
	inFlight := startCopyHealthUpload(t, f.repos, f.bucket.ID, "01J000000000000000000RPL13", 10, "in-flight-checksum", 1)
	mustExec(t, f.db, `INSERT INTO storage_upload_copies (upload_id, copy_index, provider_id, transfer_method, status, storage_data_set_id, created_at, updated_at)
		VALUES (?, 0, '101', ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		inFlight.ID, model.StorageCopyTransferMethodPeerPull, model.StorageUploadCopyStatusCommitting, f.source.ID)
	if err := f.repos.Replacements.CompleteRetirement(ctx, row.ID, 5000); !errors.Is(err, storagereplacement.ErrPrematureComplete) {
		t.Fatalf("CompleteRetirement with an in-flight source write = %v, want ErrPrematureComplete", err)
	}
	mustExec(t, f.db, `DELETE FROM storage_upload_copies WHERE upload_id = ?`, inFlight.ID)

	if err := f.repos.Replacements.CompleteRetirement(ctx, row.ID, 5000); err != nil {
		t.Fatalf("CompleteRetirement: %v", err)
	}
	source, err := f.repos.Uploads.GetDataSetBindingByID(ctx, f.source.ID)
	if err != nil || source == nil || source.Status != model.StorageDataSetStatusRetired {
		t.Fatalf("source = %#v err=%v, want retired", source, err)
	}
	done, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || done == nil || done.Status != storagereplacement.StatusCompleted {
		t.Fatalf("replacement = %#v err=%v, want completed", done, err)
	}
	// Completing twice is harmless, which keeps a retried task safe.
	if err := f.repos.Replacements.CompleteRetirement(ctx, row.ID, 5000); err != nil {
		t.Fatalf("second CompleteRetirement: %v", err)
	}
}

// An upload that is still in flight when seeding runs must still get migrated.
// Judging eligibility at seeding time skipped it forever, and the retirement
// coverage gate then blocked on content no item was ever created for.
func TestStorageReplacementRepo_SeedingCoversUploadsStillInFlight(t *testing.T) {
	f := newReplacementFixture(t, "replacement-inflight", "01J000000000000000000RPL14")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")

	// A second upload starts before activation and has not committed yet.
	inFlightVersion := newObjectVersion(f.bucket.ID, "later.txt", "01J000000000000000000RPL15", 10)
	inFlightVersion.Checksum = "in-flight-checksum"
	if _, err := f.repos.Objects.CreateVersionAndSetCurrent(ctx, inFlightVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	inFlight := startCopyHealthUpload(t, f.repos, f.bucket.ID, inFlightVersion.VersionID, 10, inFlightVersion.Checksum, 1)
	if err := f.repos.Uploads.CreateUploadCopiesForBindings(ctx, inFlight.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: f.source.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       f.source.ProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}

	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 10); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}

	// The in-flight upload commits to the retiring generation afterwards.
	inFlightCopy, err := f.repos.Uploads.GetUploadCopyForDataSet(ctx, inFlight.ID, f.source.ID)
	if err != nil || inFlightCopy == nil {
		t.Fatalf("GetUploadCopyForDataSet = %#v err=%v", inFlightCopy, err)
	}
	if err := f.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: inFlightCopy.ID,
		UploadID:            inFlight.ID,
		CopyIndex:           0,
		PieceCID:            "bafk2bzacestorhealth",
		PieceID:             onChainIDPtr(t, "7001"),
		RetrievalURL:        "https://source.example/in-flight",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}

	var seeded int
	if err = f.db.NewRaw(`SELECT COUNT(*) FROM storage_replacement_items WHERE replacement_id = ? AND upload_id = ?`,
		row.ID, inFlight.ID).Scan(ctx, &seeded); err != nil {
		t.Fatalf("count seeded items: %v", err)
	}
	if seeded != 1 {
		t.Fatal("the upload that was still in flight during seeding was never given migration work")
	}

	// The retirement gate must therefore see it as owed work, not as a
	// permanent coverage gap with no item behind it.
	gate, err := f.repos.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate: %v", err)
	}
	if gate.WaitingItems == 0 {
		t.Fatalf("gate = %#v, want the in-flight upload counted as outstanding work", gate)
	}
}

// An item that can never be satisfied must reach a terminal status. Leaving it
// executable made the coordinator pick the same item forever and held the
// retirement gate open.
func TestStorageReplacementRepo_UnsatisfiableItemsSettleTerminally(t *testing.T) {
	f := newReplacementFixture(t, "replacement-settle", "01J000000000000000000RPL16")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 10); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}
	item, err := f.repos.Replacements.NextExecutableItem(ctx, row.ID)
	if err != nil || item == nil {
		t.Fatalf("NextExecutableItem = %#v err=%v", item, err)
	}

	// The content stops being referenced before the item runs.
	mustExec(t, f.db, `DELETE FROM object_versions WHERE version_id = ?`, f.version.VersionID)

	task := seedClaimedReplacementTask(t, f, row.ID)
	if _, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID,
		ItemID:        item.ID,
		TaskID:        task.ID,
		TaskClaimedAt: *task.ClaimedAt,
	}); !errors.Is(err, storagereplacement.ErrItemCancelled) {
		t.Fatalf("AcquireItem = %v, want ErrItemCancelled", err)
	}

	// The decisive part: the item must not come back.
	next, err := f.repos.Replacements.NextExecutableItem(ctx, row.ID)
	if err != nil {
		t.Fatalf("NextExecutableItem: %v", err)
	}
	if next != nil {
		t.Fatalf("item %d is still executable after being cancelled, so the coordinator would loop on it", next.ID)
	}
	gate, err := f.repos.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate: %v", err)
	}
	if gate.WaitingItems != 0 {
		t.Fatalf("gate = %#v, want no outstanding items once they are settled", gate)
	}
}

// A commit rejected on the retiring generation must be reset on that generation,
// not on whichever one currently owns the slot.
func TestStorageReplacementRepo_ResetRejectedCommitTargetsTheRecordedCopy(t *testing.T) {
	f := newReplacementFixture(t, "replacement-reset", "01J000000000000000000RPL17")
	ctx := context.Background()
	sourceCopy, err := f.repos.Uploads.GetUploadCopyForDataSet(ctx, f.upload.ID, f.source.ID)
	if err != nil || sourceCopy == nil {
		t.Fatalf("GetUploadCopyForDataSet = %#v err=%v", sourceCopy, err)
	}
	mustExec(t, f.db, `UPDATE storage_upload_copies SET status = ?, commit_transaction_id = '0xrejected' WHERE id = ?`,
		model.StorageUploadCopyStatusCommitting, sourceCopy.ID)

	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	// Without the recorded copy id this resolves to the new generation and fails,
	// leaving the old one stuck in committing and the retirement gate blocked.
	if err := f.repos.Uploads.ResetRejectedUploadCopyCommit(ctx, repository.ResetRejectedUploadCopyCommitInput{
		StorageUploadCopyID: sourceCopy.ID,
		UploadID:            f.upload.ID,
		CopyIndex:           0,
		CommitTransactionID: "0xrejected",
		LastError:           "provider rejected the commit",
	}); err != nil {
		t.Fatalf("ResetRejectedUploadCopyCommit: %v", err)
	}
	got, err := f.repos.Uploads.GetUploadCopyByID(ctx, sourceCopy.ID)
	if err != nil || got == nil || got.Status != model.StorageUploadCopyStatusPieceReady {
		t.Fatalf("retiring copy = %#v err=%v, want it reset to piece_ready", got, err)
	}
}

// A target a later confirmation abandoned keeps costing money until its own
// service ends. Retiring it must never touch the source, which the successor
// still depends on.
func TestStorageReplacementRepo_AbandonedTargetRetiresWithoutTouchingTheSource(t *testing.T) {
	f := newReplacementFixture(t, "replacement-abandoned", "01J000000000000000000RPL18")
	ctx := context.Background()
	first := f.authorize(t, "202")
	f.readyTarget(t, first, "2002")
	second := f.authorize(t, "303")

	superseded, err := f.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || superseded == nil || superseded.Status != storagereplacement.StatusSuperseded {
		t.Fatalf("first replacement = %#v err=%v, want superseded", superseded, err)
	}
	candidates, err := f.repos.Replacements.ListSupersededCleanupCandidates(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ListSupersededCleanupCandidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].ID != first.ID {
		t.Fatalf("cleanup candidates = %#v, want the superseded replacement", candidates)
	}

	// The abandoned target holds nothing anyone depends on.
	sole, err := f.repos.Replacements.CountAbandonedTargetSoleCopies(ctx, first.TargetDataSetID)
	if err != nil {
		t.Fatalf("CountAbandonedTargetSoleCopies: %v", err)
	}
	if sole != 0 {
		t.Fatalf("sole copies = %d, want none on an unused target", sole)
	}
	if err := f.repos.Replacements.RetireAbandonedTarget(ctx, first.ID); err != nil {
		t.Fatalf("RetireAbandonedTarget: %v", err)
	}

	abandoned, err := f.repos.Uploads.GetDataSetBindingByID(ctx, first.TargetDataSetID)
	if err != nil || abandoned == nil || abandoned.Status != model.StorageDataSetStatusRetired {
		t.Fatalf("abandoned target = %#v err=%v, want retired", abandoned, err)
	}
	source, err := f.repos.Uploads.GetDataSetBindingByID(ctx, f.source.ID)
	if err != nil || source == nil || source.Status == model.StorageDataSetStatusRetired || !source.IsCurrent {
		t.Fatalf("source = %#v err=%v, want it untouched and still current", source, err)
	}
	// The replacement record stays superseded; cleanup never rewrites it.
	got, err := f.repos.Replacements.GetByID(ctx, first.ID)
	if err != nil || got == nil || got.Status != storagereplacement.StatusSuperseded {
		t.Fatalf("replacement = %#v err=%v, want it left superseded", got, err)
	}
	if second.ID == first.ID {
		t.Fatal("the later confirmation reused the superseded record")
	}
}

func seedClaimedReplacementTask(t *testing.T, f *replacementFixture, replacementID int64) *model.Task {
	t.Helper()
	ctx := context.Background()
	task := storagereplacement.NewMigrateTask(replacementID, f.bucket.ID, "", 5, time.Now())
	task.IdempotencyKey += ":acquire-test"
	if err := f.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create coordinator task: %v", err)
	}
	claimed, err := f.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimReady = %#v err=%v", claimed, err)
	}
	return claimed
}

// Content the retiring generation is still writing is not "never stored". A
// cancelled item never migrates, the write then commits, and the coverage gate
// blocks forever on content no item exists for.
func TestStorageReplacementRepo_InFlightSourceCopyIsParkedNotCancelled(t *testing.T) {
	f := newReplacementFixture(t, "replacement-inflight-acquire", "01J000000000000000000RPL19")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")

	inFlightVersion := newObjectVersion(f.bucket.ID, "pending.txt", "01J000000000000000000RPL20", 10)
	inFlightVersion.Checksum = "pending-checksum"
	if _, err := f.repos.Objects.CreateVersionAndSetCurrent(ctx, inFlightVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	inFlight := startCopyHealthUpload(t, f.repos, f.bucket.ID, inFlightVersion.VersionID, 10, inFlightVersion.Checksum, 1)
	if err := f.repos.Uploads.CreateUploadCopiesForBindings(ctx, inFlight.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: f.source.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       f.source.ProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}

	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 10); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}

	var itemID int64
	if err := f.db.NewRaw(`SELECT id FROM storage_replacement_items WHERE replacement_id = ? AND upload_id = ?`,
		row.ID, inFlight.ID).Scan(ctx, &itemID); err != nil {
		t.Fatalf("select in-flight item: %v", err)
	}
	task := seedClaimedReplacementTask(t, f, row.ID)
	if _, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID,
		ItemID:        itemID,
		TaskID:        task.ID,
		TaskClaimedAt: *task.ClaimedAt,
	}); !errors.Is(err, storagereplacement.ErrItemDeferred) {
		t.Fatalf("AcquireItem while the source is still writing = %v, want ErrItemDeferred", err)
	}

	// Parked, not cancelled: it must come back once the source finishes.
	var status storagereplacement.ItemStatus
	if err := f.db.NewRaw(`SELECT status FROM storage_replacement_items WHERE id = ?`, itemID).Scan(ctx, &status); err != nil {
		t.Fatalf("select item status: %v", err)
	}
	if status != storagereplacement.ItemStatusWaitingSource {
		t.Fatalf("item status = %s, want waiting_source so the coordinator revisits it", status)
	}
	gate, err := f.repos.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate: %v", err)
	}
	if gate.WaitingItems == 0 {
		t.Fatalf("gate = %#v, want the parked item to keep retirement open", gate)
	}
}

// An upload the retiring generation never stored, and never will, owes the
// target nothing.
func TestStorageReplacementRepo_ItemWithNoSourceCopyIsCancelled(t *testing.T) {
	f := newReplacementFixture(t, "replacement-nosource", "01J000000000000000000RPL21")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")

	elsewhereVersion := newObjectVersion(f.bucket.ID, "elsewhere.txt", "01J000000000000000000RPL22", 10)
	elsewhereVersion.Checksum = "elsewhere-checksum"
	if _, err := f.repos.Objects.CreateVersionAndSetCurrent(ctx, elsewhereVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	elsewhere := startCopyHealthUpload(t, f.repos, f.bucket.ID, elsewhereVersion.VersionID, 10, elsewhereVersion.Checksum, 1)

	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 10); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}
	var itemID int64
	if err := f.db.NewRaw(`SELECT id FROM storage_replacement_items WHERE replacement_id = ? AND upload_id = ?`,
		row.ID, elsewhere.ID).Scan(ctx, &itemID); err != nil {
		t.Fatalf("select item: %v", err)
	}
	task := seedClaimedReplacementTask(t, f, row.ID)
	if _, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID,
		ItemID:        itemID,
		TaskID:        task.ID,
		TaskClaimedAt: *task.ClaimedAt,
	}); !errors.Is(err, storagereplacement.ErrItemCancelled) {
		t.Fatalf("AcquireItem = %v, want ErrItemCancelled", err)
	}
	var status storagereplacement.ItemStatus
	if err := f.db.NewRaw(`SELECT status FROM storage_replacement_items WHERE id = ?`, itemID).Scan(ctx, &status); err != nil {
		t.Fatalf("select item status: %v", err)
	}
	if status != storagereplacement.ItemStatusCancelled {
		t.Fatalf("item status = %s, want cancelled", status)
	}
}

// A generation some unfinished replacement is migrating into cannot become a
// source: that replacement needs it to keep owning the slot.
func TestStorageReplacementRepo_AuthorizeRejectsReplacingALiveTarget(t *testing.T) {
	f := newReplacementFixture(t, "replacement-chain", "01J000000000000000000RPL23")
	ctx := context.Background()
	first := f.authorize(t, "202")
	target := f.readyTarget(t, first, "2002")
	if err := f.repos.Replacements.Activate(ctx, first.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := f.repos.Replacements.MarkFailed(ctx, first.ID, nil, "migration exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	if _, _, err := f.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID:         f.bucket.ID,
		SourceDataSetID:  target.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "404"),
		ClientRequestID:  "replace-live-target",
		MaxRetries:       5,
	}); !errors.Is(err, storagereplacement.ErrActiveReplacement) {
		t.Fatalf("replacing a live replacement's target = %v, want ErrActiveReplacement", err)
	}
}

// After an activation a failure bound to the retiring generation must land on
// that generation. Resolving through the slot would either mark the replacement
// copy failed or update nothing, leaving the original stuck mid-transfer and
// holding retirement open.
func TestStorageUploadRepo_CopyFailureFollowsTheRecordedCopy(t *testing.T) {
	f := newReplacementFixture(t, "replacement-failure", "01J000000000000000000RPL24")
	ctx := context.Background()
	sourceCopy, err := f.repos.Uploads.GetUploadCopyForDataSet(ctx, f.upload.ID, f.source.ID)
	if err != nil || sourceCopy == nil {
		t.Fatalf("GetUploadCopyForDataSet = %#v err=%v", sourceCopy, err)
	}
	mustExec(t, f.db, `UPDATE storage_upload_copies SET status = ? WHERE id = ?`,
		model.StorageUploadCopyStatusPieceReady, sourceCopy.ID)

	row := f.authorize(t, "202")
	target := f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := f.repos.Uploads.CreateUploadCopiesForBindings(ctx, f.upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: target.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodPeerPull,
		ProviderID:       target.ProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings target: %v", err)
	}
	targetCopy, err := f.repos.Uploads.GetUploadCopyForDataSet(ctx, f.upload.ID, target.ID)
	if err != nil || targetCopy == nil {
		t.Fatalf("GetUploadCopyForDataSet target = %#v err=%v", targetCopy, err)
	}

	if err := f.repos.Uploads.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{
		StorageUploadCopyID: sourceCopy.ID,
		UploadID:            f.upload.ID,
		CopyIndex:           0,
		LastError:           "ingress store: provider rejected the piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyFailed: %v", err)
	}
	gotSource, err := f.repos.Uploads.GetUploadCopyByID(ctx, sourceCopy.ID)
	if err != nil || gotSource == nil || gotSource.Status != model.StorageUploadCopyStatusFailed {
		t.Fatalf("retiring copy = %#v err=%v, want failed", gotSource, err)
	}
	gotTarget, err := f.repos.Uploads.GetUploadCopyByID(ctx, targetCopy.ID)
	if err != nil || gotTarget == nil || gotTarget.Status != model.StorageUploadCopyStatusPending {
		t.Fatalf("replacement copy = %#v err=%v, want it untouched", gotTarget, err)
	}
}

// The dedicated retry exists to resume work the worker gave up on, and the
// worker gives up by driving the coordinator task to a terminal status:
// migration exhausts its retries, cleanup fails outright. Moving the record
// back to a working status without reviving that task leaves the replacement
// permanently stuck — displayed as in progress, with the retry hidden because
// the status is no longer retryable, and nothing queued to make progress.
func TestStorageReplacementRepo_RetryMakesTheCoordinatorClaimableAgain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		markStuck  func(t *testing.T, f *replacementFixture, row *storagereplacement.Replacement)
		taskStatus model.TaskStatus
		taskType   model.TaskType
		key        func(int64) string
	}{
		{
			name: "migration exhausted its retries",
			markStuck: func(t *testing.T, f *replacementFixture, row *storagereplacement.Replacement) {
				if err := f.repos.Replacements.MarkFailed(context.Background(), row.ID, nil, "copy replacement item: exhausted"); err != nil {
					t.Fatalf("MarkFailed: %v", err)
				}
			},
			taskStatus: model.TaskStatusExhausted,
			taskType:   model.TaskTypeUpload,
			key:        storagereplacement.MigrateTaskKey,
		},
		{
			name: "cleanup could not end the service",
			markStuck: func(t *testing.T, f *replacementFixture, row *storagereplacement.Replacement) {
				if err := f.repos.Replacements.BeginRetirement(context.Background(), row.ID); err != nil {
					t.Fatalf("BeginRetirement: %v", err)
				}
				if err := f.repos.Replacements.MarkCleanupAttention(context.Background(), row.ID, "payment debt"); err != nil {
					t.Fatalf("MarkCleanupAttention: %v", err)
				}
			},
			taskStatus: model.TaskStatusFailed,
			taskType:   model.TaskTypeStorageCleanup,
			key:        storagereplacement.RetireTaskKey,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementFixture(t, "retry-revives-"+model.NewVersionID()[:8], model.NewVersionID())
			ctx := context.Background()
			row := f.authorize(t, "202")
			f.readyTarget(t, row, "2002")
			if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
				t.Fatalf("Activate: %v", err)
			}
			tc.markStuck(t, f, row)

			// Put the coordinator where the worker leaves it on the way in.
			key := tc.key(row.ID)
			if _, err := f.repos.Tasks.EnsureRecurring(ctx, storagereplacement.NewRetireTask(row.ID, f.bucket.ID, 5, time.Now())); err != nil {
				t.Fatalf("seed retire coordinator: %v", err)
			}
			if _, err := f.db.NewUpdate().
				Model((*model.Task)(nil)).
				Set("status = ?", tc.taskStatus).
				Set("retry_count = 5").
				Where("idempotency_key = ?", key).
				Exec(ctx); err != nil {
				t.Fatalf("mark coordinator %s: %v", tc.taskStatus, err)
			}

			if _, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: row.ID, MaxRetries: 5}); err != nil {
				t.Fatalf("Retry: %v", err)
			}

			// The only assertion that matters: a worker can pick the work up.
			claimed, err := f.repos.Tasks.ClaimReady(ctx, tc.taskType, time.Minute)
			if err != nil {
				t.Fatalf("ClaimReady %s: %v", tc.taskType, err)
			}
			if claimed == nil || claimed.IdempotencyKey != key {
				t.Fatalf("claimed = %#v, want the resumed coordinator %s", claimed, key)
			}
			if claimed.RetryCount != 0 {
				t.Fatalf("retry count = %d, want the operator's retry to restore the budget", claimed.RetryCount)
			}
		})
	}
}

// Automatic recurrence must keep its own rule: a coordinator that gave up is
// not restarted just because recovery ran again.
func TestTaskRepo_StartupRecoveryDoesNotRestartAbandonedCoordinators(t *testing.T) {
	f := newReplacementFixture(t, "startup-vs-exhausted", model.NewVersionID())
	ctx := context.Background()
	row := f.authorize(t, "202")
	key := storagereplacement.MigrateTaskKey(row.ID)
	if _, err := f.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusExhausted).
		Where("idempotency_key = ?", key).
		Exec(ctx); err != nil {
		t.Fatalf("mark coordinator exhausted: %v", err)
	}
	if _, err := f.repos.Tasks.EnsureRecurring(ctx, storagereplacement.NewMigrateTask(row.ID, f.bucket.ID, "", 5, time.Now())); err != nil {
		t.Fatalf("EnsureRecurring: %v", err)
	}
	claimed, err := f.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed != nil {
		t.Fatal("startup recovery restarted a coordinator that had given up and needs an operator")
	}
}

// Abandoned-target cleanup cannot use the Data Sets retry: the replacement is
// superseded. ResumeCoordinator is the only way a failed leftover terminator
// becomes claimable again after a restart.
func TestTaskRepo_ResumeCoordinatorRevivesAbandonedTargetCleanup(t *testing.T) {
	f := newReplacementFixture(t, "resume-abandoned-cleanup", model.NewVersionID())
	ctx := context.Background()
	task := storagereplacement.NewAbandonedTargetTask(11, f.bucket.ID, 5, time.Now())
	if err := f.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).
		Set("retry_count = 5").
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if _, err := f.repos.Tasks.EnsureRecurring(ctx, storagereplacement.NewAbandonedTargetTask(11, f.bucket.ID, 5, time.Now())); err != nil {
		t.Fatalf("EnsureRecurring: %v", err)
	}
	claimed, err := f.repos.Tasks.ClaimReady(ctx, model.TaskTypeStorageCleanup, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady after EnsureRecurring: %v", err)
	}
	if claimed != nil {
		t.Fatal("automatic recurrence revived abandoned cleanup that had failed")
	}
	if _, err := f.repos.Tasks.ResumeCoordinator(ctx, storagereplacement.NewAbandonedTargetTask(11, f.bucket.ID, 5, time.Now())); err != nil {
		t.Fatalf("ResumeCoordinator: %v", err)
	}
	claimed, err = f.repos.Tasks.ClaimReady(ctx, model.TaskTypeStorageCleanup, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady after ResumeCoordinator: %v", err)
	}
	if claimed == nil || claimed.IdempotencyKey != storagereplacement.AbandonedTargetTaskKey(11) {
		t.Fatalf("claimed = %#v, want the abandoned-target coordinator", claimed)
	}
}

// Mutual exclusion is per copy row. A pending item, or a running item that has
// not attached its target copy yet, must not make every other upload on the
// replica stand down.
func TestStorageReplacementRepo_HeldItemCopyIDIsTheRunningTargetCopy(t *testing.T) {
	f := newReplacementFixture(t, "replacement-held-copy", "01J000000000000000000RPL21")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 10); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}

	held, err := f.repos.Replacements.HeldItemCopyID(ctx, row.ID)
	if err != nil || held != 0 {
		t.Fatalf("HeldItemCopyID before claim = %d err=%v, want 0", held, err)
	}

	item, err := f.repos.Replacements.NextExecutableItem(ctx, row.ID)
	if err != nil || item == nil {
		t.Fatalf("NextExecutableItem = %#v err=%v", item, err)
	}
	copyRow, err := f.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: row.ID,
		ItemID:        item.ID,
		UploadID:      item.UploadID,
	})
	if err != nil || copyRow == nil {
		t.Fatalf("AttachTargetCopy = %#v err=%v", copyRow, err)
	}
	held, err = f.repos.Replacements.HeldItemCopyID(ctx, row.ID)
	if err != nil || held != 0 {
		t.Fatalf("HeldItemCopyID after attach = %d err=%v, want 0 until the item is running", held, err)
	}

	mustExec(t, f.db, `UPDATE storage_replacement_items SET status = ? WHERE id = ?`,
		storagereplacement.ItemStatusRunning, item.ID)
	held, err = f.repos.Replacements.HeldItemCopyID(ctx, row.ID)
	if err != nil || held != copyRow.ID {
		t.Fatalf("HeldItemCopyID while running = %d err=%v, want copy %d", held, err, copyRow.ID)
	}

	if err := f.repos.Replacements.MarkItemCopied(ctx, item.ID); err != nil {
		t.Fatalf("MarkItemCopied: %v", err)
	}
	held, err = f.repos.Replacements.HeldItemCopyID(ctx, row.ID)
	if err != nil || held != 0 {
		t.Fatalf("HeldItemCopyID after copy = %d err=%v, want 0", held, err)
	}
}
