package repository_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
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

func TestStorageReplacementRepo_DurableItemClaimFencesRetriesAndRecovery(t *testing.T) {
	f := newReplacementFixture(t, "replacement-item-claim", "01J000000000000000ITEMQ01")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, done, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 200, 1); err != nil || !done {
		t.Fatalf("SeedMigrationBatchWithBudget done=%v err=%v", done, err)
	}

	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil || item.MaxRetries == nil || *item.MaxRetries != 1 {
		t.Fatalf("first item claim = %#v err=%v", item, err)
	}
	firstToken := storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
	staleToken := firstToken
	staleToken.ClaimedAt = staleToken.ClaimedAt.Add(-time.Second)
	if err := f.repos.Replacements.RenewReplacementItemLease(ctx, staleToken, time.Minute); !errors.Is(err, repository.ErrItemClaimLost) {
		t.Fatalf("stale renewal error = %v, want ErrItemClaimLost", err)
	}

	status, err := f.repos.Replacements.RetryReplacementItemClaim(ctx, firstToken, time.Now(), "temporary failure")
	if err != nil || status != storagereplacement.ItemStatusRetrying {
		t.Fatalf("first retry status=%s err=%v", status, err)
	}
	item, err = f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil || item.RetryCount != 1 {
		t.Fatalf("retry item claim = %#v err=%v", item, err)
	}
	secondToken := storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
	status, err = f.repos.Replacements.RetryReplacementItemClaim(ctx, secondToken, time.Now(), "still failing")
	if err != nil || status != storagereplacement.ItemStatusFailed {
		t.Fatalf("exhausted retry status=%s err=%v", status, err)
	}
	if err := f.repos.Replacements.CompleteReplacementItemClaim(ctx, firstToken); !errors.Is(err, repository.ErrItemClaimLost) {
		t.Fatalf("stale completion error = %v, want ErrItemClaimLost", err)
	}

	progresses, err := f.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses: %v", err)
	}
	if progress := progresses[row.ID]; progress.ItemsFailed != 1 || progress.ItemsProcessed != 0 || progress.Percent == nil || *progress.Percent != 0 {
		t.Fatalf("progress after exhaustion = %#v", progress)
	}
	if err := f.repos.Replacements.MarkFailed(ctx, row.ID, nil, "item retries exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if _, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{
		ReplacementID:  row.ID,
		MaxRetries:     5,
		ItemMaxRetries: 3,
	}); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	item, err = f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.MaxRetries == nil || *item.MaxRetries != 3 || item.RetryCount != 0 {
		t.Fatalf("operator-retried item = %#v err=%v", item, err)
	}
}

func TestStorageReplacementRepo_FailedReplacementReleasesClaimWithoutCancellingWork(t *testing.T) {
	f := newReplacementFixture(t, "replacement-item-failed-release", "01J000000000000000ITEMQ16")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, done, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 200, 3); err != nil || !done {
		t.Fatalf("SeedMigrationBatchWithBudget done=%v err=%v", done, err)
	}

	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("ClaimReadyReplacementItem = %#v err=%v", item, err)
	}
	if err := f.repos.Replacements.MarkFailed(ctx, row.ID, nil, "coordinator exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if _, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID,
		ItemID:        item.ID,
		ItemClaimedAt: *item.ClaimedAt,
	}); !errors.Is(err, storagereplacement.ErrItemDeferred) {
		t.Fatalf("AcquireItem after replacement failure = %v, want ErrItemDeferred", err)
	}

	var released storagereplacement.Item
	if err := f.db.NewSelect().Model(&released).Where("id = ?", item.ID).Scan(ctx); err != nil {
		t.Fatalf("load released item: %v", err)
	}
	if released.Status != storagereplacement.ItemStatusPending || released.ClaimedAt != nil || released.LeaseUntil != nil {
		t.Fatalf("released item = %#v, want pending without a claim", released)
	}
	if _, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{
		ReplacementID:  row.ID,
		MaxRetries:     5,
		ItemMaxRetries: 3,
	}); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	reclaimed, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || reclaimed == nil || reclaimed.ID != item.ID {
		t.Fatalf("reclaimed item = %#v err=%v, want item %d", reclaimed, err, item.ID)
	}
}

func TestStorageReplacementRepo_GlobalClaimsAreFairAcrossReplacements(t *testing.T) {
	f := newReplacementFixture(t, "replacement-item-fairness", "01J000000000000000ITEMQ06")
	ctx := context.Background()
	first := f.authorize(t, "202")
	f.readyTarget(t, first, "2002")
	if err := f.repos.Replacements.Activate(ctx, first.ID); err != nil {
		t.Fatalf("Activate first: %v", err)
	}

	secondSource, err := f.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: f.bucket.ID, ProviderID: onChainID(t, "303"), CopyIndex: 1,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding second source: %v", err)
	}
	if err := f.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: secondSource.ID, DataSetID: onChainID(t, "3003"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady second source: %v", err)
	}
	second, _, err := f.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: f.bucket.ID, SourceDataSetID: secondSource.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "404"), ClientRequestID: "fairness-second", MaxRetries: 5,
	})
	if err != nil {
		t.Fatalf("Authorize second: %v", err)
	}
	if err := f.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: second.TargetDataSetID, DataSetID: onChainID(t, "4004"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady second target: %v", err)
	}
	if err := f.repos.Replacements.Activate(ctx, second.ID); err != nil {
		t.Fatalf("Activate second: %v", err)
	}

	extraUpload, err := f.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: f.bucket.ID, ContentSize: 1, Checksum: "fairness-extra", RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt extra: %v", err)
	}
	maxRetries := 5
	now := time.Now().Add(-time.Second)
	for _, replacementID := range []int64{first.ID, second.ID} {
		for _, uploadID := range []int64{f.upload.ID, extraUpload.ID} {
			if _, err := f.db.NewInsert().Model(&storagereplacement.Item{
				ReplacementID: replacementID, UploadID: uploadID,
				Status: storagereplacement.ItemStatusPending, ScheduledAt: now,
				MaxRetries: &maxRetries, CreatedAt: now, UpdatedAt: now,
			}).Exec(ctx); err != nil {
				t.Fatalf("insert fairness item: %v", err)
			}
		}
	}

	want := []int64{first.ID, second.ID, first.ID, second.ID}
	for i, replacementID := range want {
		item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
		if err != nil || item == nil {
			t.Fatalf("claim %d = %#v err=%v", i, item, err)
		}
		if item.ReplacementID != replacementID {
			t.Fatalf("claim %d replacement = %d, want %d; sequence must alternate while both have work", i, item.ReplacementID, replacementID)
		}
	}
}

func TestStorageReplacementRepo_WaitingSourcePreservesRetryBudgetAcrossLease(t *testing.T) {
	f := newReplacementFixture(t, "replacement-item-wait", "01J000000000000000ITEMQ02")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 200, 2); err != nil {
		t.Fatalf("SeedMigrationBatchWithBudget: %v", err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("ClaimReadyReplacementItem = %#v err=%v", item, err)
	}
	token := storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
	if err := f.repos.Replacements.WaitReplacementItemClaim(ctx, token, time.Now(), "no readable source"); err != nil {
		t.Fatalf("WaitReplacementItemClaim: %v", err)
	}
	item, err = f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.RetryCount != 0 {
		t.Fatalf("waiting-source reclaim = %#v err=%v, want retry count unchanged", item, err)
	}
}

func TestStorageReplacementRepo_CopiedItemResumesReadableSourceWaitWithoutWakingCoordinator(t *testing.T) {
	f := newReplacementFixture(t, "replacement-item-readable-again", "01J000000000000000ITEMQ20")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, done, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 200, 2); err != nil || !done {
		t.Fatalf("SeedMigrationBatchWithBudget done=%v err=%v", done, err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("ClaimReadyReplacementItem = %#v err=%v", item, err)
	}
	if err := f.repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonReadableSource); err != nil {
		t.Fatalf("MarkWaiting: %v", err)
	}
	waiting, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || waiting == nil {
		t.Fatalf("GetByID waiting = %#v err=%v", waiting, err)
	}

	task, err := f.repos.Tasks.GetByIdempotencyKey(ctx, storagereplacement.MigrateTaskKey(row.ID))
	if err != nil || task == nil {
		t.Fatalf("GetByIdempotencyKey = %#v err=%v", task, err)
	}
	nextPoll := time.Now().Add(time.Hour).Truncate(time.Second)
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).
		Set("scheduled_at = ?", nextPoll).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("delay coordinator: %v", err)
	}

	token := storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
	if err := f.repos.Replacements.CompleteReplacementItemClaim(ctx, token); err != nil {
		t.Fatalf("CompleteReplacementItemClaim: %v", err)
	}
	resumed, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || resumed == nil || resumed.Status != storagereplacement.StatusMigrating || resumed.WaitReason != nil {
		t.Fatalf("replacement after readable copy = %#v err=%v, want migrating without a wait reason", resumed, err)
	}
	if resumed.StateVersion != waiting.StateVersion+1 {
		t.Fatalf("state version after readable copy = %d, want %d", resumed.StateVersion, waiting.StateVersion+1)
	}
	unchangedTask, err := f.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || unchangedTask == nil || !unchangedTask.ScheduledAt.Equal(nextPoll) {
		t.Fatalf("coordinator after item completion = %#v err=%v, want poll time %s unchanged", unchangedTask, err, nextPoll)
	}
	execution, err := f.repos.Replacements.ReplacementExecution(ctx, row.ID)
	if err != nil || !execution.SeedingComplete || execution.ItemsTotal != 1 || execution.ItemsCopied != 1 ||
		execution.HasPending || execution.HasActive || execution.HasRetrying || execution.HasWaitingSource || execution.HasFailed {
		t.Fatalf("replacement execution after copy = %#v err=%v", execution, err)
	}
}

func TestStorageReplacementRepo_ExpiredLeaseIsRecoveredAndFenced(t *testing.T) {
	f := newReplacementFixture(t, "replacement-item-expiry", "01J000000000000000ITEMQ03")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 200, 2); err != nil {
		t.Fatalf("SeedMigrationBatchWithBudget: %v", err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("first claim = %#v err=%v", item, err)
	}
	stale := storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
	mustExec(t, f.db, `UPDATE storage_replacement_items SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), item.ID)
	if err := f.repos.Replacements.CancelReplacementItemClaim(ctx, stale); !errors.Is(err, repository.ErrItemClaimLost) {
		t.Fatalf("expired cancellation error = %v, want ErrItemClaimLost", err)
	}
	if _, err := f.repos.Replacements.RetryReplacementItemClaim(
		ctx, stale, time.Now(), "late retry",
	); !errors.Is(err, repository.ErrItemClaimLost) {
		t.Fatalf("expired retry transition error = %v, want ErrItemClaimLost", err)
	}

	reclaimed, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || reclaimed == nil || reclaimed.ClaimedAt == nil {
		t.Fatalf("directly reclaimed expired item = %#v err=%v", reclaimed, err)
	}
	if reclaimed.ClaimedAt.Equal(stale.ClaimedAt) {
		t.Fatal("reclaimed item reused the expired fencing token")
	}
	if err := f.repos.Replacements.CancelReplacementItemClaim(ctx, stale); !errors.Is(err, repository.ErrItemClaimLost) {
		t.Fatalf("stale cancellation error = %v, want ErrItemClaimLost", err)
	}
	var stillClaimed storagereplacement.Item
	if err := f.db.NewSelect().Model(&stillClaimed).Where("id = ?", reclaimed.ID).Scan(ctx); err != nil {
		t.Fatalf("reload reclaimed item: %v", err)
	}
	if stillClaimed.Status != storagereplacement.ItemStatusRunning || stillClaimed.ClaimedAt == nil ||
		!stillClaimed.ClaimedAt.Equal(*reclaimed.ClaimedAt) {
		t.Fatalf("stale cancellation changed the new claim: %#v", stillClaimed)
	}
	if err := f.repos.Replacements.CompleteReplacementItemClaim(ctx, stale); !errors.Is(err, repository.ErrItemClaimLost) {
		t.Fatalf("stale completion error = %v, want ErrItemClaimLost", err)
	}
	mustExec(t, f.db, `UPDATE storage_replacement_items SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), reclaimed.ID)
	released, err := f.repos.Replacements.ReleaseExpiredItemLeases(ctx)
	if err != nil || released != 1 {
		t.Fatalf("ReleaseExpiredItemLeases count=%d err=%v", released, err)
	}
}

func TestStorageReplacementRepo_RunningClaimIsVisibleBeforeTargetCopyAttach(t *testing.T) {
	f := newReplacementFixture(t, "replacement-pre-attach-claim", "01J000000000000000ITEMQ19")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 200, 2); err != nil {
		t.Fatalf("SeedMigrationBatchWithBudget: %v", err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil || item.TargetCopyID != nil {
		t.Fatalf("pre-attach claim = %#v err=%v", item, err)
	}

	claim, err := f.repos.Replacements.RunningReplacementItemClaimForUpload(ctx, row.ID, f.upload.ID)
	if err != nil || claim == nil {
		t.Fatalf("RunningReplacementItemClaimForUpload = %#v err=%v", claim, err)
	}
	if claim.ItemID != item.ID || !claim.ClaimedAt.Equal(*item.ClaimedAt) {
		t.Fatalf("claim = %#v, want item %d claimed at %s", claim, item.ID, item.ClaimedAt)
	}
}

func TestStorageReplacementRepo_StaleWorkerCannotPauseResumedMigration(t *testing.T) {
	f := newReplacementFixture(t, "replacement-pause-version", "01J000000000000000ITEMQ04")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	migrating, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || migrating == nil {
		t.Fatalf("GetByID migrating = %#v err=%v", migrating, err)
	}
	staleVersion := migrating.StateVersion
	if err := f.repos.Replacements.PauseMigration(ctx, row.ID, staleVersion, storagereplacement.WaitReasonTarget); err != nil {
		t.Fatalf("PauseMigration: %v", err)
	}
	if err := f.repos.Replacements.MarkMigrating(ctx, row.ID); err != nil {
		t.Fatalf("MarkMigrating: %v", err)
	}
	if err := f.repos.Replacements.PauseMigration(ctx, row.ID, staleVersion, storagereplacement.WaitReasonTarget); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("stale PauseMigration error = %v, want ErrConflict", err)
	}
	current, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || current == nil || current.Status != storagereplacement.StatusMigrating {
		t.Fatalf("replacement after stale pause = %#v err=%v, want migrating", current, err)
	}
}

func TestStorageReplacementRepo_TargetPauseSupersedesReadableSourceWait(t *testing.T) {
	f := newReplacementFixture(t, "replacement-pause-source-wait", "01J000000000000000ITEMQ14")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := f.repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonReadableSource); err != nil {
		t.Fatalf("MarkWaiting: %v", err)
	}
	waiting, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || waiting == nil {
		t.Fatalf("GetByID waiting = %#v err=%v", waiting, err)
	}
	if err := f.repos.Replacements.PauseMigration(
		ctx, row.ID, waiting.StateVersion, storagereplacement.WaitReasonTarget,
	); err != nil {
		t.Fatalf("PauseMigration from readable-source wait: %v", err)
	}
	paused, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || paused == nil || paused.Status != storagereplacement.StatusWaiting ||
		paused.WaitReason == nil || *paused.WaitReason != storagereplacement.WaitReasonTarget {
		t.Fatalf("paused replacement = %#v err=%v, want waiting/target", paused, err)
	}
}

func TestStorageReplacementRepo_ProgressPhaseFollowsTargetGenerationOwnership(t *testing.T) {
	f := newReplacementFixture(t, "replacement-progress-phase", "01J000000000000000ITEMQ15")
	ctx := context.Background()
	row := f.authorize(t, "202")
	if err := f.repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonFunding); err != nil {
		t.Fatalf("MarkWaiting before activation: %v", err)
	}
	progresses, err := f.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses before activation: %v", err)
	}
	if progress := progresses[row.ID]; progress.Phase != storagereplacement.PhasePrepare {
		t.Fatalf("progress before activation = %#v, want prepare phase", progress)
	}

	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := f.repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonReadableSource); err != nil {
		t.Fatalf("MarkWaiting after activation: %v", err)
	}
	progresses, err = f.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses after activation: %v", err)
	}
	if progress := progresses[row.ID]; progress.Phase != storagereplacement.PhaseMigrate {
		t.Fatalf("progress after activation = %#v, want migrate phase", progress)
	}
}

func TestStorageReplacementRepo_NextRetryAtReportsOnlyFutureRetries(t *testing.T) {
	f := newReplacementFixture(t, "replacement-progress-retry", "01J000000000000000ITEMQ21")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, done, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 200, 2); err != nil || !done {
		t.Fatalf("SeedMigrationBatchWithBudget done=%v err=%v", done, err)
	}

	var waitingItemID int64
	if err := f.db.NewRaw(`SELECT id FROM storage_replacement_items WHERE replacement_id = ?`, row.ID).
		Scan(ctx, &waitingItemID); err != nil {
		t.Fatalf("select waiting item: %v", err)
	}
	extraUpload, err := f.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: f.bucket.ID, ContentSize: 1, Checksum: "replacement-progress-retry-extra", RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	waitingCheck := time.Now().Add(time.Minute).Truncate(time.Second)
	retryAt := waitingCheck.Add(time.Hour)
	maxRetries := 2
	mustExec(t, f.db, `UPDATE storage_replacement_items SET status = ?, scheduled_at = ? WHERE id = ?`,
		storagereplacement.ItemStatusWaitingSource, waitingCheck, waitingItemID)
	if _, err := f.db.NewInsert().Model(&storagereplacement.Item{
		ReplacementID: row.ID,
		UploadID:      extraUpload.ID,
		Status:        storagereplacement.ItemStatusRetrying,
		ScheduledAt:   retryAt,
		MaxRetries:    &maxRetries,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}).Exec(ctx); err != nil {
		t.Fatalf("insert retrying item: %v", err)
	}
	mustExec(t, f.db, `UPDATE storage_replacements SET items_total = 2 WHERE id = ?`, row.ID)

	progresses, err := f.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses: %v", err)
	}
	progress := progresses[row.ID]
	if progress.NextRetryAt == nil || !progress.NextRetryAt.Equal(retryAt) {
		t.Fatalf("next retry = %v, want retrying item at %s rather than readable-source check at %s", progress.NextRetryAt, retryAt, waitingCheck)
	}
	execution, err := f.repos.Replacements.ReplacementExecution(ctx, row.ID)
	if err != nil || !execution.HasRetrying || !execution.HasWaitingSource {
		t.Fatalf("replacement execution = %#v err=%v, want retrying and readable-source work", execution, err)
	}
	pastRetry := time.Now().Add(-time.Minute).Truncate(time.Second)
	mustExec(t, f.db, `UPDATE storage_replacement_items SET status = ?, scheduled_at = ? WHERE id = ?`,
		storagereplacement.ItemStatusRetrying, pastRetry, waitingItemID)
	progresses, err = f.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses with overdue retry: %v", err)
	}
	if progress := progresses[row.ID]; progress.NextRetryAt == nil || !progress.NextRetryAt.Equal(retryAt) {
		t.Fatalf("next retry = %v, want future retry at %s rather than overdue retry at %s", progress.NextRetryAt, retryAt, pastRetry)
	}
	mustExec(t, f.db, `UPDATE storage_replacement_items SET scheduled_at = ? WHERE upload_id = ?`,
		pastRetry, extraUpload.ID)
	progresses, err = f.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses with only overdue retries: %v", err)
	}
	if progress := progresses[row.ID]; progress.NextRetryAt != nil {
		t.Fatalf("next retry = %v, want no future retry", progress.NextRetryAt)
	}
}

func TestStorageReplacementRepo_ProgressBucketsCommitAttentionExclusively(t *testing.T) {
	f := newReplacementFixture(t, "replacement-progress-attention", "01J000000000000000ITEMQ22")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, done, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 2); err != nil || !done {
		t.Fatalf("SeedMigrationBatchWithBudget done=%v err=%v", done, err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("ClaimReadyReplacementItem = %#v err=%v", item, err)
	}
	snapshot, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID, ItemID: item.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || snapshot == nil {
		t.Fatalf("AcquireItem = %#v err=%v", snapshot, err)
	}
	copyRow, err := f.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: row.ID, ItemID: item.ID, UploadID: snapshot.Upload.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || copyRow == nil {
		t.Fatalf("AttachTargetCopy = %#v err=%v", copyRow, err)
	}
	mustExec(t, f.db, `UPDATE storage_replacement_items
		SET status = ?, claimed_at = NULL, lease_until = NULL WHERE id = ?`,
		storagereplacement.ItemStatusPending, item.ID)
	mustExec(t, f.db, `UPDATE storage_upload_copies
		SET status = ?, commit_attempt_id = 'attempt-progress', commit_attempted_at = CURRENT_TIMESTAMP
		WHERE id = ?`, model.StorageUploadCopyStatusCommitting, copyRow.ID)

	progresses, err := f.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses active: %v", err)
	}
	active := progresses[row.ID]
	if active.ItemsActive != 1 || active.ItemsPending != 0 || active.ItemsAttention != 0 {
		t.Fatalf("active progress = %#v, want one exclusive active item", active)
	}

	mustExec(t, f.db, `UPDATE storage_replacement_items SET status = ? WHERE id = ?`,
		storagereplacement.ItemStatusFailed, item.ID)
	mustExec(t, f.db, `UPDATE storage_upload_copies
		SET commit_attention_code = 'attempt_only_ambiguous', commit_attention_at = CURRENT_TIMESTAMP
		WHERE id = ?`, copyRow.ID)
	progresses, err = f.repos.Replacements.ReplacementProgresses(ctx, []int64{row.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses attention: %v", err)
	}
	attention := progresses[row.ID]
	if attention.ItemsAttention != 1 || attention.ItemsActive != 0 || attention.ItemsPending != 0 ||
		attention.ItemsRetrying != 0 || attention.ItemsWaitingSource != 0 || attention.ItemsFailed != 0 {
		t.Fatalf("attention progress = %#v, want one mutually exclusive attention item", attention)
	}
	if attention.ItemsProcessed != 0 || attention.ItemsNoLongerNeeded != 0 ||
		attention.Percent == nil || *attention.Percent != 0 {
		t.Fatalf("attention progress accounting = %#v, want outstanding item at 0%%", attention)
	}
}

func TestStorageReplacementRepo_TerminalReplacementClaimsOnlyDurableCommitWork(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       storagereplacement.Status
		capacityWait bool
		attempted    bool
		confirm      bool
	}{
		{name: "failed reservation is released", status: storagereplacement.StatusFailed},
		{name: "failed ready-only reservation is released", status: storagereplacement.StatusFailed, capacityWait: true},
		{name: "failed submission is recoverable", status: storagereplacement.StatusFailed, attempted: true},
		{name: "superseded submission is confirmed", status: storagereplacement.StatusSuperseded, attempted: true},
		{name: "superseded submission settles without live owner", status: storagereplacement.StatusSuperseded, attempted: true, confirm: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementFixture(t, "replacement-terminal-"+strings.ReplaceAll(tc.name, " ", "-"), "01J000000000000000ITEMQ23")
			ctx := context.Background()
			row := f.authorize(t, "202")
			f.readyTarget(t, row, "2002")
			if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
				t.Fatalf("Activate: %v", err)
			}
			if _, done, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 2); err != nil || !done {
				t.Fatalf("SeedMigrationBatchWithBudget done=%v err=%v", done, err)
			}
			item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
			if err != nil || item == nil || item.ClaimedAt == nil {
				t.Fatalf("initial claim = %#v err=%v", item, err)
			}
			snapshot, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
				ReplacementID: row.ID, ItemID: item.ID, ItemClaimedAt: *item.ClaimedAt,
			})
			if err != nil || snapshot == nil {
				t.Fatalf("AcquireItem = %#v err=%v", snapshot, err)
			}
			copyRow, err := f.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
				ReplacementID: row.ID, ItemID: item.ID, UploadID: snapshot.Upload.ID, ItemClaimedAt: *item.ClaimedAt,
			})
			if err != nil || copyRow == nil {
				t.Fatalf("AttachTargetCopy = %#v err=%v", copyRow, err)
			}
			if snapshot.Upload.PieceCID == nil {
				t.Fatal("fixture upload has no piece CID")
			}
			if err := f.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
				StorageUploadCopyID: copyRow.ID, RequireEligibleCopy: true,
				UploadID: snapshot.Upload.ID, CopyIndex: copyRow.CopyIndex,
				PieceCID: *snapshot.Upload.PieceCID, RetrievalURL: "https://target.example/piece",
			}); err != nil {
				t.Fatalf("MarkUploadCopyPieceReady: %v", err)
			}
			identity := storagecommit.CopyIdentity{
				StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
				CopyIndex: copyRow.CopyIndex, StorageDataSetID: *copyRow.StorageDataSetID,
				RequireEligibleCopy: true,
			}
			if tc.capacityWait {
				capacityCopies := seedCommitCopies(t, f.db, f.bucket.ID, row.TargetDataSetID, storagecommit.MaxActiveAttemptsPerDataSet)
				for i := range capacityCopies {
					if _, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
						Copy: commitCopyIdentity(capacityCopies[i]), AttemptID: fmt.Sprintf("terminal-capacity-%d", i),
					}); err != nil {
						t.Fatalf("reserve terminal capacity %d: %v", i, err)
					}
				}
			}
			reservation, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
				Copy: identity, AttemptID: "terminal-attempt",
			})
			if err != nil {
				t.Fatalf("ReserveCommitAttempt: %v", err)
			}
			if tc.capacityWait && (reservation.State != storagecommit.ReservationWaiting || reservation.Copy.CommitAttemptID != nil) {
				t.Fatalf("terminal capacity reservation = %#v, want ready-only wait", reservation)
			}
			if tc.attempted {
				if _, err := f.repos.Uploads.MarkCommitAttempted(ctx, storagecommit.AttemptInput{
					Copy: identity, AttemptID: "terminal-attempt", ExtraDataHex: "abcd",
				}); err != nil {
					t.Fatalf("MarkCommitAttempted: %v", err)
				}
				mustExec(t, f.db, `DELETE FROM object_versions WHERE version_id = ?`, f.version.VersionID)
			}
			token := storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}
			if err := f.repos.Replacements.ReleaseReplacementItemClaim(ctx, token); err != nil {
				t.Fatalf("ReleaseReplacementItemClaim: %v", err)
			}
			mustExec(t, f.db, `UPDATE storage_replacements SET status = ?, wait_reason = NULL WHERE id = ?`, tc.status, row.ID)

			claimed, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
			if err != nil || claimed == nil || claimed.ID != item.ID || claimed.ClaimedAt == nil {
				t.Fatalf("terminal claim = %#v err=%v", claimed, err)
			}
			recovered, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
				ReplacementID: row.ID, ItemID: claimed.ID, ItemClaimedAt: *claimed.ClaimedAt,
			})
			if tc.capacityWait {
				if !errors.Is(err, storagereplacement.ErrItemCancelled) || recovered != nil {
					t.Fatalf("terminal ready-only settlement = %#v err=%v, want cancellation", recovered, err)
				}
				persisted, loadErr := f.repos.Uploads.GetUploadCopyByID(ctx, copyRow.ID)
				if loadErr != nil || persisted.CommitReadyAt != nil || persisted.CommitExtraDataHex != nil || persisted.CommitAttemptID != nil {
					t.Fatalf("terminal ready-only copy = %#v err=%v, want cleared reservation", persisted, loadErr)
				}
				return
			}
			if err != nil || recovered == nil || recovered.Replacement.Status != tc.status {
				t.Fatalf("terminal recovery snapshot = %#v err=%v", recovered, err)
			}
			if tc.confirm {
				pieceID := onChainID(t, "7001")
				if err := f.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
					StorageUploadCopyID: copyRow.ID, RequireEligibleCopy: true,
					UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
					PieceCID: *snapshot.Upload.PieceCID, PieceID: &pieceID,
					RetrievalURL: "https://target.example/piece", CommitAttemptID: "terminal-attempt",
				}); err != nil {
					t.Fatalf("MarkUploadCopyCommitted without live owner: %v", err)
				}
				return
			}
			if tc.attempted {
				recoveryToken := storagereplacement.ClaimToken{ItemID: claimed.ID, ClaimedAt: *claimed.ClaimedAt}
				if err := f.repos.Replacements.ReleaseReplacementItemClaim(ctx, recoveryToken); err != nil {
					t.Fatalf("release recovery claim: %v", err)
				}
				if err := f.repos.Uploads.MarkCommitAttention(ctx, storagecommit.AttentionInput{
					Copy: storagecommit.CopyIdentity{
						StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
						CopyIndex: copyRow.CopyIndex, StorageDataSetID: *copyRow.StorageDataSetID,
					},
					AttemptID: "terminal-attempt", Code: storagecommit.AttentionAttemptOnlyAmbiguous,
				}); err != nil {
					t.Fatalf("MarkCommitAttention without live owner: %v", err)
				}
				if err := f.repos.Uploads.ReleaseCommitAttention(ctx, storagecommit.ManualReleaseInput{
					CopyID: copyRow.ID, ExpectedAttemptID: "terminal-attempt", AcknowledgePossibleDuplicate: true,
				}); err != nil {
					t.Fatalf("ReleaseCommitAttention: %v", err)
				}
				var releasedStatus storagereplacement.ItemStatus
				if err := f.db.NewSelect().Model((*storagereplacement.Item)(nil)).
					Column("status").Where("id = ?", item.ID).Scan(ctx, &releasedStatus); err != nil {
					t.Fatalf("load released item: %v", err)
				}
				wantStatus := storagereplacement.ItemStatusFailed
				if tc.status == storagereplacement.StatusSuperseded {
					wantStatus = storagereplacement.ItemStatusCancelled
				}
				if releasedStatus != wantStatus {
					t.Fatalf("released item status = %s, want %s", releasedStatus, wantStatus)
				}
			}
		})
	}
}

func TestStorageReplacementRepo_FinalRetryClearsUnattemptedReservation(t *testing.T) {
	f := newReplacementFixture(t, "replacement-final-retry-fifo", "01J000000000000000ITEMQ24")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 0); err != nil {
		t.Fatalf("SeedMigrationBatchWithBudget: %v", err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("claim = %#v err=%v", item, err)
	}
	snapshot, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID, ItemID: item.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || snapshot == nil || snapshot.Upload.PieceCID == nil {
		t.Fatalf("AcquireItem = %#v err=%v", snapshot, err)
	}
	copyRow, err := f.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: row.ID, ItemID: item.ID, UploadID: snapshot.Upload.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || copyRow == nil {
		t.Fatalf("AttachTargetCopy = %#v err=%v", copyRow, err)
	}
	if err := f.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		StorageUploadCopyID: copyRow.ID, RequireEligibleCopy: true,
		UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
		PieceCID: *snapshot.Upload.PieceCID, RetrievalURL: "https://target.example/piece", CommitExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	targetIdentity := storagecommit.CopyIdentity{
		StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
		StorageDataSetID: row.TargetDataSetID, RequireEligibleCopy: true,
	}
	targetReservation, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
		Copy: targetIdentity, AttemptID: "target-unattempted",
	})
	if err != nil || targetReservation.State != storagecommit.ReservationAcquired ||
		targetReservation.Copy.CommitAttemptID == nil || targetReservation.Copy.CommitAttemptedAt != nil {
		t.Fatalf("target reservation = %#v err=%v, want unattempted token", targetReservation, err)
	}
	capacityCopies := seedCommitCopies(t, f.db, f.bucket.ID, row.TargetDataSetID, storagecommit.MaxActiveAttemptsPerDataSet-1)
	for i := range capacityCopies {
		if _, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
			Copy: commitCopyIdentity(capacityCopies[i]), AttemptID: fmt.Sprintf("retry-capacity-%d", i),
		}); err != nil {
			t.Fatalf("reserve capacity %d: %v", i, err)
		}
	}
	follower := seedCommitCopies(t, f.db, f.bucket.ID, row.TargetDataSetID, 1)[0]
	if followerReservation, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
		Copy: commitCopyIdentity(follower), AttemptID: "follower-waiting",
	}); err != nil || followerReservation.State != storagecommit.ReservationWaiting {
		t.Fatalf("follower reservation = %#v err=%v, want waiting", followerReservation, err)
	}
	status, err := f.repos.Replacements.RetryReplacementItemClaim(ctx, storagereplacement.ClaimToken{
		ItemID: item.ID, ClaimedAt: *item.ClaimedAt,
	}, time.Now(), "presign failed")
	if err != nil || status != storagereplacement.ItemStatusFailed {
		t.Fatalf("final retry status=%s err=%v, want failed", status, err)
	}
	persisted, err := f.repos.Uploads.GetUploadCopyByID(ctx, copyRow.ID)
	if err != nil || persisted.CommitReadyAt != nil || persisted.CommitExtraDataHex != nil || persisted.CommitAttemptID != nil {
		t.Fatalf("failed target reservation = %#v err=%v, want cleared", persisted, err)
	}
	admitted, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
		Copy: commitCopyIdentity(follower), AttemptID: "follower-admitted",
	})
	if err != nil || admitted.State != storagecommit.ReservationAcquired {
		t.Fatalf("follower after terminal cleanup = %#v err=%v, want acquired", admitted, err)
	}
}

func TestStorageReplacementRepo_FailedAttemptedItemRemainsConfirmable(t *testing.T) {
	f := newReplacementFixture(t, "replacement-failed-attempt-confirmation", "01J000000000000000ITEMQ25")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 0); err != nil {
		t.Fatalf("SeedMigrationBatchWithBudget: %v", err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("claim = %#v err=%v", item, err)
	}
	snapshot, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID, ItemID: item.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || snapshot == nil || snapshot.Upload.PieceCID == nil {
		t.Fatalf("AcquireItem = %#v err=%v", snapshot, err)
	}
	copyRow, err := f.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: row.ID, ItemID: item.ID, UploadID: snapshot.Upload.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || copyRow == nil {
		t.Fatalf("AttachTargetCopy = %#v err=%v", copyRow, err)
	}
	const (
		attemptID     = "failed-item-attempt"
		transactionID = "0xfailed-item-attempt"
	)
	if err := f.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		StorageUploadCopyID: copyRow.ID, RequireEligibleCopy: true,
		UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
		PieceCID: *snapshot.Upload.PieceCID, RetrievalURL: "https://target.example/piece", CommitExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	identity := storagecommit.CopyIdentity{
		StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
		StorageDataSetID: row.TargetDataSetID, RequireEligibleCopy: true,
	}
	if _, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
		Copy: identity, AttemptID: attemptID,
	}); err != nil {
		t.Fatalf("ReserveCommitAttempt: %v", err)
	}
	if _, err := f.repos.Uploads.MarkCommitAttempted(ctx, storagecommit.AttemptInput{
		Copy: identity, AttemptID: attemptID, ExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("MarkCommitAttempted: %v", err)
	}
	if err := f.repos.Uploads.RecordCommitTransaction(ctx, storagecommit.EvidenceInput{
		Copy: identity, AttemptID: attemptID, TransactionID: transactionID,
	}); err != nil {
		t.Fatalf("RecordCommitTransaction: %v", err)
	}
	status, err := f.repos.Replacements.RetryReplacementItemClaim(ctx, storagereplacement.ClaimToken{
		ItemID: item.ID, ClaimedAt: *item.ClaimedAt,
	}, time.Now(), "settlement failed")
	if err != nil || status != storagereplacement.ItemStatusFailed {
		t.Fatalf("final retry status=%s err=%v, want failed", status, err)
	}
	persisted, err := f.repos.Uploads.GetUploadCopyByID(ctx, copyRow.ID)
	if err != nil || persisted.CommitAttemptID == nil || *persisted.CommitAttemptID != attemptID ||
		persisted.CommitAttemptedAt == nil {
		t.Fatalf("attempted fence after item exhaustion = %#v err=%v", persisted, err)
	}
	if claimed, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute); err != nil || claimed != nil {
		t.Fatalf("active replacement claimed failed item = %#v err=%v", claimed, err)
	}
	if err := f.repos.Replacements.MarkFailed(ctx, row.ID, nil, "item retries exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	claimed, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || claimed == nil || claimed.ID != item.ID || claimed.ClaimedAt == nil {
		t.Fatalf("terminal confirmation claim = %#v err=%v", claimed, err)
	}
	recovered, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID, ItemID: claimed.ID, ItemClaimedAt: *claimed.ClaimedAt,
	})
	if err != nil || recovered == nil || recovered.Replacement.Status != storagereplacement.StatusFailed {
		t.Fatalf("terminal confirmation snapshot = %#v err=%v", recovered, err)
	}
	pieceID := onChainID(t, "3002")
	if err := f.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: copyRow.ID, RequireEligibleCopy: true,
		UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
		PieceCID: *snapshot.Upload.PieceCID, PieceID: &pieceID,
		RetrievalURL: "https://target.example/piece", CommitExtraDataHex: "abcd",
		CommitTransactionID: transactionID, CommitAttemptID: attemptID,
		CommitConfirmedTransactionID: transactionID,
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if err := f.repos.Replacements.CompleteReplacementItemClaim(ctx, storagereplacement.ClaimToken{
		ItemID: claimed.ID, ClaimedAt: *claimed.ClaimedAt,
	}); err != nil {
		t.Fatalf("CompleteReplacementItemClaim: %v", err)
	}
	persisted, err = f.repos.Uploads.GetUploadCopyByID(ctx, copyRow.ID)
	if err != nil || persisted.Status != model.StorageUploadCopyStatusCommitted || persisted.CommitAttemptID != nil {
		t.Fatalf("confirmed failed-item copy = %#v err=%v", persisted, err)
	}
	var completed storagereplacement.Item
	if err := f.db.NewSelect().Model(&completed).Where("id = ?", item.ID).Scan(ctx); err != nil {
		t.Fatalf("reload completed item: %v", err)
	}
	if completed.Status != storagereplacement.ItemStatusCopied {
		t.Fatalf("completed item status = %s, want copied", completed.Status)
	}
}

func TestStorageReplacementRepo_ProviderEvidenceIsMonotonic(t *testing.T) {
	f := newReplacementFixture(t, "replacement-item-evidence", "01J000000000000000ITEMQ05")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 200, 2); err != nil {
		t.Fatalf("SeedMigrationBatchWithBudget: %v", err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("claim = %#v err=%v", item, err)
	}
	snapshot, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID, ItemID: item.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || snapshot == nil {
		t.Fatalf("AcquireItem = %#v err=%v", snapshot, err)
	}
	copyRow, err := f.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: row.ID, ItemID: item.ID, UploadID: snapshot.Upload.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || copyRow == nil {
		t.Fatalf("AttachTargetCopy = %#v err=%v", copyRow, err)
	}
	pieceCID := "bafk2bzaceproviderreplacement"
	if snapshot.Upload.PieceCID != nil {
		pieceCID = *snapshot.Upload.PieceCID
	}
	ready := repository.MarkUploadCopyPieceReadyInput{
		StorageUploadCopyID: copyRow.ID, RequireEligibleCopy: true,
		UploadID: snapshot.Upload.ID, CopyIndex: copyRow.CopyIndex,
		PieceCID: pieceCID, RetrievalURL: "https://target.example/piece",
	}
	if err := f.repos.Uploads.MarkUploadCopyPieceReady(ctx, ready); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	ready.RetrievalURL = "https://stale.example/different"
	if err := f.repos.Uploads.MarkUploadCopyPieceReady(ctx, ready); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("different retrieval evidence error = %v, want ErrConflict", err)
	}
	identity := storagecommit.CopyIdentity{
		StorageUploadCopyID: copyRow.ID, RequireEligibleCopy: true,
		UploadID: snapshot.Upload.ID, CopyIndex: copyRow.CopyIndex, StorageDataSetID: row.TargetDataSetID,
	}
	if _, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
		Copy: identity, AttemptID: "provider-evidence",
	}); err != nil {
		t.Fatalf("ReserveCommitAttempt: %v", err)
	}
	if _, err := f.repos.Uploads.MarkCommitAttempted(ctx, storagecommit.AttemptInput{
		Copy: identity, AttemptID: "provider-evidence", ExtraDataHex: "01",
	}); err != nil {
		t.Fatalf("MarkCommitAttempted: %v", err)
	}
	if err := f.repos.Uploads.RecordCommitTransaction(ctx, storagecommit.EvidenceInput{
		Copy: identity, AttemptID: "provider-evidence", TransactionID: "0xsubmitted",
	}); err != nil {
		t.Fatalf("RecordCommitTransaction: %v", err)
	}
	if err := f.repos.Uploads.RecordCommitTransaction(ctx, storagecommit.EvidenceInput{
		Copy: identity, AttemptID: "provider-evidence", TransactionID: "0xsubmitted",
	}); err != nil {
		t.Fatalf("idempotent RecordCommitTransaction: %v", err)
	}
	if err := f.repos.Uploads.RecordCommitTransaction(ctx, storagecommit.EvidenceInput{
		Copy: identity, AttemptID: "provider-evidence", TransactionID: "0xdifferent",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("different commit evidence error = %v, want ErrConflict", err)
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
		inserted, done, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 2, 5)
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
	inserted, done, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 2, 5)
	if err != nil || inserted != 0 || !done {
		t.Fatalf("re-seed = (%d, %v, %v), want no new work", inserted, done, err)
	}
}

func TestStorageReplacementRepo_RetryOnlyResumesOperatorAttentionStates(t *testing.T) {
	f := newReplacementFixture(t, "replacement-retry", "01J000000000000000000RPL11")
	ctx := context.Background()
	row := f.authorize(t, "202")

	if _, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: row.ID, MaxRetries: 5, ItemMaxRetries: 5}); !errors.Is(err, storagereplacement.ErrNotRetryable) {
		t.Fatalf("retry while preparing = %v, want ErrNotRetryable", err)
	}
	if err := f.repos.Replacements.MarkFailed(ctx, row.ID, nil, "creation exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	resumed, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: row.ID, MaxRetries: 5, ItemMaxRetries: 5})
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	// The target never activated, so the retry resumes preparation.
	if resumed.Status != storagereplacement.StatusPreparingTarget || resumed.LastError != nil {
		t.Fatalf("resumed = %#v, want preparing_target with the error cleared", resumed)
	}

	superseded := f.authorize(t, "303")
	if _, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: row.ID, MaxRetries: 5, ItemMaxRetries: 5}); !errors.Is(err, storagereplacement.ErrSuperseded) {
		t.Fatalf("retry after supersede = %v, want ErrSuperseded", err)
	}
	if superseded.ID == row.ID {
		t.Fatal("supersede reused the replacement row")
	}
}

func TestStorageReplacementRepo_FailCoordinatorRollsBackBothRecords(t *testing.T) {
	f := newReplacementFixture(t, "replacement-coordinator-failure", "01J000000000000000ITEMQ17")
	ctx := context.Background()
	row := f.authorize(t, "202")
	claimed, err := f.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil || claimed.ClaimedAt == nil {
		t.Fatalf("ClaimReady coordinator = %#v err=%v", claimed, err)
	}

	stale := *claimed
	staleClaimedAt := claimed.ClaimedAt.Add(-time.Second)
	stale.ClaimedAt = &staleClaimedAt
	if err := f.repos.Replacements.FailCoordinator(ctx, repository.ReplacementCoordinatorFailureInput{
		ReplacementID: row.ID,
		Task:          &stale,
		LastError:     "stored items need attention",
	}); err == nil {
		t.Fatal("FailCoordinator with stale task claim succeeded")
	}
	unchanged, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || unchanged == nil || unchanged.Status != storagereplacement.StatusPreparingTarget {
		t.Fatalf("replacement after rollback = %#v err=%v, want preparing_target", unchanged, err)
	}
	activeTask, err := f.repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil || activeTask == nil || activeTask.Status != model.TaskStatusRunning {
		t.Fatalf("task after rollback = %#v err=%v, want running", activeTask, err)
	}

	if err := f.repos.Replacements.FailCoordinator(ctx, repository.ReplacementCoordinatorFailureInput{
		ReplacementID: row.ID,
		Task:          claimed,
		LastError:     "stored items need attention",
	}); err != nil {
		t.Fatalf("FailCoordinator: %v", err)
	}
	failed, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || failed == nil || failed.Status != storagereplacement.StatusFailed {
		t.Fatalf("failed replacement = %#v err=%v", failed, err)
	}
	failedTask, err := f.repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil || failedTask == nil || failedTask.Status != model.TaskStatusFailed {
		t.Fatalf("failed coordinator = %#v err=%v", failedTask, err)
	}
}

func TestStorageReplacementRepo_ExhaustedCoordinatorFailsAtomically(t *testing.T) {
	f := newReplacementFixture(t, "replacement-coordinator-exhausted", "01J000000000000000ITEMQ18")
	ctx := context.Background()
	row := f.authorize(t, "202")
	task, err := f.repos.Tasks.GetByIdempotencyKey(ctx, storagereplacement.MigrateTaskKey(row.ID))
	if err != nil || task == nil {
		t.Fatalf("GetByIdempotencyKey coordinator = %#v err=%v", task, err)
	}
	if _, err := f.db.NewUpdate().Model((*model.Task)(nil)).
		Set("max_retries = ?", 1).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("set coordinator retry budget: %v", err)
	}
	claimed, err := f.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimReady coordinator = %#v err=%v", claimed, err)
	}

	status, err := f.repos.Replacements.ScheduleCoordinatorRetry(ctx, repository.ReplacementCoordinatorRetryInput{
		ReplacementID: row.ID,
		Task:          claimed,
		LastError:     "database unavailable",
		Backoff:       time.Second,
	})
	if err != nil || status != model.TaskStatusExhausted {
		t.Fatalf("ScheduleCoordinatorRetry status=%s err=%v", status, err)
	}
	failed, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || failed == nil || failed.Status != storagereplacement.StatusFailed {
		t.Fatalf("replacement after exhaustion = %#v err=%v, want failed", failed, err)
	}
	if failed.LastError == nil || !strings.Contains(*failed.LastError, "max retries reached") {
		t.Fatalf("replacement last error = %v, want exhausted retry context", failed.LastError)
	}
	exhausted, err := f.repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil || exhausted == nil || exhausted.Status != model.TaskStatusExhausted {
		t.Fatalf("coordinator after exhaustion = %#v err=%v, want exhausted", exhausted, err)
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
	claimedAt := time.Now()
	leaseUntil := claimedAt.Add(time.Minute)
	item := &storagereplacement.Item{
		ReplacementID: replacement.ID, UploadID: orphan.ID,
		Status: storagereplacement.ItemStatusRunning, ClaimedAt: &claimedAt, LeaseUntil: &leaseUntil,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if _, err := f.db.NewInsert().Model(item).Exec(ctx); err != nil {
		t.Fatalf("insert replacement item: %v", err)
	}
	if _, err := f.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("items_total = ?", 1).Set("seeding_complete = ?", true).
		Where("id = ?", replacement.ID).Exec(ctx); err != nil {
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
	got, err := f.repos.Replacements.GetByID(ctx, replacement.ID)
	if err != nil || got == nil || got.ItemsCopied != 0 || got.ItemsTotal != 1 {
		t.Fatalf("replacement progress after late completion = %#v err=%v, want copied 0 of historical total 1", got, err)
	}
	progresses, err := f.repos.Replacements.ReplacementProgresses(ctx, []int64{replacement.ID})
	if err != nil {
		t.Fatalf("ReplacementProgresses: %v", err)
	}
	progress := progresses[replacement.ID]
	if progress.ItemsNoLongerNeeded != 1 || progress.ItemsProcessed != 1 || progress.Percent == nil || *progress.Percent != 100 {
		t.Fatalf("progress after provenance deletion = %#v, want one no-longer-needed item at 100%%", progress)
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
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 5); err != nil {
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

	var itemID int64
	if err := f.db.NewRaw(`SELECT id FROM storage_replacement_items WHERE replacement_id = ? LIMIT 1`, row.ID).Scan(ctx, &itemID); err != nil {
		t.Fatalf("select replacement item: %v", err)
	}
	for _, status := range []storagereplacement.ItemStatus{
		storagereplacement.ItemStatusRetrying,
		storagereplacement.ItemStatusWaitingSource,
		storagereplacement.ItemStatusFailed,
	} {
		mustExec(t, f.db, `UPDATE storage_replacement_items SET status = ? WHERE id = ?`, status, itemID)
		gate, err = f.repos.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
		if err != nil {
			t.Fatalf("EvaluateRetirementGate for %s: %v", status, err)
		}
		if gate.WaitingItems != 1 {
			t.Fatalf("gate for %s = %#v, want the item to block retirement", status, gate)
		}
	}
	mustExec(t, f.db, `UPDATE storage_replacement_items SET status = ?, claimed_at = NULL, lease_until = NULL WHERE id = ?`,
		storagereplacement.ItemStatusCopied, itemID)

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
	var targetCopyID int64
	if err := f.db.NewRaw(`SELECT id FROM storage_upload_copies WHERE upload_id = ? AND storage_data_set_id = ?`,
		f.upload.ID, target.ID).Scan(ctx, &targetCopyID); err != nil {
		t.Fatalf("select target copy: %v", err)
	}
	mustExec(t, f.db, `UPDATE storage_replacement_items SET target_copy_id = ? WHERE id = ?`, targetCopyID, itemID)
	mustExec(t, f.db, `UPDATE storage_upload_copies
		SET status = ?, commit_attempt_id = 'attempt-retirement', commit_attempted_at = CURRENT_TIMESTAMP
		WHERE id = ?`, model.StorageUploadCopyStatusCommitting, targetCopyID)
	gate, err = f.repos.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
	if err != nil {
		t.Fatalf("EvaluateRetirementGate with confirmation attempt: %v", err)
	}
	if gate.ActiveAttempts != 1 || !slices.Contains(gate.Blockers, "confirmation_attempts") {
		t.Fatalf("gate = %#v, want active replacement confirmation blocker", gate)
	}
	mustExec(t, f.db, `UPDATE storage_upload_copies
		SET status = ?, commit_attempt_id = NULL, commit_attempted_at = NULL WHERE id = ?`,
		model.StorageUploadCopyStatusCommitted, targetCopyID)

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
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 5); err != nil {
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
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 5); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}
	var itemID int64
	if err := f.db.NewRaw(`SELECT id FROM storage_replacement_items WHERE replacement_id = ? LIMIT 1`, row.ID).Scan(ctx, &itemID); err != nil {
		t.Fatalf("select replacement item: %v", err)
	}
	item := claimSpecificReplacementItem(t, f, row.ID, itemID)

	// The content stops being referenced before the item runs.
	mustExec(t, f.db, `DELETE FROM object_versions WHERE version_id = ?`, f.version.VersionID)

	if _, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID,
		ItemID:        item.ID,
		ItemClaimedAt: *item.ClaimedAt,
	}); !errors.Is(err, storagereplacement.ErrItemCancelled) {
		t.Fatalf("AcquireItem = %v, want ErrItemCancelled", err)
	}

	// The decisive part: the item must not come back.
	next, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReadyReplacementItem: %v", err)
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

func TestStorageReplacementRepo_CancelClaimOnlyClearsUnattemptedReservation(t *testing.T) {
	f := newReplacementFixture(t, "replacement-cancelled-fifo", "01J000000000000000000RPL30")
	ctx := context.Background()
	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 5); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ClaimedAt == nil {
		t.Fatalf("claim = %#v err=%v", item, err)
	}
	snapshot, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID, ItemID: item.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || snapshot == nil {
		t.Fatalf("AcquireItem: snapshot=%#v err=%v", snapshot, err)
	}
	copyRow, err := f.repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: row.ID, ItemID: item.ID, UploadID: snapshot.Upload.ID, ItemClaimedAt: *item.ClaimedAt,
	})
	if err != nil || copyRow == nil || snapshot.Upload.PieceCID == nil {
		t.Fatalf("AttachTargetCopy: copy=%#v err=%v", copyRow, err)
	}
	if err := f.repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		StorageUploadCopyID: copyRow.ID, RequireEligibleCopy: true,
		UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
		PieceCID: *snapshot.Upload.PieceCID, RetrievalURL: "https://target.example/piece", CommitExtraDataHex: "abcd",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	reservation, err := f.repos.Uploads.ReserveCommitAttempt(ctx, storagecommit.ReserveInput{
		Copy: storagecommit.CopyIdentity{
			StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID, CopyIndex: copyRow.CopyIndex,
			StorageDataSetID: row.TargetDataSetID, RequireEligibleCopy: true,
		},
		AttemptID: "cancelled-fifo",
	})
	if err != nil || reservation.State != storagecommit.ReservationAcquired || reservation.Copy.CommitAttemptID == nil ||
		reservation.Copy.CommitAttemptedAt != nil {
		t.Fatalf("unattempted reservation = %#v err=%v", reservation, err)
	}
	if err := f.repos.Replacements.CancelReplacementItemClaim(ctx, storagereplacement.ClaimToken{
		ItemID: item.ID, ClaimedAt: *item.ClaimedAt,
	}); err != nil {
		t.Fatalf("CancelReplacementItemClaim: %v", err)
	}
	var cancelled storagereplacement.Item
	if err := f.db.NewSelect().Model(&cancelled).Where("id = ?", item.ID).Scan(ctx); err != nil {
		t.Fatalf("reload cancelled item: %v", err)
	}
	if cancelled.Status != storagereplacement.ItemStatusCancelled || cancelled.ClaimedAt != nil || cancelled.LeaseUntil != nil {
		t.Fatalf("cancelled item = %#v", cancelled)
	}
	persisted, err := f.repos.Uploads.GetUploadCopyByID(ctx, copyRow.ID)
	if err != nil || persisted == nil || persisted.CommitReadyAt != nil || persisted.CommitExtraDataHex != nil ||
		persisted.CommitAttemptID != nil || persisted.CommitAttemptedAt != nil {
		t.Fatalf("cancelled FIFO copy = %#v err=%v", persisted, err)
	}

	pieceID := onChainID(t, "3002")
	if err := f.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID:          copyRow.ID,
		UploadID:                     copyRow.UploadID,
		CopyIndex:                    copyRow.CopyIndex,
		PieceCID:                     *snapshot.Upload.PieceCID,
		PieceID:                      &pieceID,
		RetrievalURL:                 "https://target.example/piece",
		CommitExtraDataHex:           "abcd",
		CommitTransactionID:          "0xcommitted",
		CommitConfirmedTransactionID: "0xcommitted",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	claimedAt := time.Now()
	mustExec(t, f.db, `UPDATE storage_replacement_items
		SET status = ?, claimed_at = ?, lease_until = ?, updated_at = ?
		WHERE id = ?`, storagereplacement.ItemStatusRunning, claimedAt, claimedAt.Add(time.Minute), claimedAt, item.ID)
	if err := f.repos.Replacements.CancelReplacementItemClaim(ctx, storagereplacement.ClaimToken{
		ItemID: item.ID, ClaimedAt: claimedAt,
	}); err != nil {
		t.Fatalf("CancelReplacementItemClaim for committed copy: %v", err)
	}
	persisted, err = f.repos.Uploads.GetUploadCopyByID(ctx, copyRow.ID)
	if err != nil || persisted == nil || persisted.Status != model.StorageUploadCopyStatusCommitted ||
		persisted.CommitExtraDataHex == nil || *persisted.CommitExtraDataHex != "abcd" ||
		persisted.CommitTransactionID == nil || *persisted.CommitTransactionID != "0xcommitted" ||
		persisted.CommitConfirmedTransactionID == nil || *persisted.CommitConfirmedTransactionID != "0xcommitted" {
		t.Fatalf("committed copy evidence after claim cancellation = %#v err=%v", persisted, err)
	}
}

// A rejected attempt on the retiring generation must reset that generation,
// not whichever one currently owns the slot.
func TestStorageReplacementRepo_ResetCommitAttemptTargetsTheRecordedCopy(t *testing.T) {
	f := newReplacementFixture(t, "replacement-reset", "01J000000000000000000RPL17")
	ctx := context.Background()
	sourceCopy, err := f.repos.Uploads.GetUploadCopyForDataSet(ctx, f.upload.ID, f.source.ID)
	if err != nil || sourceCopy == nil {
		t.Fatalf("GetUploadCopyForDataSet = %#v err=%v", sourceCopy, err)
	}
	mustExec(t, f.db, `UPDATE storage_upload_copies SET status = ? WHERE id = ?`,
		model.StorageUploadCopyStatusPieceReady, sourceCopy.ID)
	identity := storagecommit.CopyIdentity{
		StorageUploadCopyID: sourceCopy.ID, UploadID: sourceCopy.UploadID,
		CopyIndex: sourceCopy.CopyIndex, StorageDataSetID: f.source.ID,
	}
	seedRepositoryCommitAttempt(t, f.repos, *sourceCopy, "retiring-rejected", "abcd", "0xrejected")

	row := f.authorize(t, "202")
	f.readyTarget(t, row, "2002")
	if err := f.repos.Replacements.Activate(ctx, row.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	if err := f.repos.Uploads.ResetCommitAttempt(ctx, storagecommit.ResetInput{
		Copy: identity, AttemptID: "retiring-rejected", LastError: "provider rejected the commit",
	}); err != nil {
		t.Fatalf("ResetCommitAttempt: %v", err)
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
	reservedCopy := seedCommitCopies(t, f.db, f.bucket.ID, first.TargetDataSetID, 1)[0]
	mustExec(t, f.db, `UPDATE storage_upload_copies SET commit_attempt_id = ? WHERE id = ?`, "abandoned-reservation", reservedCopy.ID)
	if err := f.repos.Replacements.RetireAbandonedTarget(ctx, first.ID); !errors.Is(err, storagereplacement.ErrPrematureComplete) {
		t.Fatalf("RetireAbandonedTarget with reservation = %v, want premature-complete", err)
	}
	mustExec(t, f.db, `UPDATE storage_upload_copies SET commit_attempt_id = NULL WHERE id = ?`, reservedCopy.ID)
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

func claimSpecificReplacementItem(
	t *testing.T,
	f *replacementFixture,
	replacementID int64,
	itemID int64,
) *storagereplacement.Item {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	mustExec(t, f.db, `UPDATE storage_replacement_items SET scheduled_at = ? WHERE replacement_id = ? AND id <> ?`,
		now.Add(time.Hour), replacementID, itemID)
	mustExec(t, f.db, `UPDATE storage_replacement_items SET scheduled_at = ? WHERE id = ?`, now, itemID)
	item, err := f.repos.Replacements.ClaimReadyReplacementItem(ctx, time.Minute)
	if err != nil || item == nil || item.ID != itemID || item.ClaimedAt == nil {
		t.Fatalf("ClaimReadyReplacementItem = %#v err=%v, want item %d", item, err, itemID)
	}
	return item
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
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 5); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}

	var itemID int64
	if err := f.db.NewRaw(`SELECT id FROM storage_replacement_items WHERE replacement_id = ? AND upload_id = ?`,
		row.ID, inFlight.ID).Scan(ctx, &itemID); err != nil {
		t.Fatalf("select in-flight item: %v", err)
	}
	item := claimSpecificReplacementItem(t, f, row.ID, itemID)
	if _, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID,
		ItemID:        itemID,
		ItemClaimedAt: *item.ClaimedAt,
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
	if _, _, err := f.repos.Replacements.SeedMigrationBatchWithBudget(ctx, row.ID, 10, 5); err != nil {
		t.Fatalf("SeedMigrationBatch: %v", err)
	}
	var itemID int64
	if err := f.db.NewRaw(`SELECT id FROM storage_replacement_items WHERE replacement_id = ? AND upload_id = ?`,
		row.ID, elsewhere.ID).Scan(ctx, &itemID); err != nil {
		t.Fatalf("select item: %v", err)
	}
	item := claimSpecificReplacementItem(t, f, row.ID, itemID)
	if _, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: row.ID,
		ItemID:        itemID,
		ItemClaimedAt: *item.ClaimedAt,
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

			if _, err := f.repos.Replacements.Retry(ctx, repository.RetryReplacementInput{ReplacementID: row.ID, MaxRetries: 5, ItemMaxRetries: 5}); err != nil {
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
