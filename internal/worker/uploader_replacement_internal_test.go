package worker

import (
	"context"
	"log/slog"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
)

// In-place recovery and an approved replacement both want to finish the same
// generation's work. The replacement wins while it is running, otherwise the
// two would race each other over one copy row.
func TestEnsureReplicaRepairTaskStandsDownForApprovedReplacement(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := testutil.SeedBucket(t, db, "repair-vs-replacement")

	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(),
		BucketID:  bucket.ID,
		Key:       "file.txt",
		Size:      11,
		ETag:      "etag",
		Checksum:  "sum",
		CacheKey:  ".versions/repair-vs-replacement",
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	providerID := mustOnChainID(t, "101")
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        providerID,
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		UploadID:  upload.ID,
		DataSetID: mustOnChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       providerID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	established, err := repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || established == nil {
		t.Fatalf("GetDataSetBindingByID: %#v err=%v", established, err)
	}

	// Without a replacement the generation repairs itself as usual.
	created, err := ensureReplicaRepairTask(ctx, repos, established, 5)
	if err != nil {
		t.Fatalf("ensureReplicaRepairTask: %v", err)
	}
	if !created {
		t.Fatal("recovery did not queue repair work for an unfinished copy")
	}
	if _, err := db.NewDelete().
		Model((*model.Task)(nil)).
		Where("idempotency_key = ?", replicaRepairTaskKey(established.ID)).
		Exec(ctx); err != nil {
		t.Fatalf("clear repair task: %v", err)
	}

	replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID:         bucket.ID,
		SourceDataSetID:  established.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: mustOnChainID(t, "202"),
		ClientRequestID:  "internal-replacement-1",
		MaxRetries:       5,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	created, err = ensureReplicaRepairTask(ctx, repos, established, 5)
	if err != nil {
		t.Fatalf("ensureReplicaRepairTask during replacement: %v", err)
	}
	if created {
		t.Fatal("recovery queued repair work while an approved replacement owns the generation")
	}

	// The generation being written *to* needs the same protection: repairing the
	// target in place would race the coordinator over one copy row, and the two
	// gates key off different task payloads so neither would see the other.
	target, err := repos.Uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("GetDataSetBindingByID target = %#v err=%v", target, err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: target.ID, DataSetID: mustOnChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady target: %v", err)
	}
	if err := repos.Uploads.MarkDataSetUnavailable(ctx, target.ID, "target provider unreachable"); err != nil {
		t.Fatalf("MarkDataSetUnavailable target: %v", err)
	}
	unavailableTarget, err := repos.Uploads.GetDataSetBindingByID(ctx, target.ID)
	if err != nil || unavailableTarget == nil {
		t.Fatalf("reload target = %#v err=%v", unavailableTarget, err)
	}
	created, err = ensureReplicaRepairTask(ctx, repos, unavailableTarget, 5)
	if err != nil {
		t.Fatalf("ensureReplicaRepairTask for target: %v", err)
	}
	if created {
		t.Fatal("recovery queued repair work on the generation the replacement is writing to")
	}

	// A replacement that has given up must not hold the slot hostage.
	if err := repos.Replacements.MarkFailed(ctx, replacement.ID, nil, "target creation exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	created, err = ensureReplicaRepairTask(ctx, repos, established, 5)
	if err != nil {
		t.Fatalf("ensureReplicaRepairTask after failure: %v", err)
	}
	if !created {
		t.Fatal("recovery stayed blocked after the replacement terminally failed")
	}
}

func mustOnChainID(t *testing.T, value string) types.OnChainID {
	t.Helper()
	id, err := types.ParseOnChainID("test id", value)
	if err != nil {
		t.Fatalf("parse on-chain id %q: %v", value, err)
	}
	return id
}

// An upload already in flight belongs to the generation its copy was bound to.
// Resolving the data set by replica slot instead would follow the slot to the
// replacement target the moment it activates, storing the piece on the new
// provider while the copy row being updated still points at the old one.
func TestTaskCopyDataSetFollowsTheCopyNotTheSlot(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := testutil.SeedBucket(t, db, "in-flight-across-activation")

	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(),
		BucketID:  bucket.ID,
		Key:       "file.txt",
		Size:      11,
		ETag:      "etag",
		Checksum:  "sum",
		CacheKey:  ".versions/in-flight-across-activation",
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	source, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        mustOnChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, UploadID: upload.ID, DataSetID: mustOnChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: source.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       mustOnChainID(t, "101"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	inFlight, err := repos.Uploads.GetUploadCopy(ctx, upload.ID, 0)
	if err != nil || inFlight == nil {
		t.Fatalf("GetUploadCopy = %#v err=%v", inFlight, err)
	}

	// The task was queued while the source still owned the slot.
	task := newUploadStageTask(
		repository.ObjectVersionRef{ObjectID: version.ObjectID, VersionID: version.VersionID},
		5, uploadStageIngressStore, upload.ID, 0, model.StorageCopyTransferMethodIngress, inFlight.ID)

	replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID:         bucket.ID,
		SourceDataSetID:  source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: mustOnChainID(t, "202"),
		ClientRequestID:  "internal-replacement-2",
		MaxRetries:       5,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: replacement.TargetDataSetID, DataSetID: mustOnChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady target: %v", err)
	}
	if err := repos.Replacements.Activate(ctx, replacement.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	current, err := repos.Uploads.GetDataSetBindingByCopyIndex(ctx, bucket.ID, 0)
	if err != nil || current == nil || current.ID != replacement.TargetDataSetID {
		t.Fatalf("slot owner after activation = %#v err=%v, want the target", current, err)
	}

	u := &Uploader{repos: repos}
	// A retry after activation must keep the source binding whether it resumes
	// immediately after copy creation, after storing the piece, or while commit
	// confirmation is in progress.
	for _, status := range []model.StorageUploadCopyStatus{
		model.StorageUploadCopyStatusPending,
		model.StorageUploadCopyStatusPieceReady,
		model.StorageUploadCopyStatusCommitting,
	} {
		if _, err := db.NewUpdate().Model((*model.StorageUploadCopy)(nil)).
			Set("status = ?", status).
			Where("id = ?", inFlight.ID).
			Exec(ctx); err != nil {
			t.Fatalf("set in-flight status %s: %v", status, err)
		}
		preserved, err := u.preserveInFlightUploadBindings(
			ctx,
			upload.ID,
			newBucketBindingPlan([]model.StorageDataSet{*current}, upload.ID),
		)
		if err != nil {
			t.Fatalf("preserve binding at %s: %v", status, err)
		}
		if got := preserved.byCopyIndex[0]; got == nil || got.ID != source.ID {
			t.Fatalf("binding at %s = %#v, want source generation %d", status, got, source.ID)
		}
	}
	resolved, err := u.taskCopyDataSet(ctx, task, bucket.ID, upload.ID, 0)
	if err != nil || resolved == nil {
		t.Fatalf("taskCopyDataSet = %#v err=%v", resolved, err)
	}
	if resolved.ID != source.ID {
		t.Fatalf("resolved data set %d, want the retiring generation %d the copy is bound to", resolved.ID, source.ID)
	}

	// A task queued before copy ids existed has nothing to anchor to and stays
	// resolvable through the slot, which is where it belongs.
	legacy := newUploadStageTask(
		repository.ObjectVersionRef{ObjectID: version.ObjectID, VersionID: version.VersionID},
		5, uploadStageIngressStore, upload.ID, 0, model.StorageCopyTransferMethodIngress, 0)
	viaSlot, err := u.taskCopyDataSet(ctx, legacy, bucket.ID, upload.ID, 0)
	if err != nil || viaSlot == nil {
		t.Fatalf("taskCopyDataSet legacy = %#v err=%v", viaSlot, err)
	}
	if viaSlot.ID != replacement.TargetDataSetID {
		t.Fatalf("legacy task resolved to %d, want the current generation %d", viaSlot.ID, replacement.TargetDataSetID)
	}
}

// A replacement that has already asked the old service to end is past
// migration, even when a dependency wait has moved it out of the retiring
// status. Restarting it as a migration would re-run the copy pass and, worse,
// leave nothing driving the termination it already started.
func TestEnqueueReplacementCoordinatorResumesRetirementAfterTermination(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	m := &Manager{repos: repos, logger: slog.Default(), uploadMaxRetries: 5}

	epoch := int64(4200)
	for _, tc := range []struct {
		name    string
		row     storagereplacement.Replacement
		wantKey func(int64) string
	}{
		{"migrating", storagereplacement.Replacement{ID: 1, BucketID: 9, Status: storagereplacement.StatusMigrating}, storagereplacement.MigrateTaskKey},
		{"waiting before termination", storagereplacement.Replacement{ID: 2, BucketID: 9, Status: storagereplacement.StatusWaiting}, storagereplacement.MigrateTaskKey},
		{"retiring", storagereplacement.Replacement{ID: 3, BucketID: 9, Status: storagereplacement.StatusRetiring}, storagereplacement.RetireTaskKey},
		{"waiting after termination", storagereplacement.Replacement{ID: 4, BucketID: 9, Status: storagereplacement.StatusWaiting, TerminationEpoch: &epoch}, storagereplacement.RetireTaskKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := tc.row
			m.enqueueReplacementCoordinator(ctx, &row)
			task, err := repos.Tasks.GetByIdempotencyKey(ctx, tc.wantKey(row.ID))
			if err != nil || task == nil {
				t.Fatalf("coordinator for %s = %#v err=%v, want %s", tc.name, task, err, tc.wantKey(row.ID))
			}
		})
	}

	// The retirement cases must not also have queued a migration pass.
	for _, id := range []int64{3, 4} {
		task, err := repos.Tasks.GetByIdempotencyKey(ctx, storagereplacement.MigrateTaskKey(id))
		if err != nil {
			t.Fatalf("GetByIdempotencyKey: %v", err)
		}
		if task != nil {
			t.Fatalf("replacement %d was restarted as a migration after its service was already ended", id)
		}
	}
}
