package worker_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

func TestManager_RecoverOnStartup_ReleasesExpiredLeases(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-lease-bucket")
	objID, versionID := seedManagerVersion(t, repos, bucket, "mgr-lease-key", model.ObjectStateCached)

	// Create a task and claim it, then manually set its lease_until to the past
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: "upload:" + versionID,
		Status:         model.TaskStatusRunning,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("creating task: %v", err)
	}

	// Set lease_until and claimed_at to past (expired lease)
	pastLease := time.Now().Add(-1 * time.Hour)
	_, err := db.NewUpdate().Model((*model.Task)(nil)).
		Set("claimed_at = ?", pastLease).
		Set("lease_until = ?", pastLease).
		Set("started_at = ?", pastLease).
		Where("id = ?", task.ID).
		Exec(ctx)
	if err != nil {
		t.Fatalf("updating lease: %v", err)
	}

	// Start manager with no workers — returns immediately after recovery
	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	// Task should be back to queued.
	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("getting task: %v", err)
	}
	if got.Status != model.TaskStatusQueued {
		t.Errorf("expected task status queued after lease release, got %s", got.Status)
	}
}

func TestManager_RecoverOnStartup_PreservesActiveLeases(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-active-lease-bucket")
	objID, versionID := seedManagerVersion(t, repos, bucket, "mgr-active-lease-key", model.ObjectStateCached)
	now := time.Now()
	leaseUntil := now.Add(10 * time.Minute)
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: "upload:" + versionID,
		Status:         model.TaskStatusRunning,
		MaxRetries:     5,
		ScheduledAt:    now,
		ClaimedAt:      &now,
		LeaseUntil:     &leaseUntil,
		StartedAt:      &now,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create running task: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.TaskStatusRunning {
		t.Fatalf("task status = %s, want running", got.Status)
	}
	if got.LeaseUntil == nil || !got.LeaseUntil.After(now) {
		t.Fatalf("lease_until = %v, want active lease after %s", got.LeaseUntil, now)
	}
}

func TestManager_RecoverOnStartup_DoesNotResetStagedUploadingState(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-stale-bucket")
	objID, versionID := seedManagerVersion(t, repos, bucket, "mgr-stale-key", model.ObjectStateCached)

	// Transition to uploading
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition: %v", err)
	}

	// Manually set updated_at to more than 10 minutes ago (stale threshold)
	staleTime := time.Now().Add(-15 * time.Minute)
	_, err := db.NewUpdate().Model((*model.ObjectVersion)(nil)).
		Set("updated_at = ?", staleTime).
		Where("version_id = ?", versionID).
		Exec(ctx)
	if err != nil {
		t.Fatalf("setting stale timestamp: %v", err)
	}

	// Start manager. Staged uploads keep durable progress in upload/copy rows,
	// so startup must not downgrade uploading back to cached.
	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	got, err := repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil {
		t.Fatalf("getting object: %v", err)
	}
	if got.State != model.ObjectStateUploading {
		t.Errorf("expected object to remain uploading, got %s", got.State)
	}
	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List prepare_upload tasks: %v", err)
	}
	if total != 1 || len(tasks) != 1 {
		t.Fatalf("prepare_upload tasks total=%d tasks=%#v, want one orphan recovery task", total, tasks)
	}
	if tasks[0].RefID != objID || tasks[0].RefVersionID != versionID || tasks[0].MaxRetries != 9 {
		t.Fatalf("prepare_upload task = %#v, want recovered task for uploading version", tasks[0])
	}
}

func TestManager_RecoverOnStartup_RequeuesOrphanCommittingState(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-orphan-commit-bucket")
	objID, versionID := seedManagerVersion(t, repos, bucket, "mgr-orphan-commit-key", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	got, err := repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil {
		t.Fatalf("getting object: %v", err)
	}
	if got.State != model.ObjectStateUploading {
		t.Fatalf("object state = %s, want uploading", got.State)
	}
	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List prepare_upload tasks: %v", err)
	}
	if total != 1 || len(tasks) != 1 {
		t.Fatalf("prepare_upload tasks total=%d tasks=%#v, want one orphan recovery task", total, tasks)
	}
	if tasks[0].RefID != objID || tasks[0].RefVersionID != versionID {
		t.Fatalf("prepare_upload task = %#v, want recovered task for committing version", tasks[0])
	}
}

func TestManager_RecoverOnStartup_ReenqueuesPrimaryCommitStage(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-primary-commit-recover")
	objID, versionID := seedManagerVersion(t, repos, bucket, "recover-primary-commit", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzaceprimaryrecover",
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "ingress_commit", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List ingress_commit tasks: %v", err)
	}
	if total != 1 || len(tasks) != 1 {
		t.Fatalf("ingress_commit tasks total=%d tasks=%#v, want one", total, tasks)
	}
	if tasks[0].RefID != objID || tasks[0].RefVersionID != versionID || tasks[0].Payload["upload_id"] == nil {
		t.Fatalf("ingress_commit task = %#v, want recovered task for source version", tasks[0])
	}
}

func TestManager_RecoverOnStartup_BindsCommittedIngressBeforeSingleCopyFinalize(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-single-copy-commit-recover")
	objID, versionID := seedManagerVersion(t, repos, bucket, "recover-single-copy", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacesinglecopyrecover",
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://ingress.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	got, err := repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if err != nil || got == nil {
		t.Fatalf("GetCurrentVersionByObjectID: got=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateStored || got.StorageUploadID == nil || *got.StorageUploadID != upload.ID {
		t.Fatalf("object after recovery = state:%s upload:%v, want stored on recovered upload %d", got.State, got.StorageUploadID, upload.ID)
	}
	gotUpload, err := repos.Uploads.GetByID(ctx, upload.ID)
	if err != nil || gotUpload == nil {
		t.Fatalf("GetByID(upload): upload=%v err=%v", gotUpload, err)
	}
	if gotUpload.Status != model.StorageUploadStatusComplete {
		t.Fatalf("upload status = %s, want complete", gotUpload.Status)
	}
}

func TestManager_RecoverOnStartup_MakesExpiredPrimaryCommitTaskClaimable(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-primary-commit-interrupted")
	objID, versionID := seedManagerVersion(t, repos, bucket, "interrupted-primary-commit", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzaceprimaryinterrupted",
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	stage := "ingress_commit"
	now := time.Now()
	leaseUntil := now.Add(-time.Minute)
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s:ingress_commit:%d", versionID, upload.ID),
		Payload:        map[string]interface{}{"upload_id": upload.ID, "copy_index": 0, "transfer_method": string(model.StorageCopyTransferMethodIngress)},
		Status:         model.TaskStatusRunning,
		MaxRetries:     9,
		ScheduledAt:    now,
		ClaimedAt:      &now,
		LeaseUntil:     &leaseUntil,
		StartedAt:      &now,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create running task: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil {
		t.Fatal("expired ingress_commit task was not claimable after startup recovery")
	}
	if claimed.ID != task.ID {
		t.Fatalf("claimed task ID = %d, want expired task %d", claimed.ID, task.ID)
	}
}

func TestManager_RecoverOnStartup_ReenqueuesReplicatingSecondaryStage(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-secondary-recover")
	objID, versionID := seedManagerVersion(t, repos, bucket, "recover-secondary", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("primary binding: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("secondary binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacesecondaryrecover",
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "ensure_dataset", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List ensure_dataset tasks: %v", err)
	}
	if total != 1 || len(tasks) != 1 {
		t.Fatalf("ensure_dataset tasks total=%d tasks=%#v, want one secondary recovery task", total, tasks)
	}
	if tasks[0].RefID != objID || tasks[0].RefVersionID != versionID || tasks[0].Payload["copy_index"] == nil {
		t.Fatalf("ensure_dataset task = %#v, want recovered secondary task", tasks[0])
	}
}

func TestManager_RecoverOnStartup_QueuesRepairForUnavailablePeerDeficit(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-peer-unavailable-recover")
	objID, versionID := seedManagerVersion(t, repos, bucket, "recover-peer-unavailable", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	ingress, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("ingress binding: %v", err)
	}
	peer, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("peer binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: ingress.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady ingress: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: peer.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002"), ClientDataSetID: onChainIDPtr(t, "9002")}); err != nil {
		t.Fatalf("MarkDataSetReady peer: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: ingress.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: peer.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacepeerdeficit",
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://ingress.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}
	if err := repos.Uploads.MarkDataSetUnavailable(ctx, peer.ID, "provider dataset retired"); err != nil {
		t.Fatalf("MarkDataSetUnavailable peer: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	ensureTasks, ensureTotal, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "ensure_dataset", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List ensure_dataset tasks: %v", err)
	}
	if ensureTotal != 0 || len(ensureTasks) != 0 {
		t.Fatalf("ensure_dataset tasks total=%d tasks=%#v, want no recovery for unavailable peer", ensureTotal, ensureTasks)
	}
	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List repair prepare tasks: %v", err)
	}
	if total != 1 || len(tasks) != 1 {
		t.Fatalf("prepare_upload tasks total=%d tasks=%#v, want one repair task", total, tasks)
	}
	if tasks[0].RefID != objID || tasks[0].RefVersionID != versionID || taskPayloadInt64ForTest(tasks[0].Payload, "upload_id") != upload.ID {
		t.Fatalf("repair task = %#v, want upload repair task for upload %d", tasks[0], upload.ID)
	}
}

func TestManager_RecoverOnStartup_QueuesRepairForFailedPeerDeficitWithRecoverablePeer(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-peer-deficit-with-pending-recover")
	objID, versionID := seedManagerVersion(t, repos, bucket, "recover-peer-deficit-pending", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	ingress, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("ingress binding: %v", err)
	}
	pendingPeer, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("pending peer binding: %v", err)
	}
	failedPeer, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "303"), CopyIndex: 2, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("failed peer binding: %v", err)
	}
	for _, input := range []repository.MarkDataSetReadyInput{
		{ID: ingress.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")},
		{ID: pendingPeer.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002"), ClientDataSetID: onChainIDPtr(t, "9002")},
		{ID: failedPeer.ID, UploadID: upload.ID, DataSetID: onChainID(t, "3003"), ClientDataSetID: onChainIDPtr(t, "9003")},
	} {
		if err := repos.Uploads.MarkDataSetReady(ctx, input); err != nil {
			t.Fatalf("MarkDataSetReady: %v", err)
		}
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: ingress.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: pendingPeer.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
		{StorageDataSetID: failedPeer.ID, CopyIndex: 2, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "303")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacepeerdeficitpending",
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://ingress.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyFailed(ctx, upload.ID, 2, "peer pull: provider failed"); err != nil {
		t.Fatalf("MarkUploadCopyFailed: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	ensureTasks, ensureTotal, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "peer_pull", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List peer_pull tasks: %v", err)
	}
	if ensureTotal != 1 || len(ensureTasks) != 1 || taskPayloadInt64ForTest(ensureTasks[0].Payload, "copy_index") != 1 {
		t.Fatalf("peer_pull tasks total=%d tasks=%#v, want pending peer recovery", ensureTotal, ensureTasks)
	}
	repairTasks, repairTotal, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List repair prepare tasks: %v", err)
	}
	if repairTotal != 1 || len(repairTasks) != 1 {
		t.Fatalf("prepare_upload tasks total=%d tasks=%#v, want repair for remaining deficit", repairTotal, repairTasks)
	}
	if repairTasks[0].RefID != objID || repairTasks[0].RefVersionID != versionID || taskPayloadInt64ForTest(repairTasks[0].Payload, "upload_id") != upload.ID {
		t.Fatalf("repair task = %#v, want upload repair task for upload %d", repairTasks[0], upload.ID)
	}
}

func TestManager_RecoverOnStartup_ReconcilesAllStagedUploads(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-staged-batch-recover")
	for i := 0; i < 101; i++ {
		_, versionID := seedManagerVersion(t, repos, bucket, fmt.Sprintf("recover-batch-%03d", i), model.ObjectStateCached)
		if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			t.Fatalf("uploading %d: %v", i, err)
		}
		version, err := repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || version == nil {
			t.Fatalf("GetVersionByID %d: version=%v err=%v", i, version, err)
		}
		if _, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
			BucketID:        bucket.ID,
			SourceVersionID: versionID,
			ContentSize:     version.Size,
			Checksum:        version.Checksum,
		}); err != nil {
			t.Fatalf("StartObjectUploadAttempt %d: %v", i, err)
		}
	}

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), "prepare_upload", string(model.TaskStatusQueued), 200, 0)
	if err != nil {
		t.Fatalf("List prepare_upload tasks: %v", err)
	}
	if total != 101 || len(tasks) != 101 {
		t.Fatalf("prepare_upload tasks total=%d len=%d, want 101", total, len(tasks))
	}
}

func TestManager_RecoverOnStartup_UsesBoundedVersionBatches(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	objects := &boundedVersionListRepo{ObjectRepository: repos.Objects}
	repos.Objects = objects

	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(context.Background())

	if objects.sawUnbounded {
		t.Fatal("startup reconciliation requested an unbounded object version list")
	}
}

func TestManager_ReconcileTasks_CreatesMissingTasks(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-reconcile-bucket")
	objID, versionID := seedManagerVersion(t, repos, bucket, "mgr-reconcile-key", model.ObjectStateCached)

	// No task exists yet — manager should create one during reconciliation
	mgr := worker.NewManager(repos, slog.Default(), false).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	// Claim the task created by reconciliation
	task, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("claiming task: %v", err)
	}
	if task == nil {
		t.Fatal("expected reconciliation to create missing upload task")
	}
	if task.RefID != objID || task.RefVersionID != versionID {
		t.Errorf("task refs mismatch: got refID=%d version=%s, want %d/%s", task.RefID, task.RefVersionID, objID, versionID)
	}
	if task.MaxRetries != 9 {
		t.Fatalf("task MaxRetries = %d, want 9", task.MaxRetries)
	}
	if task.Stage == nil || *task.Stage != "prepare_upload" {
		t.Fatalf("task Stage = %#v, want prepare_upload", task.Stage)
	}
}

func TestManager_WorkerHealth(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	w1 := &stubWorker{name: "alpha", isHealthy: true}
	w2 := &stubWorker{name: "beta", isHealthy: false}
	w3 := &stubWorker{name: "gamma", isHealthy: true}

	mgr := worker.NewManager(repos, slog.Default(), false, w1, w2, w3)
	health := mgr.WorkerHealth()

	if len(health) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(health))
	}
	if !health["alpha"] {
		t.Error("expected alpha healthy")
	}
	if health["beta"] {
		t.Error("expected beta unhealthy")
	}
	if !health["gamma"] {
		t.Error("expected gamma healthy")
	}
}

func TestManager_WorkerHealth_Empty(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	mgr := worker.NewManager(repos, slog.Default(), false)
	health := mgr.WorkerHealth()
	if len(health) != 0 {
		t.Errorf("expected empty map, got %d entries", len(health))
	}
}

func TestManager_WorkerHealth_RealWorkers(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	mc := &testutil.MockCache{}
	sm := state.NewObjectStateMachine()
	logger := slog.Default()
	poll := 50 * time.Millisecond

	up := worker.NewUploader(repos, mc, nil, nil, sm, true, 1, poll, logger)
	ev := worker.NewEvictor(repos, mc, sm, 1, poll, logger)

	mgr := worker.NewManager(repos, logger, true, up, ev)
	health := mgr.WorkerHealth()

	expected := map[string]bool{
		"uploader": true,
		"evictor":  true,
	}
	for name, wantHealthy := range expected {
		got, ok := health[name]
		if !ok {
			t.Errorf("missing worker %q in health map", name)
			continue
		}
		if got != wantHealthy {
			t.Errorf("worker %q: expected healthy=%v, got %v", name, wantHealthy, got)
		}
	}
}

func TestManager_ReconcileTasks_IdempotencyDedup(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-dedup-bucket")
	objID, versionID := seedManagerVersion(t, repos, bucket, "mgr-dedup-key", model.ObjectStateCached)

	// Pre-create the task with the same idempotency key the manager would use
	existingTask := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   versionID,
		IdempotencyKey: fmt.Sprintf("upload:%s", versionID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, existingTask); err != nil {
		t.Fatalf("creating existing task: %v", err)
	}

	// Start manager — reconciliation should skip (idempotency dedup)
	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	// Claim the task — should be exactly one (the pre-existing one)
	task, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("claiming task: %v", err)
	}
	if task == nil {
		t.Fatal("expected existing task to still be claimable")
	}
	if task.ID != existingTask.ID {
		t.Errorf("expected existing task ID %d, got %d", existingTask.ID, task.ID)
	}

	// No second task should be claimable
	dup, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if dup != nil {
		t.Error("expected no duplicate task after idempotency dedup")
	}
}

func TestManager_ReconcileTasks_AutoEvictGuard(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-autoevict-off")
	seedManagerVersion(t, repos, bucket, "stored-no-evict", model.ObjectStateStored)

	mgr := worker.NewManager(repos, slog.Default(), false)
	mgr.Start(ctx)

	task, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict task: %v", err)
	}
	if task != nil {
		t.Fatal("expected no evict_cache task when autoEvict is disabled")
	}
}

func TestManager_ReconcileTasks_AutoEvictEnabled(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-autoevict-on")
	objID, versionID := seedManagerVersion(t, repos, bucket, "stored-with-evict", model.ObjectStateStored)

	mgr := worker.NewManager(repos, slog.Default(), true).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	task, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict task: %v", err)
	}
	if task == nil {
		t.Fatal("expected evict_cache task when autoEvict is enabled")
	}
	if task.RefID != objID || task.RefVersionID != versionID {
		t.Fatalf("evict task refs = (%d,%s), want (%d,%s)", task.RefID, task.RefVersionID, objID, versionID)
	}
	if task.MaxRetries != 4 {
		t.Fatalf("evict task MaxRetries = %d, want 4", task.MaxRetries)
	}
}

func TestManager_ReconcileTasks_AutoEvictSkipsReplicating(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "mgr-autoevict-replicating")
	objID, versionID := seedManagerVersion(t, repos, bucket, "replicating-with-cache", model.ObjectStateCached)
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding primary: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding secondary: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady primary: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "piece-replicating-evict-reconcile",
		PieceID:      onChainIDPtr(t, "301"),
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}

	mgr := worker.NewManager(repos, slog.Default(), true).WithTaskMaxRetries(9, 4)
	mgr.Start(ctx)

	task, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict task: %v", err)
	}
	if task != nil {
		t.Fatalf("unexpected evict_cache task for replicating version %d/%s: %#v", objID, versionID, task)
	}
}

func seedManagerVersion(t *testing.T, repos *repository.Repositories, bucket *model.Bucket, key string, state model.ObjectState) (int64, string) {
	t.Helper()
	versionID := model.NewVersionID()
	createState := state
	if state == model.ObjectStateStored || state == model.ObjectStateCacheEvicted {
		createState = model.ObjectStateUploading
	}
	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         key,
		Size:        100,
		ETag:        "etag-" + key,
		Checksum:    "checksum-" + key,
		ContentType: "text/plain",
		CacheKey:    ".versions/" + versionID,
		State:       createState,
	}
	objID, err := repos.Objects.CreateVersionAndSetCurrent(context.Background(), version)
	if err != nil {
		t.Fatalf("creating object version: %v", err)
	}
	if state == model.ObjectStateStored || state == model.ObjectStateCacheEvicted {
		acceptManagerVersionUpload(t, repos, versionID)
		if state == model.ObjectStateCacheEvicted {
			if err := repos.Objects.UpdateVersionState(context.Background(), versionID, model.ObjectStateStored, model.ObjectStateCacheEvicted); err != nil {
				t.Fatalf("transition to cache_evicted: %v", err)
			}
		}
	}
	return objID, versionID
}

func acceptManagerVersionUpload(t *testing.T, repos *repository.Repositories, versionID string) {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for upload accept: version=%v err=%v", version, err)
	}
	pieceCID := "piece-" + versionID
	providerID := onChainID(t, "101")
	dataSetID := onChainID(t, "1001")
	pieceID := onChainIDPtr(t, "1")
	retrievalURL := "https://provider.example/piece/" + versionID
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          version.BucketID,
		ProviderID:        providerID,
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("ensure dataset binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: upload.ID, DataSetID: dataSetID}); err != nil {
		t.Fatalf("mark dataset ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       providerID,
	}}); err != nil {
		t.Fatalf("create upload copy: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      pieceID,
		RetrievalURL: retrievalURL,
	}); err != nil {
		t.Fatalf("mark copy committed: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("bind readable upload: %v", err)
	}
	if finalized, _, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID}); err != nil {
		t.Fatalf("finalize upload: %v", err)
	} else if !finalized {
		t.Fatal("finalize upload = false, want true")
	}
}

type boundedVersionListRepo struct {
	repository.ObjectRepository
	sawUnbounded bool
}

func (r *boundedVersionListRepo) ListVersionsByState(ctx context.Context, state model.ObjectState, limit int) ([]model.ObjectVersion, error) {
	if limit <= 0 {
		r.sawUnbounded = true
	}
	return r.ObjectRepository.ListVersionsByState(ctx, state, limit)
}
