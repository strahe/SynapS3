package repository_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestObjectRepo_UpdateObjectDeletionCacheCleanup(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "deletion-cache-cleanup-bucket")

	deleteVersion := func(t *testing.T, key, versionID string) {
		t.Helper()
		version := newObjectVersion(bucket.ID, key, versionID, 100)
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent: %v", err)
		}
		if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID:  bucket.ID,
			Key:       key,
			VersionID: versionID,
		}); err != nil {
			t.Fatalf("DeleteObjectVersionPermanently: %v", err)
		}
	}

	t.Run("deleted", func(t *testing.T) {
		const versionID = "01J000000000000000CACHE01"
		deleteVersion(t, "deleted.txt", versionID)

		if err := repos.Objects.UpdateObjectDeletionCacheCleanup(ctx, versionID, model.CacheCleanupStatusDeleted, ""); err != nil {
			t.Fatalf("UpdateObjectDeletionCacheCleanup: %v", err)
		}

		var deletion model.ObjectDeletion
		if err := db.NewSelect().Model(&deletion).Where("version_id = ?", versionID).Scan(ctx); err != nil {
			t.Fatalf("select deletion: %v", err)
		}
		if deletion.CacheCleanupStatus != model.CacheCleanupStatusDeleted {
			t.Errorf("CacheCleanupStatus = %q, want %q", deletion.CacheCleanupStatus, model.CacheCleanupStatusDeleted)
		}
		if deletion.CacheError != nil {
			t.Errorf("CacheError = %q, want nil", *deletion.CacheError)
		}
		if deletion.CacheCleanedAt == nil {
			t.Error("CacheCleanedAt is nil")
		}
	})

	t.Run("failed", func(t *testing.T) {
		const (
			versionID  = "01J000000000000000CACHE02"
			cacheError = "disk I/O error"
		)
		deleteVersion(t, "failed.txt", versionID)

		if err := repos.Objects.UpdateObjectDeletionCacheCleanup(ctx, versionID, model.CacheCleanupStatusFailed, cacheError); err != nil {
			t.Fatalf("UpdateObjectDeletionCacheCleanup: %v", err)
		}

		var deletion model.ObjectDeletion
		if err := db.NewSelect().Model(&deletion).Where("version_id = ?", versionID).Scan(ctx); err != nil {
			t.Fatalf("select deletion: %v", err)
		}
		if deletion.CacheCleanupStatus != model.CacheCleanupStatusFailed {
			t.Errorf("CacheCleanupStatus = %q, want %q", deletion.CacheCleanupStatus, model.CacheCleanupStatusFailed)
		}
		if deletion.CacheError == nil || *deletion.CacheError != cacheError {
			t.Errorf("CacheError = %v, want %q", deletion.CacheError, cacheError)
		}
		if deletion.CacheCleanedAt == nil {
			t.Error("CacheCleanedAt is nil")
		}
	})

	t.Run("invalid input", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			versionID string
			status    model.CacheCleanupStatus
		}{
			{name: "empty version ID", status: model.CacheCleanupStatusDeleted},
			{name: "empty status", versionID: "01J000000000000000CACHE03"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := repos.Objects.UpdateObjectDeletionCacheCleanup(ctx, tc.versionID, tc.status, "")
				if !errors.Is(err, repository.ErrInvalidInput) {
					t.Fatalf("UpdateObjectDeletionCacheCleanup error = %v, want ErrInvalidInput", err)
				}
			})
		}
	})

	t.Run("not found", func(t *testing.T) {
		err := repos.Objects.UpdateObjectDeletionCacheCleanup(ctx, "01J000000000000000MISSING", model.CacheCleanupStatusDeleted, "")
		if !errors.Is(err, repository.ErrNotFound) {
			t.Fatalf("UpdateObjectDeletionCacheCleanup error = %v, want ErrNotFound", err)
		}
	})
}

func TestObjectRepo_DeleteObjectVersionPermanentlyRemovesVersionAndQueuesStorageCleanup(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "permanent-delete-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL01", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(old): %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, oldVersion, "bafk2bzacepermdelete")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, oldVersion.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(old): %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL02", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(current): %v", err)
	}

	result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID:  bucket.ID,
		Key:       oldVersion.Key,
		VersionID: oldVersion.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	if result.CacheKey != oldVersion.CacheKey {
		t.Fatalf("cache key = %q, want %q", result.CacheKey, oldVersion.CacheKey)
	}
	if result.StorageCleanupTaskID == nil {
		t.Fatal("expected storage cleanup task id")
	}

	gotVersion, err := repos.Objects.GetVersionByID(ctx, oldVersion.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(deleted): %v", err)
	}
	if gotVersion != nil {
		t.Fatalf("deleted version still exists: %#v", gotVersion)
	}

	var deletionCount int
	if err := db.NewRaw(`SELECT COUNT(*) FROM object_deletions WHERE version_id = ? AND storage_upload_id = ?`, oldVersion.VersionID, uploadID).Scan(ctx, &deletionCount); err != nil {
		t.Fatalf("count object_deletions: %v", err)
	}
	if deletionCount != 1 {
		t.Fatalf("object_deletions count = %d, want 1", deletionCount)
	}

	task, err := repos.Tasks.GetByID(ctx, *result.StorageCleanupTaskID)
	if err != nil || task == nil {
		t.Fatalf("GetByID(cleanup task): task=%v err=%v", task, err)
	}
	if task.Type != model.TaskTypeStorageCleanup || task.RefType != "storage_upload" || task.RefID != uploadID || task.RefVersionID != "" {
		t.Fatalf("cleanup task ref = type:%s ref:%s/%d version:%q, want storage cleanup for upload %d", task.Type, task.RefType, task.RefID, task.RefVersionID, uploadID)
	}
	if task.MaxRetries != 5 {
		t.Fatalf("cleanup task max retries = %d, want default 5", task.MaxRetries)
	}

	var copyCount int
	if err := db.NewRaw(`SELECT COUNT(*) FROM storage_cleanup_copies WHERE task_id = ? AND upload_id = ? AND piece_id IS NOT NULL`, task.ID, uploadID).Scan(ctx, &copyCount); err != nil {
		t.Fatalf("count storage_cleanup_copies: %v", err)
	}
	if copyCount != 1 {
		t.Fatalf("storage_cleanup_copies count = %d, want 1", copyCount)
	}
	var deletedAt string
	if err := db.NewRaw(`SELECT deleted_at FROM object_deletions WHERE version_id = ?`, oldVersion.VersionID).Scan(ctx, &deletedAt); err != nil {
		t.Fatalf("select object deletion deleted_at: %v", err)
	}
	if deletedAt == "" {
		t.Fatal("object deletion deleted_at is empty")
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyAllowsStoppedInProgressState(t *testing.T) {
	tests := []struct {
		name  string
		state model.ObjectState
	}{
		{name: "uploading", state: model.ObjectStateUploading},
		{name: "committing", state: model.ObjectStateCommitting},
		{name: "replicating", state: model.ObjectStateReplicating},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := context.Background()
			bucket := seedBucket(t, db, "permanent-delete-"+tt.name)
			version := newObjectVersion(bucket.ID, "file.txt", model.NewVersionID(), 10)
			if tt.state != model.ObjectStateReplicating {
				version.State = tt.state
			}
			if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
				t.Fatalf("CreateVersionAndSetCurrent: %v", err)
			}
			if tt.state == model.ObjectStateReplicating {
				uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, version, "bafk2bzacepermanentbusy")
				if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, version.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateReplicating); err != nil {
					t.Fatalf("SetVersionStorageUploadAndTransition: %v", err)
				}
			}

			_, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
				BucketID: bucket.ID, Key: version.Key, VersionID: version.VersionID,
			})
			if err != nil {
				t.Fatalf("DeleteObjectVersionPermanently: %v", err)
			}
			got, loadErr := repos.Objects.GetVersionByID(ctx, version.VersionID)
			if loadErr != nil || got != nil {
				t.Fatalf("version after permanent delete = %#v err=%v, want removed", got, loadErr)
			}
		})
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyCoordinatesReplicaRepairTaskStates(t *testing.T) {
	tests := []struct {
		name       string
		status     model.TaskStatus
		wantBusy   bool
		createTask bool
	}{
		{name: "queued", status: model.TaskStatusQueued, wantBusy: true, createTask: true},
		{name: "scheduled", status: model.TaskStatusScheduled, wantBusy: true, createTask: true},
		{name: "waiting", status: model.TaskStatusWaiting, wantBusy: true, createTask: true},
		{name: "running", status: model.TaskStatusRunning, wantBusy: true, createTask: true},
		{name: "exhausted", status: model.TaskStatusExhausted, createTask: true},
		{name: "failed", status: model.TaskStatusFailed, createTask: true},
		{name: "cancelled", status: model.TaskStatusCancelled, createTask: true},
		{name: "completed", status: model.TaskStatusCompleted, createTask: true},
		{name: "no task"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := context.Background()
			fixture := seedPermanentDeleteRepairFixture(t, db, repos, "repair-task-"+strings.ReplaceAll(tt.name, " ", "-"))
			var taskID int64
			if tt.createTask {
				stage := "repair_replica"
				task := &model.Task{
					Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.bucket.ID,
					RefVersionID: fixture.version.VersionID, IdempotencyKey: "upload:repair-data-set:permanent-delete",
					Payload: map[string]interface{}{"storage_data_set_id": fixture.repair.ID, "storage_upload_copy_id": fixture.repairCopy.ID},
					Status:  tt.status, MaxRetries: 5, ScheduledAt: time.Now(),
				}
				if err := repos.Tasks.Create(ctx, task); err != nil {
					t.Fatalf("Create repair task: %v", err)
				}
				taskID = task.ID
			}

			result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
				BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
			})
			if tt.wantBusy {
				if !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) || !errors.Is(err, repository.ErrConflict) {
					t.Fatalf("DeleteObjectVersionPermanently error = %v, want storage-work conflict", err)
				}
				gotVersion, loadErr := repos.Objects.GetVersionByID(ctx, fixture.version.VersionID)
				if loadErr != nil || gotVersion == nil {
					t.Fatalf("version after rejected delete = %#v err=%v, want retained", gotVersion, loadErr)
				}
				got, loadErr := repos.Uploads.GetUploadCopyByID(ctx, fixture.repairCopy.ID)
				if loadErr != nil || got == nil || got.Status != model.StorageUploadCopyStatusPending {
					t.Fatalf("repair copy after rejected delete = %#v err=%v, want pending", got, loadErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DeleteObjectVersionPermanently: %v", err)
			}
			if result.StorageCleanupTaskID == nil {
				t.Fatal("expected cleanup task for committed replica")
			}
			got, loadErr := repos.Uploads.GetUploadCopyByID(ctx, fixture.repairCopy.ID)
			if loadErr != nil || got == nil || got.Status != model.StorageUploadCopyStatusFailed {
				t.Fatalf("repair copy after accepted delete = %#v err=%v, want failed", got, loadErr)
			}
			gotUpload, loadErr := repos.Uploads.GetByID(ctx, fixture.upload.ID)
			if loadErr != nil || gotUpload == nil || gotUpload.Status != model.StorageUploadStatusSuperseded {
				t.Fatalf("upload after last-reference delete = %#v err=%v, want superseded", gotUpload, loadErr)
			}
			if taskID > 0 {
				gotTask, taskErr := repos.Tasks.GetByID(ctx, taskID)
				if taskErr != nil || gotTask == nil || gotTask.Status != tt.status {
					t.Fatalf("repair coordinator after accepted delete = %#v err=%v, want preserved %s", gotTask, taskErr, tt.status)
				}
			}
		})
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyKeepsSubmittedRepairCommit(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "submitted-repair")
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID: fixture.upload.ID, CopyIndex: fixture.repairCopy.CopyIndex, PieceCID: "bafk2bzacepermanentrepair", RetrievalURL: "https://repair.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID: fixture.upload.ID, CopyIndex: fixture.repairCopy.CopyIndex, CommitTransactionID: "0xsubmitted",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitting: %v", err)
	}

	_, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
	})
	if !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) {
		t.Fatalf("DeleteObjectVersionPermanently error = %v, want submitted storage-work conflict", err)
	}
	got, err := repos.Uploads.GetUploadCopyByID(ctx, fixture.repairCopy.ID)
	if err != nil || got == nil || got.Status != model.StorageUploadCopyStatusCommitting || got.CommitTransactionID == nil {
		t.Fatalf("submitted repair copy = %#v err=%v, want retained committing copy", got, err)
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyPreservesSharedRepairUntilLastReference(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "shared-repair")
	follower := newObjectVersion(fixture.bucket.ID, "follower.txt", "01J000000000000000000DEL0S", fixture.version.Size)
	follower.Checksum = fixture.version.Checksum
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(follower): %v", err)
	}
	bindPermanentDeleteFollower(t, repos, fixture.upload.ID, follower)

	first, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
	})
	if err != nil || first.StorageCleanupTaskID == nil {
		t.Fatalf("DeleteObjectVersionPermanently(source): result=%#v err=%v", first, err)
	}
	copyAfterSourceDelete, err := repos.Uploads.GetUploadCopyByID(ctx, fixture.repairCopy.ID)
	if err != nil || copyAfterSourceDelete == nil || copyAfterSourceDelete.Status != model.StorageUploadCopyStatusPending {
		t.Fatalf("shared repair copy after source delete = %#v err=%v, want pending", copyAfterSourceDelete, err)
	}
	if got, err := repos.Objects.GetVersionByID(ctx, follower.VersionID); err != nil || got == nil {
		t.Fatalf("shared follower after source delete = %#v err=%v", got, err)
	}

	second, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: follower.Key, VersionID: follower.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently(last reference): %v", err)
	}
	if second.StorageCleanupTaskID == nil || *second.StorageCleanupTaskID != *first.StorageCleanupTaskID {
		t.Fatalf("cleanup task IDs = first:%v second:%v, want reused task", first.StorageCleanupTaskID, second.StorageCleanupTaskID)
	}
	copyAfterLastDelete, err := repos.Uploads.GetUploadCopyByID(ctx, fixture.repairCopy.ID)
	if err != nil || copyAfterLastDelete == nil || copyAfterLastDelete.Status != model.StorageUploadCopyStatusFailed {
		t.Fatalf("repair copy after last reference delete = %#v err=%v, want failed", copyAfterLastDelete, err)
	}
}

func TestObjectRepo_CreateVersionDoesNotAttachSupersededUploadAfterPermanentDelete(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "permanent-delete-stale-reuse")
	source := newObjectVersion(bucket.ID, "source.txt", model.NewVersionID(), 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(source): %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, source, "bafk2bzacestalereuse")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, source.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(source): %v", err)
	}

	staleUploadID := uploadID
	follower := newObjectVersion(bucket.ID, "follower.txt", model.NewVersionID(), source.Size)
	follower.Checksum = source.Checksum
	follower.StorageUploadID = &staleUploadID
	follower.State = model.ObjectStateStored
	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: bucket.ID, Key: source.Key, VersionID: source.VersionID,
	}); err != nil {
		t.Fatalf("DeleteObjectVersionPermanently(source): %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(stale follower): %v", err)
	}

	got, err := repos.Objects.GetVersionByID(ctx, follower.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID(follower): version=%#v err=%v", got, err)
	}
	if got.StorageUploadID != nil || got.State != model.ObjectStateCached {
		t.Fatalf("follower storage reference = upload:%#v state:%s, want cached without superseded upload", got.StorageUploadID, got.State)
	}
	upload, err := repos.Uploads.GetByID(ctx, uploadID)
	if err != nil || upload == nil || upload.Status != model.StorageUploadStatusSuperseded {
		t.Fatalf("deleted source upload = %#v err=%v, want superseded", upload, err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, follower.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateVersionState follower cached to uploading: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForVersion(ctx, repository.BindReadableUploadForVersionInput{
		UploadID: uploadID, BucketID: bucket.ID, ContentSize: follower.Size, Checksum: follower.Checksum, VersionID: follower.VersionID,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("BindReadableUploadForVersion error = %v, want superseded upload conflict", err)
	}
	if _, _, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: uploadID}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet error = %v, want superseded upload conflict", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
		UploadID: uploadID, CopyIndex: 0, CommitTransactionID: "0xlate",
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkUploadCopyCommitting error = %v, want superseded upload conflict", err)
	}
}

func TestStorageCleanupRepo_SourceVersionIsAnObjectReferenceBeforeBinding(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "storage-cleanup-source-reference")
	version := newObjectVersion(bucket.ID, "source.txt", model.NewVersionID(), 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: version.VersionID, ContentSize: version.Size, Checksum: version.Checksum, RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}

	hasReferences, err := repos.StorageCleanup.UploadHasObjectReferences(ctx, upload.ID)
	if err != nil {
		t.Fatalf("UploadHasObjectReferences: %v", err)
	}
	if !hasReferences {
		t.Fatal("UploadHasObjectReferences = false, want unbound source version to keep upload referenced")
	}
	if err := repos.StorageCleanup.DeleteUploadProvenanceIfUnreferenced(ctx, upload.ID); err != nil {
		t.Fatalf("DeleteUploadProvenanceIfUnreferenced(referenced): %v", err)
	}
	if retained, err := repos.Uploads.GetByID(ctx, upload.ID); err != nil || retained == nil {
		t.Fatalf("referenced upload after provenance cleanup = %#v err=%v, want retained", retained, err)
	}
	if _, err := db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("status = ?", model.StorageUploadStatusFailed).
		Where("id = ?", upload.ID).
		Exec(ctx); err != nil {
		t.Fatalf("retire first upload fixture: %v", err)
	}
	currentUploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, version, "bafk2bzacecurrentsource")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, version.VersionID, currentUploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(current): %v", err)
	}
	hasReferences, err = repos.StorageCleanup.UploadHasObjectReferences(ctx, upload.ID)
	if err != nil {
		t.Fatalf("UploadHasObjectReferences(old): %v", err)
	}
	if hasReferences {
		t.Fatal("UploadHasObjectReferences(old) = true, want rebound source to release historical upload")
	}
	if err := repos.StorageCleanup.DeleteUploadProvenanceIfUnreferenced(ctx, upload.ID); err != nil {
		t.Fatalf("DeleteUploadProvenanceIfUnreferenced(unreferenced): %v", err)
	}
	if removed, err := repos.Uploads.GetByID(ctx, upload.ID); err != nil || removed != nil {
		t.Fatalf("unreferenced upload after provenance cleanup = %#v err=%v, want removed", removed, err)
	}
}

func TestTaskRepo_RetryCannotReattachSupersededUpload(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "retry-superseded-upload")
	stage := "peer_pull"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: fixture.version.ObjectID,
		RefVersionID: fixture.version.VersionID, IdempotencyKey: "upload:retry-superseded-upload",
		Payload: map[string]interface{}{"upload_id": fixture.upload.ID}, Status: model.TaskStatusExhausted,
		MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create exhausted upload task: %v", err)
	}
	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
	}); err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	if err := repos.Tasks.RetryExhausted(ctx, task.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("RetryExhausted error = %v, want superseded upload conflict", err)
	}
	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || got == nil || got.Status != model.TaskStatusExhausted {
		t.Fatalf("task after rejected retry = %#v err=%v, want exhausted", got, err)
	}
}

func TestStorageUploadRepo_AcquireUploadTaskProtectsRunningWorkAndRejectsDeletedWork(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "ordinary-upload-execution")
	stage := "peer_pull"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: fixture.version.ObjectID,
		RefVersionID: fixture.version.VersionID, IdempotencyKey: "upload:ordinary-execution-owner",
		Payload: map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": fixture.repairCopy.CopyIndex},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create ordinary upload task: %v", err)
	}
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil || claimed.ClaimedAt == nil {
		t.Fatalf("ClaimReady(ordinary): task=%#v err=%v", claimed, err)
	}
	if err := repos.Uploads.AcquireUploadTask(ctx, repository.AcquireUploadTaskInput{
		TaskID: claimed.ID, TaskClaimedAt: *claimed.ClaimedAt, UploadID: fixture.upload.ID, VersionID: fixture.version.VersionID,
	}); err != nil {
		t.Fatalf("AcquireUploadTask: %v", err)
	}
	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
	}); !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) {
		t.Fatalf("DeleteObjectVersionPermanently while ordinary task owns execution = %v, want storage busy", err)
	}
	if err := repos.Tasks.Complete(ctx, claimed); err != nil {
		t.Fatalf("Complete ordinary upload task: %v", err)
	}
	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
	}); err != nil {
		t.Fatalf("DeleteObjectVersionPermanently after task stopped: %v", err)
	}

	late := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: fixture.version.ObjectID,
		RefVersionID: fixture.version.VersionID, IdempotencyKey: "upload:ordinary-execution-late",
		Payload: map[string]interface{}{"upload_id": fixture.upload.ID, "copy_index": fixture.repairCopy.CopyIndex},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := repos.Tasks.Create(ctx, late); err != nil {
		t.Fatalf("Create late ordinary upload task: %v", err)
	}
	lateClaim, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || lateClaim == nil || lateClaim.ClaimedAt == nil {
		t.Fatalf("ClaimReady(late ordinary): task=%#v err=%v", lateClaim, err)
	}
	if err := repos.Uploads.AcquireUploadTask(ctx, repository.AcquireUploadTaskInput{
		TaskID: lateClaim.ID, TaskClaimedAt: *lateClaim.ClaimedAt, UploadID: fixture.upload.ID, VersionID: fixture.version.VersionID,
	}); !errors.Is(err, repository.ErrUploadTaskCancelled) {
		t.Fatalf("AcquireUploadTask after permanent delete = %v, want cancelled", err)
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyKeepsSharedVersionsWhileRepairIsRunning(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "shared-running-repair")
	follower := newObjectVersion(fixture.bucket.ID, "follower.txt", "01J000000000000000000DEL0X", fixture.version.Size)
	follower.Checksum = fixture.version.Checksum
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(follower): %v", err)
	}
	bindPermanentDeleteFollower(t, repos, fixture.upload.ID, follower)
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.bucket.ID,
		RefVersionID: fixture.version.VersionID, IdempotencyKey: "upload:repair-data-set:shared-running-repair",
		Payload: map[string]interface{}{"storage_data_set_id": fixture.repair.ID, "storage_upload_copy_id": fixture.repairCopy.ID},
		Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil || claimed.ID != task.ID || claimed.Status != model.TaskStatusRunning || claimed.ClaimedAt == nil {
		t.Fatalf("ClaimReady: task=%#v err=%v", claimed, err)
	}

	for _, version := range []*model.ObjectVersion{fixture.version, follower} {
		_, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID: fixture.bucket.ID, Key: version.Key, VersionID: version.VersionID,
		})
		if !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) {
			t.Fatalf("DeleteObjectVersionPermanently(%s) error = %v, want storage-work conflict", version.VersionID, err)
		}
	}
	for _, versionID := range []string{fixture.version.VersionID, follower.VersionID} {
		got, loadErr := repos.Objects.GetVersionByID(ctx, versionID)
		if loadErr != nil || got == nil {
			t.Fatalf("shared version %s after rejected delete = %#v err=%v, want retained", versionID, got, loadErr)
		}
	}
	copyRow, err := repos.Uploads.GetUploadCopyByID(ctx, fixture.repairCopy.ID)
	if err != nil || copyRow == nil || copyRow.Status != model.StorageUploadCopyStatusPending {
		t.Fatalf("repair copy after rejected shared deletes = %#v err=%v, want pending", copyRow, err)
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyAddsLateCommittedCleanupSnapshot(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "late-cleanup-snapshot")
	follower := newObjectVersion(fixture.bucket.ID, "follower.txt", "01J000000000000000000DEL0T", fixture.version.Size)
	follower.Checksum = fixture.version.Checksum
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(follower): %v", err)
	}
	bindPermanentDeleteFollower(t, repos, fixture.upload.ID, follower)

	first, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
	})
	if err != nil || first.StorageCleanupTaskID == nil {
		t.Fatalf("DeleteObjectVersionPermanently(source): result=%#v err=%v", first, err)
	}
	cleanupCopies, err := repos.StorageCleanup.ListCopiesForTask(ctx, *first.StorageCleanupTaskID)
	if err != nil || len(cleanupCopies) != 1 || cleanupCopies[0].CopyIndex != 0 {
		t.Fatalf("initial cleanup copies = %#v err=%v, want committed copy 0", cleanupCopies, err)
	}
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeStorageCleanup, time.Minute)
	if err != nil || claimed == nil || claimed.ID != *first.StorageCleanupTaskID {
		t.Fatalf("ClaimReady(cleanup): task=%#v err=%v", claimed, err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(ctx, cleanupCopies[0].ID); err != nil {
		t.Fatalf("MarkCopyRemoved: %v", err)
	}
	if err := repos.Tasks.Complete(ctx, claimed); err != nil {
		t.Fatalf("Complete(cleanup): %v", err)
	}

	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		StorageUploadCopyID: fixture.repairCopy.ID,
		UploadID:            fixture.upload.ID,
		CopyIndex:           fixture.repairCopy.CopyIndex,
		PieceCID:            "bafk2bzacepermanentrepair",
		PieceID:             onChainIDPtr(t, "3002"),
		RetrievalURL:        "https://repair.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted(repair): %v", err)
	}
	second, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: follower.Key, VersionID: follower.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently(last reference): %v", err)
	}
	if second.StorageCleanupTaskID == nil || *second.StorageCleanupTaskID != *first.StorageCleanupTaskID {
		t.Fatalf("cleanup task IDs = first:%v second:%v, want reused task", first.StorageCleanupTaskID, second.StorageCleanupTaskID)
	}
	cleanupTask, err := repos.Tasks.GetByID(ctx, *first.StorageCleanupTaskID)
	if err != nil || cleanupTask == nil || cleanupTask.Status != model.TaskStatusQueued {
		t.Fatalf("cleanup task after late snapshot = %#v err=%v, want requeued", cleanupTask, err)
	}
	cleanupCopies, err = repos.StorageCleanup.ListCopiesForTask(ctx, *first.StorageCleanupTaskID)
	if err != nil || len(cleanupCopies) != 2 {
		t.Fatalf("cleanup copies after late commit = %#v err=%v, want two snapshots", cleanupCopies, err)
	}
	if cleanupCopies[0].Status != model.StorageCleanupCopyStatusRemoved || cleanupCopies[1].CopyIndex != 1 || cleanupCopies[1].Status != model.StorageCleanupCopyStatusPending {
		t.Fatalf("cleanup copy states after late commit = %#v, want removed copy 0 and pending copy 1", cleanupCopies)
	}
}

func TestStorageUploadRepo_ExactRepairWritesCannotRevivePermanentlyDeletedCopy(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "exact-write-guard")
	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
	}); err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "piece ready",
			run: func() error {
				return repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
					StorageUploadCopyID: fixture.repairCopy.ID, UploadID: fixture.upload.ID,
					CopyIndex: fixture.repairCopy.CopyIndex, PieceCID: "bafk2bzacepermanentrepair", RetrievalURL: "https://repair.example/piece",
				})
			},
		},
		{
			name: "committing",
			run: func() error {
				return repos.Uploads.MarkUploadCopyCommitting(ctx, repository.MarkUploadCopyCommittingInput{
					StorageUploadCopyID: fixture.repairCopy.ID, UploadID: fixture.upload.ID, CopyIndex: fixture.repairCopy.CopyIndex,
				})
			},
		},
		{
			name: "committed",
			run: func() error {
				return repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
					StorageUploadCopyID: fixture.repairCopy.ID, UploadID: fixture.upload.ID, CopyIndex: fixture.repairCopy.CopyIndex,
					PieceCID: "bafk2bzacepermanentrepair", PieceID: onChainIDPtr(t, "3002"), RetrievalURL: "https://repair.example/piece",
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("exact repair write error = %v, want ErrConflict", err)
			}
		})
	}
	copyRow, err := repos.Uploads.GetUploadCopyByID(ctx, fixture.repairCopy.ID)
	if err != nil || copyRow == nil || copyRow.Status != model.StorageUploadCopyStatusFailed {
		t.Fatalf("repair copy after rejected writes = %#v err=%v, want failed", copyRow, err)
	}
}

func TestStorageUploadRepo_AcquireReplicaRepairItemUsesSharedSurvivor(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "acquire-shared-survivor")
	follower := newObjectVersion(fixture.bucket.ID, "follower.txt", "01J000000000000000000DEL0U", fixture.version.Size)
	follower.Checksum = fixture.version.Checksum
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(follower): %v", err)
	}
	bindPermanentDeleteFollower(t, repos, fixture.upload.ID, follower)
	stage := "repair_replica"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.bucket.ID,
		RefVersionID: fixture.version.VersionID, IdempotencyKey: "upload:repair-data-set:acquire-shared-survivor",
		Payload: map[string]interface{}{"storage_data_set_id": fixture.repair.ID, "storage_upload_copy_id": fixture.repairCopy.ID},
		Status:  model.TaskStatusExhausted, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create repair task: %v", err)
	}
	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
	}); err != nil {
		t.Fatalf("DeleteObjectVersionPermanently(source): %v", err)
	}
	if err := repos.Tasks.RetryExhausted(ctx, task.ID); err != nil {
		t.Fatalf("RetryExhausted: %v", err)
	}
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil || claimed.ID != task.ID || claimed.ClaimedAt == nil {
		t.Fatalf("ClaimReady(repair): task=%#v err=%v", claimed, err)
	}
	item, err := repos.Uploads.AcquireReplicaRepairItem(ctx, repository.AcquireReplicaRepairItemInput{
		TaskID: claimed.ID, TaskClaimedAt: *claimed.ClaimedAt, StorageDataSetID: fixture.repair.ID,
		StorageUploadCopyID: fixture.repairCopy.ID, BucketID: fixture.bucket.ID,
	})
	if err != nil {
		t.Fatalf("AcquireReplicaRepairItem: %v", err)
	}
	if item.Version.VersionID != follower.VersionID || item.Copy.ID != fixture.repairCopy.ID || item.Upload.ID != fixture.upload.ID {
		t.Fatalf("acquired repair item = %#v, want surviving version %s and exact copy/upload", item, follower.VersionID)
	}
	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: fixture.bucket.ID, Key: follower.Key, VersionID: follower.VersionID,
	}); !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) {
		t.Fatalf("DeleteObjectVersionPermanently while repair owns item error = %v, want storage-work conflict", err)
	}
}

func TestStorageUploadRepo_AcquireReplicaRepairItemRejectsDeletedWorkAndLostClaim(t *testing.T) {
	t.Run("deleted work", func(t *testing.T) {
		db := testDB(t)
		repos := repository.NewRepositories(db)
		ctx := context.Background()
		fixture := seedPermanentDeleteRepairFixture(t, db, repos, "acquire-deleted-work")
		stage := "repair_replica"
		task := &model.Task{
			Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.bucket.ID,
			RefVersionID: fixture.version.VersionID, IdempotencyKey: "upload:repair-data-set:acquire-deleted-work",
			Payload: map[string]interface{}{"storage_data_set_id": fixture.repair.ID, "storage_upload_copy_id": fixture.repairCopy.ID},
			Status:  model.TaskStatusExhausted, MaxRetries: 5, ScheduledAt: time.Now(),
		}
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create repair task: %v", err)
		}
		if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID: fixture.bucket.ID, Key: fixture.version.Key, VersionID: fixture.version.VersionID,
		}); err != nil {
			t.Fatalf("DeleteObjectVersionPermanently: %v", err)
		}
		if err := repos.Tasks.RetryExhausted(ctx, task.ID); err != nil {
			t.Fatalf("RetryExhausted: %v", err)
		}
		claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
		if err != nil || claimed == nil || claimed.ClaimedAt == nil {
			t.Fatalf("ClaimReady: task=%#v err=%v", claimed, err)
		}
		_, err = repos.Uploads.AcquireReplicaRepairItem(ctx, repository.AcquireReplicaRepairItemInput{
			TaskID: claimed.ID, TaskClaimedAt: *claimed.ClaimedAt, StorageDataSetID: fixture.repair.ID,
			StorageUploadCopyID: fixture.repairCopy.ID, BucketID: fixture.bucket.ID,
		})
		if !errors.Is(err, repository.ErrReplicaRepairItemCancelled) {
			t.Fatalf("AcquireReplicaRepairItem error = %v, want cancelled item", err)
		}
	})

	t.Run("lost claim", func(t *testing.T) {
		db := testDB(t)
		repos := repository.NewRepositories(db)
		ctx := context.Background()
		fixture := seedPermanentDeleteRepairFixture(t, db, repos, "acquire-lost-claim")
		stage := "repair_replica"
		task := &model.Task{
			Type: model.TaskTypeUpload, Stage: &stage, RefType: "bucket", RefID: fixture.bucket.ID,
			RefVersionID: fixture.version.VersionID, IdempotencyKey: "upload:repair-data-set:acquire-lost-claim",
			Payload: map[string]interface{}{"storage_data_set_id": fixture.repair.ID, "storage_upload_copy_id": fixture.repairCopy.ID},
			Status:  model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
		}
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create repair task: %v", err)
		}
		claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
		if err != nil || claimed == nil || claimed.ClaimedAt == nil {
			t.Fatalf("ClaimReady: task=%#v err=%v", claimed, err)
		}
		if err := repos.Tasks.Complete(ctx, claimed); err != nil {
			t.Fatalf("Complete: %v", err)
		}
		_, err = repos.Uploads.AcquireReplicaRepairItem(ctx, repository.AcquireReplicaRepairItemInput{
			TaskID: claimed.ID, TaskClaimedAt: *claimed.ClaimedAt, StorageDataSetID: fixture.repair.ID,
			StorageUploadCopyID: fixture.repairCopy.ID, BucketID: fixture.bucket.ID,
		})
		if !errors.Is(err, repository.ErrTaskClaimLost) {
			t.Fatalf("AcquireReplicaRepairItem error = %v, want lost claim", err)
		}
	})
}

type permanentDeleteRepairFixture struct {
	bucket     *model.Bucket
	version    *model.ObjectVersion
	upload     *model.StorageUpload
	repair     *model.StorageDataSet
	repairCopy *model.StorageUploadCopy
}

func seedPermanentDeleteRepairFixture(t *testing.T, db *bun.DB, repos *repository.Repositories, suffix string) permanentDeleteRepairFixture {
	t.Helper()
	ctx := context.Background()
	bucket := seedBucket(t, db, "permanent-delete-"+suffix)
	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0R", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: version.VersionID, ContentSize: version.Size, Checksum: version.Checksum, RequestedCopies: 2,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateVersionState cached to uploading: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding primary: %v", err)
	}
	repair, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "202"), CopyIndex: 1, CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding repair: %v", err)
	}
	for _, input := range []repository.MarkDataSetReadyInput{
		{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001")},
		{ID: repair.ID, UploadID: upload.ID, DataSetID: onChainID(t, "2002")},
	} {
		if err := repos.Uploads.MarkDataSetReady(ctx, input); err != nil {
			t.Fatalf("MarkDataSetReady(%d): %v", input.ID, err)
		}
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: repair.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID: upload.ID, CopyIndex: 0, PieceCID: "bafk2bzacepermanentrepair", PieceID: onChainIDPtr(t, "3001"), RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted primary: %v", err)
	}
	bindReadableUploadForContent(t, repos, upload.ID, bucket.ID, version.Size, version.Checksum)
	gotVersion, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || gotVersion == nil || gotVersion.State != model.ObjectStateReplicating || gotVersion.StorageUploadID == nil || *gotVersion.StorageUploadID != upload.ID {
		t.Fatalf("source after first readable replica = %#v err=%v, want replicating on upload %d", gotVersion, err, upload.ID)
	}
	if err := repos.Uploads.MarkDataSetUnavailable(ctx, repair.ID, "temporary outage"); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	repairCopy, err := repos.Uploads.GetUploadCopy(ctx, upload.ID, 1)
	if err != nil || repairCopy == nil {
		t.Fatalf("GetUploadCopy(repair): copy=%#v err=%v", repairCopy, err)
	}
	return permanentDeleteRepairFixture{bucket: bucket, version: version, upload: upload, repair: repair, repairCopy: repairCopy}
}

func bindPermanentDeleteFollower(t *testing.T, repos *repository.Repositories, uploadID int64, version *model.ObjectVersion) {
	t.Helper()
	ctx := context.Background()
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateVersionState follower cached to uploading: %v", err)
	}
	refs, err := repos.Uploads.BindReadableUploadForVersion(ctx, repository.BindReadableUploadForVersionInput{
		UploadID: uploadID, BucketID: version.BucketID, ContentSize: version.Size, Checksum: version.Checksum, VersionID: version.VersionID,
	})
	if err != nil {
		t.Fatalf("BindReadableUploadForVersion(follower): %v", err)
	}
	if len(refs) != 1 || refs[0].VersionID != version.VersionID {
		t.Fatalf("BindReadableUploadForVersion refs = %#v, want follower %s", refs, version.VersionID)
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyUsesConfiguredStorageCleanupMaxRetries(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "permanent-delete-retries-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0A", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(old): %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, oldVersion, "bafk2bzaceretryzero")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, oldVersion.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(old): %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0B", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(current): %v", err)
	}

	maxRetries := 0
	result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID:                 bucket.ID,
		Key:                      oldVersion.Key,
		VersionID:                oldVersion.VersionID,
		StorageCleanupMaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	if result.StorageCleanupTaskID == nil {
		t.Fatal("expected storage cleanup task id")
	}
	task, err := repos.Tasks.GetByID(ctx, *result.StorageCleanupTaskID)
	if err != nil || task == nil {
		t.Fatalf("GetByID(cleanup task): task=%v err=%v", task, err)
	}
	if task.MaxRetries != 0 {
		t.Fatalf("cleanup task max retries = %d, want explicit 0", task.MaxRetries)
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyQueuesCleanupForSharedStorageUpload(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "shared-permanent-delete-bucket")

	leader := newObjectVersion(bucket.ID, "leader.txt", "01J000000000000000000DEL03", 10)
	leader.Checksum = "shared-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, leader); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(leader): %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, leader, "bafk2bzaceshareddelete")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, leader.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(leader): %v", err)
	}
	replacement := newObjectVersion(bucket.ID, "leader.txt", "01J000000000000000000DEL06", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, replacement); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(replacement): %v", err)
	}

	follower := newObjectVersion(bucket.ID, "follower.txt", "01J000000000000000000DEL04", 10)
	follower.Checksum = leader.Checksum
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(follower): %v", err)
	}
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, follower.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(follower): %v", err)
	}

	result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID:  bucket.ID,
		Key:       leader.Key,
		VersionID: leader.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	if result.StorageCleanupTaskID == nil {
		t.Fatal("expected storage cleanup task id for worker reference recheck")
	}

	gotFollower, err := repos.Objects.GetVersionByID(ctx, follower.VersionID)
	if err != nil || gotFollower == nil {
		t.Fatalf("GetVersionByID(follower): version=%v err=%v", gotFollower, err)
	}
	if gotFollower.StorageUploadID == nil || *gotFollower.StorageUploadID != uploadID {
		t.Fatalf("follower storage upload = %#v, want %d", gotFollower.StorageUploadID, uploadID)
	}

	var cleanupTasks int
	if err := db.NewRaw(`SELECT COUNT(*) FROM tasks WHERE type = ? AND ref_type = ? AND ref_id = ?`, model.TaskTypeStorageCleanup, "storage_upload", uploadID).Scan(ctx, &cleanupTasks); err != nil {
		t.Fatalf("count cleanup tasks: %v", err)
	}
	if cleanupTasks != 1 {
		t.Fatalf("cleanup task count = %d, want one idempotent task", cleanupTasks)
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyQueuesCleanupForSharedPieceIdentity(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "shared-piece-delete-bucket")

	oldVersion := newObjectVersion(bucket.ID, "old.txt", "01J000000000000000000DEL07", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(old): %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, oldVersion, "bafk2bzacesharedpiece")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, oldVersion.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(old): %v", err)
	}
	replacement := newObjectVersion(bucket.ID, "old.txt", "01J000000000000000000DEL08", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, replacement); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(replacement): %v", err)
	}

	var dataSetID string
	if err := db.NewRaw(`SELECT storage_data_set.data_set_id
		FROM storage_upload_copies AS storage_copy
		JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		WHERE storage_copy.upload_id = ?`, uploadID).Scan(ctx, &dataSetID); err != nil {
		t.Fatalf("load storage data set id: %v", err)
	}
	otherVersion := newObjectVersion(bucket.ID, "other.txt", "01J000000000000000000DEL09", 30)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, otherVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(other): %v", err)
	}
	otherUpload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: otherVersion.VersionID,
		ContentSize:     otherVersion.Size,
		Checksum:        otherVersion.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt(other): %v", err)
	}
	seedCommittedUploadCopies(t, repos, bucket.ID, otherUpload.ID, "bafk2bzacesharedpiece", []storageUploadCopySeed{
		{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, dataSetID), PieceID: onChainIDPtr(t, "2001"), TransferMethod: model.StorageCopyTransferMethodPeerPull, RetrievalURL: strPtr("https://provider.example/" + otherVersion.VersionID)},
	})
	bindReadableUploadForContent(t, repos, otherUpload.ID, bucket.ID, otherVersion.Size, otherVersion.Checksum)
	finalizeUploadForTest(t, repos, otherUpload.ID)
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, otherVersion.VersionID, otherUpload.ID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(other): %v", err)
	}

	result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID:  bucket.ID,
		Key:       oldVersion.Key,
		VersionID: oldVersion.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	if result.StorageCleanupTaskID == nil {
		t.Fatal("expected storage cleanup task id for worker piece reference recheck")
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyRequeuesRetainedCleanupTask(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "retained-cleanup-requeue-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0C", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(old): %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, oldVersion, "bafk2bzaceretained")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, oldVersion.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(old): %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0D", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(current): %v", err)
	}

	now := time.Now()
	statusMessage := "Remote replicas kept because another object version still uses them"
	task := &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          uploadID,
		IdempotencyKey: "storage_cleanup:" + strconv.FormatInt(uploadID, 10),
		Status:         model.TaskStatusCompleted,
		MaxRetries:     1,
		StatusMessage:  &statusMessage,
		ScheduledAt:    now.Add(-time.Hour),
		CompletedAt:    &now,
		Payload: map[string]interface{}{
			"storage_upload_id":       uploadID,
			"deleted_source_version":  "stale-version",
			"deleted_source_versions": []string{"stale-version"},
		},
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create retained cleanup task: %v", err)
	}
	if _, err := db.NewInsert().Model(&model.StorageCleanupCopy{
		TaskID:          task.ID,
		UploadID:        uploadID,
		CopyIndex:       0,
		ProviderID:      onChainIDPtr(t, "101"),
		DataSetID:       onChainIDPtr(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "5001"),
		PieceID:         onChainIDPtr(t, "2001"),
		PieceCID:        "bafk2bzaceretained",
		Status:          model.StorageCleanupCopyStatusPending,
	}).Exec(ctx); err != nil {
		t.Fatalf("insert retained cleanup copy: %v", err)
	}

	maxRetries := 9
	result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID:                 bucket.ID,
		Key:                      oldVersion.Key,
		VersionID:                oldVersion.VersionID,
		StorageCleanupMaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	if result.StorageCleanupTaskID == nil || *result.StorageCleanupTaskID != task.ID {
		t.Fatalf("cleanup task id = %v, want retained task %d", result.StorageCleanupTaskID, task.ID)
	}
	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID(retained cleanup): task=%v err=%v", got, err)
	}
	if got.Status != model.TaskStatusQueued || got.CompletedAt != nil || got.StatusMessage != nil || got.LastError != nil || got.WaitReason != nil || got.RetryCount != 0 {
		t.Fatalf("requeued task diagnostics = status:%s completed:%v message:%v error:%v wait:%v retries:%d, want clean queued task", got.Status, got.CompletedAt, got.StatusMessage, got.LastError, got.WaitReason, got.RetryCount)
	}
	if got.MaxRetries != 9 {
		t.Fatalf("requeued task max retries = %d, want 9", got.MaxRetries)
	}
	if got.Payload["deleted_source_version"] != "stale-version" {
		t.Fatalf("deleted_source_version = %#v, want stale-version", got.Payload["deleted_source_version"])
	}
	gotVersions := payloadStringSlice(got.Payload, "deleted_source_versions")
	wantVersions := []string{"stale-version", oldVersion.VersionID}
	if len(gotVersions) != len(wantVersions) {
		t.Fatalf("deleted_source_versions = %#v, want %#v", gotVersions, wantVersions)
	}
	for i := range wantVersions {
		if gotVersions[i] != wantVersions[i] {
			t.Fatalf("deleted_source_versions = %#v, want %#v", gotVersions, wantVersions)
		}
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyReusesActiveCleanupTask(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "active-cleanup-reuse-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0G", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(old): %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, oldVersion, "bafk2bzaceactivecleanup")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, oldVersion.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(old): %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0H", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(current): %v", err)
	}

	task := &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          uploadID,
		IdempotencyKey: "storage_cleanup:" + strconv.FormatInt(uploadID, 10),
		Status:         model.TaskStatusQueued,
		MaxRetries:     1,
		ScheduledAt:    time.Now(),
		Payload: map[string]interface{}{
			"storage_upload_id":      uploadID,
			"deleted_source_version": "existing-version",
		},
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create active cleanup task: %v", err)
	}

	maxRetries := 9
	result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID:                 bucket.ID,
		Key:                      oldVersion.Key,
		VersionID:                oldVersion.VersionID,
		StorageCleanupMaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	if result.StorageCleanupTaskID == nil || *result.StorageCleanupTaskID != task.ID {
		t.Fatalf("cleanup task id = %v, want active task %d", result.StorageCleanupTaskID, task.ID)
	}
	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID(active cleanup): task=%v err=%v", got, err)
	}
	if got.Status != model.TaskStatusQueued || got.MaxRetries != 1 || got.Payload["deleted_source_version"] != "existing-version" {
		t.Fatalf("active task changed = status:%s maxRetries:%d payload:%#v, want preserved status and retry config", got.Status, got.MaxRetries, got.Payload)
	}
	gotVersions := payloadStringSlice(got.Payload, "deleted_source_versions")
	wantVersions := []string{"existing-version", oldVersion.VersionID}
	if len(gotVersions) != len(wantVersions) {
		t.Fatalf("deleted_source_versions = %#v, want %#v", gotVersions, wantVersions)
	}
	for i := range wantVersions {
		if gotVersions[i] != wantVersions[i] {
			t.Fatalf("deleted_source_versions = %#v, want %#v", gotVersions, wantVersions)
		}
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyDoesNotRequeueTerminalCleanupTask(t *testing.T) {
	for _, status := range []model.TaskStatus{
		model.TaskStatusFailed,
		model.TaskStatusExhausted,
		model.TaskStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			ctx := context.Background()
			bucket := seedBucket(t, db, "terminal-cleanup-"+string(status)+"-bucket")

			oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0E", 10)
			if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
				t.Fatalf("CreateVersionAndSetCurrent(old): %v", err)
			}
			uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, oldVersion, "bafk2bzaceterminalcleanup")
			if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, oldVersion.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
				t.Fatalf("SetVersionStorageUploadAndTransition(old): %v", err)
			}
			currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0F", 20)
			if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
				t.Fatalf("CreateVersionAndSetCurrent(current): %v", err)
			}

			lastError := "remote cleanup stopped"
			task := &model.Task{
				Type:           model.TaskTypeStorageCleanup,
				RefType:        "storage_upload",
				RefID:          uploadID,
				IdempotencyKey: "storage_cleanup:" + strconv.FormatInt(uploadID, 10),
				Status:         status,
				MaxRetries:     1,
				LastError:      &lastError,
				ScheduledAt:    time.Now(),
				Payload:        map[string]interface{}{"storage_upload_id": uploadID},
			}
			if err := repos.Tasks.Create(ctx, task); err != nil {
				t.Fatalf("Create terminal cleanup task: %v", err)
			}

			result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
				BucketID:  bucket.ID,
				Key:       oldVersion.Key,
				VersionID: oldVersion.VersionID,
			})
			if err != nil {
				t.Fatalf("DeleteObjectVersionPermanently: %v", err)
			}
			if result.StorageCleanupTaskID == nil || *result.StorageCleanupTaskID != task.ID {
				t.Fatalf("cleanup task id = %v, want terminal task %d", result.StorageCleanupTaskID, task.ID)
			}
			got, err := repos.Tasks.GetByID(ctx, task.ID)
			if err != nil || got == nil {
				t.Fatalf("GetByID(terminal cleanup): task=%v err=%v", got, err)
			}
			if got.Status != status || got.LastError == nil || *got.LastError != lastError {
				t.Fatalf("terminal task changed = status:%s error:%v, want preserved %s task", got.Status, got.LastError, status)
			}
		})
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyDeletesCurrentDataVersionAndPromotesPreviousVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "current-permanent-delete-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL05", 10)
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(old): %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL06", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(current): %v", err)
	}

	result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID:  bucket.ID,
		Key:       currentVersion.Key,
		VersionID: currentVersion.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	if result.CacheKey != currentVersion.CacheKey {
		t.Fatalf("cache key = %q, want %q", result.CacheKey, currentVersion.CacheKey)
	}

	gotDeleted, err := repos.Objects.GetVersionByID(ctx, currentVersion.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(deleted): %v", err)
	}
	if gotDeleted != nil {
		t.Fatalf("deleted current version still exists: %#v", gotDeleted)
	}
	gotOld, err := repos.Objects.GetVersionByID(ctx, oldVersion.VersionID)
	if err != nil || gotOld == nil {
		t.Fatalf("GetVersionByID(promoted): version=%v err=%v", gotOld, err)
	}
	if !gotOld.IsCurrent {
		t.Fatalf("old version is_current = false, want true")
	}
	gotObject, err := repos.Objects.GetObjectByID(ctx, objectID)
	if err != nil || gotObject == nil {
		t.Fatalf("GetObjectByID: object=%v err=%v", gotObject, err)
	}
}

func TestObjectRepo_DeleteObjectVersionPermanentlyDeletesObjectWhenOnlyVersionWasCurrent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "single-current-permanent-delete-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL07", 10)
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}

	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID:  bucket.ID,
		Key:       version.Key,
		VersionID: version.VersionID,
	}); err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}

	gotVersion, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(deleted): %v", err)
	}
	if gotVersion != nil {
		t.Fatalf("deleted only version still exists: %#v", gotVersion)
	}
	gotObject, err := repos.Objects.GetObjectByID(ctx, objectID)
	if err != nil {
		t.Fatalf("GetObjectByID: %v", err)
	}
	if gotObject != nil {
		t.Fatalf("object row still exists after deleting only version: %#v", gotObject)
	}
}

func TestObjectRepo_DeleteDeletedObjectPermanentlyRemovesAllVersionsAndQueuesStorageCleanup(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "deleted-object-permanent-delete-bucket")

	first := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DOB01", 10)
	first.Checksum = "deleted-object-shared-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, first); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(first): %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, first, "bafk2bzacedeletedobject")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, first.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(first): %v", err)
	}

	second := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DOB02", 10)
	second.Checksum = first.Checksum
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, second); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(second): %v", err)
	}
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, second.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(second): %v", err)
	}

	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J000000000000000000DOB03")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	result, err := repos.Objects.DeleteDeletedObjectPermanently(ctx, repository.DeleteDeletedObjectInput{
		BucketID:              bucket.ID,
		Key:                   "file.txt",
		DeleteMarkerVersionID: marker.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteDeletedObjectPermanently: %v", err)
	}
	if result.DataVersionsDeleted != 2 || result.DeleteMarkersDeleted != 1 {
		t.Fatalf("deleted counts = data:%d markers:%d, want 2 data and 1 marker", result.DataVersionsDeleted, result.DeleteMarkersDeleted)
	}
	if len(result.DeletedVersions) != 2 {
		t.Fatalf("deleted version snapshots len = %d, want 2", len(result.DeletedVersions))
	}
	if len(result.StorageCleanupTaskIDs) != 1 {
		t.Fatalf("storage cleanup task ids = %#v, want one task", result.StorageCleanupTaskIDs)
	}

	gotObject, err := repos.Objects.GetObjectByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetObjectByBucketAndKey: %v", err)
	}
	if gotObject != nil {
		t.Fatalf("object still exists after deleted object permanent delete: %#v", gotObject)
	}
	for _, versionID := range []string{first.VersionID, second.VersionID, marker.VersionID} {
		gotVersion, err := repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil {
			t.Fatalf("GetVersionByID(%s): %v", versionID, err)
		}
		if gotVersion != nil {
			t.Fatalf("version %s still exists after deleted object permanent delete", versionID)
		}
	}

	var deletionCount int
	if err := db.NewRaw(`SELECT COUNT(*) FROM object_deletions WHERE key = ?`, "file.txt").Scan(ctx, &deletionCount); err != nil {
		t.Fatalf("count object_deletions: %v", err)
	}
	if deletionCount != 2 {
		t.Fatalf("object_deletions count = %d, want one row for each data version", deletionCount)
	}

	var markerDeletionCount int
	if err := db.NewRaw(`SELECT COUNT(*) FROM object_deletions WHERE version_id = ?`, marker.VersionID).Scan(ctx, &markerDeletionCount); err != nil {
		t.Fatalf("count marker object_deletions: %v", err)
	}
	if markerDeletionCount != 0 {
		t.Fatalf("delete marker audit rows = %d, want 0", markerDeletionCount)
	}

	task, err := repos.Tasks.GetByID(ctx, result.StorageCleanupTaskIDs[0])
	if err != nil || task == nil {
		t.Fatalf("GetByID(cleanup task): task=%v err=%v", task, err)
	}
	if task.RefID != uploadID || task.Type != model.TaskTypeStorageCleanup {
		t.Fatalf("cleanup task = type:%s refID:%d, want storage cleanup for upload %d", task.Type, task.RefID, uploadID)
	}
}

func TestObjectRepo_DeleteDeletedObjectPermanentlyReportsActiveStorageWork(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "deleted-object-active-storage-work")
	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DEL0V", 10)
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	stage := "prepare_upload"
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: objectID, RefVersionID: version.VersionID,
		IdempotencyKey: "upload:" + version.VersionID, Status: model.TaskStatusQueued, MaxRetries: 5, ScheduledAt: time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create upload task: %v", err)
	}
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, version.Key, "01J000000000000000000DEL0W")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	_, err = repos.Objects.DeleteDeletedObjectPermanently(ctx, repository.DeleteDeletedObjectInput{
		BucketID: bucket.ID, Key: version.Key, DeleteMarkerVersionID: marker.VersionID,
	})
	if !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) {
		t.Fatalf("DeleteDeletedObjectPermanently error = %v, want storage-work conflict", err)
	}
	if got, loadErr := repos.Objects.GetVersionByID(ctx, version.VersionID); loadErr != nil || got == nil {
		t.Fatalf("data version after rejected delete = %#v err=%v", got, loadErr)
	}
	if got, loadErr := repos.Objects.GetVersionByID(ctx, marker.VersionID); loadErr != nil || got == nil {
		t.Fatalf("delete marker after rejected delete = %#v err=%v", got, loadErr)
	}
}

func TestObjectRepo_DeleteDeletedObjectPermanentlyCancelsStoppedReplicaRepair(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	fixture := seedPermanentDeleteRepairFixture(t, db, repos, "deleted-object-stopped-repair")
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, fixture.bucket.ID, fixture.version.Key, model.NewVersionID())
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	if _, err := repos.Objects.DeleteDeletedObjectPermanently(ctx, repository.DeleteDeletedObjectInput{
		BucketID: fixture.bucket.ID, Key: fixture.version.Key, DeleteMarkerVersionID: marker.VersionID,
	}); err != nil {
		t.Fatalf("DeleteDeletedObjectPermanently: %v", err)
	}
	copyRow, err := repos.Uploads.GetUploadCopyByID(ctx, fixture.repairCopy.ID)
	if err != nil || copyRow == nil || copyRow.Status != model.StorageUploadCopyStatusFailed {
		t.Fatalf("repair copy after deleted-object cleanup = %#v err=%v, want failed", copyRow, err)
	}
	upload, err := repos.Uploads.GetByID(ctx, fixture.upload.ID)
	if err != nil || upload == nil || upload.Status != model.StorageUploadStatusSuperseded {
		t.Fatalf("upload after deleted-object cleanup = %#v err=%v, want superseded", upload, err)
	}
}

func TestObjectRepo_DeleteDeletedObjectPermanentlyScopesStorageCleanupPayloadsByUpload(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "deleted-object-scoped-cleanup-payload-bucket")

	acceptUpload := func(version *model.ObjectVersion, pieceCID string) int64 {
		t.Helper()
		upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
			BucketID:        bucket.ID,
			SourceVersionID: version.VersionID,
			ContentSize:     version.Size,
			Checksum:        version.Checksum,
			RequestedCopies: 1,
		})
		if err != nil {
			t.Fatalf("StartObjectUploadAttempt(%s): %v", version.VersionID, err)
		}
		uploadIDText := strconv.FormatInt(upload.ID, 10)
		seedCommittedUploadCopies(t, repos, bucket.ID, upload.ID, pieceCID, []storageUploadCopySeed{{
			ProviderID:     onChainIDPtr(t, "101"),
			DataSetID:      onChainIDPtr(t, "1001"+uploadIDText),
			PieceID:        onChainIDPtr(t, "2001"+uploadIDText),
			TransferMethod: model.StorageCopyTransferMethodIngress,
			RetrievalURL:   strPtr("https://provider.example/" + version.VersionID),
			IsNewDataSet:   true,
		}})
		bindReadableUploadForContent(t, repos, upload.ID, bucket.ID, version.Size, version.Checksum)
		finalizeUploadForTest(t, repos, upload.ID)
		return upload.ID
	}

	first := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DOB08", 10)
	first.Checksum = "deleted-object-first-upload-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, first); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(first): %v", err)
	}
	firstUploadID := acceptUpload(first, "bafk2bzacescopedpayloadone")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, first.VersionID, firstUploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(first): %v", err)
	}

	second := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DOB09", 20)
	second.Checksum = "deleted-object-second-upload-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, second); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(second): %v", err)
	}
	secondUploadID := acceptUpload(second, "bafk2bzacescopedpayloadtwo")
	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, second.VersionID, secondUploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition(second): %v", err)
	}

	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J000000000000000000DOB0A")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	result, err := repos.Objects.DeleteDeletedObjectPermanently(ctx, repository.DeleteDeletedObjectInput{
		BucketID:              bucket.ID,
		Key:                   "file.txt",
		DeleteMarkerVersionID: marker.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteDeletedObjectPermanently: %v", err)
	}
	if len(result.StorageCleanupTaskIDs) != 2 {
		t.Fatalf("storage cleanup task ids = %#v, want one task per storage upload", result.StorageCleanupTaskIDs)
	}

	wantVersionByUpload := map[int64]string{
		firstUploadID:  first.VersionID,
		secondUploadID: second.VersionID,
	}
	seenUploads := make(map[int64]bool)
	for _, taskID := range result.StorageCleanupTaskIDs {
		task, err := repos.Tasks.GetByID(ctx, taskID)
		if err != nil || task == nil {
			t.Fatalf("GetByID(cleanup task %d): task=%v err=%v", taskID, task, err)
		}
		wantVersion, ok := wantVersionByUpload[task.RefID]
		if !ok {
			t.Fatalf("cleanup task %d ref upload = %d, want one of %#v", taskID, task.RefID, wantVersionByUpload)
		}
		gotVersions := payloadStringSlice(task.Payload, "deleted_source_versions")
		if len(gotVersions) != 1 || gotVersions[0] != wantVersion {
			t.Fatalf("cleanup task %d deleted_source_versions = %#v, want only %q", taskID, gotVersions, wantVersion)
		}
		if gotLegacy, ok := task.Payload["deleted_source_version"].(string); !ok || gotLegacy != wantVersion {
			t.Fatalf("cleanup task %d deleted_source_version = %#v, want %q", taskID, task.Payload["deleted_source_version"], wantVersion)
		}
		seenUploads[task.RefID] = true
	}
	if len(seenUploads) != len(wantVersionByUpload) {
		t.Fatalf("cleanup task uploads = %#v, want uploads %#v", seenUploads, wantVersionByUpload)
	}
}

func TestObjectRepo_DeleteDeletedObjectPermanentlyRejectsStaleMarker(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "deleted-object-stale-marker-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DOB04", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J000000000000000000DOB05")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}
	replacement := newObjectVersion(bucket.ID, "file.txt", "01J000000000000000000DOB06", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, replacement); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent(replacement): %v", err)
	}

	_, err = repos.Objects.DeleteDeletedObjectPermanently(ctx, repository.DeleteDeletedObjectInput{
		BucketID:              bucket.ID,
		Key:                   "file.txt",
		DeleteMarkerVersionID: marker.VersionID,
	})
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("DeleteDeletedObjectPermanently error = %v, want ErrConflict", err)
	}

	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("original data version should remain: version=%v err=%v", got, err)
	}
}

func TestObjectRepo_DeleteDeletedObjectPermanentlyAllowsMarkerOnlyObject(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "deleted-object-marker-only-bucket")

	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "missing.txt", "01J000000000000000000DOB07")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	result, err := repos.Objects.DeleteDeletedObjectPermanently(ctx, repository.DeleteDeletedObjectInput{
		BucketID:              bucket.ID,
		Key:                   "missing.txt",
		DeleteMarkerVersionID: marker.VersionID,
	})
	if err != nil {
		t.Fatalf("DeleteDeletedObjectPermanently: %v", err)
	}
	if result.DataVersionsDeleted != 0 || result.DeleteMarkersDeleted != 1 || len(result.StorageCleanupTaskIDs) != 0 {
		t.Fatalf("result = data:%d markers:%d cleanup:%#v, want marker-only delete", result.DataVersionsDeleted, result.DeleteMarkersDeleted, result.StorageCleanupTaskIDs)
	}
	gotObject, err := repos.Objects.GetObjectByBucketAndKey(ctx, bucket.ID, "missing.txt")
	if err != nil {
		t.Fatalf("GetObjectByBucketAndKey: %v", err)
	}
	if gotObject != nil {
		t.Fatalf("marker-only object still exists: %#v", gotObject)
	}
}

func payloadStringSlice(payload map[string]interface{}, key string) []string {
	values, ok := payload[key].([]interface{})
	if ok {
		out := make([]string, 0, len(values))
		for _, value := range values {
			if text, ok := value.(string); ok {
				out = append(out, text)
			}
		}
		return out
	}
	textValues, ok := payload[key].([]string)
	if ok {
		return textValues
	}
	return nil
}
