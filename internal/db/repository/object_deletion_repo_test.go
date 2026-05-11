package repository_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

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
