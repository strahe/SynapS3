package repository_test

import (
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestIngressProgressRejectsStaleTransferWriters(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "progress-fence")
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 10,
		Checksum: testutil.StorageChecksum("progress-fence"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}
	binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByContentID: content.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0, ProviderID: binding.ProviderID,
		TransferMethod: model.StorageCopyTransferMethodIngress,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "progress.bin", ContentID: &content.ID,
		Size: content.ContentSize, ETag: "progress", ContentType: "application/octet-stream",
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 1 {
		t.Fatalf("ListCopies = %#v, err=%v", copies, err)
	}
	enqueueTask := func(key string) *model.Task {
		taskRow, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
			Type: model.TaskTypeStorageStore, IdempotencyKey: key, InputVersion: 1,
			Input: []byte(`{}`), InputHash: key, Status: model.TaskStatusPending,
			ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
		})
		if err != nil || !created {
			t.Fatalf("Enqueue(%s) = %#v, created=%v, err=%v", key, taskRow, created, err)
		}
		return taskRow
	}
	firstTask := enqueueTask("progress-first")
	if err := repos.Contents.BindCopyTask(t.Context(), copies[0].ID, 1, firstTask.ID); err != nil {
		t.Fatalf("BindCopyTask(first): %v", err)
	}
	staleClaim, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || staleClaim == nil || staleClaim.ID != firstTask.ID {
		t.Fatalf("ClaimNext(first) = %#v, err=%v", staleClaim, err)
	}
	if _, err := repos.Contents.AuthorizeCopyTask(t.Context(), copies[0].ID, 1, firstTask.ID, staleClaim.ClaimGeneration); err != nil {
		t.Fatalf("AuthorizeCopyTask(first): %v", err)
	}
	if _, err := db.NewRaw(`UPDATE tasks SET lease_until = ? WHERE id = ?`, time.Now().Add(-time.Second), firstTask.ID).Exec(t.Context()); err != nil {
		t.Fatalf("expire first copy claim: %v", err)
	}
	freshClaim, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || freshClaim == nil || freshClaim.ID != firstTask.ID {
		t.Fatalf("ClaimNext(fresh) = %#v, err=%v", freshClaim, err)
	}
	if _, err := repos.Contents.AuthorizeCopyTask(t.Context(), copies[0].ID, 1, firstTask.ID, staleClaim.ClaimGeneration); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("AuthorizeCopyTask(stale) error = %v, want conflict", err)
	}
	if _, err := repos.Contents.AuthorizeCopyTask(t.Context(), copies[0].ID, 1, firstTask.ID, freshClaim.ClaimGeneration); err != nil {
		t.Fatalf("AuthorizeCopyTask(fresh): %v", err)
	}
	if _, err := repos.Contents.BeginIngressStoreProgress(t.Context(), repository.BeginIngressStoreProgressInput{
		CopyID: copies[0].ID, Generation: 1, TaskID: firstTask.ID, Attempt: 1,
	}); err != nil {
		t.Fatalf("BeginIngressStoreProgress(first): %v", err)
	}
	if _, err := repos.Contents.RecordIngressStoreProgress(t.Context(), repository.RecordIngressStoreProgressInput{
		CopyID: copies[0].ID, Generation: 1, TaskID: firstTask.ID, Attempt: 1, BytesUploaded: 4,
	}); err != nil {
		t.Fatalf("RecordIngressStoreProgress(first): %v", err)
	}

	secondTask := enqueueTask("progress-second")
	if err := repos.Contents.ReplaceCopyTask(t.Context(), copies[0].ID, 1, firstTask.ID, 2, secondTask.ID); err != nil {
		t.Fatalf("ReplaceCopyTask: %v", err)
	}
	if err := repos.Tasks.Settle(t.Context(), freshClaim.ID, freshClaim.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover, RetentionUntil: new(time.Now().Add(time.Hour)),
	}); err != nil {
		t.Fatalf("complete first task: %v", err)
	}
	if _, err := repos.Contents.BeginIngressStoreProgress(t.Context(), repository.BeginIngressStoreProgressInput{
		CopyID: copies[0].ID, Generation: 2, TaskID: secondTask.ID, Attempt: 2,
	}); err != nil {
		t.Fatalf("BeginIngressStoreProgress(second): %v", err)
	}
	for _, stale := range []repository.RecordIngressStoreProgressInput{
		{CopyID: copies[0].ID, Generation: 1, TaskID: firstTask.ID, Attempt: 1, BytesUploaded: 9},
		{CopyID: copies[0].ID, Generation: 2, TaskID: secondTask.ID, Attempt: 1, BytesUploaded: 9},
	} {
		if _, err := repos.Contents.RecordIngressStoreProgress(t.Context(), stale); !errors.Is(err, repository.ErrConflict) {
			t.Fatalf("stale progress error = %v, want conflict", err)
		}
	}
	if _, err := repos.Contents.RecordIngressStoreProgress(t.Context(), repository.RecordIngressStoreProgressInput{
		CopyID: copies[0].ID, Generation: 2, TaskID: secondTask.ID, Attempt: 2, BytesUploaded: 7,
	}); err != nil {
		t.Fatalf("RecordIngressStoreProgress(second): %v", err)
	}
	if err := repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
		StorageCopyID: copies[0].ID, ContentID: content.ID, CopyIndex: 0, LastError: "owner deleted",
	}); err != nil {
		t.Fatalf("MarkUploadCopyFailed: %v", err)
	}
	if _, err := repos.Contents.RecordIngressStoreProgress(t.Context(), repository.RecordIngressStoreProgressInput{
		CopyID: copies[0].ID, Generation: 2, TaskID: secondTask.ID, Attempt: 2, BytesUploaded: 10,
	}); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("terminal progress error = %v, want conflict", err)
	}
	stored, err := repos.Contents.GetUploadCopyByID(t.Context(), copies[0].ID)
	if err != nil || stored.IngressStoreAttempt != 2 || stored.IngressBytesTransferred != 7 {
		t.Fatalf("stored progress = %#v, err=%v", stored, err)
	}
}

func TestPermanentDeleteClearsTerminalStoreFence(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "terminal-store-delete")
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 128,
		Checksum: testutil.StorageChecksum("terminal-store-delete"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}
	binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "201"), CopyIndex: 0, CreatedByContentID: content.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0, ProviderID: binding.ProviderID,
		TransferMethod: model.StorageCopyTransferMethodIngress,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "unknown-store.bin", ContentID: &content.ID,
		Size: content.ContentSize, ETag: "unknown-store", ContentType: "application/octet-stream",
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	copies, err := repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 1 {
		t.Fatalf("ListCopies = %#v, err=%v", copies, err)
	}
	taskRow, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
		Type: model.TaskTypeStorageStore, IdempotencyKey: "terminal-store-delete", InputVersion: 1,
		Input: []byte(`{}`), InputHash: "terminal-store-delete", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})
	if err != nil || !created {
		t.Fatalf("Enqueue = %#v, created=%v, err=%v", taskRow, created, err)
	}
	if err := repos.Contents.BindCopyTask(t.Context(), copies[0].ID, 1, taskRow.ID); err != nil {
		t.Fatalf("BindCopyTask: %v", err)
	}
	claimed, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("ClaimNext = %#v, err=%v", claimed, err)
	}
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
		FailureReason: new("store_outcome_unknown"), LastError: new("store outcome unknown"),
	}); err != nil {
		t.Fatalf("fail store task: %v", err)
	}
	if _, err := repos.Objects.DeleteObjectVersionPermanently(t.Context(), repository.DeleteObjectVersionInput{
		BucketID: bucket.ID, Key: version.Key, VersionID: version.VersionID,
	}); err != nil {
		t.Fatalf("DeleteObjectVersionPermanently: %v", err)
	}
	stored, err := repos.Contents.GetUploadCopyByID(t.Context(), copies[0].ID)
	if err != nil || stored.Status != model.StorageCopyStatusFailed || stored.ActiveTaskID != nil {
		t.Fatalf("copy after permanent delete = %#v, err=%v", stored, err)
	}
}
