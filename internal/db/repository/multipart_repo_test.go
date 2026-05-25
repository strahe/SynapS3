package repository_test

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestMultipartRepo_CreateAndGet(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "mp-test")
	ctx := context.Background()

	upload := &model.MultipartUpload{
		BucketID:    bucket.ID,
		Key:         "some/key",
		UploadID:    "upload-001",
		ContentType: "text/plain",
	}
	if err := repos.Multiparts.Create(ctx, upload); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repos.Multiparts.GetByUploadID(ctx, "upload-001")
	if err != nil {
		t.Fatalf("GetByUploadID: %v", err)
	}
	if got == nil {
		t.Fatal("expected upload, got nil")
	}
	if got.Key != "some/key" || got.Status != model.MultipartStatusInitiated {
		t.Errorf("unexpected upload: key=%s status=%s", got.Key, got.Status)
	}
}

func TestMultipartRepo_GetByUploadID_NotFound(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	got, err := repos.Multiparts.GetByUploadID(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil for nonexistent upload")
	}
}

func TestMultipartRepo_SetStatus_CAS(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "cas-test")
	ctx := context.Background()

	upload := &model.MultipartUpload{
		BucketID: bucket.ID,
		Key:      "key",
		UploadID: "upload-cas",
	}
	if err := repos.Multiparts.Create(ctx, upload); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Valid transition: initiated → completing
	if err := repos.Multiparts.SetStatus(ctx, "upload-cas", model.MultipartStatusInitiated, model.MultipartStatusCompleting); err != nil {
		t.Fatalf("SetStatus initiated→completing: %v", err)
	}

	// Invalid: try initiated → aborted (already completing)
	err := repos.Multiparts.SetStatus(ctx, "upload-cas", model.MultipartStatusInitiated, model.MultipartStatusAborted)
	if err == nil {
		t.Fatal("expected CAS failure, got nil")
	}

	// Valid: completing → completed
	if err := repos.Multiparts.SetStatus(ctx, "upload-cas", model.MultipartStatusCompleting, model.MultipartStatusCompleted); err != nil {
		t.Fatalf("SetStatus completing→completed: %v", err)
	}
}

func TestMultipartRepo_ListByBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "list-test")
	ctx := context.Background()

	for _, uid := range []string{"u1", "u2", "u3"} {
		u := &model.MultipartUpload{BucketID: bucket.ID, Key: "dir/" + uid, UploadID: uid}
		if err := repos.Multiparts.Create(ctx, u); err != nil {
			t.Fatalf("Create %s: %v", uid, err)
		}
	}
	// One with different prefix
	if err := repos.Multiparts.Create(ctx, &model.MultipartUpload{BucketID: bucket.ID, Key: "other/x", UploadID: "u4"}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Multiparts.Create(ctx, &model.MultipartUpload{BucketID: bucket.ID, Key: "Dir/u5", UploadID: "u5"}); err != nil {
		t.Fatal(err)
	}

	uploads, err := repos.Multiparts.ListByBucket(ctx, bucket.ID, "dir/", "", "", 10)
	if err != nil {
		t.Fatalf("ListByBucket: %v", err)
	}
	if len(uploads) != 3 {
		t.Errorf("expected 3 uploads with prefix dir/, got %d", len(uploads))
	}

	// With marker
	uploads, err = repos.Multiparts.ListByBucket(ctx, bucket.ID, "", "dir/u1", "", 10)
	if err != nil {
		t.Fatalf("ListByBucket with marker: %v", err)
	}
	if len(uploads) != 3 { // u2, u3, u4
		t.Errorf("expected 3 uploads after marker, got %d", len(uploads))
	}
}

func TestMultipartRepo_CountActiveByBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucketA := seedBucket(t, db, "count-mp-a")
	bucketB := seedBucket(t, db, "count-mp-b")
	ctx := context.Background()

	for _, upload := range []*model.MultipartUpload{
		{BucketID: bucketA.ID, Key: "a-1", UploadID: "count-mp-a-1", Status: model.MultipartStatusInitiated},
		{BucketID: bucketA.ID, Key: "a-2", UploadID: "count-mp-a-2", Status: model.MultipartStatusCompleting},
		{BucketID: bucketA.ID, Key: "a-3", UploadID: "count-mp-a-3", Status: model.MultipartStatusCompleted},
		{BucketID: bucketA.ID, Key: "a-4", UploadID: "count-mp-a-4", Status: model.MultipartStatusAborted},
		{BucketID: bucketB.ID, Key: "b-1", UploadID: "count-mp-b-1", Status: model.MultipartStatusInitiated},
	} {
		if err := repos.Multiparts.Create(ctx, upload); err != nil {
			t.Fatalf("Create(%s): %v", upload.UploadID, err)
		}
	}

	countA, err := repos.Multiparts.CountActiveByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("CountActiveByBucket bucketA: %v", err)
	}
	if countA != 2 {
		t.Fatalf("active multipart count for bucketA = %d, want 2", countA)
	}

	countB, err := repos.Multiparts.CountActiveByBucket(ctx, bucketB.ID)
	if err != nil {
		t.Fatalf("CountActiveByBucket bucketB: %v", err)
	}
	if countB != 1 {
		t.Fatalf("active multipart count for bucketB = %d, want 1", countB)
	}
}

func TestMultipartRepo_Parts_CRUD(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "parts-test")
	ctx := context.Background()

	upload := &model.MultipartUpload{BucketID: bucket.ID, Key: "key", UploadID: "up-parts"}
	if err := repos.Multiparts.Create(ctx, upload); err != nil {
		t.Fatal(err)
	}

	// Create parts
	for i := 1; i <= 3; i++ {
		part := &model.MultipartPart{
			UploadID:   "up-parts",
			PartNumber: i,
			Size:       int64(i * 1000),
			ETag:       "etag-" + string(rune('0'+i)),
		}
		if err := repos.Multiparts.CreatePart(ctx, part); err != nil {
			t.Fatalf("CreatePart %d: %v", i, err)
		}
	}

	// List parts
	parts, err := repos.Multiparts.GetParts(ctx, "up-parts", 0, 10)
	if err != nil {
		t.Fatalf("GetParts: %v", err)
	}
	if len(parts) != 3 {
		t.Errorf("expected 3 parts, got %d", len(parts))
	}

	// GetPartsByNumbers
	specific, err := repos.Multiparts.GetPartsByNumbers(ctx, "up-parts", []int{1, 3})
	if err != nil {
		t.Fatalf("GetPartsByNumbers: %v", err)
	}
	if len(specific) != 2 {
		t.Errorf("expected 2 parts, got %d", len(specific))
	}

	// UPSERT: overwrite part 2
	overwrite := &model.MultipartPart{
		UploadID:   "up-parts",
		PartNumber: 2,
		Size:       9999,
		ETag:       "etag-new",
	}
	if err := repos.Multiparts.CreatePart(ctx, overwrite); err != nil {
		t.Fatalf("CreatePart upsert: %v", err)
	}

	parts, _ = repos.Multiparts.GetParts(ctx, "up-parts", 0, 10)
	if len(parts) != 3 {
		t.Errorf("expected 3 parts after upsert, got %d", len(parts))
	}
	if parts[1].Size != 9999 || parts[1].ETag != "etag-new" {
		t.Errorf("part 2 not updated: size=%d etag=%s", parts[1].Size, parts[1].ETag)
	}

	// DeleteParts
	if err := repos.Multiparts.DeleteParts(ctx, "up-parts"); err != nil {
		t.Fatal(err)
	}
	parts, _ = repos.Multiparts.GetParts(ctx, "up-parts", 0, 10)
	if len(parts) != 0 {
		t.Errorf("expected 0 parts after delete, got %d", len(parts))
	}
}

func TestMultipartRepo_Delete(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "del-test")
	ctx := context.Background()

	upload := &model.MultipartUpload{BucketID: bucket.ID, Key: "key", UploadID: "up-del"}
	if err := repos.Multiparts.Create(ctx, upload); err != nil {
		t.Fatal(err)
	}

	if err := repos.Multiparts.Delete(ctx, "up-del"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, _ := repos.Multiparts.GetByUploadID(ctx, "up-del")
	if got != nil {
		t.Fatal("expected nil after delete")
	}
}
