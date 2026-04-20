package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestObjectRepo_UpsertAndBumpGeneration_NewObject(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "obj-bucket")

	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "hello.txt",
		Size:        42,
		ETag:        "abc123",
		Checksum:    "sha256-xxx",
		ContentType: "text/plain",
		CachePath:   "/cache/hello.txt",
		MaxRetries:  5,
	}

	id, gen, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("UpsertAndBumpGeneration (new): %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero ID")
	}
	if gen != 1 {
		t.Errorf("expected generation 1, got %d", gen)
	}
}

func TestObjectRepo_UpsertAndBumpGeneration_Overwrite(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "overwrite-bucket")

	obj1 := &model.Object{
		BucketID:    bucket.ID,
		Key:         "file.txt",
		Size:        10,
		ETag:        "e1",
		Checksum:    "c1",
		ContentType: "text/plain",
		CachePath:   "/cache/v1",
		MaxRetries:  5,
	}
	id1, gen1, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj1)
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	obj2 := &model.Object{
		BucketID:    bucket.ID,
		Key:         "file.txt",
		Size:        20,
		ETag:        "e2",
		Checksum:    "c2",
		ContentType: "application/octet-stream",
		CachePath:   "/cache/v2",
		MaxRetries:  5,
	}
	id2, gen2, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj2)
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	if id2 != id1 {
		t.Errorf("expected same ID on overwrite (%d), got %d", id1, id2)
	}
	if gen2 != gen1+1 {
		t.Errorf("expected generation %d, got %d", gen1+1, gen2)
	}

	// Verify updated fields.
	got, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if got.Size != 20 {
		t.Errorf("expected size 20, got %d", got.Size)
	}
	if got.ETag != "e2" {
		t.Errorf("expected etag e2, got %s", got.ETag)
	}
}

func TestObjectRepo_UpsertAndBumpGeneration_ReuploadAfterDelete(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "reupload-bucket")

	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "deleted.txt",
		Size:        10,
		ETag:        "e1",
		Checksum:    "c1",
		ContentType: "text/plain",
		CachePath:   "/cache/d1",
		MaxRetries:  5,
	}
	id1, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("initial upsert: %v", err)
	}

	// Soft-delete.
	if err := repos.Objects.SoftDelete(ctx, id1); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	// Should not be visible.
	got, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "deleted.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey after delete: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil after soft delete")
	}

	// Re-upload: should reclaim the row.
	obj2 := &model.Object{
		BucketID:    bucket.ID,
		Key:         "deleted.txt",
		Size:        20,
		ETag:        "e2",
		Checksum:    "c2",
		ContentType: "text/plain",
		CachePath:   "/cache/d2",
		MaxRetries:  5,
	}
	id2, gen2, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj2)
	if err != nil {
		t.Fatalf("re-upload upsert: %v", err)
	}
	if id2 != id1 {
		t.Errorf("expected same ID %d, got %d", id1, id2)
	}
	if gen2 != 2 {
		t.Errorf("expected generation 2, got %d", gen2)
	}

	// Should now be visible again.
	got, err = repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "deleted.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey after re-upload: %v", err)
	}
	if got == nil {
		t.Fatal("expected object to be visible after re-upload")
	}
}

func TestObjectRepo_ListByBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "list-bucket")

	for _, key := range []string{"a.txt", "b.txt", "dir/c.txt"} {
		obj := &model.Object{
			BucketID:    bucket.ID,
			Key:         key,
			Size:        1,
			ETag:        "e",
			Checksum:    "c",
			ContentType: "text/plain",
			CachePath:   "/cache/" + key,
			MaxRetries:  5,
		}
		if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
			t.Fatalf("upsert %s: %v", key, err)
		}
	}

	// List all.
	all, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "", 0)
	if err != nil {
		t.Fatalf("ListByBucket: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 objects, got %d", len(all))
	}

	// List with prefix.
	prefixed, err := repos.Objects.ListByBucket(ctx, bucket.ID, "dir/", "", 0)
	if err != nil {
		t.Fatalf("ListByBucket(prefix): %v", err)
	}
	if len(prefixed) != 1 {
		t.Errorf("expected 1 prefixed object, got %d", len(prefixed))
	}

	// List with limit.
	limited, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "", 2)
	if err != nil {
		t.Fatalf("ListByBucket(limit): %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("expected 2 limited objects, got %d", len(limited))
	}

	// List with afterKey (pagination).
	afterKey, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "a.txt", 0)
	if err != nil {
		t.Fatalf("ListByBucket(afterKey): %v", err)
	}
	if len(afterKey) != 2 {
		t.Errorf("expected 2 objects after 'a.txt', got %d", len(afterKey))
	}
}

func TestObjectRepo_UpdateState(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "state-bucket")

	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "state.txt",
		Size:        1,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/state.txt",
		MaxRetries:  5,
	}
	id, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Valid transition.
	if err := repos.Objects.UpdateState(ctx, id, 1, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}

	// Invalid transition (wrong from state).
	err = repos.Objects.UpdateState(ctx, id, 1, model.ObjectStateCached, model.ObjectStateStored)
	if err == nil {
		t.Fatal("expected error for invalid state transition")
	}
}

func TestObjectRepo_SetPieceCIDAndTransition(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "piececid-bucket")
	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "piececid.txt",
		Size:        1,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/piececid.txt",
		MaxRetries:  5,
	}
	id, gen, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Transition cached→uploading first
	if err := repos.Objects.UpdateState(ctx, id, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}

	// SetPieceCIDAndTransition uploading→uploaded
	if err := repos.Objects.SetPieceCIDAndTransition(ctx, id, gen, "baga6ea4seaq123", model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("SetPieceCIDAndTransition: %v", err)
	}

	got, _ := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "piececid.txt")
	if got.PieceCID == nil || *got.PieceCID != "baga6ea4seaq123" {
		t.Errorf("PieceCID = %v, want %q", got.PieceCID, "baga6ea4seaq123")
	}
	if got.State != model.ObjectStateStored {
		t.Errorf("State = %s, want stored", got.State)
	}

	// Stale generation should fail
	err = repos.Objects.SetPieceCIDAndTransition(ctx, id, gen-1, "stale", model.ObjectStateStored, model.ObjectStateCacheEvicted)
	if err == nil {
		t.Fatal("expected error for stale generation")
	}
}

func TestObjectRepo_ListByState(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "liststate-bucket")
	for i, key := range []string{"a.txt", "b.txt", "c.txt"} {
		obj := &model.Object{
			BucketID:    bucket.ID,
			Key:         key,
			Size:        int64(i + 1),
			ETag:        "e",
			Checksum:    "c",
			ContentType: "text/plain",
			CachePath:   "/cache/" + key,
			MaxRetries:  5,
		}
		if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
			t.Fatalf("upsert %s: %v", key, err)
		}
	}

	// All should be in "cached" state by default
	list, err := repos.Objects.ListByState(ctx, model.ObjectStateCached, 10)
	if err != nil {
		t.Fatalf("ListByState: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("expected 3 cached objects, got %d", len(list))
	}

	// With limit
	list2, err := repos.Objects.ListByState(ctx, model.ObjectStateCached, 2)
	if err != nil {
		t.Fatalf("ListByState with limit: %v", err)
	}
	if len(list2) != 2 {
		t.Errorf("expected 2 objects with limit, got %d", len(list2))
	}
}

func TestObjectRepo_ResetStaleStates(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "stale-bucket")
	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "stale.txt",
		Size:        1,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/stale.txt",
		MaxRetries:  5,
	}
	id, gen, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Move to uploading
	if err := repos.Objects.UpdateState(ctx, id, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}

	// Reset stale uploading→cached (with future threshold — everything is stale)
	count, err := repos.Objects.ResetStaleStates(ctx, model.ObjectStateUploading, model.ObjectStateCached, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ResetStaleStates: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 reset, got %d", count)
	}

	got, _ := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "stale.txt")
	if got.State != model.ObjectStateCached {
		t.Errorf("expected cached after reset, got %s", got.State)
	}
}

func TestObjectRepo_UpdateState_StaleGeneration(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "stalegen-bucket")
	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "gentest.txt",
		Size:        1,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/gentest.txt",
		MaxRetries:  5,
	}
	id, gen, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Valid generation should succeed
	if err := repos.Objects.UpdateState(ctx, id, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}

	// Stale generation should fail
	err = repos.Objects.UpdateState(ctx, id, gen-1, model.ObjectStateUploading, model.ObjectStateStored)
	if err == nil {
		t.Fatal("expected error for stale generation")
	}
}

func TestObjectRepo_SoftDelete_ExcludesFromList(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "softdel-bucket")

	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "gone.txt",
		Size:        1,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/gone.txt",
		MaxRetries:  5,
	}
	id, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if err := repos.Objects.SoftDelete(ctx, id); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	list, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "", 0)
	if err != nil {
		t.Fatalf("ListByBucket: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected 0 objects after soft delete, got %d", len(list))
	}
}

func TestRepos_WithTx(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// WithTx should roll back on error.
	err := repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		b := &model.Bucket{Name: "tx-bucket", Status: model.BucketStatusActive}
		if err := txRepos.Buckets.Create(ctx, b); err != nil {
			return err
		}
		return context.Canceled // force rollback
	})
	if err == nil {
		t.Fatal("expected error from WithTx")
	}

	// Bucket should not exist after rollback.
	got, err := repos.Buckets.GetByName(ctx, "tx-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil after rollback")
	}

	// WithTx should commit on success.
	err = repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		b := &model.Bucket{Name: "tx-committed", Status: model.BucketStatusActive}
		return txRepos.Buckets.Create(ctx, b)
	})
	if err != nil {
		t.Fatalf("WithTx commit: %v", err)
	}

	got, err = repos.Buckets.GetByName(ctx, "tx-committed")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if got == nil {
		t.Fatal("expected bucket after commit")
	}
}

func TestObjectRepo_CountByState(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Empty table.
	counts, err := repos.Objects.CountByState(ctx)
	if err != nil {
		t.Fatalf("CountByState empty: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("expected 0 counts, got %d", len(counts))
	}

	bucket := seedBucket(t, db, "countstate-bucket")

	// Seed three objects (all start as "cached").
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		obj := &model.Object{
			BucketID:    bucket.ID,
			Key:         key,
			Size:        1,
			ETag:        "e",
			Checksum:    "c",
			ContentType: "text/plain",
			CachePath:   "/cache/" + key,
			MaxRetries:  5,
		}
		if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
			t.Fatalf("upsert %s: %v", key, err)
		}
	}

	// Transition one to uploading.
	objs, _ := repos.Objects.ListByState(ctx, model.ObjectStateCached, 1)
	if len(objs) == 0 {
		t.Fatal("expected at least one cached object")
	}
	if err := repos.Objects.UpdateState(ctx, objs[0].ID, objs[0].Generation, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}

	counts, err = repos.Objects.CountByState(ctx)
	if err != nil {
		t.Fatalf("CountByState: %v", err)
	}

	lookup := make(map[string]int64)
	for _, c := range counts {
		lookup[c.State] = c.Count
	}

	if lookup[string(model.ObjectStateCached)] != 2 {
		t.Errorf("expected 2 cached, got %d", lookup[string(model.ObjectStateCached)])
	}
	if lookup[string(model.ObjectStateUploading)] != 1 {
		t.Errorf("expected 1 uploading, got %d", lookup[string(model.ObjectStateUploading)])
	}
}

func TestObjectRepo_CountByState_ExcludesSoftDeleted(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "countdel-bucket")
	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "del.txt",
		Size:        1,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/del.txt",
		MaxRetries:  5,
	}
	id, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if err := repos.Objects.SoftDelete(ctx, id); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	counts, err := repos.Objects.CountByState(ctx)
	if err != nil {
		t.Fatalf("CountByState: %v", err)
	}
	if len(counts) != 0 {
		t.Errorf("expected 0 counts after soft-delete, got %d", len(counts))
	}
}

func TestObjectRepo_TotalSize(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Empty table should return 0.
	total, err := repos.Objects.TotalSize(ctx)
	if err != nil {
		t.Fatalf("TotalSize empty: %v", err)
	}
	if total != 0 {
		t.Fatalf("expected 0, got %d", total)
	}

	bucket := seedBucket(t, db, "totalsize-bucket")

	// Insert objects with known sizes.
	for _, tc := range []struct {
		key  string
		size int64
	}{
		{"a.txt", 100},
		{"b.txt", 250},
		{"c.txt", 650},
	} {
		obj := &model.Object{
			BucketID:    bucket.ID,
			Key:         tc.key,
			Size:        tc.size,
			ETag:        "e",
			Checksum:    "c",
			ContentType: "text/plain",
			CachePath:   "/cache/" + tc.key,
			MaxRetries:  5,
		}
		if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
			t.Fatalf("upsert %s: %v", tc.key, err)
		}
	}

	total, err = repos.Objects.TotalSize(ctx)
	if err != nil {
		t.Fatalf("TotalSize: %v", err)
	}
	if total != 1000 {
		t.Errorf("expected 1000, got %d", total)
	}

	// Soft-deleted objects should be excluded.
	objs, _ := repos.Objects.ListByBucket(ctx, bucket.ID, "", "", 1)
	if err := repos.Objects.SoftDelete(ctx, objs[0].ID); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	total, err = repos.Objects.TotalSize(ctx)
	if err != nil {
		t.Fatalf("TotalSize after delete: %v", err)
	}
	if total != 900 {
		t.Errorf("expected 900 after soft-delete, got %d", total)
	}
}

func TestObjectRepo_CountByBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucketA := seedBucket(t, db, "count-bucket-a")
	bucketB := seedBucket(t, db, "count-bucket-b")

	// Empty bucket.
	count, err := repos.Objects.CountByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("CountByBucket empty: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}

	// Insert 3 objects in bucket A and 1 in bucket B.
	for _, key := range []string{"a1.txt", "a2.txt", "a3.txt"} {
		obj := &model.Object{
			BucketID:    bucketA.ID,
			Key:         key,
			Size:        10,
			ETag:        "e",
			Checksum:    "c",
			ContentType: "text/plain",
			CachePath:   "/cache/" + key,
			MaxRetries:  5,
		}
		if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
			t.Fatalf("upsert %s: %v", key, err)
		}
	}
	objB := &model.Object{
		BucketID:    bucketB.ID,
		Key:         "b1.txt",
		Size:        10,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/b1.txt",
		MaxRetries:  5,
	}
	if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, objB); err != nil {
		t.Fatalf("upsert b1.txt: %v", err)
	}

	countA, err := repos.Objects.CountByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("CountByBucket A: %v", err)
	}
	if countA != 3 {
		t.Errorf("expected 3 in bucket A, got %d", countA)
	}

	countB, err := repos.Objects.CountByBucket(ctx, bucketB.ID)
	if err != nil {
		t.Fatalf("CountByBucket B: %v", err)
	}
	if countB != 1 {
		t.Errorf("expected 1 in bucket B, got %d", countB)
	}

	// Soft-deleted objects should be excluded.
	objs, _ := repos.Objects.ListByBucket(ctx, bucketA.ID, "", "", 1)
	if err := repos.Objects.SoftDelete(ctx, objs[0].ID); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	countA, err = repos.Objects.CountByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("CountByBucket after delete: %v", err)
	}
	if countA != 2 {
		t.Errorf("expected 2 after soft-delete, got %d", countA)
	}
}

func TestObjectRepo_TotalSizeByBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucketA := seedBucket(t, db, "szbucket-a")
	bucketB := seedBucket(t, db, "szbucket-b")

	// Empty bucket.
	total, err := repos.Objects.TotalSizeByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("TotalSizeByBucket empty: %v", err)
	}
	if total != 0 {
		t.Fatalf("expected 0, got %d", total)
	}

	// Bucket A: 100 + 200 = 300
	for _, tc := range []struct {
		key  string
		size int64
	}{
		{"x.txt", 100},
		{"y.txt", 200},
	} {
		obj := &model.Object{
			BucketID:    bucketA.ID,
			Key:         tc.key,
			Size:        tc.size,
			ETag:        "e",
			Checksum:    "c",
			ContentType: "text/plain",
			CachePath:   "/cache/" + tc.key,
			MaxRetries:  5,
		}
		if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
			t.Fatalf("upsert %s: %v", tc.key, err)
		}
	}

	// Bucket B: 500
	objB := &model.Object{
		BucketID:    bucketB.ID,
		Key:         "z.txt",
		Size:        500,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/z.txt",
		MaxRetries:  5,
	}
	if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, objB); err != nil {
		t.Fatalf("upsert z.txt: %v", err)
	}

	totalA, err := repos.Objects.TotalSizeByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("TotalSizeByBucket A: %v", err)
	}
	if totalA != 300 {
		t.Errorf("expected 300 for bucket A, got %d", totalA)
	}

	totalB, err := repos.Objects.TotalSizeByBucket(ctx, bucketB.ID)
	if err != nil {
		t.Fatalf("TotalSizeByBucket B: %v", err)
	}
	if totalB != 500 {
		t.Errorf("expected 500 for bucket B, got %d", totalB)
	}

	// Soft-deleted objects should be excluded.
	objs, _ := repos.Objects.ListByBucket(ctx, bucketA.ID, "", "", 1)
	if err := repos.Objects.SoftDelete(ctx, objs[0].ID); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	totalA, err = repos.Objects.TotalSizeByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("TotalSizeByBucket after delete: %v", err)
	}
	if totalA != 200 {
		t.Errorf("expected 200 after soft-delete, got %d", totalA)
	}
}

func TestObjectRepo_AggregateByBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucketA := seedBucket(t, db, "aggbucket-a")
	bucketB := seedBucket(t, db, "aggbucket-b")
	bucketEmpty := seedBucket(t, db, "aggbucket-empty")

	// Bucket A: 2 objects, sizes 100 + 200
	for _, tc := range []struct {
		key  string
		size int64
	}{
		{"a1.txt", 100},
		{"a2.txt", 200},
	} {
		obj := &model.Object{
			BucketID:    bucketA.ID,
			Key:         tc.key,
			Size:        tc.size,
			ETag:        "e",
			Checksum:    "c",
			ContentType: "text/plain",
			CachePath:   "/cache/" + tc.key,
			MaxRetries:  5,
		}
		if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
			t.Fatalf("upsert %s: %v", tc.key, err)
		}
	}

	// Bucket B: 1 object, size 500
	objB := &model.Object{
		BucketID:    bucketB.ID,
		Key:         "b1.txt",
		Size:        500,
		ETag:        "e",
		Checksum:    "c",
		ContentType: "text/plain",
		CachePath:   "/cache/b1.txt",
		MaxRetries:  5,
	}
	if _, _, err := repos.Objects.UpsertAndBumpGeneration(ctx, objB); err != nil {
		t.Fatalf("upsert b1.txt: %v", err)
	}

	stats, err := repos.Objects.AggregateByBucket(ctx)
	if err != nil {
		t.Fatalf("AggregateByBucket: %v", err)
	}

	// Bucket A: count=2, total_size=300
	if s := stats[bucketA.ID]; s.Count != 2 || s.TotalSize != 300 {
		t.Errorf("bucket A: expected count=2 size=300, got count=%d size=%d", s.Count, s.TotalSize)
	}

	// Bucket B: count=1, total_size=500
	if s := stats[bucketB.ID]; s.Count != 1 || s.TotalSize != 500 {
		t.Errorf("bucket B: expected count=1 size=500, got count=%d size=%d", s.Count, s.TotalSize)
	}

	// Empty bucket should not appear in stats.
	if s, ok := stats[bucketEmpty.ID]; ok {
		t.Errorf("empty bucket should not be in stats, got count=%d size=%d", s.Count, s.TotalSize)
	}
}
