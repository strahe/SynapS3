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
	err = repos.Objects.UpdateState(ctx, id, 1, model.ObjectStateCached, model.ObjectStateUploaded)
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
	if err := repos.Objects.SetPieceCIDAndTransition(ctx, id, gen, "baga6ea4seaq123", model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatalf("SetPieceCIDAndTransition: %v", err)
	}

	got, _ := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "piececid.txt")
	if got.PieceCID == nil || *got.PieceCID != "baga6ea4seaq123" {
		t.Errorf("PieceCID = %v, want %q", got.PieceCID, "baga6ea4seaq123")
	}
	if got.State != model.ObjectStateUploaded {
		t.Errorf("State = %s, want uploaded", got.State)
	}

	// Stale generation should fail
	err = repos.Objects.SetPieceCIDAndTransition(ctx, id, gen-1, "stale", model.ObjectStateUploaded, model.ObjectStateOnChaining)
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
	err = repos.Objects.UpdateState(ctx, id, gen-1, model.ObjectStateUploading, model.ObjectStateUploaded)
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
