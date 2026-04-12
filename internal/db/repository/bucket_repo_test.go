package repository_test

import (
	"context"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestBucketRepo_CreateAndGetByName(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := &model.Bucket{Name: "test-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if bucket.ID == 0 {
		t.Fatal("expected bucket ID to be assigned")
	}

	got, err := repos.Buckets.GetByName(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if got == nil {
		t.Fatal("expected bucket, got nil")
	}
	if got.ID != bucket.ID {
		t.Errorf("expected ID %d, got %d", bucket.ID, got.ID)
	}
}

func TestBucketRepo_GetByName_NotFound(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	got, err := repos.Buckets.GetByName(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil for nonexistent bucket")
	}
}

func TestBucketRepo_GetByID(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "by-id", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := repos.Buckets.GetByID(ctx, bucket.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got == nil || got.Name != "by-id" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestBucketRepo_ListActive(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		b := &model.Bucket{Name: name, Status: model.BucketStatusActive}
		if err := repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("Create(%s): %v", name, err)
		}
	}
	// Add a deleted bucket — should not appear in ListActive.
	del := &model.Bucket{Name: "deleted", Status: model.BucketStatusDeleted}
	if err := repos.Buckets.Create(ctx, del); err != nil {
		t.Fatalf("Create(deleted): %v", err)
	}

	list, err := repos.Buckets.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("expected 3 active buckets, got %d", len(list))
	}
}

func TestBucketRepo_UpdateStatus(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "cas-status", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// CAS succeeds: active → creating
	if err := repos.Buckets.UpdateStatus(ctx, bucket.ID, model.BucketStatusActive, model.BucketStatusCreating); err != nil {
		t.Fatalf("UpdateStatus active→creating: %v", err)
	}
	got, _ := repos.Buckets.GetByID(ctx, bucket.ID)
	if got.Status != model.BucketStatusCreating {
		t.Errorf("expected creating, got %s", got.Status)
	}

	// CAS fails: wrong from state
	err := repos.Buckets.UpdateStatus(ctx, bucket.ID, model.BucketStatusActive, model.BucketStatusDeleting)
	if err == nil {
		t.Fatal("expected error for wrong from state")
	}
}

func TestBucketRepo_SetProofSetID(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "proofset-id", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := repos.Buckets.SetProofSetID(ctx, bucket.ID, "ps-12345"); err != nil {
		t.Fatalf("SetProofSetID: %v", err)
	}
	got, _ := repos.Buckets.GetByID(ctx, bucket.ID)
	if got.ProofSetID == nil || *got.ProofSetID != "ps-12345" {
		t.Errorf("expected ProofSetID ps-12345, got %v", got.ProofSetID)
	}
}

func TestBucketRepo_HardDelete(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "hard-del", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := repos.Buckets.HardDelete(ctx, bucket.ID); err != nil {
		t.Fatalf("HardDelete: %v", err)
	}

	got, err := repos.Buckets.GetByID(ctx, bucket.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil after hard delete")
	}
}

func TestBucketRepo_SoftDelete(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "to-delete", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := repos.Buckets.SoftDelete(ctx, bucket.ID); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	got, err := repos.Buckets.GetByID(ctx, bucket.ID)
	if err != nil {
		t.Fatalf("GetByID after delete: %v", err)
	}
	if got == nil {
		t.Fatal("expected bucket to still exist")
	}
	if got.Status != model.BucketStatusDeleted {
		t.Errorf("expected status %q, got %q", model.BucketStatusDeleted, got.Status)
	}

	// Should not appear in ListActive.
	list, err := repos.Buckets.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected 0 active, got %d", len(list))
	}
}

func TestBucketRepo_List(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Empty table.
	list, err := repos.Buckets.List(ctx)
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expected 0 buckets, got %d", len(list))
	}

	// Create buckets with different statuses.
	for _, tc := range []struct {
		name   string
		status model.BucketStatus
	}{
		{"alpha", model.BucketStatusActive},
		{"beta", model.BucketStatusCreating},
		{"gamma", model.BucketStatusDeleted},
		{"delta", model.BucketStatusDeleting},
	} {
		b := &model.Bucket{Name: tc.name, Status: tc.status}
		if err := repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("Create(%s): %v", tc.name, err)
		}
	}

	list, err = repos.Buckets.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 4 {
		t.Errorf("expected 4 buckets, got %d", len(list))
	}

	// Verify ordered by name ASC.
	expectedNames := []string{"alpha", "beta", "delta", "gamma"}
	for i, name := range expectedNames {
		if list[i].Name != name {
			t.Errorf("list[%d].Name = %q, want %q", i, list[i].Name, name)
		}
	}
}

func TestBucketRepo_CountByStatus(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Empty table.
	counts, err := repos.Buckets.CountByStatus(ctx)
	if err != nil {
		t.Fatalf("CountByStatus empty: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("expected 0 counts, got %d", len(counts))
	}

	// Seed buckets: 2 active, 1 creating, 1 deleted.
	for _, tc := range []struct {
		name   string
		status model.BucketStatus
	}{
		{"a1", model.BucketStatusActive},
		{"a2", model.BucketStatusActive},
		{"c1", model.BucketStatusCreating},
		{"d1", model.BucketStatusDeleted},
	} {
		b := &model.Bucket{Name: tc.name, Status: tc.status}
		if err := repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("Create(%s): %v", tc.name, err)
		}
	}

	counts, err = repos.Buckets.CountByStatus(ctx)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}

	lookup := make(map[string]int64)
	for _, c := range counts {
		lookup[c.Status] = c.Count
	}

	if lookup[string(model.BucketStatusActive)] != 2 {
		t.Errorf("expected 2 active, got %d", lookup[string(model.BucketStatusActive)])
	}
	if lookup[string(model.BucketStatusCreating)] != 1 {
		t.Errorf("expected 1 creating, got %d", lookup[string(model.BucketStatusCreating)])
	}
	if lookup[string(model.BucketStatusDeleted)] != 1 {
		t.Errorf("expected 1 deleted, got %d", lookup[string(model.BucketStatusDeleted)])
	}
}

func TestBucketRepo_CountWithProofSet_ExcludesDeletedBuckets(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		status     model.BucketStatus
		proofSetID *string
	}{
		{name: "active-with-proof", status: model.BucketStatusActive, proofSetID: strptr("ps-1")},
		{name: "deleting-with-proof", status: model.BucketStatusDeleting, proofSetID: strptr("ps-2")},
		{name: "deleted-with-proof", status: model.BucketStatusDeleted, proofSetID: strptr("ps-3")},
		{name: "active-without-proof", status: model.BucketStatusActive, proofSetID: nil},
	} {
		b := &model.Bucket{Name: tc.name, Status: tc.status, ProofSetID: tc.proofSetID}
		if err := repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("Create(%s): %v", tc.name, err)
		}
	}

	count, err := repos.Buckets.CountWithProofSet(ctx)
	if err != nil {
		t.Fatalf("CountWithProofSet: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 buckets with proof sets, got %d", count)
	}
}

func strptr(s string) *string {
	return &s
}
