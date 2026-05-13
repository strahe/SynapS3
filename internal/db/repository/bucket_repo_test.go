package repository_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/auth"
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

func TestBucketRepo_SetDefaultCopies(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "copies-policy", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	copies := 4
	if err := repos.Buckets.SetDefaultCopies(ctx, bucket.Name, &copies); err != nil {
		t.Fatalf("SetDefaultCopies set: %v", err)
	}
	got, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName after set: %v", err)
	}
	if got == nil || got.DefaultCopies == nil || *got.DefaultCopies != copies {
		t.Fatalf("DefaultCopies after set = %#v, want %d", got, copies)
	}

	if err := repos.Buckets.SetDefaultCopies(ctx, bucket.Name, nil); err != nil {
		t.Fatalf("SetDefaultCopies clear: %v", err)
	}
	got, err = repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName after clear: %v", err)
	}
	if got == nil || got.DefaultCopies != nil {
		t.Fatalf("DefaultCopies after clear = %#v, want nil", got)
	}
}

func TestBucketRepo_SetDefaultCopiesMissingBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	copies := 3
	if err := repos.Buckets.SetDefaultCopies(ctx, "missing-copies-policy", &copies); err == nil {
		t.Fatal("SetDefaultCopies missing bucket succeeded, want error")
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

	// CAS succeeds: active → active (no-op transition)
	if err := repos.Buckets.UpdateStatus(ctx, bucket.ID, model.BucketStatusActive, model.BucketStatusActive); err != nil {
		t.Fatalf("UpdateStatus active→active: %v", err)
	}
	got, _ := repos.Buckets.GetByID(ctx, bucket.ID)
	if got.Status != model.BucketStatusActive {
		t.Errorf("expected active, got %s", got.Status)
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
	if got != nil {
		t.Fatal("expected bucket to be deleted")
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

	// Create multiple active buckets.
	for _, name := range []string{"alpha", "beta", "gamma", "delta"} {
		b := &model.Bucket{Name: name, Status: model.BucketStatusActive}
		if err := repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("Create(%s): %v", name, err)
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

	// Seed buckets: 3 active.
	for _, name := range []string{"a1", "a2", "a3"} {
		b := &model.Bucket{Name: name, Status: model.BucketStatusActive}
		if err := repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("Create(%s): %v", name, err)
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

	if lookup[string(model.BucketStatusActive)] != 3 {
		t.Errorf("expected 3 active, got %d", lookup[string(model.BucketStatusActive)])
	}
}

func TestBucketRepo_AggregateCountsByOwner(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	for _, accessKey := range []string{"owner-a", "owner-b"} {
		if err := repos.S3Accounts.Create(ctx, &model.S3Account{
			AccessKey: accessKey,
			SecretKey: "secret-" + accessKey,
			Role:      auth.RoleUserPlus,
		}); err != nil {
			t.Fatalf("S3Accounts.Create(%s): %v", accessKey, err)
		}
	}

	for _, seed := range []struct {
		name  string
		owner *string
	}{
		{name: "owner-a-one", owner: strptr("owner-a")},
		{name: "owner-a-two", owner: strptr("owner-a")},
		{name: "owner-b-one", owner: strptr("owner-b")},
		{name: "unassigned", owner: nil},
	} {
		b := &model.Bucket{Name: seed.name, Status: model.BucketStatusActive, OwnerAccessKey: seed.owner}
		if err := repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("Create(%s): %v", seed.name, err)
		}
	}

	counts, err := repos.Buckets.AggregateCountsByOwner(ctx)
	if err != nil {
		t.Fatalf("AggregateCountsByOwner: %v", err)
	}
	if len(counts) != 2 {
		t.Fatalf("len = %d, want 2", len(counts))
	}
	if counts["owner-a"] != 2 {
		t.Fatalf("owner-a count = %d, want 2", counts["owner-a"])
	}
	if counts["owner-b"] != 1 {
		t.Fatalf("owner-b count = %d, want 1", counts["owner-b"])
	}
	if _, ok := counts[""]; ok {
		t.Fatal("unexpected aggregate entry for empty owner")
	}
}

func TestBucketRepo_ListACLsReturnsOnlyOwnershipFields(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	acl, err := json.Marshal(auth.ACL{Owner: "owner-access"})
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}
	for _, bucket := range []*model.Bucket{
		{Name: "owned", Status: model.BucketStatusActive, ACL: acl},
		{Name: "unassigned", Status: model.BucketStatusActive},
	} {
		if err := repos.Buckets.Create(ctx, bucket); err != nil {
			t.Fatalf("Create(%s): %v", bucket.Name, err)
		}
	}

	snapshots, err := repos.Buckets.ListACLs(ctx)
	if err != nil {
		t.Fatalf("ListACLs: %v", err)
	}
	if len(snapshots) != 2 {
		t.Fatalf("len = %d, want 2", len(snapshots))
	}
	if snapshots[0].Name != "owned" || snapshots[0].Status != model.BucketStatusActive || string(snapshots[0].ACL) != string(acl) {
		t.Fatalf("owned snapshot = %#v, want name/status/acl", snapshots[0])
	}
	if snapshots[1].Name != "unassigned" || snapshots[1].ACL != nil {
		t.Fatalf("unassigned snapshot = %#v, want nil ACL", snapshots[1])
	}
}

func TestBucketRepo_CountStorageDataSets(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "datasets-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "dataset-count",
		ContentSize:     1,
		Checksum:        "sum",
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	seedCommittedUploadCopies(t, repos, bucket.ID, upload.ID, "bafk2bzacedatasetcount", []storageUploadCopySeed{
		{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "2001"), TransferMethod: model.StorageCopyTransferMethodIngress, RetrievalURL: strptr("https://provider.example/1")},
		{ProviderID: onChainIDPtr(t, "202"), DataSetID: onChainIDPtr(t, "2002"), PieceID: onChainIDPtr(t, "3001"), TransferMethod: model.StorageCopyTransferMethodPeerPull, RetrievalURL: strptr("https://provider.example/2")},
	})

	count, err := repos.Buckets.CountStorageDataSets(ctx)
	if err != nil {
		t.Fatalf("CountStorageDataSets: %v", err)
	}
	if count != 2 {
		t.Fatalf("storage data set count = %d, want 2", count)
	}
}

func strptr(s string) *string {
	return &s
}
