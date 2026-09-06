package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/versity/versitygw/auth"
)

func TestBucketRepo_CreateAndGetByName(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)

	ctx := context.Background()

	bucket := &model.Bucket{Name: "test-bucket", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
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

func TestBucketRepo_GetNamesByIDs(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	names, err := repos.Buckets.GetNamesByIDs(ctx, nil)
	if err != nil {
		t.Fatalf("GetNamesByIDs empty: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("GetNamesByIDs empty = %v, want empty map", names)
	}

	first := &model.Bucket{Name: "names-first", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
	second := &model.Bucket{Name: "names-second", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
	if err := repos.Buckets.Create(ctx, first); err != nil {
		t.Fatalf("Create first: %v", err)
	}
	if err := repos.Buckets.Create(ctx, second); err != nil {
		t.Fatalf("Create second: %v", err)
	}

	names, err = repos.Buckets.GetNamesByIDs(ctx, []int64{first.ID, second.ID, 999})
	if err != nil {
		t.Fatalf("GetNamesByIDs: %v", err)
	}
	if len(names) != 2 || names[first.ID] != first.Name || names[second.ID] != second.Name {
		t.Fatalf("GetNamesByIDs = %v, want {%d: %q, %d: %q}", names, first.ID, first.Name, second.ID, second.Name)
	}
}

func TestBucketRepo_GetByID(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "by-id", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
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

func TestBucketRepo_UpdateCopyPolicy(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "copies-policy", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 2}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	copies := 4
	minimum := 2
	updated, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name:                    bucket.Name,
		SetDefaultCopies:        true,
		DefaultCopies:           &copies,
		SetMinimumDurableCopies: true,
		MinimumDurableCopies:    &minimum,
	})
	if err != nil {
		t.Fatalf("UpdateCopyPolicy set: %v", err)
	}
	if updated == nil || updated.DefaultCopies != copies || updated.MinimumDurableCopies != minimum {
		t.Fatalf("UpdateCopyPolicy result = %#v, want target/minimum %d/%d", updated, copies, minimum)
	}
	assertActiveReplicaSlots(t, ctx, repos, bucket.ID, copies)
	got, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName after set: %v", err)
	}
	if got == nil || got.DefaultCopies != copies || got.MinimumDurableCopies != minimum {
		t.Fatalf("copy policy after set = %#v, want target/minimum %d/%d", got, copies, minimum)
	}

	updated, err = repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name:                    bucket.Name,
		SetMinimumDurableCopies: true,
	})
	if err != nil {
		t.Fatalf("UpdateCopyPolicy clear minimum: %v", err)
	}
	if updated == nil || updated.DefaultCopies != copies || updated.MinimumDurableCopies != copies {
		t.Fatalf("UpdateCopyPolicy clear result = %#v, want target %d and a minimum matching it", updated, copies)
	}
	got, err = repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName after clear: %v", err)
	}
	if got == nil || got.DefaultCopies != copies || got.MinimumDurableCopies != copies {
		t.Fatalf("copy policy after minimum clear = %#v, want target %d and a minimum matching it", got, copies)
	}

	grown := 6
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: &grown,
	}); err != nil {
		t.Fatalf("UpdateCopyPolicy grow: %v", err)
	}
	assertActiveReplicaSlots(t, ctx, repos, bucket.ID, grown)
	// Setting the same target again is not a lowering.
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: &grown,
	}); err != nil {
		t.Fatalf("UpdateCopyPolicy repeat target: %v", err)
	}
	assertActiveReplicaSlots(t, ctx, repos, bucket.ID, grown)
}

// Lowering the replica target would leave the replicas above it running and
// billed with nothing to retire them, so it is refused outright and leaves both
// the policy and the slots untouched.
func TestBucketRepo_UpdateCopyPolicyRefusesLoweringTheTarget(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "copies-no-lowering", Status: model.BucketStatusActive, DefaultCopies: 4, MinimumDurableCopies: 2}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	lowered := 3
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: &lowered,
	}); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("lowering the replica target error = %v, want ErrInvalidInput", err)
	}
	got, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil || got == nil || got.DefaultCopies != 4 || got.MinimumDurableCopies != 2 {
		t.Fatalf("policy after refused lowering = %#v err=%v, want unchanged 4/2", got, err)
	}
	assertActiveReplicaSlots(t, ctx, repos, bucket.ID, 4)

	// Lowering only the minimum stays legal: it releases cache sooner, it does
	// not strand a paid replica.
	loweredMinimum := 1
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name: bucket.Name, SetMinimumDurableCopies: true, MinimumDurableCopies: &loweredMinimum,
	}); err != nil {
		t.Fatalf("lowering the minimum: %v", err)
	}
	got, err = repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil || got == nil || got.DefaultCopies != 4 || got.MinimumDurableCopies != 1 {
		t.Fatalf("policy after minimum lowering = %#v err=%v, want 4/1", got, err)
	}
}

func assertActiveReplicaSlots(t *testing.T, ctx context.Context, repos *repository.Repositories, bucketID int64, want int) {
	t.Helper()
	slots, err := repos.Buckets.ActiveReplicaSlots(ctx, bucketID)
	if err != nil {
		t.Fatalf("ActiveReplicaSlots: %v", err)
	}
	if len(slots) != want {
		t.Fatalf("active replica slots = %v, want %d", slots, want)
	}
	for i, copyIndex := range slots {
		if copyIndex != i {
			t.Fatalf("active replica slots = %v, want contiguous 0..%d", slots, want-1)
		}
	}
}

func TestBucketRepo_SetDefaultCopies(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "set-default-copies", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	copies := 5
	if err := repos.Buckets.SetDefaultCopies(ctx, bucket.Name, &copies); err != nil {
		t.Fatalf("SetDefaultCopies: %v", err)
	}
	got, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if got == nil || got.DefaultCopies != copies {
		t.Fatalf("DefaultCopies = %#v, want %d", got, copies)
	}

	if err := repos.Buckets.SetDefaultCopies(ctx, "missing-bucket", &copies); err == nil {
		t.Fatal("SetDefaultCopies missing bucket succeeded")
	}
}

func TestBucketRepo_UpdateCopyPolicyMissingBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	copies := 3
	updated, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name:             "missing-copies-policy",
		SetDefaultCopies: true,
		DefaultCopies:    &copies,
	})
	if err != nil || updated != nil {
		t.Fatalf("UpdateCopyPolicy missing bucket = %#v, %v; want nil, nil", updated, err)
	}
}

func TestBucketRepo_SetACL(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := &model.Bucket{Name: "acl-bucket", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	acl := []byte(`{"owner":"user1"}`)
	if err := repos.Buckets.SetACL(ctx, bucket.Name, acl); err != nil {
		t.Fatalf("SetACL: %v", err)
	}

	got, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName after set: %v", err)
	}
	if got == nil {
		t.Fatal("GetByName after set returned nil")
	}
	if string(got.ACL) != string(acl) {
		t.Fatalf("ACL after set = %s, want %s", got.ACL, acl)
	}

	if err := repos.Buckets.SetACL(ctx, "missing-bucket", acl); err == nil {
		t.Fatal("SetACL on missing bucket succeeded, want error")
	}
}

func TestBucketRepo_SetOwnerAndACL(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	owner := new("user2")
	if err := repos.S3Accounts.Create(ctx, &model.S3Account{
		AccessKey: *owner,
		SecretKey: "secret-" + *owner,
		Role:      auth.RoleUserPlus,
	}); err != nil {
		t.Fatalf("S3Accounts.Create: %v", err)
	}

	bucket := &model.Bucket{Name: "owner-acl-bucket", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}

	acl := []byte(`{"owner":"user2"}`)
	if err := repos.Buckets.SetOwnerAndACL(ctx, bucket.Name, owner, acl); err != nil {
		t.Fatalf("SetOwnerAndACL: %v", err)
	}

	got, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil {
		t.Fatalf("GetByName after set: %v", err)
	}
	if got == nil {
		t.Fatal("GetByName after set returned nil")
	}
	if string(got.ACL) != string(acl) {
		t.Fatalf("ACL after set = %s, want %s", got.ACL, acl)
	}
	if got.OwnerAccessKey == nil || *got.OwnerAccessKey != *owner {
		t.Fatalf("Owner after set = %v, want %v", got.OwnerAccessKey, owner)
	}

	if err := repos.Buckets.SetOwnerAndACL(ctx, "missing-bucket", owner, acl); err == nil {
		t.Fatal("SetOwnerAndACL on missing bucket succeeded, want error")
	}
}

func TestBucketRepo_ListActive(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		b := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
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

	bucket := &model.Bucket{Name: "cas-status", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
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

	bucket := &model.Bucket{Name: "hard-del", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
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

	bucket := &model.Bucket{Name: "to-delete", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
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
		b := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
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
		b := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
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
		{name: "owner-a-one", owner: new("owner-a")},
		{name: "owner-a-two", owner: new("owner-a")},
		{name: "owner-b-one", owner: new("owner-b")},
		{name: "unassigned", owner: nil},
	} {
		b := &model.Bucket{Name: seed.name, Status: model.BucketStatusActive, OwnerAccessKey: seed.owner, DefaultCopies: 8, MinimumDurableCopies: 8}
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
		{Name: "owned", Status: model.BucketStatusActive, ACL: acl, DefaultCopies: 8, MinimumDurableCopies: 8},
		{Name: "unassigned", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8},
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

	bucket := &model.Bucket{Name: "datasets-bucket", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}
	origin := newObjectVersion(bucket.ID, "dataset-count", "dataset-count", 1)
	if _, err := createVersion(t, repos, origin); err != nil {
		t.Fatalf("Create origin version: %v", err)
	}
	upload, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID:        bucket.ID,
		ContentSize:     1,
		Checksum:        testutil.StorageChecksum("sum"),
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}
	seedCommittedUploadCopies(t, db, repos, bucket.ID, upload.ID, "bafk2bzacedatasetcount", []storageUploadCopySeed{
		{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001"), PieceID: onChainIDPtr(t, "2001"), TransferMethod: model.StorageCopyTransferMethodIngress, RetrievalURL: new("https://provider.example/1")},
		{ProviderID: onChainIDPtr(t, "202"), DataSetID: onChainIDPtr(t, "2002"), PieceID: onChainIDPtr(t, "3001"), TransferMethod: model.StorageCopyTransferMethodPeerPull, RetrievalURL: new("https://provider.example/2")},
	})

	count, err := repos.Buckets.CountStorageDataSets(ctx)
	if err != nil {
		t.Fatalf("CountStorageDataSets: %v", err)
	}
	if count != 2 {
		t.Fatalf("storage data set count = %d, want 2", count)
	}
}

func TestBucketRepo_PromoteReadyRequiresEveryConfiguredSlot(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := &model.Bucket{Name: "provisioning-bucket", Status: model.BucketStatusProvisioning, DefaultCopies: 8, MinimumDurableCopies: 8}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}

	markSlotReady := func(copyIndex int, provider, dataSet string) {
		t.Helper()
		providerID := onChainIDPtr(t, provider)
		dataSetID := onChainIDPtr(t, dataSet)
		clientDataSetID := onChainIDPtr(t, dataSet+"1")
		binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: *providerID, CopyIndex: copyIndex,
		})
		if err != nil {
			t.Fatalf("EnsureDataSetBinding(%d): %v", copyIndex, err)
		}
		if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID: binding.ID, DataSetID: *dataSetID, ClientDataSetID: clientDataSetID,
		}); err != nil {
			t.Fatalf("MarkDataSetReady(%d): %v", copyIndex, err)
		}
	}

	markSlotReady(0, "101", "1001")
	promoted, err := repos.Buckets.PromoteReadyIfProvisioned(ctx, bucket.ID, 2)
	if err != nil || promoted {
		t.Fatalf("promote with one slot = %v, err=%v; want false", promoted, err)
	}
	markSlotReady(2, "303", "3003")
	promoted, err = repos.Buckets.PromoteReadyIfProvisioned(ctx, bucket.ID, 2)
	if err != nil || promoted {
		t.Fatalf("promote with wrong second slot = %v, err=%v; want false", promoted, err)
	}
	markSlotReady(1, "202", "2002")
	promoted, err = repos.Buckets.PromoteReadyIfProvisioned(ctx, bucket.ID, 2)
	if err != nil || !promoted {
		t.Fatalf("promote with configured slots = %v, err=%v; want true", promoted, err)
	}
	stored, err := repos.Buckets.GetByID(ctx, bucket.ID)
	if err != nil || stored == nil || stored.Status != model.BucketStatusReady {
		t.Fatalf("ready bucket = %#v, err=%v", stored, err)
	}
}

// A row inserted without explicit timestamps must still carry them, and carry
// them in bun's encoding rather than the database's own clock format.
func TestModelInsertsStampAuditTimestamps(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)
	bucket := &model.Bucket{Name: "stamped-bucket", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create: %v", err)
	}
	stored, err := repos.Buckets.GetByName(ctx, bucket.Name)
	if err != nil || stored == nil {
		t.Fatalf("GetByName: bucket=%#v err=%v", stored, err)
	}
	if stored.CreatedAt.Before(before) || stored.UpdatedAt.Before(before) {
		t.Fatalf("stamped timestamps = created:%s updated:%s, want at or after %s", stored.CreatedAt, stored.UpdatedAt, before)
	}
	// Second granularity is what a database default would produce; the hook
	// writes the same instant bun renders everywhere else.
	var raw string
	if err := db.NewRaw(`SELECT created_at FROM buckets WHERE id = ?`, bucket.ID).Scan(ctx, &raw); err != nil {
		t.Fatalf("read raw created_at: %v", err)
	}
	if !strings.Contains(raw, ".") || strings.Contains(raw, " ") {
		t.Fatalf("stored created_at = %q, want bun's fractional encoding rather than the database clock's", raw)
	}
}
