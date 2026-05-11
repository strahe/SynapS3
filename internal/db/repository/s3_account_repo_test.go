package repository_test

import (
	"context"
	"errors"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/auth"
)

func TestS3AccountRepo_CRUDAndRootFiltering(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	root := &model.S3Account{AccessKey: "root-access", SecretKey: "root-secret", Role: auth.RoleAdmin, IsRoot: true}
	user := &model.S3Account{AccessKey: "user-access", SecretKey: "user-secret", Role: auth.RoleUserPlus}
	if err := repos.S3Accounts.Create(ctx, root); err != nil {
		t.Fatalf("Create root: %v", err)
	}
	if err := repos.S3Accounts.Create(ctx, user); err != nil {
		t.Fatalf("Create user: %v", err)
	}

	gotRoot, err := repos.S3Accounts.GetByAccessKey(ctx, "root-access")
	if err != nil {
		t.Fatalf("Get root: %v", err)
	}
	if gotRoot == nil || !gotRoot.IsRoot || gotRoot.Role != auth.RoleAdmin {
		t.Fatalf("root account = %#v, want root admin", gotRoot)
	}
	if err := repos.S3Accounts.Create(ctx, root); !errors.Is(err, repository.ErrAlreadyExists) {
		t.Fatalf("Create duplicate root error = %v, want ErrAlreadyExists", err)
	}

	users, err := repos.S3Accounts.ListNonRoot(ctx)
	if err != nil {
		t.Fatalf("ListNonRoot: %v", err)
	}
	if len(users) != 1 || users[0].AccessKey != "user-access" {
		t.Fatalf("users = %#v, want only non-root user", users)
	}

	gotRootAccount, err := repos.S3Accounts.GetRoot(ctx)
	if err != nil {
		t.Fatalf("GetRoot: %v", err)
	}
	if gotRootAccount == nil || gotRootAccount.AccessKey != "root-access" {
		t.Fatalf("GetRoot = %#v, want root-access", gotRootAccount)
	}

	if err := repos.S3Accounts.Delete(ctx, "root-access"); err != nil {
		t.Fatalf("Delete root: %v", err)
	}
	missingRoot, err := repos.S3Accounts.GetRoot(ctx)
	if err != nil {
		t.Fatalf("GetRoot after delete: %v", err)
	}
	if missingRoot != nil {
		t.Fatalf("GetRoot after delete = %#v, want nil", missingRoot)
	}
	if err := repos.S3Accounts.Create(ctx, root); err != nil {
		t.Fatalf("Create root again: %v", err)
	}

	locked, err := repos.S3Accounts.LockByAccessKey(ctx, "root-access")
	if err != nil {
		t.Fatalf("LockByAccessKey: %v", err)
	}
	if locked == nil || locked.AccessKey != "root-access" {
		t.Fatalf("LockByAccessKey = %#v, want root-access", locked)
	}
	missingLock, err := repos.S3Accounts.LockByAccessKey(ctx, "not-found")
	if err != nil {
		t.Fatalf("LockByAccessKey missing: %v", err)
	}
	if missingLock != nil {
		t.Fatalf("LockByAccessKey missing = %#v, want nil", missingLock)
	}

	if err := repos.S3Accounts.Update(ctx, "user-access", repository.S3AccountUpdate{Role: auth.RoleAdmin}); err != nil {
		t.Fatalf("Update role: %v", err)
	}
	updated, err := repos.S3Accounts.GetByAccessKey(ctx, "user-access")
	if err != nil {
		t.Fatalf("Get updated user: %v", err)
	}
	if updated.Role != auth.RoleAdmin {
		t.Fatalf("role = %q, want admin", updated.Role)
	}
	if err := repos.S3Accounts.Update(ctx, "missing", repository.S3AccountUpdate{Role: auth.RoleAdmin}); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("Update missing error = %v, want ErrNotFound", err)
	}

	if err := repos.S3Accounts.Delete(ctx, "user-access"); err != nil {
		t.Fatalf("Delete user: %v", err)
	}
	missing, err := repos.S3Accounts.GetByAccessKey(ctx, "user-access")
	if err != nil {
		t.Fatalf("Get deleted user: %v", err)
	}
	if missing != nil {
		t.Fatalf("deleted user = %#v, want nil", missing)
	}

	if err := repos.S3Accounts.Delete(ctx, "missing"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("Delete missing error = %v, want ErrNotFound", err)
	}
}

func TestBucketRepo_OwnerAccessKeyCountAndACLUpdate(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	for _, account := range []*model.S3Account{
		{AccessKey: "root-access", SecretKey: "root-secret", Role: auth.RoleAdmin, IsRoot: true},
		{AccessKey: "owner-a", SecretKey: "secret-a", Role: auth.RoleUserPlus},
		{AccessKey: "owner-b", SecretKey: "secret-b", Role: auth.RoleUserPlus},
	} {
		if err := repos.S3Accounts.Create(ctx, account); err != nil {
			t.Fatalf("Create account %s: %v", account.AccessKey, err)
		}
	}

	ownerA := "owner-a"
	root := "root-access"
	for _, bucket := range []*model.Bucket{
		{Name: "a-one", Status: model.BucketStatusActive, OwnerAccessKey: &ownerA},
		{Name: "a-two", Status: model.BucketStatusActive, OwnerAccessKey: &ownerA},
		{Name: "root-one", Status: model.BucketStatusActive, OwnerAccessKey: &root},
		{Name: "unassigned", Status: model.BucketStatusActive},
	} {
		if err := repos.Buckets.Create(ctx, bucket); err != nil {
			t.Fatalf("Create bucket %s: %v", bucket.Name, err)
		}
	}

	count, err := repos.Buckets.CountByOwner(ctx, "owner-a")
	if err != nil {
		t.Fatalf("CountByOwner owner-a: %v", err)
	}
	if count != 2 {
		t.Fatalf("owner-a count = %d, want 2", count)
	}
	count, err = repos.Buckets.CountByOwner(ctx, "root-access")
	if err != nil {
		t.Fatalf("CountByOwner root: %v", err)
	}
	if count != 1 {
		t.Fatalf("root count = %d, want 1", count)
	}

	ownerB := "owner-b"
	if err := repos.Buckets.SetOwnerAndACL(ctx, "a-two", &ownerB, []byte(`{"Owner":"owner-b"}`)); err != nil {
		t.Fatalf("SetOwnerAndACL: %v", err)
	}
	updated, err := repos.Buckets.GetByName(ctx, "a-two")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated.OwnerAccessKey == nil || *updated.OwnerAccessKey != "owner-b" {
		t.Fatalf("owner = %v, want owner-b", updated.OwnerAccessKey)
	}
}
