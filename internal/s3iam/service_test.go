package s3iam_test

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/s3iam"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
)

func TestServiceEnsureRootAndListNonRootAccounts(t *testing.T) {
	repos := repository.NewRepositories(testutil.NewTestDB(t))
	svc := s3iam.NewService(repos)
	ctx := context.Background()

	root, err := svc.EnsureRootAccount(ctx)
	if err != nil {
		t.Fatalf("EnsureRootAccount: %v", err)
	}
	if root.Access == "" || root.Secret == "" || root.Role != auth.RoleAdmin {
		t.Fatalf("root account = %#v, want generated admin credentials", root)
	}
	again, err := svc.EnsureRootAccount(ctx)
	if err != nil {
		t.Fatalf("EnsureRootAccount again: %v", err)
	}
	if again.Access != root.Access || again.Secret != root.Secret {
		t.Fatalf("root changed across EnsureRootAccount: first=%#v second=%#v", root, again)
	}

	if err := svc.CreateAccount(auth.Account{Access: "user-access", Secret: "user-secret", Role: auth.RoleUserPlus}); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	rootLookup, err := svc.GetUserAccount(root.Access)
	if err != nil {
		t.Fatalf("GetUserAccount root: %v", err)
	}
	if rootLookup.Role != auth.RoleAdmin {
		t.Fatalf("root role = %q, want admin", rootLookup.Role)
	}
	users, err := svc.ListUserAccounts()
	if err != nil {
		t.Fatalf("ListUserAccounts: %v", err)
	}
	if len(users) != 1 || users[0].Access != "user-access" {
		t.Fatalf("users = %#v, want only non-root user", users)
	}
	for _, tt := range []struct {
		name   string
		access []string
		want   []string
	}{
		{name: "empty"},
		{name: "existing accounts", access: []string{root.Access, "user-access"}},
		{name: "missing accounts", access: []string{"missing-first", root.Access, "user-access", "missing-last"}, want: []string{"missing-first", "missing-last"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			missing, err := svc.ResolveAccounts(tt.access)
			if err != nil {
				t.Fatalf("ResolveAccounts: %v", err)
			}
			if !slices.Equal(missing, tt.want) {
				t.Fatalf("missing accounts = %v, want %v", missing, tt.want)
			}
		})
	}
}

func TestServiceResolveAccountsReturnsLookupError(t *testing.T) {
	db := testutil.NewTestDB(t)
	svc := s3iam.NewService(repository.NewRepositories(db))
	if err := db.Close(); err != nil {
		t.Fatalf("Close database: %v", err)
	}
	_, lookupErr := svc.GetUserAccount("unavailable-account")
	if lookupErr == nil || errors.Is(lookupErr, auth.ErrNoSuchUser) {
		t.Fatalf("GetUserAccount = %v, want database error", lookupErr)
	}
	missing, err := svc.ResolveAccounts([]string{"unavailable-account"})
	if !errors.Is(err, errors.Unwrap(lookupErr)) || len(missing) != 0 {
		t.Fatalf("ResolveAccounts = %v, %v, want lookup error %v", missing, err, lookupErr)
	}
}

func TestServiceRejectsRootMutationAndHandlesUserLifecycle(t *testing.T) {
	repos := repository.NewRepositories(testutil.NewTestDB(t))
	svc := s3iam.NewService(repos)
	ctx := context.Background()
	root, err := svc.EnsureRootAccount(ctx)
	if err != nil {
		t.Fatalf("EnsureRootAccount: %v", err)
	}

	if err := svc.DeleteUserAccount(root.Access); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Fatalf("Delete root error = %v, want ErrNoSuchUser", err)
	}
	if err := svc.CreateAccount(auth.Account{Access: "user-access", Secret: "user-secret", Role: auth.RoleUser}); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if err := svc.CreateAccount(auth.Account{Access: "user-access", Secret: "other", Role: auth.RoleUser}); !errors.Is(err, auth.ErrUserExists) {
		t.Fatalf("Create duplicate error = %v, want ErrUserExists", err)
	}
	if err := svc.UpdateUserAccount("user-access", auth.MutableProps{Role: auth.RoleAdmin}); err != nil {
		t.Fatalf("UpdateUserAccount: %v", err)
	}
	updated, err := svc.GetUserAccount("user-access")
	if err != nil {
		t.Fatalf("GetUserAccount updated: %v", err)
	}
	if updated.Role != auth.RoleAdmin {
		t.Fatalf("role = %q, want admin", updated.Role)
	}
	if err := svc.DeleteUserAccount("user-access"); err != nil {
		t.Fatalf("DeleteUserAccount: %v", err)
	}
	if _, err := svc.GetUserAccount("user-access"); !errors.Is(err, auth.ErrNoSuchUser) {
		t.Fatalf("Get deleted error = %v, want ErrNoSuchUser", err)
	}
}

func TestServiceDeleteUserAccountRejectsOwnedBuckets(t *testing.T) {
	repos := repository.NewRepositories(testutil.NewTestDB(t))
	svc := s3iam.NewService(repos)
	ctx := context.Background()

	if err := svc.CreateAccount(auth.Account{Access: "owner-access", Secret: "owner-secret", Role: auth.RoleUserPlus}); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	owner := "owner-access"
	if err := repos.Buckets.Create(ctx, &model.Bucket{
		Name:           "owned-bucket",
		OwnerAccessKey: &owner,
		Status:         model.BucketStatusActive,
		DefaultCopies:  8, MinimumDurableCopies: 8,
	}); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}

	err := svc.DeleteUserAccount("owner-access")
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("DeleteUserAccount error = %v, want s3 API error", err)
	}
	if apiErr.HTTPStatusCode != http.StatusConflict || apiErr.Code != "XAdminUserOwnsBuckets" {
		t.Fatalf("DeleteUserAccount API error = %#v, want user owns buckets conflict", apiErr)
	}
	if _, err := svc.GetUserAccount("owner-access"); err != nil {
		t.Fatalf("GetUserAccount after rejected delete: %v", err)
	}
}
