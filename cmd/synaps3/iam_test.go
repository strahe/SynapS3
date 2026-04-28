package main

import (
	"context"
	"errors"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/versity/versitygw/auth"
)

func TestNewS3IAMServiceUsesDBBackedRootAccount(t *testing.T) {
	repos := repository.NewRepositories(testutil.NewTestDB(t))

	iamSvc, root, err := newS3IAMService(context.Background(), repos)
	if err != nil {
		t.Fatalf("newS3IAMService: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })

	if root.Access == "" || root.Secret == "" || root.Role != auth.RoleAdmin {
		t.Fatalf("root = %#v, want generated admin root account", root)
	}
	account, err := iamSvc.GetUserAccount(root.Access)
	if err != nil {
		t.Fatalf("GetUserAccount root: %v", err)
	}
	if account.Access != root.Access || account.Secret != root.Secret || account.Role != auth.RoleAdmin {
		t.Fatalf("account = %#v, want root %#v", account, root)
	}
}

func TestNewS3IAMServiceReusesExistingDBRootAccount(t *testing.T) {
	repos := repository.NewRepositories(testutil.NewTestDB(t))

	_, firstRoot, err := newS3IAMService(context.Background(), repos)
	if err != nil {
		t.Fatalf("first newS3IAMService: %v", err)
	}
	_, secondRoot, err := newS3IAMService(context.Background(), repos)
	if err != nil {
		t.Fatalf("second newS3IAMService: %v", err)
	}
	if firstRoot.Access != secondRoot.Access || firstRoot.Secret != secondRoot.Secret {
		t.Fatalf("root changed: first=%#v second=%#v", firstRoot, secondRoot)
	}
}

func TestNewS3IAMServiceRejectsUnknownAccessKeyAsNoSuchUser(t *testing.T) {
	repos := repository.NewRepositories(testutil.NewTestDB(t))
	iamSvc, _, err := newS3IAMService(context.Background(), repos)
	if err != nil {
		t.Fatalf("newS3IAMService: %v", err)
	}
	t.Cleanup(func() { _ = iamSvc.Shutdown() })

	acct, err := iamSvc.GetUserAccount("wrong-access")
	if !errors.Is(err, auth.ErrNoSuchUser) {
		t.Fatalf("GetUserAccount wrong access error = %v, want ErrNoSuchUser", err)
	}
	if acct != (auth.Account{}) {
		t.Fatalf("account = %#v, want zero value", acct)
	}
}
