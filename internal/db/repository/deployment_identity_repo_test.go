package repository_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func TestDeploymentIdentityBindsOnceAndRejectsDifferentContext(t *testing.T) {
	database := testDB(t)
	repos := repository.NewRepositories(database)
	input := repository.BindDeploymentIdentityInput{Payer: "0x00000000000000000000000000000000000000A1", ChainID: 314159}
	first, err := repos.DeploymentIdentity.BindOrVerify(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Payer != "0x00000000000000000000000000000000000000a1" || first.CreatedAt.IsZero() {
		t.Fatalf("identity = %#v", first)
	}
	if _, err := database.NewInsert().Model(&model.S3Account{AccessKey: "bound-account", SecretKey: "secret", Role: "user"}).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := repos.DeploymentIdentity.BindOrVerify(t.Context(), input)
	if err != nil || !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("repeat = %#v, %v", second, err)
	}
	for _, different := range []repository.BindDeploymentIdentityInput{
		{Payer: "0x00000000000000000000000000000000000000b2", ChainID: input.ChainID},
		{Payer: input.Payer, ChainID: 314},
	} {
		if _, err := repos.DeploymentIdentity.BindOrVerify(t.Context(), different); !errors.Is(err, repository.ErrDeploymentIdentityMismatch) {
			t.Fatalf("different identity error = %v", err)
		}
	}
}

func TestDeploymentIdentityRefusesAccountOnlyDatabase(t *testing.T) {
	database := testDB(t)
	repos := repository.NewRepositories(database)
	if _, err := database.NewInsert().Model(&model.S3Account{AccessKey: "unbound-account", SecretKey: "secret", Role: "user"}).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.DeploymentIdentity.BindOrVerify(t.Context(), repository.BindDeploymentIdentityInput{Payer: "0x00000000000000000000000000000000000000a1", ChainID: 314159}); !errors.Is(err, repository.ErrDeploymentIdentityMissing) {
		t.Fatalf("unbound database error = %v", err)
	}
	count, err := database.NewSelect().Model((*model.DeploymentIdentity)(nil)).Count(t.Context())
	if err != nil || count != 0 {
		t.Fatalf("identity rows = %d, %v", count, err)
	}
}

func TestConcurrentDeploymentIdentityBindingHasOneWinner(t *testing.T) {
	database := testDB(t)
	repos := repository.NewRepositories(database)
	inputs := []repository.BindDeploymentIdentityInput{
		{Payer: "0x00000000000000000000000000000000000000a1", ChainID: 314159},
		{Payer: "0x00000000000000000000000000000000000000b2", ChainID: 314159},
	}
	errorsByCall := make([]error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	start := make(chan struct{})
	var done sync.WaitGroup
	for i := range inputs {
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, errorsByCall[i] = repos.DeploymentIdentity.BindOrVerify(t.Context(), inputs[i])
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	succeeded := 0
	for _, err := range errorsByCall {
		if err == nil {
			succeeded++
		} else if !errors.Is(err, repository.ErrDeploymentIdentityMismatch) {
			t.Fatal(err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful competing bindings = %d", succeeded)
	}
}
