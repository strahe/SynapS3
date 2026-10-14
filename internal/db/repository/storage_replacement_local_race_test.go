package repository_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/testutil"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func TestPostgresUncreatedReplacementAdmissionAndFencing(t *testing.T) {
	uncreatedReplacementAdmissionAndFencing(t, testDB(t))
}

func TestPostgresLocalReplacementBatchesPreserveFrozenCopyObligations(t *testing.T) {
	localReplacementBatchesPreserveFrozenCopyObligations(t, testDB(t))
}

func TestPostgresLocalReplacementRetryAndCompletion(t *testing.T) {
	localReplacementRetryAndCompletion(t, testDB(t))
}

func TestPostgresRejectedLocalTargetCanBeReplacedAgain(t *testing.T) {
	rejectedLocalTargetCanBeReplacedAgain(t, testDB(t))
}

func TestPostgresReplacementUploadWakeScope(t *testing.T) {
	replacementUploadWakeScope(t, testDB(t))
}

func TestPostgresReadyTargetRejectsStaleInitializationFailure(t *testing.T) {
	readyTargetRejectsStaleInitializationFailure(t, testDB(t))
}

func TestPostgresRemoteSourceCannotCompleteLocally(t *testing.T) {
	remoteSourceCannotCompleteLocally(t, testDB(t))
}

func TestPostgresAutomaticReplacementsReserveTargetAcrossReplicas(t *testing.T) {
	db := testDB(t)
	repos, firstSource, _ := localReplacementSource(t, db, "automatic-target-race")
	testutil.OpenBucketReplicaSlots(t, db, firstSource.BucketID, 2)
	secondSource, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: firstSource.BucketID, ProviderID: onChainID(t, "303"), CopyIndex: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	markSourceReady(t, repos, secondSource.ID)
	hold := newPostgresRaceHold("first", func(query string) bool {
		return strings.Contains(query, "buckets") && strings.Contains(query, "for no key update")
	})
	testutil.AddQueryHook(db, hold)
	defer hold.Release()
	firstInput := repository.AuthorizeReplacementInput{
		BucketID: firstSource.BucketID, SourceDataSetID: firstSource.ID,
		SelectionMode: storagereplacement.SelectionModeAutomatic, TargetProviderID: onChainID(t, "202"), ClientRequestID: "first",
	}
	secondInput := firstInput
	secondInput.SourceDataSetID, secondInput.ClientRequestID = secondSource.ID, "second"
	firstResult, secondResult := make(chan error, 1), make(chan error, 1)
	var first *storagereplacement.Replacement
	go func() {
		var err error
		first, _, err = repos.Replacements.Authorize(postgresRaceContext(t.Context(), "first"), firstInput)
		firstResult <- err
	}()
	waitPostgresSignal(t, hold.reached, "first authorization acquired the bucket lock")
	go func() {
		_, _, err := repos.Replacements.Authorize(t.Context(), secondInput)
		secondResult <- err
	}()
	waitForPostgresLockWait(t, db, "buckets")
	hold.Release()
	if err := waitPostgresResult(t, firstResult, "first authorization"); err != nil {
		t.Fatal(err)
	}
	if err := waitPostgresResult(t, secondResult, "second authorization"); !errors.Is(err, storagereplacement.ErrTargetInUse) {
		t.Fatalf("second authorization = %v, want target in use", err)
	}
	replacements, err := repos.Replacements.ListForBucket(t.Context(), firstSource.BucketID, 0)
	if err != nil || len(replacements) != 1 || replacements[0].ID != first.ID {
		t.Fatalf("authorized replacements = %#v, err=%v", replacements, err)
	}
	replayed, created, err := repos.Replacements.Authorize(t.Context(), firstInput)
	if err != nil || created || replayed.ID != first.ID || replayed.TargetDataSetID != first.TargetDataSetID {
		t.Fatalf("automatic replay = %#v, created=%v, err=%v", replayed, created, err)
	}
}

func TestPostgresUncreatedReplacementSerializesWithCreatePermission(t *testing.T) {
	for _, first := range []string{"replacement", "creation"} {
		t.Run(first, func(t *testing.T) {
			db := testDB(t)
			repos, source, ensure := localReplacementSource(t, db, "create-permission-race")
			hold := newPostgresRaceHold(first, func(query string) bool {
				return strings.Contains(query, "storage_data_sets") &&
					(strings.Contains(query, "for update") || strings.Contains(query, "for no key update") || strings.HasPrefix(query, "update"))
			})
			testutil.AddQueryHook(db, hold)
			defer hold.Release()
			operations := map[string]func(context.Context) error{
				"replacement": func(ctx context.Context) error {
					_, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
						BucketID: source.BucketID, SourceDataSetID: source.ID,
						SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "202"), ClientRequestID: "race",
					})
					return err
				},
				"creation": func(ctx context.Context) error {
					return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
						return repository.NewRepositories(tx).Contents.RecordDataSetClientID(ctx, source.ID, ensure.ID, onChainID(t, "909"))
					})
				},
			}
			second := "creation"
			if first == second {
				second = "replacement"
			}
			firstResult, secondResult := make(chan error, 1), make(chan error, 1)
			go func() { firstResult <- operations[first](postgresRaceContext(t.Context(), first)) }()
			select {
			case <-hold.reached:
			case <-time.After(5 * time.Second):
				t.Fatal("first operation did not acquire the data set lock")
			}
			go func() { secondResult <- operations[second](postgresRaceContext(t.Context(), second)) }()
			waitForPostgresLockWait(t, db, "storage_data_sets")
			hold.Release()
			if err := <-firstResult; err != nil {
				t.Fatal(err)
			}
			want := repository.ErrConflict
			if second == "replacement" {
				want = nil
			}
			if err := <-secondResult; !errors.Is(err, want) {
				t.Fatalf("second operation = %v, want %v", err, want)
			}
			stored, err := repos.Contents.GetDataSetBindingByID(t.Context(), source.ID)
			if err != nil {
				t.Fatal(err)
			}
			if (stored.ClientDataSetID != nil) != (first == "creation") {
				t.Fatalf("creation permission escaped replacement: %#v", stored)
			}
			if stored.EnsureTaskID != nil {
				t.Fatalf("replacement retained creation permission: %#v", stored)
			}
		})
	}
}
