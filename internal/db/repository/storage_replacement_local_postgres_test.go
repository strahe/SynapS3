//go:build postgres

package repository_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func TestPostgresUncreatedReplacementAdmissionAndFencing(t *testing.T) {
	uncreatedReplacementAdmissionAndFencing(t, migratedPostgresDB(t))
}

func TestPostgresLocalReplacementBatchesPreserveFrozenCopyObligations(t *testing.T) {
	localReplacementBatchesPreserveFrozenCopyObligations(t, migratedPostgresDB(t))
}

func TestPostgresLocalReplacementRetryAndCompletion(t *testing.T) {
	localReplacementRetryAndCompletion(t, migratedPostgresDB(t))
}

func TestPostgresRejectedLocalTargetCanBeReplacedAgain(t *testing.T) {
	rejectedLocalTargetCanBeReplacedAgain(t, migratedPostgresDB(t))
}

func TestPostgresReplacementUploadWakeScope(t *testing.T) {
	replacementUploadWakeScope(t, migratedPostgresDB(t))
}

func TestPostgresReplacementObservationAuthorization(t *testing.T) {
	replacementObservationAuthorization(t, migratedPostgresDB(t))
}

func TestPostgresReadyTargetRejectsStaleRefusalSettlement(t *testing.T) {
	readyTargetRejectsStaleRefusalSettlement(t, migratedPostgresDB(t))
}

func TestPostgresRemoteSourceCannotCompleteLocally(t *testing.T) {
	remoteSourceCannotCompleteLocally(t, migratedPostgresDB(t))
}

func TestPostgresUncreatedReplacementSerializesWithCreatePermission(t *testing.T) {
	for _, first := range []string{"replacement", "creation"} {
		t.Run(first, func(t *testing.T) {
			db := migratedPostgresDB(t)
			repos, source, ensure := localReplacementSource(t, db, "create-permission-race")
			hold := newPostgresRaceHold(first, func(query string) bool {
				return strings.Contains(query, "storage_data_sets") &&
					(strings.Contains(query, "for update") || strings.HasPrefix(query, "update"))
			})
			db.AddQueryHook(hold)
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
				want = storagereplacement.ErrSourceOutcomeUnknown
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
		})
	}
}

func TestPostgresReadyBindingSerializesWithRefusalSettlement(t *testing.T) {
	for _, first := range []string{"ready", "refusal"} {
		t.Run(first, func(t *testing.T) {
			db := migratedPostgresDB(t)
			repos, source, _ := localReplacementSource(t, db, "ready-refusal-race")
			row, _, err := repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual, TargetProviderID: onChainID(t, "202"), ClientRequestID: "first"})
			if err != nil {
				t.Fatal(err)
			}
			target, err := repos.Contents.GetDataSetBindingByID(t.Context(), row.TargetDataSetID)
			if err != nil {
				t.Fatal(err)
			}
			localRefusal(t, db, repos, target, localEnsureTask(t, repos, target.ID))
			hold := newPostgresRaceHold(first, func(query string) bool {
				return strings.Contains(query, "storage_replacements") &&
					(strings.Contains(query, "for update") || strings.HasPrefix(query, "update"))
			})
			db.AddQueryHook(hold)
			defer hold.Release()
			reason := storagereplacement.FailureReasonTargetRejected
			operations := map[string]func(context.Context) error{
				"ready": func(ctx context.Context) error {
					return repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: target.ID, DataSetID: onChainID(t, "2002")})
				},
				"refusal": func(ctx context.Context) error { return repos.Replacements.MarkFailed(ctx, row.ID, &reason, "refused") },
			}
			second := "refusal"
			if first == second {
				second = "ready"
			}
			firstResult, secondResult := make(chan error, 1), make(chan error, 1)
			go func() { firstResult <- operations[first](postgresRaceContext(t.Context(), first)) }()
			select {
			case <-hold.reached:
			case err := <-firstResult:
				t.Fatalf("first writer exited before the lock synchronization: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("first writer did not acquire the replacement lock")
			}
			go func() { secondResult <- operations[second](postgresRaceContext(t.Context(), second)) }()
			waitForPostgresLockWait(t, db, "storage_replacements")
			hold.Release()
			if err := <-firstResult; err != nil {
				t.Fatal(err)
			}
			if err := <-secondResult; (second == "refusal" && !errors.Is(err, repository.ErrConflict)) || (second == "ready" && err != nil) {
				t.Fatalf("second writer = %v", err)
			}
			stored, err := repos.Replacements.GetByID(t.Context(), row.ID)
			if err != nil || stored.FailureReason != nil {
				t.Fatalf("ready service kept a refusal failure: %#v err=%v", stored, err)
			}
			ready, err := repos.Contents.GetDataSetBindingByID(t.Context(), target.ID)
			if err != nil || ready.DataSetID == nil || len(ready.CreationRejection) != 0 {
				t.Fatalf("service binding was lost: %#v err=%v", ready, err)
			}
			if first == "refusal" {
				if err := repos.Replacements.RetryEligibility(t.Context(), row.ID); err != nil {
					t.Fatalf("bound service could not resume: %v", err)
				}
			}
		})
	}
}
