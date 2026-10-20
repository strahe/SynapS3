package repository_test

import (
	"errors"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
)

func TestDataSetCreationKeepsTheCurrentTransactionPair(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "submitted"
		if ready {
			name = "immediately confirmed"
		}
		t.Run(name, func(t *testing.T) {
			db := testDB(t)
			repos := repository.NewRepositories(db)
			bucket := seedBucket(t, db, "creation-pair")
			binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
			})
			if err != nil {
				t.Fatal(err)
			}
			original := repository.MarkDataSetCreatingInput{ID: binding.ID, TxHash: "0xfirst", ConfirmedTxHash: "0xfirst-confirmed", StatusURL: "https://provider.example/first"}
			if err := repos.Contents.MarkDataSetCreating(t.Context(), original); err != nil {
				t.Fatal(err)
			}
			for _, input := range []repository.MarkDataSetCreatingInput{{ID: binding.ID}, {ID: binding.ID, TxHash: original.TxHash}} {
				if err := repos.Contents.MarkDataSetCreating(t.Context(), input); err != nil {
					t.Fatal(err)
				}
			}
			conflict := original
			conflict.ConfirmedTxHash = "0xconflicting"
			if err := repos.Contents.MarkDataSetCreating(t.Context(), conflict); !errors.Is(err, repository.ErrConflict) {
				t.Fatalf("confirmation conflict=%v", err)
			}
			current, err := repos.Contents.GetDataSetBindingByID(t.Context(), binding.ID)
			if err != nil || current.TxHash == nil || *current.TxHash != original.TxHash || current.ConfirmedTxHash == nil || *current.ConfirmedTxHash != original.ConfirmedTxHash || current.CreateStatusURL == nil || *current.CreateStatusURL != original.StatusURL {
				t.Fatalf("original transaction pair=%#v err=%v", current, err)
			}
			if ready {
				err = repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
					ID: binding.ID, DataSetID: onChainID(t, "7001"), TxHash: "0xsecond", ConfirmedTxHash: "0xsecond-confirmed",
				})
			} else {
				err = repos.Contents.MarkDataSetCreating(t.Context(), repository.MarkDataSetCreatingInput{ID: binding.ID, TxHash: "0xsecond"})
			}
			if err != nil {
				t.Fatal(err)
			}
			current, err = repos.Contents.GetDataSetBindingByID(t.Context(), binding.ID)
			if err != nil || current.TxHash == nil || *current.TxHash != "0xsecond" || current.CreateStatusURL != nil {
				t.Fatalf("new transaction pair=%#v err=%v", current, err)
			}
			if ready && (current.ConfirmedTxHash == nil || *current.ConfirmedTxHash != "0xsecond-confirmed") || !ready && current.ConfirmedTxHash != nil {
				t.Fatalf("new confirmation=%#v", current.ConfirmedTxHash)
			}
		})
	}
}
