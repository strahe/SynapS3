//go:build postgres

package repository_test

import (
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
)

func TestPostgresCompleteSnapshotOrdering(t *testing.T) {
	t.Run("newer snapshot wins", func(t *testing.T) {
		assertNewerCompleteSnapshotsWin(t, newPostgresTaskDB(t))
	})
	t.Run("observations stored ahead of the clock are replaced", func(t *testing.T) {
		assertObservationsAheadOfTheClockAreReplaced(t, newPostgresTaskDB(t))
	})
	t.Run("equal time keeps the first commit", func(t *testing.T) {
		db := newPostgresTaskDB(t)
		ctx := t.Context()
		repos := repository.NewRepositories(db)
		bucket := seedBucket(t, db, "snapshot-race")
		local := seedStorageDataSet(t, db, bucket.ID, "101", "1001", model.StorageDataSetStatusReady)
		at := time.Date(2026, 9, 26, 11, 0, 0, 0, time.UTC)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := repository.NewRepositories(tx).Observability.ReplaceDataSetStates(ctx, at, dataSetSnapshot(local, observability.StatusAvailable)); err != nil {
			t.Fatalf("first snapshot: %v", err)
		}
		late := make(chan error, 1)
		go func() {
			late <- repos.Observability.ReplaceDataSetStates(ctx, at, dataSetSnapshot(local, observability.StatusUnavailable))
		}()
		select {
		case err := <-late:
			t.Fatalf("a snapshot at the same time finished while the first held the collection: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := <-late; err != nil {
			t.Fatalf("late snapshot: %v", err)
		}
		page, err := repos.Observability.ListDataSetStates(ctx, observability.ListOptions{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) != 1 || page.Items[0].Status != observability.StatusAvailable ||
			page.LastCheckedAt == nil || !page.LastCheckedAt.Equal(at) {
			t.Fatalf("data sets = %#v, want the first committed snapshot", page)
		}
	})
}
