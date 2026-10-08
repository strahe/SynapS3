//go:build postgres

package repository_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestPostgresCopyRetryAdmissionAndHistory(t *testing.T) {
	copyRetryAdmissionAndHistory(t, migratedPostgresDB(t))
}

func TestPostgresCopyRetryFailedIngressWithSuccessor(t *testing.T) {
	copyRetryFailedIngressWithSuccessor(t, migratedPostgresDB(t))
}

func TestPostgresOrdinaryCacheRestoreProtectsCache(t *testing.T) {
	ordinaryCacheRestoreProtectsCache(t, migratedPostgresDB(t))
}

func TestPostgresCopyRetrySerializesReplacement(t *testing.T) {
	for _, first := range []string{"retry", "replacement"} {
		t.Run(first, func(t *testing.T) {
			db := postgresRaceDB(t)
			copyRow := copyRetryAdmissionAndHistory(t, db)
			repos := repository.NewRepositories(db)
			if err := repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex, LastError: "failed again"}); err != nil {
				t.Fatal(err)
			}
			hold := newPostgresRaceHold(first, func(query string) bool {
				if first == "retry" {
					return strings.Contains(query, `"buckets"`) && strings.Contains(query, "for share")
				}
				return strings.HasPrefix(query, "update") && strings.Contains(query, `"buckets"`)
			})
			db.AddQueryHook(hold)
			defer hold.Release()
			gate := cacheaccess.NewGate()
			runRetry := func() error {
				release := gate.HoldRead(model.ContentCacheKey(copyRow.ContentID))
				defer release()
				_, err := repos.Contents.RetryFailedCopy(postgresRaceContext(t.Context(), "retry"), copyRow.ID)
				return err
			}
			runReplacement := func() error {
				_, _, err := repos.Replacements.Authorize(postgresRaceContext(t.Context(), "replacement"), repository.AuthorizeReplacementInput{BucketID: copyRow.BucketID, SourceDataSetID: copyRow.StorageDataSetID, TargetProviderID: onChainID(t, "900"), ClientRequestID: "retry-race", SelectionMode: storagereplacement.SelectionModeManual, PriceListFingerprint: "prices"})
				return err
			}
			firstDone := make(chan error, 1)
			secondDone := make(chan error, 1)
			if first == "retry" {
				go func() { firstDone <- runRetry() }()
			} else {
				go func() { firstDone <- runReplacement() }()
			}
			select {
			case <-hold.reached:
			case err := <-firstDone:
				t.Fatalf("first operation did not reach lock: %v", err)
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			if first == "retry" {
				go func() { secondDone <- runReplacement() }()
			} else {
				go func() { secondDone <- runRetry() }()
			}
			waitForPostgresLockWait(t, db, "buckets")
			hold.Release()
			if err := <-firstDone; err != nil {
				t.Fatal(err)
			}
			err := <-secondDone
			if first == "replacement" {
				var blocked *repository.CopyRetryBlockedError
				if !errors.As(err, &blocked) || blocked.Block != storagepipeline.CopyRetryReplacementInProgress {
					t.Fatalf("retry after replacement=%v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				incomplete, err := repos.Contents.ListIncompleteCopiesForDataSet(t.Context(), copyRow.StorageDataSetID)
				if err != nil || len(incomplete) != 1 || incomplete[0].ID != copyRow.ID {
					t.Fatalf("source work=%+v err=%v", incomplete, err)
				}
			}
		})
	}
}

func TestPostgresCopyRetryConcurrentRequests(t *testing.T) {
	db := postgresRaceDB(t)
	copyRow := copyRetryAdmissionAndHistory(t, db)
	repos := repository.NewRepositories(db)
	if err := repos.Contents.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex, LastError: "failed again"}); err != nil {
		t.Fatal(err)
	}
	hold := newPostgresRaceHold("first", func(query string) bool {
		return strings.HasPrefix(query, "update") && strings.Contains(query, `"storage_contents"`)
	})
	db.AddQueryHook(hold)
	defer hold.Release()
	done := make(chan error, 2)
	go func() {
		_, err := repos.Contents.RetryFailedCopy(postgresRaceContext(t.Context(), "first"), copyRow.ID)
		done <- err
	}()
	select {
	case <-hold.reached:
	case err := <-done:
		t.Fatalf("first operation did not reach lock: %v", err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	go func() { _, err := repos.Contents.RetryFailedCopy(t.Context(), copyRow.ID); done <- err }()
	waitForPostgresLockWait(t, db, "storage_contents")
	hold.Release()
	err1, err2 := <-done, <-done
	if !((err1 == nil && errors.Is(err2, repository.ErrConflict)) || (err2 == nil && errors.Is(err1, repository.ErrConflict))) {
		t.Fatalf("concurrent retry=%v, %v", err1, err2)
	}
}

func TestPostgresCopyRetryWaitsForCacheEviction(t *testing.T) {
	db := postgresRaceDB(t)
	copyRow := copyRetryAdmissionAndHistory(t, db)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket, err := repos.Buckets.GetByID(ctx, copyRow.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	one := 1
	if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{Name: bucket.Name, SetMinimumDurableCopies: true, MinimumDurableCopies: &one}); err != nil {
		t.Fatal(err)
	}
	source, err := repos.Contents.GetUploadCopy(ctx, copyRow.ContentID, 0)
	if err != nil {
		t.Fatal(err)
	}
	piece := onChainID(t, "0")
	testutil.CommitStorageCopy(t, db, repos, testutil.CommitCopyInput{StorageCopyID: source.ID, ContentID: copyRow.ContentID, CopyIndex: 0, PieceCID: "bafk2bzacecretryeviction", PieceID: &piece, RetrievalURL: "https://source.example/piece"})
	if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex, LastError: "failed again"}); err != nil {
		t.Fatal(err)
	}
	evict, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: model.TaskTypeCacheEvict, IdempotencyKey: "retry-race-evict", InputVersion: 1, Input: []byte(`{}`), InputHash: "evict"}))
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := repos.CacheEvictions.PrepareEviction(ctx, copyRow.ContentID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.CacheEvictions.BindEvictionTask(ctx, copyRow.ContentID, reservation.Generation, evict.ID); err != nil {
		t.Fatal(err)
	}
	hold := newPostgresRaceHold("eviction", func(query string) bool {
		return strings.HasPrefix(query, "update") && strings.Contains(query, `"storage_contents"`)
	})
	db.AddQueryHook(hold)
	defer hold.Release()
	gate := cacheaccess.NewGate()
	key := model.ContentCacheKey(copyRow.ContentID)
	evictionDone := make(chan error, 1)
	go func() {
		var evictionErr error
		gate.GuardDeletion(key, func() {
			evictionErr = repos.WithTx(postgresRaceContext(ctx, "eviction"), func(tx *repository.Repositories) error {
				if _, err := tx.CacheEvictions.AuthorizeDeletion(postgresRaceContext(ctx, "eviction"), copyRow.ContentID, reservation.Generation, evict.ID, nil); err != nil {
					return err
				}
				return tx.CacheEvictions.RecordDeletion(ctx, copyRow.ContentID, reservation.Generation, evict.ID)
			})
		})
		evictionDone <- evictionErr
	}()
	select {
	case <-hold.reached:
	case err := <-evictionDone:
		t.Fatalf("eviction did not reach content lock: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	retryDone := make(chan error, 1)
	go func() {
		release := gate.HoldRead(key)
		defer release()
		reopened, err := repos.Contents.RetryFailedCopy(ctx, copyRow.ID)
		if err == nil && reopened.TransferMethod != model.StorageCopyTransferMethodPeerPull {
			err = errors.New("retry selected deleted cache instead of the readable replica")
		}
		retryDone <- err
	}()
	select {
	case err := <-retryDone:
		t.Fatalf("retry passed the gate during eviction: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	hold.Release()
	if err := <-evictionDone; err != nil {
		t.Fatal(err)
	}
	if err := <-retryDone; err != nil {
		t.Fatal(err)
	}
	entry, err := repos.CacheEvictions.GetCacheEntry(ctx, copyRow.ContentID)
	if err != nil || entry.InCache || entry.CacheActiveTaskID != nil {
		t.Fatalf("cache after eviction=%+v err=%v", entry, err)
	}
}
