//go:build postgres

package repository_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	appdb "github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testpg"
	"github.com/uptrace/bun"
)

type storageContentLockContextKey struct{}

type storageContentLockBarrier struct {
	blockedOperation string
	locked           chan struct{}
	release          chan struct{}
	attempted        chan struct{}
	lockOnce         sync.Once
	attemptOnce      sync.Once
}

func (h *storageContentLockBarrier) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if storageContentLockQuery(event.Query) && ctx.Value(storageContentLockContextKey{}) != h.blockedOperation {
		h.attemptOnce.Do(func() { close(h.attempted) })
	}
	return ctx
}

func (h *storageContentLockBarrier) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if !storageContentLockQuery(event.Query) || ctx.Value(storageContentLockContextKey{}) != h.blockedOperation {
		return
	}
	h.lockOnce.Do(func() {
		close(h.locked)
		<-h.release
	})
}

func storageContentLockQuery(query string) bool {
	query = strings.ToLower(query)
	return strings.Contains(query, "update") && strings.Contains(query, "storage_contents") && strings.Contains(query, "updated_at = updated_at")
}

func migratedPostgresDB(t *testing.T) *bun.DB {
	t.Helper()
	db, err := appdb.New(config.DatabaseConfig{Driver: "postgres", DSN: testpg.SchemaDSN(t), MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatalf("opening postgres test connections: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL test database: %v", err)
		}
	})
	if err := appdb.RunMigrations(context.Background(), db); err != nil {
		t.Fatalf("running postgres test migrations: %v", err)
	}
	return db
}

func waitPostgresSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitPostgresResult(t *testing.T, result <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

// TestPostgresPermanentDeleteSerializesContentReuse pins the race that content
// dedup creates: one writer permanently deletes the last version of a content
// while another writes a new version onto the same content. Both must serialize
// on the content row. Cache release rechecks after both operations commit, so
// the surviving follower must retain the shared file regardless of lock order.
func TestPostgresPermanentDeleteSerializesContentReuse(t *testing.T) {
	for _, tc := range []struct {
		name             string
		blockedOperation string
	}{
		{name: "delete wins", blockedOperation: "delete"},
		{name: "reuse wins", blockedOperation: "reuse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := migratedPostgresDB(t)
			repos := repository.NewRepositories(db)
			ctx := context.Background()
			bucket := seedBucket(t, db, "postgres-permanent-delete-"+strings.ReplaceAll(tc.name, " ", "-"))
			contentID := seedContent(t, repos, bucket.ID, "postgres-shared-content", 10)

			source := newObjectVersion(bucket.ID, "source.txt", model.NewVersionID(), 10)
			source.ContentID = &contentID
			if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source); err != nil {
				t.Fatalf("CreateVersionAndSetCurrent(source): %v", err)
			}
			follower := newObjectVersion(bucket.ID, "follower.txt", model.NewVersionID(), source.Size)
			follower.ContentID = &contentID

			barrier := &storageContentLockBarrier{
				blockedOperation: tc.blockedOperation,
				locked:           make(chan struct{}),
				release:          make(chan struct{}),
				attempted:        make(chan struct{}),
			}
			defer func() {
				select {
				case <-barrier.release:
				default:
					close(barrier.release)
				}
			}()
			db.AddQueryHook(barrier)
			deleteCtx := context.WithValue(ctx, storageContentLockContextKey{}, "delete")
			reuseCtx := context.WithValue(ctx, storageContentLockContextKey{}, "reuse")
			deleteResult := make(chan error, 1)
			reuseResult := make(chan error, 1)
			startDelete := func() {
				go func() {
					_, err := repos.Objects.DeleteObjectVersionPermanently(deleteCtx, repository.DeleteObjectVersionInput{
						BucketID: bucket.ID, Key: source.Key, VersionID: source.VersionID,
					})
					deleteResult <- err
				}()
			}
			startReuse := func() {
				go func() {
					_, err := repos.Objects.CreateVersionAndSetCurrent(reuseCtx, follower)
					reuseResult <- err
				}()
			}
			if tc.blockedOperation == "delete" {
				startDelete()
			} else {
				startReuse()
			}
			waitPostgresSignal(t, barrier.locked, "first storage content lock")
			if tc.blockedOperation == "delete" {
				startReuse()
			} else {
				startDelete()
			}
			waitPostgresSignal(t, barrier.attempted, "competing storage content lock")
			close(barrier.release)
			if err := waitPostgresResult(t, deleteResult, "permanent delete"); err != nil {
				t.Fatalf("DeleteObjectVersionPermanently: %v", err)
			}
			if err := waitPostgresResult(t, reuseResult, "content reuse"); err != nil {
				t.Fatalf("CreateVersionAndSetCurrent(follower): %v", err)
			}

			unreferenced, err := repos.Objects.ContentIsUnreferenced(ctx, contentID)
			if err != nil {
				t.Fatalf("ContentIsUnreferenced: %v", err)
			}
			// Whoever won, the follower survives, so the bytes are still named.
			if unreferenced {
				t.Fatal("content is unreferenced after the reuse committed")
			}
			deleteCalls := 0
			released, err := repos.Objects.ReleaseContentCacheIfUnreferenced(ctx, contentID, func() error {
				deleteCalls++
				return nil
			})
			if err != nil || released || deleteCalls != 0 {
				t.Fatalf("cache release = %t, %v calls=%d, want retained", released, err, deleteCalls)
			}
		})
	}
}
