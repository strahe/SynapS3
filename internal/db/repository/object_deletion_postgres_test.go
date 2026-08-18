package repository_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	appdb "github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

type storageUploadLockContextKey struct{}

type storageUploadLockBarrier struct {
	blockedOperation string
	locked           chan struct{}
	release          chan struct{}
	attempted        chan struct{}
	lockOnce         sync.Once
	attemptOnce      sync.Once
}

func (h *storageUploadLockBarrier) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if storageUploadLockQuery(event.Query) && ctx.Value(storageUploadLockContextKey{}) != h.blockedOperation {
		h.attemptOnce.Do(func() { close(h.attempted) })
	}
	return ctx
}

func (h *storageUploadLockBarrier) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if !storageUploadLockQuery(event.Query) || ctx.Value(storageUploadLockContextKey{}) != h.blockedOperation {
		return
	}
	h.lockOnce.Do(func() {
		close(h.locked)
		<-h.release
	})
}

func storageUploadLockQuery(query string) bool {
	query = strings.ToLower(query)
	return strings.Contains(query, "update") && strings.Contains(query, "storage_uploads") && strings.Contains(query, "status = status")
}

func TestPostgresPermanentDeleteSerializesContentReuse(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}

	for _, tc := range []struct {
		name             string
		blockedOperation string
		wantFollowerBind bool
		wantUploadStatus model.StorageUploadStatus
	}{
		{name: "delete wins", blockedOperation: "delete", wantUploadStatus: model.StorageUploadStatusSuperseded},
		{name: "reuse wins", blockedOperation: "reuse", wantFollowerBind: true, wantUploadStatus: model.StorageUploadStatusComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := permanentDeletePostgresDB(t, dsn)
			repos := repository.NewRepositories(db)
			ctx := context.Background()
			bucket := seedBucket(t, db, "postgres-permanent-delete-"+strings.ReplaceAll(tc.name, " ", "-"))
			source := newObjectVersion(bucket.ID, "source.txt", model.NewVersionID(), 10)
			if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source); err != nil {
				t.Fatalf("CreateVersionAndSetCurrent(source): %v", err)
			}
			uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, source, "bafk2bzacepostgresreuse")
			if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, source.VersionID, uploadID, model.ObjectStateCached, model.ObjectStateStored); err != nil {
				t.Fatalf("SetVersionStorageUploadAndTransition(source): %v", err)
			}
			followerUploadID := uploadID
			follower := newObjectVersion(bucket.ID, "follower.txt", model.NewVersionID(), source.Size)
			follower.Checksum = source.Checksum
			follower.StorageUploadID = &followerUploadID
			follower.State = model.ObjectStateStored

			barrier := &storageUploadLockBarrier{
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
			deleteCtx := context.WithValue(ctx, storageUploadLockContextKey{}, "delete")
			reuseCtx := context.WithValue(ctx, storageUploadLockContextKey{}, "reuse")
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
			waitPostgresDeleteSignal(t, barrier.locked, "first storage upload lock")
			if tc.blockedOperation == "delete" {
				startReuse()
			} else {
				startDelete()
			}
			waitPostgresDeleteSignal(t, barrier.attempted, "competing storage upload lock")
			close(barrier.release)
			if err := waitPostgresDeleteResult(t, deleteResult, "permanent delete"); err != nil {
				t.Fatalf("DeleteObjectVersionPermanently: %v", err)
			}
			if err := waitPostgresDeleteResult(t, reuseResult, "content reuse"); err != nil {
				t.Fatalf("CreateVersionAndSetCurrent(follower): %v", err)
			}

			gotFollower, err := repos.Objects.GetVersionByID(ctx, follower.VersionID)
			if err != nil || gotFollower == nil {
				t.Fatalf("GetVersionByID(follower): version=%#v err=%v", gotFollower, err)
			}
			if tc.wantFollowerBind {
				if gotFollower.StorageUploadID == nil || *gotFollower.StorageUploadID != uploadID || gotFollower.State != model.ObjectStateStored {
					t.Fatalf("reuse-winner follower = upload:%#v state:%s, want stored upload %d", gotFollower.StorageUploadID, gotFollower.State, uploadID)
				}
			} else if gotFollower.StorageUploadID != nil || gotFollower.State != model.ObjectStateCached {
				t.Fatalf("delete-winner follower = upload:%#v state:%s, want cached without stale upload", gotFollower.StorageUploadID, gotFollower.State)
			}
			upload, err := repos.Uploads.GetByID(ctx, uploadID)
			if err != nil || upload == nil || upload.Status != tc.wantUploadStatus {
				t.Fatalf("upload after concurrent delete/reuse = %#v err=%v, want %s", upload, err, tc.wantUploadStatus)
			}
		})
	}
}

func permanentDeletePostgresDB(t *testing.T, dsn string) *bun.DB {
	t.Helper()
	ctx := context.Background()
	adminDB, err := appdb.New(config.DatabaseConfig{Driver: "postgres", DSN: dsn, MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("opening postgres admin connection: %v", err)
	}
	schema := fmt.Sprintf("synaps3_permanent_delete_%d", time.Now().UnixNano())
	quotedSchema := quotePostgresIdentifier(schema)
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		_ = adminDB.Close()
		t.Fatalf("creating postgres test schema: %v", err)
	}
	testDSN, err := postgresDSNWithSearchPath(dsn, schema)
	if err != nil {
		_ = adminDB.Close()
		t.Fatalf("adding postgres test search path: %v", err)
	}
	db, err := appdb.New(config.DatabaseConfig{Driver: "postgres", DSN: testDSN, MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		_ = adminDB.Close()
		t.Fatalf("opening postgres test connections: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = adminDB.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+quotedSchema+" CASCADE")
		_ = adminDB.Close()
	})
	if err := appdb.RunMigrations(ctx, db); err != nil {
		t.Fatalf("running postgres test migrations: %v", err)
	}
	return db
}

func postgresDSNWithSearchPath(dsn, schema string) (string, error) {
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		return u.String(), nil
	}
	return strings.TrimSpace(dsn) + " search_path=" + schema, nil
}

func waitPostgresDeleteSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitPostgresDeleteResult(t *testing.T, result <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}
