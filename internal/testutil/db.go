// Package testutil provides shared test infrastructure for SynapS3 packages.
// It is only compiled and linked in test binaries.
package testutil

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	"modernc.org/sqlite"
)

var testDBCounter atomic.Uint64

// StorageChecksum turns a readable fixture identity into the canonical
// lowercase SHA-256 representation required by storage_contents.
func StorageChecksum(identity string) string {
	if len(identity) == sha256.Size*2 {
		if decoded, err := hex.DecodeString(identity); err == nil && hex.EncodeToString(decoded) == identity {
			return identity
		}
	}
	digest := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(digest[:])
}

// NewTestDB creates a fresh in-memory SQLite DB with all migrations applied.
// The database is closed automatically when the test completes.
func NewTestDB(t *testing.T) *bun.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:synaps3-test-%d?mode=memory&cache=shared&_pragma=foreign_keys(1)", testDBCounter.Add(1))
	return newTestSQLiteDB(t, dsn)
}

// NewTestFileDB creates a fresh file-backed SQLite DB with all migrations applied.
// Use it for tests that cancel database contexts while worker goroutines are active.
func NewTestFileDB(t *testing.T) *bun.DB {
	t.Helper()

	path := filepath.ToSlash(filepath.Join(t.TempDir(), "synaps3-test.db"))
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_txlock=immediate"
	return newTestSQLiteDB(t, dsn)
}

func newTestSQLiteDB(t *testing.T, dsn string) *bun.DB {
	t.Helper()

	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("opening test db: %v", err)
	}
	sqldb.SetMaxOpenConns(1)
	sqldb.SetMaxIdleConns(1)

	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	if err := restoreMigratedTemplate(context.Background(), sqldb); err != nil {
		t.Fatalf("copying migrated test schema: %v", err)
	}

	return db
}

// migratedTemplateURI names a shared in-memory database that holds every
// migration, so each test database is a copy instead of a fresh migration run.
const migratedTemplateURI = "file:synaps3-migrated-template?mode=memory&cache=shared"

var migratedTemplate struct {
	once sync.Once
	// holder keeps the shared in-memory template alive for the test process.
	holder *sql.Conn
	err    error
	mu     sync.Mutex
}

func buildMigratedTemplate() (*sql.Conn, error) {
	ctx := context.Background()
	sqldb, err := sql.Open("sqlite", migratedTemplateURI+"&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	db := bun.NewDB(sqldb, sqlitedialect.New())
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		return nil, fmt.Errorf("init migrator: %w", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("running migrations: %w", err)
	}
	return sqldb.Conn(ctx)
}

// restoreMigratedTemplate copies the migrated template into the database of
// sqldb's single connection with SQLite's backup API.
func restoreMigratedTemplate(ctx context.Context, sqldb *sql.DB) error {
	migratedTemplate.once.Do(func() {
		migratedTemplate.holder, migratedTemplate.err = buildMigratedTemplate()
	})
	if migratedTemplate.err != nil {
		return migratedTemplate.err
	}
	migratedTemplate.mu.Lock()
	defer migratedTemplate.mu.Unlock()

	conn, err := sqldb.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return conn.Raw(func(driverConn any) error {
		restorer, ok := driverConn.(interface {
			NewRestore(string) (*sqlite.Backup, error)
		})
		if !ok {
			return errors.New("sqlite driver connection cannot restore a backup")
		}
		backup, err := restorer.NewRestore(migratedTemplateURI)
		if err != nil {
			return err
		}
		for more := true; more; {
			if more, err = backup.Step(-1); err != nil {
				_ = backup.Finish()
				return err
			}
		}
		return backup.Finish()
	})
}

// NewTestRepos creates a Repositories instance backed by an in-memory SQLite DB.
func NewTestRepos(t *testing.T) *repository.Repositories {
	t.Helper()
	return repository.NewRepositories(NewTestDB(t))
}

// SeedBucket inserts an active bucket and returns it.
func SeedBucket(t *testing.T, db *bun.DB, name string) *model.Bucket {
	t.Helper()
	return SeedBucketWithCopies(t, db, name, model.BucketStatusActive, 1)
}

// SeedBucketWithCopies inserts a bucket whose durability policy is the given
// replica target, along with the replica slots that target opens.
func SeedBucketWithCopies(t *testing.T, db *bun.DB, name string, status model.BucketStatus, copies int) *model.Bucket {
	t.Helper()
	bucket := &model.Bucket{Name: name, Status: status, DefaultCopies: copies, MinimumDurableCopies: copies}
	if _, err := db.NewInsert().Model(bucket).Exec(context.Background()); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	OpenBucketReplicaSlots(t, db, bucket.ID, copies)
	return bucket
}

// SeedBucketWithStatus inserts a bucket with the specified status.
func SeedBucketWithStatus(t *testing.T, db *bun.DB, name string, status model.BucketStatus) *model.Bucket {
	t.Helper()
	return SeedBucketWithCopies(t, db, name, status, 1)
}

// OpenBucketReplicaSlots inserts the replica slots a bucket seeded outside
// repository.Buckets.Create would otherwise be missing. Data sets, copies,
// replacements, cleanups and observability rows all reference these, so a test
// that seeds a bucket directly must open its slots too.
func OpenBucketReplicaSlots(tb testing.TB, db bun.IDB, bucketID int64, copies int) {
	tb.Helper()
	slots := make([]model.BucketReplicaSlot, 0, copies)
	now := time.Now().UTC()
	for copyIndex := range copies {
		slots = append(slots, model.BucketReplicaSlot{
			BucketID: bucketID, CopyIndex: copyIndex,
			Status: model.BucketReplicaSlotStatusActive, CreatedAt: now, UpdatedAt: now,
		})
	}
	if _, err := db.NewInsert().Model(&slots).
		On("CONFLICT (bucket_id, copy_index) DO NOTHING").
		Exec(context.Background()); err != nil {
		tb.Fatalf("opening bucket replica slots: %v", err)
	}
}
