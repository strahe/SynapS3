// Package testutil provides shared test infrastructure for SynapS3 packages.
// It is only compiled and linked in test binaries.
package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testpg"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// testDBMaxOpenConns bounds each test database so packages running in
// parallel stay well below the server's connection limit.
const testDBMaxOpenConns = 8

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

// NewTestDB creates a private PostgreSQL schema with all migrations applied.
// The connections close and the schema is dropped when the test completes.
func NewTestDB(t *testing.T) *bun.DB {
	t.Helper()
	config, err := pgx.ParseConfig(testpg.SchemaDSN(t))
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	sqldb := stdlib.OpenDB(*config)
	sqldb.SetMaxOpenConns(testDBMaxOpenConns)
	db := bun.NewDB(sqldb, pgdialect.New())
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL test database: %v", err)
		}
	})
	ctx := context.Background()
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}
	return db
}

// NewTestRepos creates a Repositories instance backed by NewTestDB.
func NewTestRepos(t *testing.T) *repository.Repositories {
	t.Helper()
	return repository.NewRepositories(NewTestDB(t))
}

// AddQueryHook attaches hook to db in place. Tests use it to observe or pause
// the queries of components that already hold db; bun's WithQueryHook returns
// a copy those components never see.
func AddQueryHook(db *bun.DB, hook bun.QueryHook) {
	db.AddQueryHook(hook) //nolint:staticcheck // WithQueryHook cannot reach existing holders of db.
}

// JSONEqual reports whether a and b hold the same JSON value; two absent values
// are equal. PostgreSQL stores jsonb normalized, so a value read back need not
// repeat the bytes written.
func JSONEqual(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	var left, right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

var failWritesCounter atomic.Uint64

// FailWrites makes matching row writes to table fail with message, to inject a
// database failure at one statement. event is a trigger event such as "INSERT"
// or "UPDATE OF status"; when, if not empty, is a condition over NEW and OLD.
// The returned function removes the fault.
func FailWrites(tb testing.TB, db bun.IDB, table, event, when, message string) func() {
	tb.Helper()
	name := fmt.Sprintf("test_fail_writes_%d", failWritesCounter.Add(1))
	condition := ""
	if when != "" {
		condition = "WHEN (" + when + ") "
	}
	statements := []string{
		fmt.Sprintf("CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION USING MESSAGE = '%s'; END$$",
			name, strings.ReplaceAll(message, "'", "''")),
		fmt.Sprintf("CREATE TRIGGER %s BEFORE %s ON %s FOR EACH ROW %sEXECUTE FUNCTION %s()", name, event, table, condition, name),
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			tb.Fatalf("install write fault on %s: %v", table, err)
		}
	}
	return func() {
		tb.Helper()
		for _, statement := range []string{
			fmt.Sprintf("DROP TRIGGER %s ON %s", name, table),
			fmt.Sprintf("DROP FUNCTION %s()", name),
		} {
			if _, err := db.ExecContext(context.Background(), statement); err != nil {
				tb.Fatalf("remove write fault on %s: %v", table, err)
			}
		}
	}
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
