package repository_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/strahe/synaps3/internal/config"
	appdb "github.com/strahe/synaps3/internal/db"
	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

type replacementLockContextKey struct{}

type replacementBucketLockBarrier struct {
	winner      string
	locked      chan struct{}
	release     chan struct{}
	competitor  chan struct{}
	lockedOnce  sync.Once
	attemptOnce sync.Once
}

func (h *replacementBucketLockBarrier) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if replacementBucketLockQuery(event.Query) && ctx.Value(replacementLockContextKey{}) != h.winner {
		h.attemptOnce.Do(func() { close(h.competitor) })
	}
	return ctx
}

func (h *replacementBucketLockBarrier) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if !replacementBucketLockQuery(event.Query) || ctx.Value(replacementLockContextKey{}) != h.winner {
		return
	}
	h.lockedOnce.Do(func() {
		close(h.locked)
		<-h.release
	})
}

func replacementBucketLockQuery(query string) bool {
	query = strings.ToLower(query)
	return strings.Contains(query, "update") && strings.Contains(query, "buckets") &&
		strings.Contains(query, "updated_at = updated_at")
}

// The generation indexes rely on partial-index semantics and on ON CONFLICT
// inferring a partial index. Those differ enough between SQLite and PostgreSQL
// that the guarantees have to be checked on both.
func TestPostgresStorageReplacementSchemaParity(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	db := newPostgresReplacementDB(t, ctx, dsn)
	repos := repository.NewRepositories(db)

	bucket := &model.Bucket{Name: "pg-replacement", Status: model.BucketStatusActive}
	if _, err := db.NewInsert().Model(bucket).Exec(ctx); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	source, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}

	row, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID:         bucket.ID,
		SourceDataSetID:  source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"),
		ClientRequestID:  "postgres-replacement",
		MaxRetries:       5,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	// A second live replacement for one source must be impossible.
	if _, err := db.NewInsert().Model(&storagereplacement.Replacement{
		BucketID: bucket.ID, CopyIndex: 0,
		SourceDataSetID: source.ID, TargetDataSetID: row.TargetDataSetID,
		SelectionMode: storagereplacement.SelectionModeManual,
		Status:        storagereplacement.StatusPreparingTarget,
		ConfirmedAt:   time.Now(), CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}).Exec(ctx); err == nil {
		t.Fatal("PostgreSQL accepted a second active replacement for one source")
	}

	// The slot may hold several generations, but only one current one.
	if _, err := db.NewInsert().Model(&model.StorageDataSet{
		BucketID: bucket.ID, ProviderID: onChainID(t, "303"), CopyIndex: 0,
		Generation: 5, IsCurrent: true, Status: model.StorageDataSetStatusPending,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}).Exec(ctx); err == nil {
		t.Fatal("PostgreSQL accepted a second current generation for one slot")
	}
	if _, err := db.NewInsert().Model(&model.StorageDataSet{
		BucketID: bucket.ID, ProviderID: onChainID(t, "303"), CopyIndex: 0,
		Generation: 5, IsCurrent: false, Status: model.StorageDataSetStatusPending,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}).Exec(ctx); err != nil {
		t.Fatalf("PostgreSQL rejected a historical generation: %v", err)
	}

	upload := &model.StorageUpload{
		BucketID: bucket.ID, SourceVersionID: "01J00000000000000000000PG1",
		ContentSize: 10, Checksum: "pg-sum", Status: model.StorageUploadStatusRunning, RequestedCopies: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if _, err := db.NewInsert().Model(upload).Exec(ctx); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	// ON CONFLICT must infer the partial index, so a repeated bind is a no-op
	// rather than an error or a duplicate.
	for range 2 {
		if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
			StorageDataSetID: source.ID, CopyIndex: 0,
			TransferMethod: model.StorageCopyTransferMethodIngress,
			ProviderID:     onChainID(t, "101"),
		}}); err != nil {
			t.Fatalf("CreateUploadCopiesForBindings: %v", err)
		}
	}
	count, err := db.NewSelect().Model((*model.StorageUploadCopy)(nil)).
		Where("upload_id = ?", upload.ID).Count(ctx)
	if err != nil {
		t.Fatalf("count copies: %v", err)
	}
	if count != 1 {
		t.Fatalf("copies = %d, want the repeated bind to be a no-op", count)
	}

	// Unbound copies are distinct under NULL, so they need their own guard.
	if _, err := db.NewInsert().Model(&model.StorageUploadCopy{
		UploadID: upload.ID, CopyIndex: 3,
		TransferMethod: model.StorageCopyTransferMethodPeerPull,
		CreatedAt:      time.Now(), UpdatedAt: time.Now(),
	}).Exec(ctx); err != nil {
		t.Fatalf("seed unbound copy: %v", err)
	}
	if _, err := db.NewInsert().Model(&model.StorageUploadCopy{
		UploadID: upload.ID, CopyIndex: 3,
		TransferMethod: model.StorageCopyTransferMethodPeerPull,
		CreatedAt:      time.Now(), UpdatedAt: time.Now(),
	}).Exec(ctx); err == nil {
		t.Fatal("PostgreSQL accepted a duplicate unbound copy for one slot")
	}
}

func TestPostgresAuthorizeAndActivateSerializeOnBucket(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}

	for _, winner := range []string{"activate", "authorize"} {
		t.Run(winner+" wins", func(t *testing.T) {
			ctx := context.Background()
			db := newPostgresReplacementDB(t, ctx, dsn)
			fixture := seedPostgresReplacement(t, db, "pg-replacement-lock-"+winner)
			barrier := &replacementBucketLockBarrier{
				winner: winner, locked: make(chan struct{}), release: make(chan struct{}), competitor: make(chan struct{}),
			}
			defer func() {
				select {
				case <-barrier.release:
				default:
					close(barrier.release)
				}
			}()
			db.AddQueryHook(barrier)

			activateResult := make(chan error, 1)
			authorizeResult := make(chan error, 1)
			var successor *storagereplacement.Replacement
			startActivate := func() {
				go func() {
					activateCtx := context.WithValue(ctx, replacementLockContextKey{}, "activate")
					activateResult <- fixture.repos.Replacements.Activate(activateCtx, fixture.row.ID)
				}()
			}
			startAuthorize := func() {
				go func() {
					authorizeCtx := context.WithValue(ctx, replacementLockContextKey{}, "authorize")
					row, _, err := fixture.repos.Replacements.Authorize(authorizeCtx, repository.AuthorizeReplacementInput{
						BucketID: fixture.bucket.ID, SourceDataSetID: fixture.source.ID,
						SelectionMode:    storagereplacement.SelectionModeManual,
						TargetProviderID: onChainID(t, "303"), ClientRequestID: "concurrent-successor", MaxRetries: 5,
					})
					successor = row
					authorizeResult <- err
				}()
			}
			if winner == "activate" {
				startActivate()
			} else {
				startAuthorize()
			}
			waitReplacementSignal(t, barrier.locked, "winning bucket lock")
			if winner == "activate" {
				startAuthorize()
			} else {
				startActivate()
			}
			waitReplacementSignal(t, barrier.competitor, "competing bucket lock")
			close(barrier.release)

			activateErr := waitReplacementResult(t, activateResult, "Activate")
			authorizeErr := waitReplacementResult(t, authorizeResult, "Authorize")
			current, err := fixture.repos.Uploads.GetDataSetBindingByCopyIndex(ctx, fixture.bucket.ID, fixture.source.CopyIndex)
			if err != nil || current == nil {
				t.Fatalf("current generation = %#v err=%v", current, err)
			}
			original, err := fixture.repos.Replacements.GetByID(ctx, fixture.row.ID)
			if err != nil || original == nil {
				t.Fatalf("original replacement = %#v err=%v", original, err)
			}

			if winner == "activate" {
				if activateErr != nil {
					t.Fatalf("Activate winner: %v", activateErr)
				}
				if !errors.Is(authorizeErr, storagereplacement.ErrSourceNotCurrent) {
					t.Fatalf("Authorize after activation = %v, want ErrSourceNotCurrent", authorizeErr)
				}
				if current.ID != fixture.target.ID || original.Status != storagereplacement.StatusMigrating {
					t.Fatalf("winner state = current:%d replacement:%s, want target %d migrating", current.ID, original.Status, fixture.target.ID)
				}
				return
			}
			if authorizeErr != nil || successor == nil {
				t.Fatalf("Authorize winner = %#v err=%v", successor, authorizeErr)
			}
			if !errors.Is(activateErr, repository.ErrConflict) {
				t.Fatalf("Activate superseded replacement = %v, want ErrConflict", activateErr)
			}
			if current.ID != fixture.source.ID || original.Status != storagereplacement.StatusSuperseded {
				t.Fatalf("winner state = current:%d replacement:%s, want source %d and superseded", current.ID, original.Status, fixture.source.ID)
			}
			if successor.TargetDataSetID == current.ID {
				t.Fatalf("superseding target %d became current before activation", successor.TargetDataSetID)
			}
		})
	}
}

func TestPostgresAuthorizeConcurrentIdempotentReplay(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	db := newPostgresReplacementDB(t, ctx, dsn)
	repos := repository.NewRepositories(db)
	bucket := &model.Bucket{Name: "pg-concurrent-idempotency", Status: model.BucketStatusActive}
	if _, err := db.NewInsert().Model(bucket).Exec(ctx); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	source := &model.StorageDataSet{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
		Generation: 1, IsCurrent: true, Status: model.StorageDataSetStatusReady,
		DataSetID: onChainIDPtr(t, "1001"), CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if _, err := db.NewInsert().Model(source).Exec(ctx); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	input := repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "concurrent-confirmation", MaxRetries: 5,
	}
	type result struct {
		row     *storagereplacement.Replacement
		created bool
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			row, created, err := repos.Replacements.Authorize(ctx, input)
			results <- result{row: row, created: created, err: err}
		}()
	}
	close(start)
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil || first.row == nil || second.row == nil {
		t.Fatalf("concurrent results = %#v and %#v", first, second)
	}
	if first.row.ID != second.row.ID || first.created == second.created {
		t.Fatalf("concurrent results = ids %d/%d created %v/%v, want one shared row and one creator",
			first.row.ID, second.row.ID, first.created, second.created)
	}
	count, err := db.NewSelect().Model((*storagereplacement.Replacement)(nil)).
		Where("bucket_id = ? AND client_request_id = ?", bucket.ID, input.ClientRequestID).Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("persisted replacements = %d err=%v, want one", count, err)
	}
}

func waitReplacementSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitReplacementResult(t *testing.T, result <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

type postgresReplacementFixture struct {
	repos  *repository.Repositories
	bucket *model.Bucket
	source *model.StorageDataSet
	target *model.StorageDataSet
	row    *storagereplacement.Replacement
}

func seedPostgresReplacement(t *testing.T, db *bun.DB, name string) *postgresReplacementFixture {
	t.Helper()
	ctx := context.Background()
	repos := repository.NewRepositories(db)
	bucket := &model.Bucket{Name: name, Status: model.BucketStatusActive}
	if _, err := db.NewInsert().Model(bucket).Exec(ctx); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	source, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: source.ID, DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("ready source: %v", err)
	}
	row, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "concurrent-original", MaxRetries: 5,
	})
	if err != nil {
		t.Fatalf("authorize replacement: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: row.TargetDataSetID, DataSetID: onChainID(t, "2002"),
	}); err != nil {
		t.Fatalf("ready target: %v", err)
	}
	target, err := repos.Uploads.GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil || target == nil {
		t.Fatalf("target = %#v err=%v", target, err)
	}
	return &postgresReplacementFixture{repos: repos, bucket: bucket, source: source, target: target, row: row}
}

func newPostgresReplacementDB(t *testing.T, ctx context.Context, dsn string) *bun.DB {
	t.Helper()
	adminDB, err := appdb.New(config.DatabaseConfig{
		Driver: "postgres", DSN: dsn, MaxOpenConns: 1, MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatalf("opening postgres test db: %v", err)
	}

	schema := fmt.Sprintf("synaps3_replacement_%d", time.Now().UnixNano())
	quoted := `"` + schema + `"`
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = adminDB.Close()
		t.Fatalf("creating schema: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := adminDB.ExecContext(dropCtx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Logf("dropping schema %s: %v", schema, err)
		}
		_ = adminDB.Close()
	})

	pgConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing postgres test DSN: %v", err)
	}
	pgConfig.RuntimeParams["search_path"] = schema
	registeredDSN := stdlib.RegisterConnConfig(pgConfig)
	t.Cleanup(func() { stdlib.UnregisterConnConfig(registeredDSN) })
	db, err := appdb.New(config.DatabaseConfig{
		Driver: "postgres", DSN: registeredDSN, MaxOpenConns: 4, MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatalf("opening schema-scoped postgres test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("migrator init: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running postgres migrations: %v", err)
	}
	return db
}
