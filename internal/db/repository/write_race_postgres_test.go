//go:build postgres

package repository_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

// These races only exist under PostgreSQL's READ COMMITTED: SQLite runs one
// writing transaction at a time, so none of the interleavings below can occur.

type postgresRaceOperation struct{}

// postgresRaceHold keeps the operation named held inside its transaction right
// after its first query that hold matches, until the test releases it.
type postgresRaceHold struct {
	held    string
	hold    func(query string) bool
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func newPostgresRaceHold(held string, hold func(query string) bool) *postgresRaceHold {
	return &postgresRaceHold{held: held, hold: hold, reached: make(chan struct{}), release: make(chan struct{})}
}

func (h *postgresRaceHold) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *postgresRaceHold) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if ctx.Value(postgresRaceOperation{}) != h.held || event.Err != nil || !h.hold(strings.ToLower(event.Query)) {
		return
	}
	h.once.Do(func() {
		close(h.reached)
		<-h.release
	})
}

func (h *postgresRaceHold) Release() {
	select {
	case <-h.release:
	default:
		close(h.release)
	}
}

func postgresRaceContext(ctx context.Context, operation string) context.Context {
	return context.WithValue(ctx, postgresRaceOperation{}, operation)
}

// waitForPostgresLockWait returns once some transaction is waiting for a lock
// held by a query on table, which proves the competing write is blocked
// rather than merely late.
func waitForPostgresLockWait(t *testing.T, db *bun.DB, table string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := db.NewRaw(`SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query ILIKE ?`,
			"%"+table+"%").Scan(context.Background(), &waiting); err != nil {
			t.Fatalf("reading lock waits: %v", err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no transaction waited for a lock on %s", table)
}

func postgresRaceDB(t *testing.T) *bun.DB {
	t.Helper()
	return migratedPostgresDB(t)
}

// Replacement drains the source while an upload plan settles against it. The
// settlement must not bind the drained source: a copy placed there after the
// switch would never be migrated to the target.
func TestPostgresUploadBindingDoesNotLandOnADrainedSource(t *testing.T) {
	db := postgresRaceDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "postgres-drained-source")
	providerID := onChainID(t, "101")
	source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding(source): %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: source.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("MarkDataSetReady(source): %v", err)
	}
	replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID:         bucket.ID,
		SourceDataSetID:  source.ID,
		SelectionMode:    storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"),
		ClientRequestID:  "postgres-drained-source",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: replacement.TargetDataSetID, DataSetID: onChainID(t, "2002")}); err != nil {
		t.Fatalf("MarkDataSetReady(target): %v", err)
	}
	contentID := seedContent(t, repos, bucket.ID, "postgres-drained-source", 10)

	hold := newPostgresRaceHold("activate", func(query string) bool {
		return strings.Contains(query, "storage_data_sets") && strings.Contains(query, "'draining'")
	})
	db.AddQueryHook(hold)
	defer hold.Release()

	activated := make(chan error, 1)
	go func() {
		activated <- repos.Replacements.Activate(postgresRaceContext(ctx, "activate"), replacement.ID)
	}()
	waitPostgresSignal(t, hold.reached, "source drained inside activation")

	settled := make(chan error, 1)
	go func() {
		settled <- repos.WithTx(ctx, func(tx *repository.Repositories) error {
			binding, err := tx.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
				BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
			})
			if err != nil {
				return err
			}
			if err := tx.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, DataSetID: onChainID(t, "1001")}); err != nil {
				return err
			}
			return tx.Contents.CreateUploadCopiesForBindings(ctx, contentID, []repository.UploadCopyBindingInput{{
				StorageDataSetID: binding.ID, CopyIndex: 0,
				TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: providerID,
			}})
		})
	}()
	waitForPostgresLockWait(t, db, "storage_data_sets")
	hold.Release()

	if err := waitPostgresResult(t, activated, "activation"); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := waitPostgresResult(t, settled, "upload settlement"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("upload settlement error = %v, want ErrConflict", err)
	}
	drained, err := repos.Contents.GetDataSetBindingByID(ctx, source.ID)
	if err != nil || drained == nil {
		t.Fatalf("GetDataSetBindingByID(source) = %#v, %v", drained, err)
	}
	if drained.Status != model.StorageDataSetStatusDraining || drained.IsCurrent {
		t.Fatalf("source after the race = %s current=%t, want draining and not current", drained.Status, drained.IsCurrent)
	}
	copies, err := repos.Contents.ListCopies(ctx, contentID)
	if err != nil {
		t.Fatalf("ListCopies: %v", err)
	}
	if len(copies) != 0 {
		t.Fatalf("copies after the race = %#v, want none on the drained source", copies)
	}
}

// Content named by a version keeps the policy it was named under. A policy
// change that commits while the first reference is still open has to wait for
// it, or the content would carry the old target although it was named after
// the change.
func TestPostgresCopyPolicyChangeWaitsForAFirstReference(t *testing.T) {
	db := postgresRaceDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := &model.Bucket{Name: "postgres-first-reference", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}
	content, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 10, Checksum: testutil.StorageChecksum("postgres-first-reference"), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent: %v", err)
	}

	hold := newPostgresRaceHold("reference", func(query string) bool {
		return strings.Contains(query, `from "buckets"`) && strings.Contains(query, "default_copies")
	})
	db.AddQueryHook(hold)
	defer hold.Release()

	version := newObjectVersion(bucket.ID, "first.txt", model.NewVersionID(), 10)
	version.ContentID = &content.ID
	referenced := make(chan error, 1)
	go func() {
		_, err := repos.Objects.CreateVersionAndSetCurrent(postgresRaceContext(ctx, "reference"), version)
		referenced <- err
	}()
	waitPostgresSignal(t, hold.reached, "policy read inside the first reference")

	raised := make(chan error, 1)
	go func() {
		copies := 3
		_, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
			Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: &copies,
		})
		raised <- err
	}()
	waitForPostgresLockWait(t, db, "buckets")
	hold.Release()

	if err := waitPostgresResult(t, referenced, "first reference"); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	if err := waitPostgresResult(t, raised, "policy change"); err != nil {
		t.Fatalf("UpdateCopyPolicy: %v", err)
	}
	stored, err := repos.Contents.GetByID(ctx, content.ID)
	if err != nil || stored == nil {
		t.Fatalf("GetByID = %#v, %v", stored, err)
	}
	if stored.RequestedCopies != 1 {
		t.Fatalf("requested copies = %d, want 1 from the policy the content was named under", stored.RequestedCopies)
	}
}

// A client retrying a wallet request while the first attempt is still running
// sends the same request ID twice at once. The loser must get the winner's
// operation inside its own transaction.
func TestPostgresWalletRequestRaceReturnsTheWinner(t *testing.T) {
	db := postgresRaceDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	input := repository.CreateWalletOperationInput{
		Type: model.WalletOperationTypeFund, ClientRequestID: "postgres-wallet-race", Amount: "100",
	}
	winner := &model.WalletOperation{
		Type: input.Type, ClientRequestID: input.ClientRequestID, Amount: input.Amount,
		Status: model.WalletOperationStatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	race := &walletInsertRace{db: db, winner: winner}
	db.AddQueryHook(race)

	loserCtx := postgresRaceContext(ctx, "loser")
	err := repos.WithTx(loserCtx, func(tx *repository.Repositories) error {
		op, created, err := tx.WalletOperations.CreateOrGet(loserCtx, input)
		if err != nil {
			return err
		}
		if created || op.ID != winner.ID {
			t.Errorf("CreateOrGet = operation %d created=%t, want the winner %d", op.ID, created, winner.ID)
		}
		// The transaction is still usable after losing the race.
		_, err = tx.WalletOperations.GetByID(loserCtx, op.ID)
		return err
	})
	if race.err != nil {
		t.Fatalf("insert the winning request: %v", race.err)
	}
	if winner.ID == 0 {
		t.Fatal("the winning request was never inserted")
	}
	if err != nil {
		t.Fatalf("losing request: %v", err)
	}
}

// walletInsertRace commits the winning request just before the loser's insert
// reaches the database.
type walletInsertRace struct {
	db     *bun.DB
	winner *model.WalletOperation
	once   sync.Once
	err    error
}

func (r *walletInsertRace) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if ctx.Value(postgresRaceOperation{}) == "loser" && strings.Contains(strings.ToLower(event.Query), `insert into "wallet_operations"`) {
		r.once.Do(func() {
			_, r.err = r.db.NewInsert().Model(r.winner).Exec(context.Background())
		})
	}
	return ctx
}

func (*walletInsertRace) AfterQuery(context.Context, *bun.QueryEvent) {}
