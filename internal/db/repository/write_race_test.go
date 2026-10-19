package repository_test

import (
	"context"
	"errors"
	"strconv"
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

// These races exist under READ COMMITTED, where concurrent writing transactions
// interleave.

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

func runPostgresRaceTx(ctx context.Context, db *bun.DB, backendPID chan<- int, fn func(*repository.Repositories) error) error {
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var pid int
		if err := tx.NewRaw("SELECT pg_backend_pid()").Scan(ctx, &pid); err != nil {
			return err
		}
		backendPID <- pid
		return fn(repository.NewRepositories(tx))
	})
}

// waitForPostgresLockWait can observe a specific competing connection,
// excluding unrelated transactions in other test schemas from the barrier.
func waitForPostgresLockWait(t *testing.T, db *bun.DB, table string, backendPID ...<-chan int) {
	t.Helper()
	var pid int
	if len(backendPID) > 0 {
		select {
		case pid = <-backendPID[0]:
		case <-time.After(5 * time.Second):
			t.Fatal("competing transaction did not report its backend PID")
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := db.NewRaw(`SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND (? = 0 OR pid = ?)
			  AND wait_event_type = 'Lock' AND query ILIKE ?`,
			pid, pid, "%"+table+"%").Scan(t.Context(), &waiting); err != nil {
			t.Fatalf("reading lock waits: %v", err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("transaction %d did not wait for a lock on %s", pid, table)
}

func TestPostgresPermanentDeleteDoesNotWaitForTaskSettlement(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "delete-task-settlement")
	contentID := seedContent(t, repos, bucket.ID, "delete-task-settlement", 10)
	version := newObjectVersion(bucket.ID, "file.txt", model.NewVersionID(), 10)
	version.ContentID = &contentID
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatal(err)
	}
	task, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{
		Type: model.TaskTypeUploadPlan, SubjectType: model.TaskSubjectStorageContent,
		SubjectKey: strconv.FormatInt(contentID, 10), InputVersion: 1, Input: []byte(`{}`),
	}))
	if err != nil {
		t.Fatal(err)
	}
	failLocalEnsure(t, db, task)

	hold := newPostgresRaceHold("delete", storageContentLockQuery)
	testutil.AddQueryHook(db, hold)
	defer hold.Release()
	deleted := make(chan error, 1)
	go func() {
		_, err := repos.Objects.DeleteObjectVersionPermanently(postgresRaceContext(ctx, "delete"), repository.DeleteObjectVersionInput{
			BucketID: bucket.ID, Key: version.Key, VersionID: version.VersionID,
		})
		deleted <- err
	}()
	waitPostgresSignal(t, hold.reached, "permanent delete content lock")
	settled := make(chan error, 1)
	backendPID := make(chan int, 1)
	go func() {
		settled <- runPostgresRaceTx(ctx, db, backendPID, func(tx *repository.Repositories) error {
			if _, err := tx.Tasks.GetForUpdate(ctx, task.ID); err != nil {
				return err
			}
			_, err := tx.Contents.LockForCopyCreation(ctx, contentID)
			return err
		})
	}()
	waitForPostgresLockWait(t, db, "storage_contents", backendPID)
	hold.Release()
	if err := waitPostgresResult(t, deleted, "permanent delete"); !errors.Is(err, repository.ErrPermanentDeleteStorageBusy) {
		t.Fatalf("DeleteObjectVersionPermanently = %v, want storage busy", err)
	}
	if err := waitPostgresResult(t, settled, "task settlement"); err != nil {
		t.Fatalf("task settlement = %v", err)
	}
	if result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: bucket.ID, Key: version.Key, VersionID: version.VersionID,
	}); err != nil || result.StorageCleanup == nil {
		t.Fatalf("delete after settlement = %#v, %v", result, err)
	}
}

func TestPostgresCopyCreationCannotCrossCleanupReservation(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "copy-cleanup-reservation")
	contentID := seedContent(t, repos, bucket.ID, "copy-cleanup-reservation", 10)
	version := newObjectVersion(bucket.ID, "file.txt", model.NewVersionID(), 10)
	version.ContentID = &contentID
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatal(err)
	}
	providerID := onChainID(t, "101")
	binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	hold := newPostgresRaceHold("delete", storageContentLockQuery)
	testutil.AddQueryHook(db, hold)
	defer hold.Release()
	deleted := make(chan error, 1)
	go func() {
		_, err := repos.Objects.DeleteObjectVersionPermanently(postgresRaceContext(ctx, "delete"), repository.DeleteObjectVersionInput{
			BucketID: bucket.ID, Key: version.Key, VersionID: version.VersionID,
		})
		deleted <- err
	}()
	waitPostgresSignal(t, hold.reached, "permanent delete content lock")
	created := make(chan error, 1)
	backendPID := make(chan int, 1)
	go func() {
		created <- runPostgresRaceTx(ctx, db, backendPID, func(tx *repository.Repositories) error {
			return tx.Contents.CreateUploadCopiesForBindings(ctx, contentID, []repository.UploadCopyBindingInput{{
				StorageDataSetID: binding.ID, CopyIndex: 0, ProviderID: providerID, TransferMethod: model.StorageCopyTransferMethodIngress,
			}})
		})
	}()
	waitForPostgresLockWait(t, db, "storage_contents", backendPID)
	hold.Release()
	if err := waitPostgresResult(t, deleted, "permanent delete"); err != nil {
		t.Fatal(err)
	}
	if err := waitPostgresResult(t, created, "copy creation"); !errors.Is(err, repository.ErrContentCleanupInProgress) {
		t.Fatalf("CreateUploadCopiesForBindings = %v, want cleanup in progress", err)
	}
	content, err := repos.Contents.GetByID(ctx, contentID)
	if err != nil || content == nil || content.CleanupGeneration == 0 || content.CleanupTaskID != nil {
		t.Fatalf("cleanup reservation = %#v, %v, want generation without a task", content, err)
	}
	if copies, err := repos.Contents.ListCopies(ctx, contentID); err != nil || len(copies) != 0 {
		t.Fatalf("copies after cleanup reservation = %#v, %v", copies, err)
	}
}

func TestPostgresReplacementSeedingDoesNotDeadlockCommit(t *testing.T) {
	db := testDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	copyRow := f.transferredCopy(t, "seed-commit")
	taskID := f.collecting(t, "seed-commit", copyRow)
	row, _, err := f.repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: f.bucket.ID, SourceDataSetID: f.dataSetID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "702"), ClientRequestID: "seed-commit",
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := localCoordinatorTask(t, f.repos, row)
	markSourceReady(t, f.repos, row.TargetDataSetID)
	if err := f.repos.Replacements.Activate(ctx, row.ID, row.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	hold := newPostgresRaceHold("seed", func(query string) bool {
		return strings.Contains(query, `from "storage_data_sets"`) && strings.Contains(query, "for no key update")
	})
	testutil.AddQueryHook(db, hold)
	defer hold.Release()
	seeded := make(chan error, 1)
	go func() {
		_, _, err := f.repos.Replacements.SeedMigrationBatch(postgresRaceContext(ctx, "seed"), row.ID, 128)
		seeded <- err
	}()
	waitPostgresSignal(t, hold.reached, "replacement data-set locks")
	sealed := make(chan error, 1)
	backendPID := make(chan int, 1)
	go func() {
		sealed <- runPostgresRaceTx(ctx, db, backendPID, func(tx *repository.Repositories) error {
			_, err := tx.Contents.SealCommitRequest(ctx, repository.SealCommitRequestInput{
				RequestID: "seed-commit", TaskID: taskID, ExtraDataHex: "abcd",
				Members: []repository.SealMember{{CopyID: copyRow.ID, ContentID: copyRow.ContentID, PieceCID: "piece-seed-commit"}},
			})
			return err
		})
	}()
	waitForPostgresLockWait(t, db, "storage_data_sets", backendPID)
	hold.Release()
	if err := waitPostgresResult(t, seeded, "replacement seeding"); !errors.Is(err, repository.ErrRepositoryContended) {
		t.Fatalf("SeedMigrationBatch = %v, want transaction contention", err)
	}
	if err := waitPostgresResult(t, sealed, "commit seal"); err != nil {
		t.Fatalf("SealCommitRequest = %v", err)
	}
	stored, err := f.repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || stored.SeedCursorContentID != 0 || stored.SeedingComplete {
		t.Fatalf("replacement after rolled-back batch = %#v, %v", stored, err)
	}
	if count, done, err := f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 128); err != nil || !done || count != 1 {
		t.Fatalf("seed after contention = %d, %t, %v", count, done, err)
	}
}

func TestPostgresReplacementSeedingRechecksContentAfterScan(t *testing.T) {
	db := testDB(t)
	repos, source, _ := localReplacementSource(t, db, "seed-cleaned-content")
	ctx := t.Context()
	copyRow := localWaitingCopy(t, repos, source, "seed-cleaned-content")
	row, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: source.BucketID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "seed-cleaned-content",
	})
	if err != nil {
		t.Fatal(err)
	}
	localCoordinatorTask(t, repos, row)
	hold := newPostgresRaceHold("seed", func(query string) bool {
		return strings.Contains(query, "select id from storage_contents where bucket_id")
	})
	testutil.AddQueryHook(db, hold)
	defer hold.Release()
	seeded := make(chan error, 1)
	go func() {
		_, _, err := repos.Replacements.SeedMigrationBatch(postgresRaceContext(ctx, "seed"), row.ID, 128)
		seeded <- err
	}()
	waitPostgresSignal(t, hold.reached, "replacement candidate scan")
	version, err := repos.Contents.GetLiveVersionForUpload(ctx, copyRow.ContentID)
	if err != nil || version == nil {
		t.Fatalf("GetLiveVersionForUpload = %#v, %v", version, err)
	}
	if _, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
		BucketID: source.BucketID, Key: version.Key, VersionID: version.VersionID,
	}); err != nil {
		t.Fatalf("delete after scan = %v", err)
	}
	hold.Release()
	if err := waitPostgresResult(t, seeded, "replacement seeding"); err != nil {
		t.Fatalf("SeedMigrationBatch = %v", err)
	}
	stored, err := repos.Replacements.GetByID(ctx, row.ID)
	if err != nil || stored.SeedCursorContentID != copyRow.ContentID || !stored.SeedingComplete {
		t.Fatalf("replacement after skipped content = %#v, %v", stored, err)
	}
	items, err := repos.Replacements.ListPendingReplacementItemsAfter(ctx, row.ID, 0, 128)
	if err != nil || len(items) != 0 {
		t.Fatalf("replacement items = %#v, %v", items, err)
	}
	if copy, err := repos.Contents.GetUploadCopyForDataSet(ctx, copyRow.ContentID, row.TargetDataSetID); err != nil || copy != nil {
		t.Fatalf("target copy = %#v, %v", copy, err)
	}
}

// Replacement drains the source while an upload plan settles against it. The
// settlement must not bind the drained source: a copy placed there after the
// switch would never be migrated to the target.
func TestPostgresUploadBindingDoesNotLandOnADrainedSource(t *testing.T) {
	db := testDB(t)
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
	testutil.AddQueryHook(db, hold)
	defer hold.Release()

	coordinator := localCoordinatorTask(t, repos, replacement)
	activated := make(chan error, 1)
	go func() {
		activated <- repos.Replacements.Activate(postgresRaceContext(ctx, "activate"), replacement.ID, replacement.TaskGeneration, coordinator.ID)
	}()
	waitPostgresSignal(t, hold.reached, "source drained inside activation")

	settled := make(chan error, 1)
	backendPID := make(chan int, 1)
	go func() {
		settled <- runPostgresRaceTx(ctx, db, backendPID, func(tx *repository.Repositories) error {
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
	waitForPostgresLockWait(t, db, "storage_data_sets", backendPID)
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
	db := testDB(t)
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
	testutil.AddQueryHook(db, hold)
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
	backendPID := make(chan int, 1)
	go func() {
		raised <- runPostgresRaceTx(ctx, db, backendPID, func(tx *repository.Repositories) error {
			copies := 3
			_, err := tx.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
				Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: &copies,
			})
			return err
		})
	}()
	waitForPostgresLockWait(t, db, "buckets", backendPID)
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
	db := testDB(t)
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
	testutil.AddQueryHook(db, race)

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
