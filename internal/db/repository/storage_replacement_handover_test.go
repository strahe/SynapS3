package repository_test

import (
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/testutil"
)

// sourceWrite creates a pending copy on the fixture's data set; live gives its
// content a live version.
func (f commitFixture) sourceWrite(t *testing.T, name string, live bool, method model.StorageCopyTransferMethod) *model.StorageCopy {
	t.Helper()
	contentID := seedContent(t, f.repos, f.bucket.ID, name, 10)
	if live {
		version := newObjectVersion(f.bucket.ID, name, model.NewVersionID(), 10)
		version.ContentID = &contentID
		if _, err := createVersion(t, f.repos, version); err != nil {
			t.Fatalf("create version %s: %v", name, err)
		}
	}
	if err := f.repos.Contents.CreateUploadCopiesForBindings(t.Context(), contentID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: f.dataSetID, CopyIndex: 0, TransferMethod: method, ProviderID: onChainID(t, "701"),
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings(%s): %v", name, err)
	}
	copyRow, err := f.repos.Contents.GetUploadCopy(t.Context(), contentID, 0)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopy(%s) = %#v, %v", name, copyRow, err)
	}
	return copyRow
}

func (f commitFixture) bindWrite(t *testing.T, copyRow *model.StorageCopy, taskType model.TaskType) *model.Task {
	t.Helper()
	task, _, err := f.repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
		Type: taskType, SubjectType: model.TaskSubjectStorageCopy, SubjectKey: strconv.FormatInt(copyRow.ID, 10),
		Generation: new(int64(1)), InputVersion: 1, Input: fmt.Appendf(nil, `{"copy_id":%d,"generation":1}`, copyRow.ID),
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repos.Contents.BindCopyTask(t.Context(), copyRow.ID, 1, task.ID); err != nil {
		t.Fatal(err)
	}
	return task
}

func (f commitFixture) pullReservation(t *testing.T, copyRow *model.StorageCopy, taskID int64) repository.ReservePullRequestInput {
	t.Helper()
	provider, dataSet, piece := onChainID(t, "703"), onChainID(t, "803"), onChainID(t, "0")
	return repository.ReservePullRequestInput{
		CopyID: copyRow.ID, Generation: 1, TaskID: taskID, AttemptID: fmt.Sprintf("pull-%d", copyRow.ID),
		SourceProviderID: &provider, SourceDataSetID: &dataSet, SourcePieceID: &piece,
		SourcePieceCID: "piece", SourceRetrievalURL: "https://source.example", ExtraDataHex: "abcd",
	}
}

func (f commitFixture) authorizeReplacement(t *testing.T) (*storagereplacement.Replacement, *model.Task) {
	t.Helper()
	row, _, err := f.repos.Replacements.Authorize(t.Context(), repository.AuthorizeReplacementInput{
		BucketID: f.bucket.ID, SourceDataSetID: f.dataSetID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "702"), ClientRequestID: "handover",
	})
	if err != nil {
		t.Fatal(err)
	}
	return row, localCoordinatorTask(t, f.repos, row)
}

func (f commitFixture) activateReplacement(t *testing.T) (*storagereplacement.Replacement, *model.Task) {
	t.Helper()
	row, coordinator := f.authorizeReplacement(t)
	markSourceReady(t, f.repos, row.TargetDataSetID)
	if err := f.repos.Replacements.Activate(t.Context(), row.ID, row.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	return row, coordinator
}

func TestReplacementHandsOverUnsignedSourceWrites(t *testing.T) {
	f := newCommitFixture(t, testDB(t))
	ctx := t.Context()
	_, sealed := f.sealedRequest(t, "sealed")
	collected := f.transferredCopy(t, "collected")
	f.collecting(t, "collecting", collected)
	uploading := f.sourceWrite(t, "uploading", true, model.StorageCopyTransferMethodIngress)
	f.bindWrite(t, uploading, model.TaskTypeStorageStore)
	deleted := f.sourceWrite(t, "deleted", false, model.StorageCopyTransferMethodIngress)
	pulling := f.sourceWrite(t, "pulling", true, model.StorageCopyTransferMethodPeerPull)
	if err := f.repos.Contents.ReservePullRequest(ctx, f.pullReservation(t, pulling, f.bindWrite(t, pulling, model.TaskTypeStoragePull).ID)); err != nil {
		t.Fatal(err)
	}

	generations := map[int64]int64{}
	for _, write := range []*model.StorageCopy{collected, uploading, deleted} {
		generations[write.ID] = f.copy(t, write.ID).WorkGeneration
	}

	row, coordinator := f.authorizeReplacement(t)
	if _, _, err := f.repos.Replacements.HandOverSourceWritesBatch(ctx, row.ID, row.TaskGeneration, coordinator.ID, 128); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("hand-over before activation = %v, want conflict", err)
	}
	markSourceReady(t, f.repos, row.TargetDataSetID)
	if err := f.repos.Replacements.Activate(ctx, row.ID, row.TaskGeneration, coordinator.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.repos.Replacements.HandOverSourceWritesBatch(ctx, row.ID, row.TaskGeneration, coordinator.ID+1, 128); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("hand-over by a stale coordinator = %v, want conflict", err)
	}
	if unsigned, err := f.repos.Replacements.HasUnsignedSourceWrites(ctx, f.dataSetID); err != nil || !unsigned {
		t.Fatalf("unsigned source writes = %v, err=%v, want work to hand over", unsigned, err)
	}
	targets, done, err := f.repos.Replacements.HandOverSourceWritesBatch(ctx, row.ID, row.TaskGeneration, coordinator.ID, 2)
	if err != nil || done || len(targets) != 2 {
		t.Fatalf("first batch targets=%#v done=%v err=%v, want two target copies and more work", targets, done, err)
	}
	_, done, err = f.repos.Replacements.HandOverSourceWritesBatch(ctx, row.ID, row.TaskGeneration, coordinator.ID, 128)
	if err != nil || !done {
		t.Fatalf("second batch done=%v err=%v", done, err)
	}
	if unsigned, err := f.repos.Replacements.HasUnsignedSourceWrites(ctx, f.dataSetID); err != nil || unsigned {
		t.Fatalf("unsigned source writes = %v, err=%v, want only signed writes left", unsigned, err)
	}

	for _, released := range []*model.StorageCopy{collected, uploading, deleted} {
		got := f.copy(t, released.ID)
		if got.Status != model.StorageCopyStatusFailed || got.ActiveTaskID != nil || got.WorkGeneration != generations[released.ID]+1 ||
			got.CommitRequestID != nil || got.LastError == nil || *got.LastError != "Moved to the replacement provider" {
			t.Fatalf("handed-over copy = %#v, want a fenced failed copy", got)
		}
	}
	if got := f.copy(t, sealed.ID); got.Status != model.StorageCopyStatusCommitting || got.CommitPosition == nil {
		t.Fatalf("sealed member = %#v, want it left to its request", got)
	}
	if got := f.copy(t, pulling.ID); got.Status != model.StorageCopyStatusPending || got.ActiveTaskID == nil {
		t.Fatalf("pull with an unresolved attempt = %#v, want it left to its attempt", got)
	}
	inFlight, err := f.repos.Contents.ListInFlightCopiesForDataSet(ctx, f.dataSetID)
	if err != nil || len(inFlight) != 2 || inFlight[0].ID != sealed.ID || inFlight[1].ID != pulling.ID {
		t.Fatalf("source writes = %#v, err=%v, want only the signed ones", inFlight, err)
	}
	for _, c := range []struct {
		copyRow *model.StorageCopy
		target  bool
	}{{collected, true}, {uploading, true}, {deleted, false}, {sealed, false}, {pulling, false}} {
		got, err := f.repos.Contents.GetUploadCopyForDataSet(ctx, c.copyRow.ContentID, row.TargetDataSetID)
		if err != nil || (got != nil) != c.target || (got != nil && got.Status != model.StorageCopyStatusPending) {
			t.Fatalf("target copy for content %d = %#v, err=%v, want present=%v", c.copyRow.ContentID, got, err, c.target)
		}
	}
	flagged, err := f.db.NewSelect().Model((*model.StorageContent)(nil)).Where("id IN (?, ?) AND error_message IS NOT NULL", collected.ContentID, uploading.ContentID).Count(ctx)
	if err != nil || flagged != 0 {
		t.Fatalf("hand-over flagged %d live uploads, err=%v", flagged, err)
	}

	// Migration owes every live content of the slot, whatever the old provider
	// finished: handed-over writes and writes still signed alike.
	for seeded := false; !seeded; {
		if _, seeded, err = f.repos.Replacements.SeedMigrationBatch(ctx, row.ID, 128); err != nil {
			t.Fatal(err)
		}
	}
	items, err := f.repos.Replacements.ListPendingReplacementItemsAfter(ctx, row.ID, 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	owed := map[int64]bool{}
	for _, item := range items {
		snapshot, err := f.repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{ReplacementID: row.ID, ItemID: item.ID})
		if err != nil || snapshot == nil {
			t.Fatalf("item for content %d = %#v, err=%v, want it migrated", item.ContentID, snapshot, err)
		}
		owed[item.ContentID] = true
	}
	want := map[int64]bool{sealed.ContentID: true, collected.ContentID: true, uploading.ContentID: true, pulling.ContentID: true}
	if !maps.Equal(owed, want) {
		t.Fatalf("migrated contents = %v, want %v", owed, want)
	}
}

// A seal or pull reservation signs a write under its content lock. Whichever of
// signing and hand-over takes the lock first decides the write; the other
// leaves it alone.
func TestPostgresReplacementHandoverSerializesWithSigning(t *testing.T) {
	for _, tt := range []struct {
		name       string
		pull       bool
		signsFirst bool
	}{
		{name: "pull reserved first", pull: true, signsFirst: true},
		{name: "pull handed over first", pull: true},
		{name: "batch sealed first", signsFirst: true},
		{name: "batch handed over first"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			f := newCommitFixture(t, db)
			ctx := t.Context()
			var write *model.StorageCopy
			var sign func(operation string) error
			if tt.pull {
				write = f.sourceWrite(t, "pulled", true, model.StorageCopyTransferMethodPeerPull)
				input := f.pullReservation(t, write, f.bindWrite(t, write, model.TaskTypeStoragePull).ID)
				sign = func(operation string) error {
					return f.repos.Contents.ReservePullRequest(postgresRaceContext(ctx, operation), input)
				}
			} else {
				write = f.transferredCopy(t, "batched")
				taskID := f.collecting(t, "batch", write)
				sign = func(operation string) error {
					_, err := f.repos.Contents.SealCommitRequest(postgresRaceContext(ctx, operation), repository.SealCommitRequestInput{
						RequestID: "batch", TaskID: taskID, ExtraDataHex: "abcd",
						Members: []repository.SealMember{{CopyID: write.ID, ContentID: write.ContentID, PieceCID: "piece-batched"}},
					})
					return err
				}
			}
			row, coordinator := f.activateReplacement(t)
			handOver := func(operation string) error {
				_, _, err := f.repos.Replacements.HandOverSourceWritesBatch(postgresRaceContext(ctx, operation), row.ID, row.TaskGeneration, coordinator.ID, 128)
				return err
			}
			first, second := handOver, sign
			if tt.signsFirst {
				first, second = sign, handOver
			}
			hold := newPostgresRaceHold("first", func(query string) bool {
				return strings.Contains(query, "storage_contents") && strings.Contains(query, "for no key update")
			})
			testutil.AddQueryHook(db, hold)
			defer hold.Release()
			firstResult, secondResult := make(chan error, 1), make(chan error, 1)
			go func() { firstResult <- first("first") }()
			waitPostgresSignal(t, hold.reached, "first transaction holds the content lock")
			go func() { secondResult <- second("second") }()
			waitForPostgresLockWait(t, db, "storage_contents")
			hold.Release()
			if err := waitPostgresResult(t, firstResult, "first transaction"); err != nil {
				t.Fatal(err)
			}
			secondErr := waitPostgresResult(t, secondResult, "second transaction")

			got := f.copy(t, write.ID)
			attempts, err := db.NewSelect().Model((*storagepull.Attempt)(nil)).Where("content_id = ?", write.ContentID).Count(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if tt.signsFirst {
				if secondErr != nil || got.Status == model.StorageCopyStatusFailed || (tt.pull && attempts != 1) || (!tt.pull && got.CommitPosition == nil) {
					t.Fatalf("signed write = %#v, attempts=%d, hand-over err=%v, want the signature kept", got, attempts, secondErr)
				}
				return
			}
			if secondErr == nil || got.Status != model.StorageCopyStatusFailed || got.CommitPosition != nil || attempts != 0 {
				t.Fatalf("handed-over write = %#v, attempts=%d, signing err=%v, want signing refused", got, attempts, secondErr)
			}
		})
	}
}

func TestPostgresReplacementHandoverLocksContentsBeforeCommitRequests(t *testing.T) {
	db := testDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	collected := f.transferredCopy(t, "collected")
	f.collecting(t, "collecting", collected)
	joining := f.transferredCopy(t, "joining")
	row, coordinator := f.activateReplacement(t)

	hold := newPostgresRaceHold("transfer", func(query string) bool {
		return strings.Contains(query, "storage_contents") && strings.Contains(query, "for no key update")
	})
	testutil.AddQueryHook(db, hold)
	defer hold.Release()
	transferred, handedOver := make(chan error, 1), make(chan error, 1)
	transferCtx := postgresRaceContext(ctx, "transfer")
	go func() {
		transferred <- runPostgresRaceTx(transferCtx, db, make(chan int, 1), func(repos *repository.Repositories) error {
			if err := repos.Contents.MarkUploadCopyPieceReady(transferCtx, repository.MarkUploadCopyPieceReadyInput{
				StorageCopyID: joining.ID, ContentID: joining.ContentID, CopyIndex: joining.CopyIndex,
				PieceCID: "piece-joining", RequireEligibleCopy: true,
			}); err != nil {
				return err
			}
			requestID, members, err := repos.Contents.JoinCollectingCommitRequest(transferCtx, repository.JoinCommitRequestInput{
				CopyID: joining.ID, StorageDataSetID: f.dataSetID,
			})
			if err != nil {
				return err
			}
			if requestID != "collecting" || members != 2 {
				return fmt.Errorf("joined request %q with %d members", requestID, members)
			}
			return nil
		})
	}()
	waitPostgresSignal(t, hold.reached, "transfer holds the later content lock")
	handoverPID := make(chan int, 1)
	var targets []model.StorageCopy
	var done bool
	go func() {
		handedOver <- runPostgresRaceTx(ctx, db, handoverPID, func(repos *repository.Repositories) error {
			var err error
			targets, done, err = repos.Replacements.HandOverSourceWritesBatch(ctx, row.ID, row.TaskGeneration, coordinator.ID, 128)
			return err
		})
	}()
	waitForPostgresLockWait(t, db, "storage_contents", handoverPID)
	hold.Release()
	if err := waitPostgresResult(t, transferred, "transfer joins the collecting request"); err != nil {
		t.Fatal(err)
	}
	if err := waitPostgresResult(t, handedOver, "hand-over after transfer"); err != nil {
		t.Fatal(err)
	}
	if !done || len(targets) != 2 || targets[0].ContentID == targets[1].ContentID {
		t.Fatalf("hand-over targets=%#v done=%v, want both contents once", targets, done)
	}
	for _, source := range []*model.StorageCopy{collected, joining} {
		got := f.copy(t, source.ID)
		if got.Status != model.StorageCopyStatusFailed || got.CommitRequestID != nil {
			t.Fatalf("source copy = %#v, want it released", got)
		}
		target, err := f.repos.Contents.GetUploadCopyForDataSet(ctx, source.ContentID, row.TargetDataSetID)
		if err != nil || target == nil || target.Status != model.StorageCopyStatusPending {
			t.Fatalf("target for content %d = %#v, err=%v", source.ContentID, target, err)
		}
	}
	request, err := f.repos.Contents.GetCommitRequest(ctx, "collecting")
	if err != nil || request == nil || request.Status != storagecommit.RequestStatusAbandoned || request.TaskID != nil {
		t.Fatalf("released request = %#v, err=%v", request, err)
	}
}

func TestPostgresReplacementHandoverDiscardsRolledBackTargets(t *testing.T) {
	db := testDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	sources := []*model.StorageCopy{f.transferredCopy(t, "replay-first"), f.transferredCopy(t, "replay-second")}
	row, coordinator := f.activateReplacement(t)
	// PostgreSQL sequences retain attempt counts across transaction rollback.
	for _, statement := range []string{
		"CREATE SEQUENCE handover_attempts",
		`CREATE FUNCTION retry_handover_once() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF nextval('handover_attempts') = 1 THEN
				RAISE EXCEPTION 'injected handover serialization failure' USING ERRCODE = '40001';
			END IF;
			RETURN NEW;
		END $$`,
		fmt.Sprintf(`CREATE TRIGGER retry_handover BEFORE INSERT ON storage_replacement_items
			FOR EACH ROW WHEN (NEW.content_id = %d) EXECUTE FUNCTION retry_handover_once()`, sources[1].ContentID),
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	targets, done, err := f.repos.Replacements.HandOverSourceWritesBatch(ctx, row.ID, row.TaskGeneration, coordinator.ID, 128)
	if err != nil || !done || len(targets) != len(sources) {
		t.Fatalf("replayed hand-over targets=%#v done=%v err=%v, want both contents once", targets, done, err)
	}
	var attempts int
	if err := db.NewRaw("SELECT last_value FROM handover_attempts").Scan(ctx, &attempts); err != nil || attempts != 2 {
		t.Fatalf("hand-over attempts=%d, err=%v, want one rollback and one commit", attempts, err)
	}
	returned := make(map[int64]int64, len(targets))
	for _, target := range targets {
		if _, duplicate := returned[target.ContentID]; duplicate {
			t.Fatalf("duplicate returned target for content %d", target.ContentID)
		}
		returned[target.ContentID] = target.ID
	}
	for _, source := range sources {
		target, err := f.repos.Contents.GetUploadCopyForDataSet(ctx, source.ContentID, row.TargetDataSetID)
		if err != nil || target == nil || target.ID != returned[source.ContentID] || target.Status != model.StorageCopyStatusPending {
			t.Fatalf("committed target for content %d = %#v, returned=%v, err=%v", source.ContentID, target, returned, err)
		}
		if got := f.copy(t, source.ID); got.Status != model.StorageCopyStatusFailed || got.WorkGeneration != source.WorkGeneration+1 {
			t.Fatalf("handed-over source = %#v, want one committed release", got)
		}
	}
	items, err := f.repos.Replacements.ListPendingReplacementItemsAfter(ctx, row.ID, 0, 128)
	if err != nil || len(items) != len(sources) {
		t.Fatalf("committed replacement items=%#v, err=%v", items, err)
	}
	for _, item := range items {
		if _, exists := returned[item.ContentID]; !exists {
			t.Fatalf("replacement item for unexpected content %d", item.ContentID)
		}
	}
}
