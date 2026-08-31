package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func seedCommitFailureCopy(
	t *testing.T,
	repos *repository.Repositories,
	bucket *model.Bucket,
	dataSet *model.StorageDataSet,
	sourceVersionID string,
) *model.StorageUploadCopy {
	t.Helper()
	upload, err := repos.Uploads.StartObjectUploadAttempt(t.Context(), repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: sourceVersionID, ContentSize: 1,
		Checksum: "checksum-" + sourceVersionID, RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(t.Context(), upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: dataSet.ID,
		CopyIndex:        dataSet.CopyIndex,
		TransferMethod:   model.StorageCopyTransferMethodPeerPull,
		ProviderID:       dataSet.ProviderID,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	copyRow, err := repos.Uploads.GetUploadCopyForDataSet(t.Context(), upload.ID, dataSet.ID)
	if err != nil || copyRow == nil {
		t.Fatalf("GetUploadCopyForDataSet: copy=%#v err=%v", copyRow, err)
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(t.Context(), repository.MarkUploadCopyPieceReadyInput{
		StorageUploadCopyID: copyRow.ID, UploadID: upload.ID, CopyIndex: copyRow.CopyIndex,
		PieceCID: "bafkqaaa", RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	copyRow, err = repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
	if err != nil {
		t.Fatalf("reload copy: %v", err)
	}
	return copyRow
}

func seedCommitFailureFixture(
	t *testing.T,
	maxRetries int,
) (*repository.Repositories, *model.StorageUploadCopy, *model.StorageDataSet, *model.Bucket, *model.Task) {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	bucket := testutil.SeedBucket(t, db, "commit-failure-"+model.NewVersionID())
	owner, err := repos.Uploads.StartObjectUploadAttempt(t.Context(), repository.StartObjectUploadAttemptInput{
		BucketID: bucket.ID, SourceVersionID: model.NewVersionID(), ContentSize: 1,
		Checksum: "commit-failure-owner", RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt owner: %v", err)
	}
	dataSet, err := repos.Uploads.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "901"), CopyIndex: 0, CreatedByUploadID: owner.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
		ID: dataSet.ID, UploadID: owner.ID, DataSetID: onChainID(t, "9001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	dataSet, err = repos.Uploads.GetDataSetBindingByID(t.Context(), dataSet.ID)
	if err != nil || dataSet == nil {
		t.Fatalf("GetDataSetBindingByID: dataSet=%#v err=%v", dataSet, err)
	}
	copyRow := seedCommitFailureCopy(t, repos, bucket, dataSet, model.NewVersionID())
	identity := storagecommit.CopyIdentity{
		StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
		CopyIndex: copyRow.CopyIndex, StorageDataSetID: dataSet.ID,
	}
	if _, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
		Copy: identity, AttemptID: "commit-failure-reservation",
	}); err != nil {
		t.Fatalf("ReserveCommitAttempt: %v", err)
	}
	stage := uploadStagePeerCommit
	task := &model.Task{
		Type: model.TaskTypeUpload, Stage: &stage, RefType: "object", RefID: bucket.ID,
		RefVersionID: model.NewVersionID(), IdempotencyKey: "commit-failure-task-" + model.NewVersionID(),
		Payload: map[string]interface{}{}, Status: model.TaskStatusQueued,
		MaxRetries: maxRetries, ScheduledAt: time.Now(),
	}
	if err := repos.Tasks.Create(t.Context(), task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	claimed, err := repos.Tasks.ClaimReady(t.Context(), model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimReady: task=%#v err=%v", claimed, err)
	}
	return repos, copyRow, dataSet, bucket, claimed
}

func TestCommitTaskFailureReservationLifecycle(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("transient retry retains FIFO", func(t *testing.T) {
		repos, copyRow, _, _, task := seedCommitFailureFixture(t, 2)
		identity := storagecommit.CopyIdentity{
			StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
			CopyIndex: copyRow.CopyIndex, StorageDataSetID: *copyRow.StorageDataSetID,
		}
		if err := repos.Uploads.ReleaseCommitAttempt(t.Context(), storagecommit.ReleaseInput{
			Copy: identity, AttemptID: "commit-failure-reservation",
		}); err != nil {
			t.Fatalf("seed ready-only reservation: %v", err)
		}
		(&Uploader{repos: repos}).handleCommitTaskFailure(t.Context(), task, copyRow, logger, "commit", errors.New("presign failed"))
		persisted, err := repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
		if err != nil || persisted.CommitReadyAt == nil || persisted.CommitAttemptID != nil {
			t.Fatalf("transient copy = %#v err=%v, want retained FIFO", persisted, err)
		}
		gotTask, err := repos.Tasks.GetByID(t.Context(), task.ID)
		if err != nil || gotTask.Status != model.TaskStatusScheduled || gotTask.RetryCount != 1 {
			t.Fatalf("transient task = %#v err=%v", gotTask, err)
		}
	})

	t.Run("terminal retry releases FIFO for successor", func(t *testing.T) {
		repos, copyRow, dataSet, bucket, task := seedCommitFailureFixture(t, 1)
		identity := storagecommit.CopyIdentity{
			StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
			CopyIndex: copyRow.CopyIndex, StorageDataSetID: *copyRow.StorageDataSetID,
		}
		if err := repos.Uploads.ReleaseCommitAttempt(t.Context(), storagecommit.ReleaseInput{
			Copy: identity, AttemptID: "commit-failure-reservation",
		}); err != nil {
			t.Fatalf("seed ready-only reservation: %v", err)
		}
		follower := seedCommitFailureCopy(t, repos, bucket, dataSet, model.NewVersionID())
		followerIdentity := storagecommit.CopyIdentity{
			StorageUploadCopyID: follower.ID, UploadID: follower.UploadID,
			CopyIndex: follower.CopyIndex, StorageDataSetID: *follower.StorageDataSetID,
		}
		waiting, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
			Copy: followerIdentity, AttemptID: "follower-waiting",
		})
		if err != nil || waiting.State != storagecommit.ReservationWaiting {
			t.Fatalf("follower before cleanup = %#v err=%v, want waiting", waiting, err)
		}
		(&Uploader{repos: repos}).handleCommitTaskFailure(t.Context(), task, copyRow, logger, "commit", errors.New("presign failed"))
		persisted, err := repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
		if err != nil || persisted.CommitReadyAt != nil || persisted.CommitAttemptID != nil {
			t.Fatalf("terminal copy = %#v err=%v, want cleared FIFO", persisted, err)
		}
		gotTask, err := repos.Tasks.GetByID(t.Context(), task.ID)
		if err != nil || gotTask.Status != model.TaskStatusExhausted || gotTask.RetryCount != 1 {
			t.Fatalf("terminal task = %#v err=%v", gotTask, err)
		}
		admitted, err := repos.Uploads.ReserveCommitAttempt(t.Context(), storagecommit.ReserveInput{
			Copy: followerIdentity, AttemptID: "follower-admitted",
		})
		if err != nil || admitted.State != storagecommit.ReservationAcquired {
			t.Fatalf("follower after cleanup = %#v err=%v, want acquired", admitted, err)
		}
	})

	t.Run("attempted fence parks without exhaustion", func(t *testing.T) {
		repos, copyRow, _, _, task := seedCommitFailureFixture(t, 1)
		identity := storagecommit.CopyIdentity{
			StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
			CopyIndex: copyRow.CopyIndex, StorageDataSetID: *copyRow.StorageDataSetID,
		}
		if _, err := repos.Uploads.MarkCommitAttempted(t.Context(), storagecommit.AttemptInput{
			Copy: identity, AttemptID: "commit-failure-reservation", ExtraDataHex: "abcd",
		}); err != nil {
			t.Fatalf("MarkCommitAttempted: %v", err)
		}
		(&Uploader{repos: repos, pollInterval: time.Second}).handleCommitTaskFailure(
			t.Context(), task, copyRow, logger, "commit", errors.New("persist evidence failed"),
		)
		persisted, err := repos.Uploads.GetUploadCopyByID(t.Context(), copyRow.ID)
		if err != nil || persisted.CommitAttemptID == nil || *persisted.CommitAttemptID != "commit-failure-reservation" ||
			persisted.CommitAttemptedAt == nil {
			t.Fatalf("attempted copy = %#v err=%v, want preserved fence", persisted, err)
		}
		gotTask, err := repos.Tasks.GetByID(t.Context(), task.ID)
		if err != nil || gotTask.Status != model.TaskStatusWaiting || gotTask.RetryCount != 0 ||
			gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonExternalConfirmation {
			t.Fatalf("attempted task = %#v err=%v, want confirmation wait", gotTask, err)
		}
	})

	t.Run("settlement rollback parks instead of stranding the claim", func(t *testing.T) {
		repos, copyRow, _, _, task := seedCommitFailureFixture(t, 1)
		identity := storagecommit.CopyIdentity{
			StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
			CopyIndex: copyRow.CopyIndex, StorageDataSetID: *copyRow.StorageDataSetID,
		}
		if err := repos.Uploads.ReleaseCommitAttempt(t.Context(), storagecommit.ReleaseInput{
			Copy: identity, AttemptID: "commit-failure-reservation",
		}); err != nil {
			t.Fatalf("seed ready-only reservation: %v", err)
		}
		// Failing the copy leaves nothing for the terminal settlement to release, so
		// its transaction rolls back without the attempt ever becoming observable.
		if err := repos.Uploads.MarkUploadCopyFailed(t.Context(), repository.MarkUploadCopyFailedInput{
			StorageUploadCopyID: copyRow.ID, UploadID: copyRow.UploadID,
			CopyIndex: copyRow.CopyIndex, LastError: "copy failed before settlement",
		}); err != nil {
			t.Fatalf("MarkUploadCopyFailed: %v", err)
		}
		(&Uploader{repos: repos, pollInterval: time.Second}).handleCommitTaskFailure(
			t.Context(), task, copyRow, logger, "commit", errors.New("presign failed"),
		)
		gotTask, err := repos.Tasks.GetByID(t.Context(), task.ID)
		if err != nil || gotTask.Status != model.TaskStatusWaiting ||
			gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonExternalConfirmation {
			t.Fatalf("rolled back task = %#v err=%v, want a parked task rather than a held claim", gotTask, err)
		}
	})
}

func TestEnsureBucketProviderBindingsPersistsPartialSelectionBeforeWaiting(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "partial-selection-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J0000000000000PARTIAL01",
		ContentSize:     1024,
		Checksum:        "partial-selection-checksum",
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	providerTarget := testutil.NewMockProviderTarget(sdktypes.NewBigInt(101), storage.NewProviderContextOptions{})
	dataSetTarget := testutil.NewMockDataSetTarget(sdktypes.NewBigInt(202), sdktypes.NewBigInt(2002), nil)
	dataSetTarget.ClientDataSetIDValue = sdktypes.NewBigInt(9002)
	client := &testutil.MockStorageClient{
		SelectUploadTargetsFunc: func(_ context.Context, opts storage.SelectUploadContextsOptions) ([]synapse.StorageTarget, error) {
			if opts.Copies != 3 {
				t.Fatalf("selection copies = %d, want 3", opts.Copies)
			}
			return []synapse.StorageTarget{providerTarget, dataSetTarget}, &synapse.NoProviderCandidatesError{
				Cause: &storage.InsufficientUploadContextsError{Requested: 3, Available: 2},
			}
		},
	}
	uploader := &Uploader{repos: repos, storage: client}

	plan, err := uploader.ensureBucketProviderBindings(ctx, bucket, upload.ID, 3)
	if !synapse.IsNoProviderCandidates(err) || plan.complete || len(plan.bindings) != 2 {
		t.Fatalf("plan = %#v, error = %T %v; want two persisted partial bindings", plan, err, err)
	}
	pending := plan.byCopyIndex[0]
	ready := plan.byCopyIndex[1]
	if pending == nil || pending.ProviderID.String() != "101" || pending.Status != model.StorageDataSetStatusPending {
		t.Fatalf("provider binding = %#v, want pending provider 101", pending)
	}
	if ready == nil || ready.ProviderID.String() != "202" || ready.Status != model.StorageDataSetStatusReady ||
		ready.DataSetID == nil || ready.DataSetID.String() != "2002" ||
		ready.ClientDataSetID == nil || ready.ClientDataSetID.String() != "9002" {
		t.Fatalf("data set binding = %#v, want complete ready identity", ready)
	}
}

func TestContextForReadyBindingBackfillsClientDataSetIDAndRejectsConflict(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "ready-binding-backfill-bucket", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J00000000000BACKFILL01",
		ContentSize:     1024,
		Checksum:        "ready-binding-backfill-checksum",
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	dataSetID := onChainID(t, "1001")
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: binding.ID, UploadID: upload.ID, DataSetID: dataSetID,
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	binding, err = repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || binding == nil {
		t.Fatalf("GetDataSetBindingByID: binding=%#v err=%v", binding, err)
	}
	target := testutil.NewMockDataSetTarget(sdktypes.NewBigInt(101), sdktypes.NewBigInt(1001), nil)
	target.ClientDataSetIDValue = sdktypes.NewBigInt(9001)
	client := &testutil.MockStorageClient{
		OpenDataSetTargetFunc: func(_ context.Context, gotDataSetID sdktypes.BigInt, opts storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
			if !gotDataSetID.Equal(sdktypes.NewBigInt(1001)) || opts.ProviderID == nil || !opts.ProviderID.Equal(sdktypes.NewBigInt(101)) {
				t.Fatalf("OpenDataSetTarget inputs = dataSet:%s provider:%v", gotDataSetID.String(), opts.ProviderID)
			}
			return target, nil
		},
	}
	uploader := &Uploader{repos: repos, storage: client}

	if _, err := uploader.contextForReadyBinding(ctx, binding); err != nil {
		t.Fatalf("contextForReadyBinding: %v", err)
	}
	if binding.ClientDataSetID == nil || binding.ClientDataSetID.String() != "9001" {
		t.Fatalf("in-memory client data set ID = %v, want 9001", binding.ClientDataSetID)
	}
	stored, err := repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || stored == nil || stored.ClientDataSetID == nil || stored.ClientDataSetID.String() != "9001" {
		t.Fatalf("stored binding = %#v err=%v, want client data set ID 9001", stored, err)
	}

	target.ClientDataSetIDValue = sdktypes.NewBigInt(9002)
	if _, err := uploader.contextForReadyBinding(ctx, binding); err == nil {
		t.Fatal("contextForReadyBinding accepted a conflicting client data set ID")
	}
}

func TestDataSetResultIDsForBindingRejectsMissingResult(t *testing.T) {
	t.Parallel()

	binding := &model.StorageDataSet{ProviderID: onChainID(t, "101")}
	if _, _, err := dataSetResultIDsForBinding(binding, nil); err == nil {
		t.Fatal("dataSetResultIDsForBinding accepted a missing creation result")
	}
}

func TestUploadProgressEventPayloadUsesOverflowSafePercent(t *testing.T) {
	updatedAt := time.Now()
	const huge = int64(1 << 62)
	payload := uploadProgressEventPayload(&model.StorageUpload{
		IngressStoreAttempt:     1,
		IngressBytesTransferred: huge,
		ContentSize:             huge,
		ProgressUpdatedAt:       &updatedAt,
	}, true)

	percent, ok := payload["percent"].(int)
	if !ok {
		t.Fatalf("percent missing or wrong type: %#v", payload["percent"])
	}
	if percent != 100 {
		t.Fatalf("percent = %d, want 100", percent)
	}
}

func TestUploadProgressEventPayloadMarksDoneWhenBytesReachTotal(t *testing.T) {
	updatedAt := time.Now()
	payload := uploadProgressEventPayload(&model.StorageUpload{
		IngressStoreAttempt:     1,
		IngressBytesTransferred: 100,
		ContentSize:             100,
		ProgressUpdatedAt:       &updatedAt,
	}, false)

	done, ok := payload["done"].(bool)
	if !ok {
		t.Fatalf("done missing or wrong type: %#v", payload["done"])
	}
	if !done {
		t.Fatal("done = false, want true when uploaded bytes reach total bytes")
	}
}

func TestUploadProgressReporterRecordRespectsCanceledContext(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "progress-context")
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J0000000000000000CTXPRG",
		ContentSize:     100,
		Checksum:        "sha256:progress-context",
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	upload, err = repos.Uploads.BeginIngressStoreProgress(ctx, upload.ID)
	if err != nil {
		t.Fatalf("BeginIngressStoreProgress: %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	reporter := &uploadProgressReporter{
		ctx:      canceledCtx,
		repos:    repos,
		uploadID: upload.ID,
		attempt:  upload.IngressStoreAttempt,
	}
	reporter.record(50, false)

	got, err := repos.Uploads.GetByID(context.Background(), upload.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.IngressBytesTransferred != 0 {
		t.Fatalf("ingress_bytes_transferred = %d, want 0 after canceled reporter context", got.IngressBytesTransferred)
	}
}

func TestUploadProgressReporterCoalescesThrottledProgress(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, db, "progress-coalesce")
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J0000000000000000COALES",
		ContentSize:     100,
		Checksum:        "sha256:progress-coalesce",
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	upload, err = repos.Uploads.BeginIngressStoreProgress(ctx, upload.ID)
	if err != nil {
		t.Fatalf("BeginIngressStoreProgress: %v", err)
	}

	reporter := &uploadProgressReporter{
		ctx:           context.Background(),
		repos:         repos,
		uploadID:      upload.ID,
		attempt:       upload.IngressStoreAttempt,
		flushInterval: 50 * time.Millisecond,
	}
	reporter.OnProgress(10)
	reporter.OnProgress(40)
	reporter.OnProgress(70)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, err := repos.Uploads.GetByID(context.Background(), upload.ID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if got.IngressBytesTransferred == 70 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	got, err := repos.Uploads.GetByID(context.Background(), upload.ID)
	if err != nil {
		t.Fatalf("GetByID final: %v", err)
	}
	t.Fatalf("ingress_bytes_transferred = %d, want coalesced latest progress 70", got.IngressBytesTransferred)
}

func TestCommitReleaseCauseKeepsDataSetEndedClassification(t *testing.T) {
	// Both the uploader and the replacement worker decide between "draining" and
	// "unavailable" from this error, so every cause a release can carry must still
	// read as an ended data set.
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "missing cause falls back to the sentinel"},
		{name: "service ended", cause: storage.ErrDataSetUnavailable},
		{name: "write blocked", cause: fmt.Errorf("submit commit: %w", &storage.DataSetPDPPaymentTerminatedError{
			DataSetID: sdktypes.NewBigInt(1001), PDPEndEpoch: sdktypes.Epoch(42),
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := commitReleaseCause(storagecommit.AdvanceResult{
				State:         storagecommit.AdvanceReleased,
				ReleaseReason: storagecommit.ReleaseDataSetUnavailable,
				Cause:         tc.cause,
			})
			if cause == nil {
				t.Fatal("release cause is nil, want an error the failure handlers can classify")
			}
			if !dataSetFailureEnded(cause, nil) {
				t.Fatalf("release cause %v is not an ended data set, so it would be marked unavailable", cause)
			}
		})
	}
}

func TestCapacityWaitNamesAttentionHeldSlots(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name          string
		attentionHeld int
		wantMessage   string
	}{
		{
			name:          "queued behind work that still moves",
			attentionHeld: 0,
			wantMessage:   "Waiting to submit stored content",
		},
		{
			// The unflagged attempts are still working through, so the queue moves.
			name:          "partial attention hold keeps the ordinary wording",
			attentionHeld: storagecommit.MaxActiveAttemptsPerDataSet - 1,
			wantMessage:   "Waiting to submit stored content",
		},
		{
			name:          "every slot flagged for attention",
			attentionHeld: storagecommit.MaxActiveAttemptsPerDataSet,
			wantMessage:   "Waiting for storage confirmations that need attention",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			repos, _, _, _, task := seedCommitFailureFixture(t, 2)
			handled := (&Uploader{repos: repos}).waitForCommitAdvance(t.Context(), task, logger, storagecommit.AdvanceResult{
				State: storagecommit.AdvanceWaitingCapacity, AttentionHeld: testCase.attentionHeld,
			})
			if !handled {
				t.Fatal("capacity wait was not handled")
			}
			gotTask, err := repos.Tasks.GetByID(t.Context(), task.ID)
			if err != nil || gotTask == nil || gotTask.StatusMessage == nil {
				t.Fatalf("task after capacity wait = %#v err=%v", gotTask, err)
			}
			if *gotTask.StatusMessage != testCase.wantMessage {
				t.Fatalf("capacity wait message = %q, want %q", *gotTask.StatusMessage, testCase.wantMessage)
			}
			if gotTask.Status != model.TaskStatusWaiting || gotTask.RetryCount != 0 {
				t.Fatalf("task after capacity wait = %#v, want waiting without retry", gotTask)
			}
		})
	}
}
