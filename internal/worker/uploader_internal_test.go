package worker

import (
	"context"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

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
