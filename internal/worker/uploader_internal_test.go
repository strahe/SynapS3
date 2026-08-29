package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

const submittedCommitTestTxHash = "0x7890abcdef1234567890abcdef1234567890abcdef1234567890abcdef123456"

func testSubmittedCommitChecker(timeout time.Duration) *synapse.PDPStatusChecker {
	return synapse.NewPDPStatusChecker(synapse.PDPStatusCheckerOptions{
		Timeout:              timeout,
		AllowPrivateNetworks: true,
	})
}

func TestWaitForSubmittedCommitClassifiesProviderTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()

	dataSetID := onChainID(t, "1001")
	u := &Uploader{statusChecker: testSubmittedCommitChecker(25 * time.Millisecond)}
	started := time.Now()
	_, err := u.waitForSubmittedCommit(context.Background(), submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &dataSetID}, submittedCommitTestTxHash, 1)
	close(release)
	if err == nil {
		t.Fatal("waitForSubmittedCommit returned nil error for a stalled provider")
	}
	if !synapse.IsProviderUnavailable(err) {
		t.Fatalf("waitForSubmittedCommit error = %T %v, want ProviderUnavailableError", err, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("waitForSubmittedCommit elapsed = %s, want bounded timeout", elapsed)
	}
}

func TestWaitForSubmittedCommitPreservesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dataSetID := onChainID(t, "1001")
	u := &Uploader{statusChecker: testSubmittedCommitChecker(time.Second)}
	_, err := u.waitForSubmittedCommit(ctx, submittedCommitTestContext{serviceURL: "https://provider.example"}, &model.StorageDataSet{DataSetID: &dataSetID}, submittedCommitTestTxHash, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForSubmittedCommit error = %T %v, want context canceled", err, err)
	}
	if synapse.IsProviderUnavailable(err) {
		t.Fatalf("waitForSubmittedCommit error = %T %v, caller cancellation must not mark provider unavailable", err, err)
	}
}

func TestWaitForSubmittedCommitClassifiesProviderUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	dataSetID := onChainID(t, "1001")
	u := &Uploader{statusChecker: testSubmittedCommitChecker(time.Second)}
	_, err := u.waitForSubmittedCommit(context.Background(), submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &dataSetID}, submittedCommitTestTxHash, 1)
	if !synapse.IsProviderUnavailable(err) {
		t.Fatalf("waitForSubmittedCommit error = %T %v, want ProviderUnavailableError", err, err)
	}
}

func TestWaitForSubmittedCommitRejectsConfirmedWithoutPieces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"piecesAdded":false}`, submittedCommitTestTxHash)
	}))
	defer srv.Close()

	dataSetID := onChainID(t, "1001")
	u := &Uploader{statusChecker: testSubmittedCommitChecker(time.Second)}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := u.waitForSubmittedCommit(ctx, submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &dataSetID}, submittedCommitTestTxHash, 1)
	if !errors.Is(err, errCommitRejected) {
		t.Fatalf("waitForSubmittedCommit error = %v, want errCommitRejected", err)
	}
}

func TestWaitForSubmittedCommitRejectsSDKTerminalStatuses(t *testing.T) {
	for _, txStatus := range []string{"failed", "reorged"} {
		t.Run(txStatus, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":%q,"dataSetId":1001,"pieceCount":0,"addMessageOk":false,"piecesAdded":false}`, submittedCommitTestTxHash, txStatus)
			}))
			defer srv.Close()

			dataSetID := onChainID(t, "1001")
			u := &Uploader{statusChecker: testSubmittedCommitChecker(time.Second)}
			_, err := u.waitForSubmittedCommit(t.Context(), submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &dataSetID}, submittedCommitTestTxHash, 1)
			if !errors.Is(err, errCommitRejected) {
				t.Fatalf("waitForSubmittedCommit error = %v, want errCommitRejected", err)
			}
		})
	}
}

func TestWaitForSubmittedCommitHasGlobalTimeout(t *testing.T) {
	originalMaxWait := submittedCommitMaxWait
	submittedCommitMaxWait = 40 * time.Millisecond
	t.Cleanup(func() { submittedCommitMaxWait = originalMaxWait })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"pending","dataSetId":1001,"pieceCount":1,"piecesAdded":false}`, submittedCommitTestTxHash)
	}))
	defer srv.Close()

	dataSetID := onChainID(t, "1001")
	u := &Uploader{statusChecker: testSubmittedCommitChecker(time.Second)}
	started := time.Now()
	_, err := u.waitForSubmittedCommit(context.Background(), submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &dataSetID}, submittedCommitTestTxHash, 1)
	if !errors.Is(err, errSubmittedCommitPending) {
		t.Fatalf("waitForSubmittedCommit error = %v, want submitted commit pending", err)
	}
	if synapse.IsProviderUnavailable(err) {
		t.Fatalf("waitForSubmittedCommit error = %T %v, pending commit must not mark provider unavailable", err, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("waitForSubmittedCommit elapsed = %s, want bounded timeout", elapsed)
	}
}

func TestPendingSubmittedCommitWaitsWithoutRetry(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	stage := uploadStageIngressCommit
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J0000000000000000COMMIT",
		IdempotencyKey: "upload:pending-commit",
		Status:         model.TaskStatusQueued,
		MaxRetries:     1,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create: %v", err)
	}
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimReady: task=%#v err=%v", claimed, err)
	}

	u := &Uploader{repos: repos}
	if !u.waitForPendingSubmittedCommit(ctx, claimed, slog.Default(), fmt.Errorf("poll commit: %w", errSubmittedCommitPending)) {
		t.Fatal("waitForPendingSubmittedCommit did not handle the pending commit")
	}
	got, err := repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.TaskStatusWaiting || got.RetryCount != 0 {
		t.Fatalf("pending commit task = %#v, want waiting without retry", got)
	}
	if got.WaitReason == nil || *got.WaitReason != model.TaskWaitReasonDependency || got.StatusMessage == nil || *got.StatusMessage != "Waiting for storage confirmation" {
		t.Fatalf("pending commit task = %#v, want storage confirmation dependency", got)
	}
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

func TestWaitForSubmittedCommitParsesBigIntStringIDsAndZeroPieceID(t *testing.T) {
	dataSetID := "18446744073709551616"
	pieceID := "18446744073709551617"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":%q,"pieceCount":2,"addMessageOk":true,"piecesAdded":true,"confirmedPieceIds":["0",%q]}`, submittedCommitTestTxHash, dataSetID, pieceID)
	}))
	defer srv.Close()

	bindingDataSetID := onChainID(t, dataSetID)
	u := &Uploader{statusChecker: testSubmittedCommitChecker(time.Second)}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result, err := u.waitForSubmittedCommit(ctx, submittedCommitTestContext{serviceURL: srv.URL, dataSetID: bindingDataSetID.SDK()}, &model.StorageDataSet{DataSetID: &bindingDataSetID}, submittedCommitTestTxHash, 2)
	if err != nil {
		t.Fatalf("waitForSubmittedCommit: %v", err)
	}
	if result.DataSet.DataSetID().String() != dataSetID || len(result.PieceIDs) != 2 || result.PieceIDs[0].String() != "0" || result.PieceIDs[1].String() != pieceID {
		t.Fatalf("commit result = %#v, want big data set and piece IDs", result)
	}
}

func TestWaitForSubmittedCommitRejectsTransactionMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"addMessageOk":true,"piecesAdded":true,"confirmedPieceIds":[2001]}`, "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	}))
	defer srv.Close()

	dataSetID := onChainID(t, "1001")
	u := &Uploader{statusChecker: testSubmittedCommitChecker(time.Second)}
	_, err := u.waitForSubmittedCommit(t.Context(), submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &dataSetID}, submittedCommitTestTxHash, 1)
	if err == nil || synapse.IsProviderUnavailable(err) {
		t.Fatalf("waitForSubmittedCommit error = %T %v, want ordinary identity mismatch", err, err)
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

type submittedCommitTestContext struct {
	serviceURL string
	dataSetID  sdktypes.BigInt
}

func (c submittedCommitTestContext) ProviderID() sdktypes.BigInt { return sdktypes.NewBigInt(1) }

func (c submittedCommitTestContext) DataSetRef() (storage.DataSetRef, bool) {
	dataSetID := c.dataSetID
	if dataSetID.IsZero() {
		dataSetID = sdktypes.NewBigInt(1001)
	}
	ref, err := storage.NewDataSetRef(c.ProviderID(), dataSetID, sdktypes.BigInt{})
	return ref, err == nil
}

func (c submittedCommitTestContext) GetProviderInfo() storage.Provider {
	return storage.Provider{ID: c.ProviderID(), ServiceURL: c.ServiceURL()}
}

func (c submittedCommitTestContext) CDNEnabled() bool { return false }

func (c submittedCommitTestContext) PieceURL(cid.Cid) string { return "" }

func (c submittedCommitTestContext) ServiceURL() string { return c.serviceURL }

func (c submittedCommitTestContext) CreateDataSet(context.Context, *storage.CreateDataSetOptions) (*storage.CreateDataSetResult, error) {
	return nil, errors.New("unused")
}

func (c submittedCommitTestContext) WaitForDataSetCreated(context.Context, storage.CreateDataSetSubmission) (*storage.CreateDataSetResult, error) {
	return nil, errors.New("unused")
}

func (c submittedCommitTestContext) Store(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
	return nil, errors.New("unused")
}

func (c submittedCommitTestContext) PresignForCommit(context.Context, []storage.PieceInput) ([]byte, error) {
	return nil, errors.New("unused")
}

func (c submittedCommitTestContext) Pull(context.Context, storage.PullRequest) (*storage.PullResult, error) {
	return nil, errors.New("unused")
}

func (c submittedCommitTestContext) Commit(context.Context, storage.CommitRequest) (*storage.CommitResult, error) {
	return nil, errors.New("unused")
}
