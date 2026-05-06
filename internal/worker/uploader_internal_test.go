package worker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestGetSubmittedCommitStatusTimesOutWhenProviderStalls(t *testing.T) {
	originalTimeout := submittedCommitRequestTimeout
	submittedCommitRequestTimeout = 25 * time.Millisecond
	t.Cleanup(func() { submittedCommitRequestTimeout = originalTimeout })

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()

	started := time.Now()
	_, err := getSubmittedCommitStatus(context.Background(), srv.URL)
	close(release)
	if err == nil {
		t.Fatal("getSubmittedCommitStatus returned nil error for a stalled provider")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("getSubmittedCommitStatus elapsed = %s, want bounded timeout", elapsed)
	}
}

func TestWaitForSubmittedCommitRejectsConfirmedWithoutPieces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"txHash":"0xcommit","txStatus":"confirmed","dataSetId":1001,"piecesAdded":false}`))
	}))
	defer srv.Close()

	dataSetID := onChainID(t, "1001")
	u := &Uploader{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := u.waitForSubmittedCommit(ctx, submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &dataSetID}, "0xcommit", 1)
	if !errors.Is(err, errCommitRejected) {
		t.Fatalf("waitForSubmittedCommit error = %v, want errCommitRejected", err)
	}
}

func TestWaitForSubmittedCommitHasGlobalTimeout(t *testing.T) {
	originalMaxWait := submittedCommitMaxWait
	submittedCommitMaxWait = 40 * time.Millisecond
	t.Cleanup(func() { submittedCommitMaxWait = originalMaxWait })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"txHash":"0xcommit","txStatus":"pending","dataSetId":1001,"piecesAdded":false}`))
	}))
	defer srv.Close()

	dataSetID := onChainID(t, "1001")
	u := &Uploader{}
	started := time.Now()
	_, err := u.waitForSubmittedCommit(context.Background(), submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &dataSetID}, "0xcommit", 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForSubmittedCommit error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("waitForSubmittedCommit elapsed = %s, want bounded timeout", elapsed)
	}
}

func TestWaitForSubmittedCommitParsesBigIntStringIDsAndZeroPieceID(t *testing.T) {
	dataSetID := "18446744073709551616"
	pieceID := "18446744073709551617"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"txHash":"0xcommit","txStatus":"confirmed","dataSetId":"` + dataSetID + `","piecesAdded":true,"confirmedPieceIds":["0","` + pieceID + `"]}`))
	}))
	defer srv.Close()

	bindingDataSetID := onChainID(t, dataSetID)
	u := &Uploader{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result, err := u.waitForSubmittedCommit(ctx, submittedCommitTestContext{serviceURL: srv.URL}, &model.StorageDataSet{DataSetID: &bindingDataSetID}, "0xcommit", 2)
	if err != nil {
		t.Fatalf("waitForSubmittedCommit: %v", err)
	}
	if result.DataSetID.String() != dataSetID || len(result.PieceIDs) != 2 || result.PieceIDs[0].String() != "0" || result.PieceIDs[1].String() != pieceID {
		t.Fatalf("commit result = %#v, want big data set and piece IDs", result)
	}
}

func TestUploadProgressEventPayloadUsesOverflowSafePercent(t *testing.T) {
	updatedAt := time.Now()
	const huge = int64(1 << 62)
	payload := uploadProgressEventPayload(&model.StorageUpload{
		PrimaryStoreAttempt:  1,
		PrimaryBytesUploaded: huge,
		ContentSize:          huge,
		ProgressUpdatedAt:    &updatedAt,
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
		PrimaryStoreAttempt:  1,
		PrimaryBytesUploaded: 100,
		ContentSize:          100,
		ProgressUpdatedAt:    &updatedAt,
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
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	upload, err = repos.Uploads.BeginPrimaryStoreProgress(ctx, upload.ID)
	if err != nil {
		t.Fatalf("BeginPrimaryStoreProgress: %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	reporter := &uploadProgressReporter{
		ctx:      canceledCtx,
		repos:    repos,
		uploadID: upload.ID,
		attempt:  upload.PrimaryStoreAttempt,
	}
	reporter.record(50, false)

	got, err := repos.Uploads.GetByID(context.Background(), upload.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.PrimaryBytesUploaded != 0 {
		t.Fatalf("primary_bytes_uploaded = %d, want 0 after canceled reporter context", got.PrimaryBytesUploaded)
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
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	upload, err = repos.Uploads.BeginPrimaryStoreProgress(ctx, upload.ID)
	if err != nil {
		t.Fatalf("BeginPrimaryStoreProgress: %v", err)
	}

	reporter := &uploadProgressReporter{
		ctx:           context.Background(),
		repos:         repos,
		uploadID:      upload.ID,
		attempt:       upload.PrimaryStoreAttempt,
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
		if got.PrimaryBytesUploaded == 70 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	got, err := repos.Uploads.GetByID(context.Background(), upload.ID)
	if err != nil {
		t.Fatalf("GetByID final: %v", err)
	}
	t.Fatalf("primary_bytes_uploaded = %d, want coalesced latest progress 70", got.PrimaryBytesUploaded)
}

type submittedCommitTestContext struct {
	serviceURL string
}

func (c submittedCommitTestContext) ProviderID() sdktypes.BigInt { return sdktypes.NewBigInt(0) }

func (c submittedCommitTestContext) DataSetID() *sdktypes.BigInt { return nil }

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
