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
	"github.com/strahe/synaps3/internal/model"
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
