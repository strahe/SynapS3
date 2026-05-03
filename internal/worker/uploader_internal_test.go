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
	"github.com/strahe/synapse-go/types"
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

	dataSetID := "1001"
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

	dataSetID := "1001"
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

type submittedCommitTestContext struct {
	serviceURL string
}

func (c submittedCommitTestContext) ProviderID() types.ProviderID { return 0 }

func (c submittedCommitTestContext) DataSetID() *types.DataSetID { return nil }

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
