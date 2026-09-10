package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

type blockingProgressRepository struct {
	repository.StorageContentRepository
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (r *blockingProgressRepository) RecordIngressStoreProgress(context.Context, repository.RecordIngressStoreProgressInput) (*model.StorageCopy, error) {
	r.calls.Add(1)
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-r.release
	now := time.Now()
	return &model.StorageCopy{ContentSize: 10, IngressStoreAttempt: 1, ProgressUpdatedAt: &now}, nil
}

func TestUploadProgressCloseWaitsForInflightWrites(t *testing.T) {
	store := &blockingProgressRepository{entered: make(chan struct{}, 1), release: make(chan struct{})}
	reporter := &uploadProgressReporter{
		ctx: t.Context(), repos: &repository.Repositories{Contents: store}, logger: slog.Default(),
		contentID: 1, copyID: 2, generation: 3, taskID: 4, attempt: 1,
	}
	reporter.OnProgress(5)
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("progress write did not start")
	}
	closed := make(chan struct{})
	go func() {
		reporter.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned before the in-flight write completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(store.release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the in-flight write completed")
	}
	reporter.OnProgress(9)
	if store.calls.Load() != 1 {
		t.Fatalf("progress writes after Close = %d, want one total write", store.calls.Load())
	}
}
