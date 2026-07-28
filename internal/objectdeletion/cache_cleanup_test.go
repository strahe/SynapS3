package objectdeletion_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectdeletion"
	"github.com/strahe/synaps3/internal/testutil"
)

type cleanupRecorder struct {
	mu     sync.Mutex
	status model.CacheCleanupStatus
}

func (r *cleanupRecorder) UpdateObjectDeletionCacheCleanup(
	_ context.Context,
	_ string,
	status model.CacheCleanupStatus,
	_ string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = status
	return nil
}

func TestRecordCacheCleanupWaitsForOpenResponseBody(t *testing.T) {
	deleted := make(chan struct{})
	mockCache := &testutil.MockCache{
		DeleteFunc: func(context.Context, string, string) error {
			close(deleted)
			return nil
		},
	}
	repos := repository.NewRepositories(testutil.NewTestDB(t))
	gate := cacheaccess.NewGate()
	tracker := cacheaccess.NewTracker(cacheaccess.DefaultPersistenceInterval, repos.Objects)
	opened, err := gate.Open(
		"version-1",
		func() (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(bytes.NewReader([]byte("cached"))), &cache.ObjectInfo{Size: 6}, nil
		},
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	recorder := new(cleanupRecorder)
	cleanupDone := make(chan model.CacheCleanupStatus, 1)
	go func() {
		cleanupDone <- objectdeletion.RecordCacheCleanup(
			context.Background(),
			mockCache,
			gate,
			tracker,
			recorder,
			slog.Default(),
			"bucket",
			"version-1",
			".versions/version-1",
		)
	}()

	select {
	case <-deleted:
		t.Fatal("permanent deletion removed a cache file while its response body was open")
	case <-time.After(20 * time.Millisecond):
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case status := <-cleanupDone:
		if status != model.CacheCleanupStatusDeleted {
			t.Fatalf("cleanup status = %s, want deleted", status)
		}
	case <-time.After(time.Second):
		t.Fatal("permanent deletion did not resume after the response body closed")
	}
}
