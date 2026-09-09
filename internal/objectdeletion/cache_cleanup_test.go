package objectdeletion_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectdeletion"
	"github.com/strahe/synaps3/internal/testutil"
)

type noopAccessStore struct{}

func (*noopAccessStore) RecordContentCacheAccess(context.Context, int64, time.Time) error {
	return nil
}

func (*noopAccessStore) RecordContentCacheCommit(context.Context, int64, time.Time) error {
	return nil
}

func newReleaseTracker() *cacheaccess.Tracker {
	return cacheaccess.NewTracker(0, &noopAccessStore{})
}

type presenceRecorder struct {
	mu         sync.Mutex
	cleared    []int64
	clearErr   error
	referenced bool
}

func (r *presenceRecorder) ClearContentCachePresence(_ context.Context, contentID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleared = append(r.cleared, contentID)
	return r.clearErr
}

func (r *presenceRecorder) ReleaseContentCacheIfUnreferenced(
	ctx context.Context,
	contentID int64,
	release func() error,
) (bool, error) {
	if r.referenced {
		return false, nil
	}
	if err := release(); err != nil {
		return false, err
	}
	if err := r.ClearContentCachePresence(ctx, contentID); err != nil {
		return false, err
	}
	return true, nil
}

func newReleaseCache(deleteErr error) (*testutil.MockCache, *[]string) {
	deleted := new([]string)
	return &testutil.MockCache{
		DeleteFunc: func(_ context.Context, _, key string) error {
			*deleted = append(*deleted, key)
			return deleteErr
		},
		GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, errors.New("unused")
		},
	}, deleted
}

func TestReleaseContentCacheDeletesTheContentKeyAndClearsPresence(t *testing.T) {
	mockCache, deleted := newReleaseCache(nil)
	recorder := &presenceRecorder{}
	gate := cacheaccess.NewGate()
	tracker := newReleaseTracker()

	outcome, err := objectdeletion.ReleaseContentCache(
		context.Background(), mockCache, gate, tracker, recorder, "bucket", 41,
	)
	if err != nil || outcome != objectdeletion.CacheReleaseReleased {
		t.Fatalf("ReleaseContentCache = %q, %v, want released", outcome, err)
	}
	if want := model.ContentCacheKey(41); len(*deleted) != 1 || (*deleted)[0] != want {
		t.Fatalf("deleted keys = %v, want [%s]", *deleted, want)
	}
	if len(recorder.cleared) != 1 || recorder.cleared[0] != 41 {
		t.Fatalf("cleared presence = %v, want [41]", recorder.cleared)
	}
}

func TestReleaseContentCacheReportsFailureWithoutClearingPresence(t *testing.T) {
	mockCache, _ := newReleaseCache(errors.New("disk is busy"))
	recorder := &presenceRecorder{}

	outcome, err := objectdeletion.ReleaseContentCache(
		context.Background(), mockCache, cacheaccess.NewGate(), newReleaseTracker(), recorder, "bucket", 41,
	)
	if err == nil || outcome != objectdeletion.CacheReleaseRetained {
		t.Fatalf("ReleaseContentCache = %q, %v, want retained with an error", outcome, err)
	}
	// Presence must survive a failed delete, or the next reader would be told
	// bytes are gone while the file is still there.
	if len(recorder.cleared) != 0 {
		t.Fatalf("cleared presence = %v, want none", recorder.cleared)
	}
}

func TestReleaseContentCacheRetainsReferencedContent(t *testing.T) {
	mockCache, deleted := newReleaseCache(nil)
	recorder := &presenceRecorder{referenced: true}

	outcome, err := objectdeletion.ReleaseContentCache(
		context.Background(), mockCache, cacheaccess.NewGate(), newReleaseTracker(), recorder, "bucket", 41,
	)
	if err != nil || outcome != objectdeletion.CacheReleaseRetained {
		t.Fatalf("ReleaseContentCache = %q, %v, want retained", outcome, err)
	}
	if len(*deleted) != 0 || len(recorder.cleared) != 0 {
		t.Fatalf("retained content deleted=%v cleared=%v", *deleted, recorder.cleared)
	}
}
