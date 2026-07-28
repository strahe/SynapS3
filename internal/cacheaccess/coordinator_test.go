package cacheaccess

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
)

func TestCoordinatorCoalescesAccessPersistenceAndRetainsLatestAccess(t *testing.T) {
	start := time.Date(2026, time.July, 28, 0, 0, 0, 0, time.UTC)
	now := start
	coordinator := NewCoordinator(time.Minute)
	coordinator.now = func() time.Time { return now }
	var writes atomic.Int64
	persist := func(context.Context, string, time.Time) error {
		writes.Add(1)
		return nil
	}
	open := func() (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(bytes.NewReader([]byte("cached"))), &cache.ObjectInfo{Size: 6}, nil
	}

	first, err := coordinator.Open(context.Background(), "version-1", nil, open, persist)
	if err != nil {
		t.Fatalf("Open(first): %v", err)
	}
	_ = first.Body.Close()
	now = now.Add(30 * time.Second)
	second, err := coordinator.Open(context.Background(), "version-1", nil, open, persist)
	if err != nil {
		t.Fatalf("Open(second): %v", err)
	}
	_ = second.Body.Close()
	if writes.Load() != 1 {
		t.Fatalf("persistence writes within interval = %d, want 1", writes.Load())
	}

	var latest time.Time
	coordinator.GuardDeletion("version-1", func(lastAccess time.Time) bool {
		latest = lastAccess
		return false
	})
	if !latest.Equal(now) {
		t.Fatalf("latest in-memory access = %v, want %v", latest, now)
	}

	now = now.Add(31 * time.Second)
	third, err := coordinator.Open(context.Background(), "version-1", nil, open, persist)
	if err != nil {
		t.Fatalf("Open(third): %v", err)
	}
	_ = third.Body.Close()
	if writes.Load() != 2 {
		t.Fatalf("persistence writes after interval = %d, want 2", writes.Load())
	}
}

func TestCoordinatorAdvancesAccessBeyondDurableSnapshotWhenClockIsEqual(t *testing.T) {
	snapshot := time.Date(2026, time.July, 28, 0, 0, 0, 0, time.UTC)
	coordinator := NewCoordinator(time.Minute)
	coordinator.now = func() time.Time { return snapshot }
	open := func() (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(bytes.NewReader([]byte("cached"))), &cache.ObjectInfo{Size: 6}, nil
	}

	opened, err := coordinator.Open(
		context.Background(),
		"version-equal-clock",
		&snapshot,
		open,
		func(context.Context, string, time.Time) error { return nil },
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = opened.Body.Close()

	var latest time.Time
	coordinator.GuardDeletion("version-equal-clock", func(lastAccess time.Time) bool {
		latest = lastAccess
		return false
	})
	if !latest.After(snapshot) {
		t.Fatalf("latest in-memory access = %v, want after durable snapshot %v", latest, snapshot)
	}
}

func TestCoordinatorRetriesFailedPersistenceWithoutFailingCacheOpen(t *testing.T) {
	start := time.Date(2026, time.July, 28, 0, 0, 0, 0, time.UTC)
	now := start
	coordinator := NewCoordinator(time.Minute)
	coordinator.now = func() time.Time { return now }
	var writes atomic.Int64
	persist := func(context.Context, string, time.Time) error {
		if writes.Add(1) == 1 {
			return errors.New("database unavailable")
		}
		return nil
	}
	open := func() (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(bytes.NewReader([]byte("cached"))), &cache.ObjectInfo{Size: 6}, nil
	}

	first, err := coordinator.Open(context.Background(), "version-2", nil, open, persist)
	if err != nil {
		t.Fatalf("Open(first): %v", err)
	}
	_ = first.Body.Close()
	if first.PersistError == nil {
		t.Fatal("first PersistError = nil, want persistence failure")
	}
	now = now.Add(30 * time.Second)
	second, err := coordinator.Open(context.Background(), "version-2", nil, open, persist)
	if err != nil {
		t.Fatalf("Open(second): %v", err)
	}
	_ = second.Body.Close()
	if second.PersistError != nil {
		t.Fatalf("second PersistError = %v, want throttled attempt", second.PersistError)
	}
	if writes.Load() != 1 {
		t.Fatalf("persistence writes within interval = %d, want 1", writes.Load())
	}
	now = now.Add(31 * time.Second)
	third, err := coordinator.Open(context.Background(), "version-2", nil, open, persist)
	if err != nil {
		t.Fatalf("Open(third): %v", err)
	}
	_ = third.Body.Close()
	if third.PersistError != nil {
		t.Fatalf("third PersistError = %v, want retry success", third.PersistError)
	}
	if writes.Load() != 2 {
		t.Fatalf("persistence writes = %d, want retry after interval", writes.Load())
	}
}

func TestCoordinatorSerializesCacheOpenWithEviction(t *testing.T) {
	coordinator := NewCoordinator(time.Minute)
	evictionEntered := make(chan struct{})
	releaseEviction := make(chan struct{})
	evictionDone := make(chan struct{})
	go func() {
		coordinator.GuardDeletion("version-3", func(time.Time) bool {
			close(evictionEntered)
			<-releaseEviction
			return true
		})
		close(evictionDone)
	}()
	<-evictionEntered

	openCalled := make(chan struct{})
	openDone := make(chan struct{})
	go func() {
		opened, err := coordinator.Open(
			context.Background(),
			"version-3",
			nil,
			func() (io.ReadCloser, *cache.ObjectInfo, error) {
				close(openCalled)
				return io.NopCloser(bytes.NewReader(nil)), &cache.ObjectInfo{}, nil
			},
			func(context.Context, string, time.Time) error { return nil },
		)
		if err == nil {
			_ = opened.Body.Close()
		}
		close(openDone)
	}()

	select {
	case <-openCalled:
		t.Fatal("cache open ran while eviction guard was held")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseEviction)
	<-evictionDone
	<-openDone
}

func TestCoordinatorDoesNotSerializeDifferentVersionsInSameShard(t *testing.T) {
	coordinator := NewCoordinator(time.Minute)
	firstVersionID := "version-shard-collision"
	secondVersionID := collidingVersionID(coordinator, firstVersionID)
	firstOpenEntered := make(chan struct{})
	releaseFirstOpen := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		opened, err := coordinator.Open(
			context.Background(),
			firstVersionID,
			nil,
			func() (io.ReadCloser, *cache.ObjectInfo, error) {
				close(firstOpenEntered)
				<-releaseFirstOpen
				return io.NopCloser(bytes.NewReader(nil)), &cache.ObjectInfo{}, nil
			},
			nil,
		)
		if err == nil {
			err = opened.Body.Close()
		}
		firstDone <- err
	}()
	<-firstOpenEntered

	secondDone := make(chan error, 1)
	go func() {
		opened, err := coordinator.Open(
			context.Background(),
			secondVersionID,
			nil,
			func() (io.ReadCloser, *cache.ObjectInfo, error) {
				return io.NopCloser(bytes.NewReader(nil)), &cache.ObjectInfo{}, nil
			},
			nil,
		)
		if err == nil {
			err = opened.Body.Close()
		}
		secondDone <- err
	}()

	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("Open(second version): %v", err)
		}
	case <-time.After(time.Second):
		close(releaseFirstOpen)
		<-firstDone
		t.Fatal("different version sharing a shard was blocked by cache I/O")
	}
	close(releaseFirstOpen)
	if err := <-firstDone; err != nil {
		t.Fatalf("Open(first version): %v", err)
	}
}

func TestCoordinatorWaitsForAccessPersistenceBeforeDeletion(t *testing.T) {
	coordinator := NewCoordinator(time.Minute)
	persistEntered := make(chan struct{})
	releasePersist := make(chan struct{})
	openDone := make(chan error, 1)
	go func() {
		opened, err := coordinator.Open(
			context.Background(),
			"version-persisting",
			nil,
			func() (io.ReadCloser, *cache.ObjectInfo, error) {
				return io.NopCloser(bytes.NewReader(nil)), &cache.ObjectInfo{}, nil
			},
			func(context.Context, string, time.Time) error {
				close(persistEntered)
				<-releasePersist
				return nil
			},
		)
		if err == nil {
			err = opened.Body.Close()
		}
		openDone <- err
	}()
	<-persistEntered

	deletionEntered := make(chan struct{})
	deletionDone := make(chan struct{})
	go func() {
		coordinator.GuardDeletion("version-persisting", func(time.Time) bool {
			close(deletionEntered)
			return false
		})
		close(deletionDone)
	}()

	select {
	case <-deletionEntered:
		t.Fatal("deletion started before access persistence completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releasePersist)
	if err := <-openDone; err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-deletionEntered
	<-deletionDone
}

func TestCoordinatorDoesNotRetainFailedCacheOpen(t *testing.T) {
	coordinator := NewCoordinator(time.Minute)
	versionID := "version-cache-miss"
	wantErr := errors.New("cache miss")

	_, err := coordinator.Open(
		context.Background(),
		versionID,
		nil,
		func() (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, wantErr
		},
		nil,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Open error = %v, want %v", err, wantErr)
	}

	shard := coordinator.shard(versionID)
	shard.mu.Lock()
	_, retained := shard.entries[versionID]
	shard.mu.Unlock()
	if retained {
		t.Fatal("failed cache open retained an empty coordination entry")
	}
}

func collidingVersionID(coordinator *Coordinator, versionID string) string {
	target := coordinator.shard(versionID)
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("version-shard-collision-%d", i)
		if candidate != versionID && coordinator.shard(candidate) == target {
			return candidate
		}
	}
}
