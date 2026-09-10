package cacheaccess

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

type recordingAccessStore struct {
	mu          sync.Mutex
	fail        bool
	accessCalls map[int64][]time.Time
	commitCalls map[int64][]time.Time
}

func newRecordingAccessStore() *recordingAccessStore {
	return &recordingAccessStore{
		accessCalls: make(map[int64][]time.Time),
		commitCalls: make(map[int64][]time.Time),
	}
}

func (s *recordingAccessStore) RecordContentCacheAccess(
	_ context.Context,
	contentID int64,
	accessedAt time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accessCalls[contentID] = append(s.accessCalls[contentID], accessedAt)
	if s.fail {
		return errors.New("database unavailable")
	}
	return nil
}

func (s *recordingAccessStore) RecordContentCacheCommit(
	_ context.Context,
	contentID int64,
	accessedAt time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitCalls[contentID] = append(s.commitCalls[contentID], accessedAt)
	if s.fail {
		return errors.New("database unavailable")
	}
	return nil
}

func (s *recordingAccessStore) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *recordingAccessStore) callCounts(contentID int64) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.accessCalls[contentID]), len(s.commitCalls[contentID])
}

func TestTrackerCoalescesAccessWritesAndKeepsLatestTimestamp(t *testing.T) {
	store := newRecordingAccessStore()
	tracker := NewTracker(time.Minute, store)
	now := time.Date(2026, time.July, 28, 1, 0, 0, 0, time.UTC)
	tracker.now = func() time.Time { return now }

	if err := tracker.RecordAccess(context.Background(), 1, nil); err != nil {
		t.Fatalf("first RecordAccess: %v", err)
	}
	now = now.Add(10 * time.Second)
	if err := tracker.RecordAccess(context.Background(), 1, nil); err != nil {
		t.Fatalf("second RecordAccess: %v", err)
	}
	accessCalls, commitCalls := store.callCounts(1)
	if accessCalls != 1 || commitCalls != 0 {
		t.Fatalf("store calls = access %d commit %d, want 1 and 0", accessCalls, commitCalls)
	}
	if got := tracker.Latest(1); !got.Equal(now) {
		t.Fatalf("Latest = %s, want %s", got, now)
	}

	now = now.Add(time.Minute)
	if err := tracker.RecordAccess(context.Background(), 1, nil); err != nil {
		t.Fatalf("third RecordAccess: %v", err)
	}
	accessCalls, _ = store.callCounts(1)
	if accessCalls != 2 {
		t.Fatalf("access writes = %d, want 2", accessCalls)
	}
}

func TestTrackerCommitFailureRetriesAsCommitDuringSweep(t *testing.T) {
	store := newRecordingAccessStore()
	store.setFail(true)
	tracker := NewTracker(time.Minute, store)
	gate := NewGate()
	now := time.Date(2026, time.July, 28, 2, 0, 0, 0, time.UTC)
	tracker.now = func() time.Time { return now }

	if err := tracker.RecordCommit(context.Background(), 2, nil); err == nil {
		t.Fatal("RecordCommit error = nil, want persistence failure")
	}
	store.setFail(false)
	now = now.Add(time.Minute)
	failed, err := tracker.sweep(context.Background(), gate)
	if err != nil || failed != 0 {
		t.Fatalf("sweep: failed=%d err=%v", failed, err)
	}
	accessCalls, commitCalls := store.callCounts(2)
	if accessCalls != 0 || commitCalls != 2 {
		t.Fatalf("store calls = access %d commit %d, want 0 and 2", accessCalls, commitCalls)
	}
}

func TestTrackerSweepDoesNotPersistForgottenCommitAfterDeletion(t *testing.T) {
	store := newRecordingAccessStore()
	store.setFail(true)
	tracker := NewTracker(0, store)
	gate := NewGate()
	if err := tracker.RecordCommit(context.Background(), 3, nil); err == nil {
		t.Fatal("RecordCommit error = nil, want persistence failure")
	}
	store.setFail(false)

	deleteStarted := make(chan struct{})
	releaseDelete := make(chan struct{})
	deleteDone := make(chan struct{})
	go func() {
		gate.GuardDeletion(model.ContentCacheKey(3), func() {
			close(deleteStarted)
			<-releaseDelete
			tracker.Forget(3)
		})
		close(deleteDone)
	}()
	waitForGateSignal(t, deleteStarted, time.Second, "deletion start")

	sweepDone := make(chan error, 1)
	go func() {
		_, err := tracker.sweep(context.Background(), gate)
		sweepDone <- err
	}()
	select {
	case err := <-sweepDone:
		t.Fatalf("sweep completed before deletion released the gate: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseDelete)
	waitForGateSignal(t, deleteDone, time.Second, "deletion completion")
	select {
	case err := <-sweepDone:
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sweep")
	}
	accessCalls, commitCalls := store.callCounts(3)
	if accessCalls != 0 || commitCalls != 1 {
		t.Fatalf("store calls = access %d commit %d, want only the initial failed commit", accessCalls, commitCalls)
	}
}

func TestTrackerStaysBoundedAndPausesLRUWhenDirtyAccessCannotPersist(t *testing.T) {
	store := newRecordingAccessStore()
	store.setFail(true)
	tracker := NewTracker(time.Minute, store)
	tracker.maxEntriesPerShard = 1
	now := time.Date(2026, time.July, 28, 3, 0, 0, 0, time.UTC)
	tracker.now = func() time.Time { return now }

	contentIDs := sameTrackerShardContentIDs(tracker, 3)
	if err := tracker.RecordAccess(context.Background(), contentIDs[0], nil); err == nil {
		t.Fatal("first RecordAccess error = nil, want persistence failure")
	}
	err := tracker.RecordAccess(context.Background(), contentIDs[1], nil)
	if !errors.Is(err, ErrLRUAccessUncertain) {
		t.Fatalf("overflow error = %v, want ErrLRUAccessUncertain", err)
	}
	if tracker.SafeForLRU() {
		t.Fatal("SafeForLRU = true after an access could not be retained")
	}

	shard := tracker.shard(contentIDs[0])
	shard.mu.Lock()
	entryCount := len(shard.entries)
	shard.mu.Unlock()
	if entryCount != 1 {
		t.Fatalf("tracked entries = %d, want hard cap 1", entryCount)
	}

	_ = tracker.RecordAccess(context.Background(), contentIDs[2], nil)
	shard.mu.Lock()
	entryCount = len(shard.entries)
	shard.mu.Unlock()
	if entryCount != 1 {
		t.Fatalf("tracked entries after another failure = %d, want hard cap 1", entryCount)
	}
}

func TestTrackerEvictsCleanEntryAtShardCapacity(t *testing.T) {
	store := newRecordingAccessStore()
	tracker := NewTracker(time.Minute, store)
	tracker.maxEntriesPerShard = 1
	now := time.Date(2026, time.July, 28, 4, 0, 0, 0, time.UTC)
	tracker.now = func() time.Time { return now }
	contentIDs := sameTrackerShardContentIDs(tracker, 2)

	if err := tracker.RecordAccess(context.Background(), contentIDs[0], nil); err != nil {
		t.Fatalf("first RecordAccess: %v", err)
	}
	now = now.Add(time.Second)
	if err := tracker.RecordAccess(context.Background(), contentIDs[1], nil); err != nil {
		t.Fatalf("second RecordAccess: %v", err)
	}

	if got := tracker.Latest(contentIDs[0]); !got.IsZero() {
		t.Fatalf("evicted clean entry access = %s, want zero", got)
	}
	if got := tracker.Latest(contentIDs[1]); !got.Equal(now) {
		t.Fatalf("retained entry access = %s, want %s", got, now)
	}
	if !tracker.SafeForLRU() {
		t.Fatal("clean capacity eviction must not pause LRU")
	}
}

func TestTrackerForgetRemovesVersionState(t *testing.T) {
	store := newRecordingAccessStore()
	tracker := NewTracker(time.Minute, store)
	if err := tracker.RecordAccess(context.Background(), 4, nil); err != nil {
		t.Fatalf("RecordAccess: %v", err)
	}
	tracker.Forget(4)
	if got := tracker.Latest(4); !got.IsZero() {
		t.Fatalf("Latest after Forget = %s, want zero", got)
	}
}

func TestTrackerAdvancesPastEqualDurableTimestamp(t *testing.T) {
	store := newRecordingAccessStore()
	tracker := NewTracker(time.Minute, store)
	durable := time.Date(2026, time.July, 28, 5, 0, 0, 123456000, time.UTC)
	tracker.now = func() time.Time { return durable }
	if err := tracker.RecordAccess(context.Background(), 5, &durable); err != nil {
		t.Fatalf("RecordAccess: %v", err)
	}
	want := durable.Add(time.Microsecond)
	if got := tracker.Latest(5); !got.Equal(want) {
		t.Fatalf("Latest = %s, want %s", got, want)
	}
}

func sameTrackerShardContentIDs(tracker *Tracker, count int) []int64 {
	const first int64 = 1000
	target := tracker.shard(first)
	contentIDs := []int64{first}
	for candidate := first + 1; len(contentIDs) < count; candidate++ {
		if tracker.shard(candidate) == target {
			contentIDs = append(contentIDs, candidate)
		}
	}
	return contentIDs
}
