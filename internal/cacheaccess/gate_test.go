package cacheaccess

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
)

func TestGateWaitsForOpenedBodyBeforeDeletion(t *testing.T) {
	gate := NewGate()
	opened, err := gate.Open(
		"version-1",
		func() (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(bytes.NewReader(nil)), &cache.ObjectInfo{}, nil
		},
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	deleteStarted := make(chan struct{})
	deleteDone := make(chan struct{})
	go func() {
		gate.GuardDeletion("version-1", func() {
			close(deleteStarted)
		})
		close(deleteDone)
	}()

	select {
	case <-deleteStarted:
		t.Fatal("deletion entered while the cache body was open")
	case <-time.After(50 * time.Millisecond):
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitForGateSignal(t, deleteStarted, time.Second, "deletion start")
	waitForGateSignal(t, deleteDone, time.Second, "deletion completion")
}

func TestGateBlocksOpenWhileDeletionRuns(t *testing.T) {
	gate := NewGate()
	deleteStarted := make(chan struct{})
	releaseDelete := make(chan struct{})
	deleteDone := make(chan struct{})
	go func() {
		gate.GuardDeletion("version-2", func() {
			close(deleteStarted)
			<-releaseDelete
		})
		close(deleteDone)
	}()
	waitForGateSignal(t, deleteStarted, time.Second, "deletion start")

	var openCalled atomic.Bool
	openDone := make(chan struct{})
	go func() {
		opened, err := gate.Open(
			"version-2",
			func() (io.ReadCloser, *cache.ObjectInfo, error) {
				openCalled.Store(true)
				return nil, nil, errors.New("cache miss")
			},
		)
		if err == nil || opened != nil {
			t.Errorf("Open result = %#v, %v, want cache miss", opened, err)
		}
		close(openDone)
	}()

	select {
	case <-openDone:
		t.Fatal("cache open completed while deletion held the version gate")
	case <-time.After(50 * time.Millisecond):
	}
	if openCalled.Load() {
		t.Fatal("cache open callback ran while deletion held the version gate")
	}

	close(releaseDelete)
	waitForGateSignal(t, deleteDone, time.Second, "deletion completion")
	waitForGateSignal(t, openDone, time.Second, "cache open completion")
	if !openCalled.Load() {
		t.Fatal("cache open callback did not run after deletion")
	}
}

func TestGateSerializesCommitWithDeletion(t *testing.T) {
	gate := NewGate()
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	commitDone := make(chan struct{})
	go func() {
		if err := gate.Commit("version-3", func() error {
			close(commitStarted)
			<-releaseCommit
			return nil
		}); err != nil {
			t.Errorf("Commit: %v", err)
		}
		close(commitDone)
	}()
	waitForGateSignal(t, commitStarted, time.Second, "commit start")

	deleteStarted := make(chan struct{})
	go gate.GuardDeletion("version-3", func() {
		close(deleteStarted)
	})
	select {
	case <-deleteStarted:
		t.Fatal("deletion entered while cache commit was active")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCommit)
	waitForGateSignal(t, commitDone, time.Second, "commit completion")
	waitForGateSignal(t, deleteStarted, time.Second, "deletion start")
}

func TestGateRetiresIdleEntries(t *testing.T) {
	gate := NewGate()
	opened, err := gate.Open(
		"version-idle",
		func() (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(bytes.NewReader(nil)), &cache.ObjectInfo{}, nil
		},
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	shard := gate.shard("version-idle")
	shard.mu.Lock()
	entryCount := len(shard.entries)
	shard.mu.Unlock()
	if entryCount != 0 {
		t.Fatalf("idle gate entries = %d, want 0", entryCount)
	}
}

func waitForGateSignal(t *testing.T, signal <-chan struct{}, timeout time.Duration, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", name)
	}
}
