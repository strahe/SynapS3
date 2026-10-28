package cacheaccess

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/strahe/synaps3/internal/cache"
)

func TestGateDeletionWaitExpiresWithoutBlockingLaterReaders(t *testing.T) {
	for _, tc := range []struct {
		name        string
		callerLimit time.Duration
		wantBusy    bool
		wantElapsed time.Duration
	}{
		{name: "deletion wait limit", wantBusy: true, wantElapsed: DeletionWaitLimit},
		{name: "caller deadline", callerLimit: time.Second, wantElapsed: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gate := NewGate()
				release, err := gate.HoldRead(t.Context(), "slow-reader")
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				ctx := t.Context()
				if tc.callerLimit > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.callerLimit)
					defer cancel()
				}
				started := time.Now()
				err = gate.GuardDeletion(ctx, "slow-reader", func() { t.Error("deletion ran while the body was open") })
				if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrDeletionBusy) != tc.wantBusy {
					t.Fatalf("deletion wait = %v, want busy = %v", err, tc.wantBusy)
				}
				if elapsed := time.Since(started); elapsed != tc.wantElapsed {
					t.Fatalf("waited %s, want %s", elapsed, tc.wantElapsed)
				}
				later, ok := gate.TryHoldRead("slow-reader")
				if !ok {
					t.Fatal("expired deletion still blocked a later reader")
				}
				later()
			})
		})
	}
}

func TestGateTryPermitsSkipBusyKeysAndRetireReferences(t *testing.T) {
	gate := NewGate()
	release, ok := gate.TryHoldDeletion("busy")
	if !ok {
		t.Fatal("exclusive permit unavailable")
	}
	if releaseRead, ok := gate.TryHoldRead("busy"); ok {
		releaseRead()
		t.Fatal("read bypassed exclusive permit")
	}
	release()
	assertGateRetired(t, gate, "busy")
	releaseUpload, err := gate.HoldUpload(t.Context(), "upload")
	if err != nil {
		t.Fatal(err)
	}
	if competing, ok := gate.TryHoldUpload("upload"); ok {
		competing()
		t.Fatal("upload gate was not exclusive")
	}
	releaseUpload()
	assertGateRetired(t, gate, "multipart:upload")
}

func TestGatePreservesSeekingOnlyForSeekableBodies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	if err := os.WriteFile(path, []byte("abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, seekable := range []bool{true, false} {
		t.Run(fmt.Sprint(seekable), func(t *testing.T) {
			gate := NewGate()
			opened, err := gate.Open(t.Context(), "key", func() (io.ReadCloser, *cache.ObjectInfo, error) {
				if seekable {
					file, err := os.Open(path)
					return file, &cache.ObjectInfo{Size: 6}, err
				}
				return io.NopCloser(bytes.NewBufferString("abcdef")), &cache.ObjectInfo{Size: 6}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			seeker, ok := opened.Body.(io.Seeker)
			if ok != seekable {
				t.Fatalf("seekable=%t, want %t", ok, seekable)
			}
			if ok {
				if _, err := seeker.Seek(4, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(opened.Body)
				if err != nil || string(data) != "ef" {
					t.Fatalf("seek read=%q, %v", data, err)
				}
			}
			if err := opened.Body.Close(); err != nil {
				t.Fatal(err)
			}
			assertGateRetired(t, gate, "key")
		})
	}
}

func TestGateWaitsForOpenedBodyBeforeDeletion(t *testing.T) {
	gate := NewGate()
	opened, err := gate.Open(
		t.Context(),
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
		if err := gate.GuardDeletion(t.Context(), "version-1", func() {
			close(deleteStarted)
		}); err != nil {
			t.Errorf("GuardDeletion: %v", err)
		}
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

func TestGateHoldReadProtectsMultipleOpensAsOneOperation(t *testing.T) {
	gate := NewGate()
	release, err := gate.HoldRead(t.Context(), "content-1")
	if err != nil {
		t.Fatalf("HoldRead: %v", err)
	}
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var canceledDeleteCalled atomic.Bool
	canceledDeleteDone := make(chan error, 1)
	go func() {
		canceledDeleteDone <- gate.GuardDeletion(ctx, "content-1", func() { canceledDeleteCalled.Store(true) })
	}()
	select {
	case err := <-canceledDeleteDone:
		t.Fatalf("deletion passed the shared read guard: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := waitForGateResult(t, canceledDeleteDone, "canceled deletion"); !errors.Is(err, context.Canceled) || errors.Is(err, ErrDeletionBusy) {
		t.Fatalf("canceled deletion error = %v, want context.Canceled", err)
	}
	if canceledDeleteCalled.Load() {
		t.Fatal("canceled deletion entered its callback")
	}
	deleteStarted := make(chan struct{})
	deleteDone := make(chan struct{})
	go func() {
		defer close(deleteDone)
		if err := gate.GuardDeletion(t.Context(), "content-1", func() { close(deleteStarted) }); err != nil {
			t.Errorf("GuardDeletion: %v", err)
		}
	}()
	release()
	release()
	waitForGateSignal(t, deleteStarted, time.Second, "deletion after shared read guard")
	waitForGateSignal(t, deleteDone, time.Second, "deletion completion")
	assertGateRetired(t, gate, "content-1")
}

func TestGateBlocksAccessWhileDeletionRuns(t *testing.T) {
	for _, operation := range []string{"open", "commit", "read"} {
		t.Run(operation, func(t *testing.T) {
			gate := NewGate()
			deleteStarted := make(chan struct{})
			releaseDelete := make(chan struct{})
			var releaseOnce sync.Once
			releaseDeletion := func() { releaseOnce.Do(func() { close(releaseDelete) }) }
			deleteDone := make(chan struct{})
			go func() {
				if err := gate.GuardDeletion(t.Context(), "version-2", func() {
					close(deleteStarted)
					<-releaseDelete
				}); err != nil {
					t.Errorf("GuardDeletion: %v", err)
				}
				close(deleteDone)
			}()
			t.Cleanup(func() {
				releaseDeletion()
				waitForGateSignal(t, deleteDone, time.Second, "deletion completion")
			})
			waitForGateSignal(t, deleteStarted, time.Second, "deletion start")

			var accessCalled atomic.Bool
			access := func(ctx context.Context) error {
				switch operation {
				case "open":
					opened, err := gate.Open(ctx, "version-2", func() (io.ReadCloser, *cache.ObjectInfo, error) {
						accessCalled.Store(true)
						return io.NopCloser(bytes.NewReader(nil)), &cache.ObjectInfo{}, nil
					})
					if err != nil {
						return err
					}
					return opened.Body.Close()
				case "commit":
					return gate.Commit(ctx, "version-2", func() error {
						accessCalled.Store(true)
						return nil
					})
				default:
					release, err := gate.HoldRead(ctx, "version-2")
					if err != nil {
						return err
					}
					accessCalled.Store(true)
					release()
					return nil
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			accessDone := make(chan error, 1)
			go func() { accessDone <- access(ctx) }()
			select {
			case err := <-accessDone:
				t.Fatalf("cache access passed the deletion gate: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			cancel()
			if err := waitForGateResult(t, accessDone, "canceled access"); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled access error = %v, want context.Canceled", err)
			}
			if accessCalled.Load() {
				t.Fatal("canceled cache access entered its callback")
			}
			releaseDeletion()
			waitForGateSignal(t, deleteDone, time.Second, "deletion completion")
			if err := access(t.Context()); err != nil || !accessCalled.Load() {
				t.Fatalf("cache access after deletion: called=%v err=%v", accessCalled.Load(), err)
			}
			assertGateRetired(t, gate, "version-2")
		})
	}
}

func TestGateSerializesCommitWithDeletion(t *testing.T) {
	gate := NewGate()
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	commitDone := make(chan struct{})
	go func() {
		if err := gate.Commit(t.Context(), "version-3", func() error {
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
	go func() {
		if err := gate.GuardDeletion(t.Context(), "version-3", func() { close(deleteStarted) }); err != nil {
			t.Errorf("GuardDeletion: %v", err)
		}
	}()
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
		t.Context(),
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

	assertGateRetired(t, gate, "version-idle")
}

func assertGateRetired(t *testing.T, gate *Gate, key string) {
	t.Helper()
	shard := gate.shard(key)
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

func waitForGateResult(t *testing.T, result <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}
