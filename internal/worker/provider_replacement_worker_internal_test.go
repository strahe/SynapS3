package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
)

type transientReplacementLeaseRepo struct {
	repository.StorageReplacementRepository
	calls   atomic.Int32
	renewed chan struct{}
}

func (r *transientReplacementLeaseRepo) RenewReplacementItemLease(
	context.Context,
	storagereplacement.ClaimToken,
	time.Duration,
) error {
	if r.calls.Add(1) == 1 {
		return errors.New("injected transient renewal failure")
	}
	if r.calls.Load() == 2 {
		close(r.renewed)
	}
	return nil
}

func TestReplacementItemLeaseRenewalRetriesTransientFailure(t *testing.T) {
	leaseTTL := 60 * time.Millisecond
	leaseUntil := time.Now().Add(time.Second)
	replacementRepo := &transientReplacementLeaseRepo{renewed: make(chan struct{})}
	w := &ProviderReplacementWorker{
		repos:    &repository.Repositories{Replacements: replacementRepo},
		leaseTTL: leaseTTL,
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	itemCtx, cancelItem := context.WithCancel(context.Background())
	leaseLost, stopRenewal := w.startLeaseRenewal(
		itemCtx,
		cancelItem,
		storagereplacement.ClaimToken{ItemID: 42, ClaimedAt: time.Now()},
		&leaseUntil,
	)
	defer stopRenewal()
	defer cancelItem()

	select {
	case <-replacementRepo.renewed:
	case <-time.After(time.Second):
		t.Fatal("lease renewal did not retry after a transient error")
	}
	if leaseLost() {
		t.Fatal("transient lease renewal error marked the claim as lost")
	}
	if itemCtx.Err() != nil {
		t.Fatalf("transient lease renewal error cancelled item context: %v", itemCtx.Err())
	}
}

type recordingReplacementPauser struct {
	replacementID        int64
	expectedStateVersion int64
	reason               storagereplacement.WaitReason
}

func (p *recordingReplacementPauser) PauseMigration(
	_ context.Context,
	replacementID, expectedStateVersion int64,
	reason storagereplacement.WaitReason,
) error {
	p.replacementID = replacementID
	p.expectedStateVersion = expectedStateVersion
	p.reason = reason
	return nil
}

func TestReplacementTargetPauseUsesExecutionSnapshotVersion(t *testing.T) {
	t.Parallel()

	pauser := new(recordingReplacementPauser)
	if err := pauseReplacementAtExecutionVersion(context.Background(), pauser, 42, 7); err != nil {
		t.Fatalf("pauseReplacementAtExecutionVersion: %v", err)
	}
	if pauser.replacementID != 42 || pauser.expectedStateVersion != 7 || pauser.reason != storagereplacement.WaitReasonTarget {
		t.Fatalf("pause request = %#v, want replacement 42 at execution version 7", pauser)
	}
}

func TestFinalizeReplacementItemKeepsClaimRetryableUntilUploadFinalizes(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected upload finalization failure")
	completed := false
	err := finalizeReplacementItem(
		func() error { return injected },
		func() error {
			completed = true
			return nil
		},
	)
	if !errors.Is(err, injected) {
		t.Fatalf("finalizeReplacementItem error = %v, want injected failure", err)
	}
	if completed {
		t.Fatal("claim completed even though upload finalization failed")
	}

	steps := make([]string, 0, 2)
	if err := finalizeReplacementItem(
		func() error {
			steps = append(steps, "finalize")
			return nil
		},
		func() error {
			steps = append(steps, "complete")
			return nil
		},
	); err != nil {
		t.Fatalf("finalizeReplacementItem success: %v", err)
	}
	if len(steps) != 2 || steps[0] != "finalize" || steps[1] != "complete" {
		t.Fatalf("settlement order = %v, want [finalize complete]", steps)
	}
}

func TestReplacementTargetContextRegistrySharesActiveContextAndRefreshesAfterRelease(t *testing.T) {
	t.Parallel()

	registry := newReplacementTargetContextRegistry()
	var creates atomic.Int32
	create := func() (synapse.DataSetTarget, error) {
		creates.Add(1)
		return nil, nil
	}

	first, err := registry.acquire(context.Background(), 42, create)
	if err != nil {
		t.Fatalf("acquire first handle: %v", err)
	}
	second, err := registry.acquire(context.Background(), 42, create)
	if err != nil {
		t.Fatalf("acquire second handle: %v", err)
	}
	first.release()
	second.release()

	if got := creates.Load(); got != 1 {
		t.Fatalf("context creates = %d, want 1", got)
	}
	if len(registry.entries) != 0 {
		t.Fatalf("registry entries after final release = %d, want 0", len(registry.entries))
	}

	third, err := registry.acquire(context.Background(), 42, create)
	if err != nil {
		t.Fatalf("acquire refreshed handle: %v", err)
	}
	third.release()
	if got := creates.Load(); got != 2 {
		t.Fatalf("context creates after refresh = %d, want 2", got)
	}
}

func TestReplacementTargetContextRegistrySerializesCommitsForSameTarget(t *testing.T) {
	t.Parallel()

	registry := newReplacementTargetContextRegistry()
	create := func() (synapse.DataSetTarget, error) { return nil, nil }
	first, err := registry.acquire(context.Background(), 42, create)
	if err != nil {
		t.Fatalf("acquire first handle: %v", err)
	}
	defer first.release()
	second, err := registry.acquire(context.Background(), 42, create)
	if err != nil {
		t.Fatalf("acquire second handle: %v", err)
	}
	defer second.release()

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.commit(context.Background(), func() error {
			close(firstEntered)
			<-releaseFirst
			return nil
		})
	}()
	<-firstEntered

	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- second.commit(context.Background(), func() error {
			close(secondEntered)
			return nil
		})
	}()
	select {
	case <-secondEntered:
		t.Fatal("second commit entered while the same target gate was held")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first commit: %v", err)
	}
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("second commit did not enter after the target gate was released")
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second commit: %v", err)
	}
}

func TestReplacementTargetContextRegistryAllowsDifferentTargetsToCommit(t *testing.T) {
	t.Parallel()

	registry := newReplacementTargetContextRegistry()
	create := func() (synapse.DataSetTarget, error) { return nil, nil }
	first, err := registry.acquire(context.Background(), 42, create)
	if err != nil {
		t.Fatalf("acquire first handle: %v", err)
	}
	defer first.release()
	second, err := registry.acquire(context.Background(), 43, create)
	if err != nil {
		t.Fatalf("acquire second handle: %v", err)
	}
	defer second.release()

	entered := make(chan int, 2)
	release := make(chan struct{})
	done := make(chan error, 2)
	run := func(id int, handle *replacementTargetContextHandle) {
		done <- handle.commit(context.Background(), func() error {
			entered <- id
			<-release
			return nil
		})
	}
	go run(42, first)
	go run(43, second)

	seen := make(map[int]bool, 2)
	for range 2 {
		select {
		case id := <-entered:
			seen[id] = true
		case <-time.After(time.Second):
			t.Fatal("different target commits did not enter concurrently")
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	if !seen[42] || !seen[43] {
		t.Fatalf("entered targets = %v, want both", seen)
	}
}
