package observability

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/types"
	sdktypes "github.com/strahe/synapse-go/types"
)

type tierTestStore struct {
	*fakeStateStore
	approvedCalls int
	endorsedCalls int
	approvedIDs   []types.OnChainID
	endorsedIDs   []types.OnChainID
	// storedAt is the collection time a newer snapshot already holds; zero
	// means each write is kept at the time it was given.
	storedAt time.Time
}

func (s *tierTestStore) RecordApprovedProviders(_ context.Context, startedAt time.Time, ids []types.OnChainID) (time.Time, error) {
	s.approvedCalls++
	s.approvedIDs = ids
	return s.stored(startedAt), nil
}

func (s *tierTestStore) RecordEndorsedProviders(_ context.Context, startedAt time.Time, ids []types.OnChainID) (time.Time, error) {
	s.endorsedCalls++
	s.endorsedIDs = ids
	return s.stored(startedAt), nil
}

func (s *tierTestStore) stored(startedAt time.Time) time.Time {
	if s.storedAt.IsZero() {
		return startedAt
	}
	return s.storedAt
}

type tierApprovedSource struct{ ids []sdktypes.BigInt }

type blockingApprovedSource struct {
	started chan struct{}
	release chan struct{}
	reads   atomic.Int32
}

func (s *blockingApprovedSource) ReadApprovedProviderIDs(ctx context.Context) ([]types.OnChainID, error) {
	s.reads.Add(1)
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return []types.OnChainID{types.NewOnChainID(101)}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s tierApprovedSource) ReadApprovedProviderIDs(context.Context) ([]types.OnChainID, error) {
	ids := make([]types.OnChainID, 0, len(s.ids))
	for _, id := range s.ids {
		ids = append(ids, types.OnChainIDFromSDK(id))
	}
	return ids, nil
}

type tierEndorsedSource struct {
	ids []sdktypes.BigInt
	err error
}

type blockingEndorsements struct {
	started chan struct{}
	release chan struct{}
}

func (s blockingEndorsements) GetEndorsedProviderIDs(ctx context.Context) ([]sdktypes.BigInt, error) {
	close(s.started)
	select {
	case <-s.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s tierEndorsedSource) GetEndorsedProviderIDs(context.Context) ([]sdktypes.BigInt, error) {
	return s.ids, s.err
}

func TestProviderTierRefreshesAreIndependent(t *testing.T) {
	store := &tierTestStore{fakeStateStore: &fakeStateStore{}}
	service := NewService(ServiceOptions{
		Store: store, ApprovedProviders: tierApprovedSource{},
		EndorsedProviders: tierEndorsedSource{err: errors.New("endorsement RPC unavailable")},
		RefreshInterval:   time.Minute,
	})
	if !service.ProviderTiersAvailable() {
		t.Fatal("tier sources should be available")
	}
	if _, _, err := service.RefreshApprovedProviders(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.RefreshEndorsedProviders(context.Background()); err == nil {
		t.Fatal("expected endorsed RPC error")
	}
	if store.approvedCalls != 1 || store.endorsedCalls != 0 || len(store.approvedIDs) != 0 {
		t.Fatalf("calls approved=%d endorsed=%d ids=%v", store.approvedCalls, store.endorsedCalls, store.approvedIDs)
	}
}

func TestProviderTierRefreshAllowsEndorsedWithoutApproved(t *testing.T) {
	store := &tierTestStore{fakeStateStore: &fakeStateStore{}}
	service := NewService(ServiceOptions{
		Store: store, ApprovedProviders: tierApprovedSource{},
		EndorsedProviders: tierEndorsedSource{ids: []sdktypes.BigInt{sdktypes.NewBigInt(202)}},
		RefreshInterval:   time.Minute,
	})
	if _, _, err := service.RefreshApprovedProviders(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.RefreshEndorsedProviders(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.approvedIDs) != 0 || len(store.endorsedIDs) != 1 || store.endorsedIDs[0].String() != "202" {
		t.Fatalf("approved=%v endorsed=%v", store.approvedIDs, store.endorsedIDs)
	}
}

func TestApprovedRefreshDoesNotWaitForEndorsedRead(t *testing.T) {
	store := &tierTestStore{fakeStateStore: &fakeStateStore{}}
	started, release := make(chan struct{}), make(chan struct{})
	service := NewService(ServiceOptions{
		Store: store, ApprovedProviders: tierApprovedSource{},
		EndorsedProviders: blockingEndorsements{started: started, release: release},
		RefreshInterval:   time.Minute,
	})
	done := make(chan error, 1)
	go func() { _, _, err := service.RefreshEndorsedProviders(context.Background()); done <- err }()
	<-started
	approvedDone := make(chan error, 1)
	go func() { _, _, err := service.RefreshApprovedProviders(context.Background()); approvedDone <- err }()
	select {
	case err := <-approvedDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("approved refresh waited for endorsed read")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentApprovedRefreshSharesOneReadAndCollectionTime(t *testing.T) {
	checkedAt := time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC)
	newerSnapshot := checkedAt.Add(time.Minute)
	store := &tierTestStore{fakeStateStore: &fakeStateStore{}, storedAt: newerSnapshot}
	source := &blockingApprovedSource{started: make(chan struct{}, 1), release: make(chan struct{})}
	service := NewService(ServiceOptions{
		Store: store, ApprovedProviders: source, EndorsedProviders: tierEndorsedSource{},
		Now: func() time.Time { return checkedAt }, RefreshInterval: time.Minute,
	})
	type result struct {
		attemptedAt time.Time
		checkedAt   time.Time
		err         error
	}
	results := make(chan result, 2)
	refresh := func() {
		attemptedAt, stored, err := service.RefreshApprovedProviders(context.Background())
		results <- result{attemptedAt, stored, err}
	}
	go refresh()
	select {
	case <-source.started:
	case <-time.After(time.Second):
		t.Fatal("approved read did not start")
	}
	go refresh()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		service.approvedRefresh.mu.Lock()
		waiters := service.approvedRefresh.call.cancellableWaiters
		service.approvedRefresh.mu.Unlock()
		if waiters == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	service.approvedRefresh.mu.Lock()
	waiters := service.approvedRefresh.call.cancellableWaiters
	service.approvedRefresh.mu.Unlock()
	if waiters != 2 {
		t.Fatalf("approved waiters = %d, want both requests sharing read", waiters)
	}
	if _, _, err := service.RefreshEndorsedProviders(context.Background()); err != nil {
		t.Fatalf("endorsed refresh blocked by approved read: %v", err)
	}
	close(source.release)
	for range 2 {
		got := <-results
		// Both callers share the read's start and the time of the snapshot the
		// store kept, which a newer read already held.
		if got.err != nil || !got.attemptedAt.Equal(checkedAt) || !got.checkedAt.Equal(newerSnapshot) {
			t.Fatalf("approved result = %#v, want shared attempt and stored time", got)
		}
	}
	if source.reads.Load() != 1 || store.approvedCalls != 1 || store.endorsedCalls != 1 {
		t.Fatalf("count reads=%d approved writes=%d endorsed writes=%d", source.reads.Load(), store.approvedCalls, store.endorsedCalls)
	}
}
