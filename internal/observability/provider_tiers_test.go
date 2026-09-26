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
}

func (s *tierTestStore) RecordApprovedProviders(_ context.Context, _ time.Time, ids []types.OnChainID) error {
	s.approvedCalls++
	s.approvedIDs = ids
	return nil
}

func (s *tierTestStore) RecordEndorsedProviders(_ context.Context, _ time.Time, ids []types.OnChainID) error {
	s.endorsedCalls++
	s.endorsedIDs = ids
	return nil
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
	if _, err := service.RefreshApprovedProviders(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RefreshEndorsedProviders(context.Background()); err == nil {
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
	if _, err := service.RefreshApprovedProviders(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RefreshEndorsedProviders(context.Background()); err != nil {
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
	go func() { _, err := service.RefreshEndorsedProviders(context.Background()); done <- err }()
	<-started
	approvedDone := make(chan error, 1)
	go func() { _, err := service.RefreshApprovedProviders(context.Background()); approvedDone <- err }()
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
	store := &tierTestStore{fakeStateStore: &fakeStateStore{}}
	source := &blockingApprovedSource{started: make(chan struct{}, 1), release: make(chan struct{})}
	checkedAt := time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC)
	service := NewService(ServiceOptions{
		Store: store, ApprovedProviders: source, EndorsedProviders: tierEndorsedSource{},
		Now: func() time.Time { return checkedAt }, RefreshInterval: time.Minute,
	})
	type result struct {
		at  time.Time
		err error
	}
	results := make(chan result, 2)
	go func() { at, err := service.RefreshApprovedProviders(context.Background()); results <- result{at, err} }()
	select {
	case <-source.started:
	case <-time.After(time.Second):
		t.Fatal("approved read did not start")
	}
	go func() { at, err := service.RefreshApprovedProviders(context.Background()); results <- result{at, err} }()
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
	if _, err := service.RefreshEndorsedProviders(context.Background()); err != nil {
		t.Fatalf("endorsed refresh blocked by approved read: %v", err)
	}
	close(source.release)
	for range 2 {
		got := <-results
		if got.err != nil || !got.at.Equal(checkedAt) {
			t.Fatalf("approved result = %#v, want shared time and success", got)
		}
	}
	if source.reads.Load() != 1 || store.approvedCalls != 1 || store.endorsedCalls != 1 {
		t.Fatalf("count reads=%d approved writes=%d endorsed writes=%d", source.reads.Load(), store.approvedCalls, store.endorsedCalls)
	}
}
