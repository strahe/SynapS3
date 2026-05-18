package observability

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestServiceMergesConcurrentProviderRefreshes(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	checker := &fakeRefreshChecker{
		checkProviders: func(context.Context, time.Time, []LocalDataSet) ([]ProviderState, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				close(started)
				<-release
			}
			return []ProviderState{{ProviderID: onChainID(t, "101"), Status: StatusAvailable}}, nil
		},
	}
	store := &fakeStateStore{}
	service := NewService(ServiceOptions{
		Checker:         checker,
		LocalDataSets:   LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) { return nil, nil }),
		Store:           store,
		RefreshInterval: time.Minute,
	})

	var wg sync.WaitGroup
	wg.Add(2)
	var firstErr, secondErr error
	go func() {
		defer wg.Done()
		_, firstErr = service.RefreshProviders(ctx, ListOptions{Limit: 10})
	}()
	<-started
	secondObserved := make(chan struct{})
	secondCtx := &observedDoneContext{Context: ctx, observed: secondObserved}
	go func() {
		defer wg.Done()
		_, secondErr = service.RefreshProviders(secondCtx, ListOptions{Limit: 10})
	}()
	<-secondObserved
	close(release)
	wg.Wait()

	if firstErr != nil || secondErr != nil {
		t.Fatalf("RefreshProviders errors = %v, %v", firstErr, secondErr)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("provider refresh calls = %d, want 1", got)
	}
	if store.providerReplaces != 1 {
		t.Fatalf("provider replaces = %d, want 1", store.providerReplaces)
	}
}

func TestServiceMergesConcurrentDataSetRefreshes(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	checker := &fakeRefreshChecker{
		checkDataSets: func(context.Context, time.Time, []LocalDataSet) ([]DataSetState, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				close(started)
				<-release
			}
			return []DataSetState{{LocalDataSetID: 1, Status: StatusAvailable}}, nil
		},
	}
	store := &fakeStateStore{}
	service := NewService(ServiceOptions{
		Checker: checker,
		LocalDataSets: LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) {
			return []LocalDataSet{{ID: 1}}, nil
		}),
		Store:           store,
		RefreshInterval: time.Minute,
	})

	var wg sync.WaitGroup
	wg.Add(2)
	var firstErr, secondErr error
	go func() {
		defer wg.Done()
		_, firstErr = service.RefreshDataSets(ctx, ListOptions{Limit: 10})
	}()
	<-started
	secondObserved := make(chan struct{})
	secondCtx := &observedDoneContext{Context: ctx, observed: secondObserved}
	go func() {
		defer wg.Done()
		_, secondErr = service.RefreshDataSets(secondCtx, ListOptions{Limit: 10})
	}()
	<-secondObserved
	close(release)
	wg.Wait()

	if firstErr != nil || secondErr != nil {
		t.Fatalf("RefreshDataSets errors = %v, %v", firstErr, secondErr)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("data set refresh calls = %d, want 1", got)
	}
	if store.dataSetReplaces != 1 {
		t.Fatalf("data set replaces = %d, want 1", store.dataSetReplaces)
	}
}

func TestServiceRefreshIgnoresRequestCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	checker := &fakeRefreshChecker{
		checkProviders: func(ctx context.Context, _ time.Time, _ []LocalDataSet) ([]ProviderState, error) {
			close(started)
			<-release
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return []ProviderState{{ProviderID: onChainID(t, "101"), Status: StatusAvailable}}, nil
		},
	}
	service := NewService(ServiceOptions{
		Checker:       checker,
		LocalDataSets: LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) { return nil, nil }),
		Store:         &fakeStateStore{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := service.RefreshProviders(ctx, ListOptions{})
		errCh <- err
	}()
	<-started
	cancel()
	close(release)

	if err := <-errCh; err != nil {
		t.Fatalf("RefreshProviders after request cancellation: %v", err)
	}
}

func TestServiceRefreshCleansUpAfterPanic(t *testing.T) {
	var calls int32
	checker := &fakeRefreshChecker{
		checkProviders: func(context.Context, time.Time, []LocalDataSet) ([]ProviderState, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				panic("boom")
			}
			return []ProviderState{{ProviderID: onChainID(t, "101"), Status: StatusAvailable}}, nil
		},
	}
	store := &fakeStateStore{}
	service := NewService(ServiceOptions{
		Checker:       checker,
		LocalDataSets: LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) { return nil, nil }),
		Store:         store,
	})

	if _, err := service.RefreshProviders(context.Background(), ListOptions{}); err == nil {
		t.Fatal("RefreshProviders panic returned nil error")
	}
	if store.providerReplaces != 0 {
		t.Fatalf("provider replaces after panic = %d, want 0", store.providerReplaces)
	}
	if _, err := service.RefreshProviders(context.Background(), ListOptions{}); err != nil {
		t.Fatalf("RefreshProviders after panic cleanup: %v", err)
	}
	if store.providerReplaces != 1 {
		t.Fatalf("provider replaces after retry = %d, want 1", store.providerReplaces)
	}
}

func TestServiceRefreshAppliesRefreshTimeout(t *testing.T) {
	checker := &fakeRefreshChecker{
		checkProviders: func(ctx context.Context, _ time.Time, _ []LocalDataSet) ([]ProviderState, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	service := NewService(ServiceOptions{
		Checker:        checker,
		LocalDataSets:  LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) { return nil, nil }),
		Store:          &fakeStateStore{},
		RefreshTimeout: 5 * time.Millisecond,
	})

	if _, err := service.RefreshProviders(context.Background(), ListOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RefreshProviders error = %v, want context deadline exceeded", err)
	}
}

func TestServiceRefreshAllAttemptsDataSetsAfterProviderFailure(t *testing.T) {
	providerErr := errors.New("provider refresh failed")
	checker := &fakeRefreshChecker{
		checkProviders: func(context.Context, time.Time, []LocalDataSet) ([]ProviderState, error) {
			return nil, providerErr
		},
		checkDataSets: func(context.Context, time.Time, []LocalDataSet) ([]DataSetState, error) {
			return []DataSetState{{LocalDataSetID: 1, Status: StatusAvailable}}, nil
		},
	}
	store := &fakeStateStore{}
	service := NewService(ServiceOptions{
		Checker: checker,
		LocalDataSets: LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) {
			return []LocalDataSet{{ID: 1}}, nil
		}),
		Store: store,
	})

	err := service.RefreshAll(context.Background())
	if !errors.Is(err, providerErr) {
		t.Fatalf("RefreshAll error = %v, want provider refresh error", err)
	}
	if store.providerReplaces != 0 {
		t.Fatalf("provider replaces = %d, want 0", store.providerReplaces)
	}
	if store.dataSetReplaces != 1 {
		t.Fatalf("data set replaces = %d, want 1", store.dataSetReplaces)
	}
}

type observedDoneContext struct {
	context.Context
	observed chan<- struct{}
	once     sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.observed)
	})
	return c.Context.Done()
}

type fakeRefreshChecker struct {
	checkProviders func(context.Context, time.Time, []LocalDataSet) ([]ProviderState, error)
	checkDataSets  func(context.Context, time.Time, []LocalDataSet) ([]DataSetState, error)
}

func (f *fakeRefreshChecker) CheckProviders(ctx context.Context, checkedAt time.Time, local []LocalDataSet) ([]ProviderState, error) {
	return f.checkProviders(ctx, checkedAt, local)
}

func (f *fakeRefreshChecker) CheckDataSets(ctx context.Context, checkedAt time.Time, local []LocalDataSet) ([]DataSetState, error) {
	return f.checkDataSets(ctx, checkedAt, local)
}

type fakeStateStore struct {
	providerReplaces int
	dataSetReplaces  int
	providers        []ProviderState
	dataSets         []DataSetState
}

func (f *fakeStateStore) ReplaceProviderStates(_ context.Context, _ time.Time, states []ProviderState) error {
	f.providerReplaces++
	f.providers = states
	return nil
}

func (f *fakeStateStore) ListProviderStates(_ context.Context, opts ListOptions) (ProviderStatePage, error) {
	return ProviderStatePage{Items: f.providers, Summary: Summary{Total: len(f.providers)}, Total: len(f.providers), Limit: opts.Limit, Offset: opts.Offset}, nil
}

func (f *fakeStateStore) ReplaceDataSetStates(_ context.Context, _ time.Time, states []DataSetState) error {
	f.dataSetReplaces++
	f.dataSets = states
	return nil
}

func (f *fakeStateStore) ListDataSetStates(_ context.Context, opts ListOptions) (DataSetStatePage, error) {
	return DataSetStatePage{Items: f.dataSets, Summary: Summary{Total: len(f.dataSets)}, Total: len(f.dataSets), Limit: opts.Limit, Offset: opts.Offset}, nil
}

func (f *fakeStateStore) GetDataSetStatesByLocalIDs(_ context.Context, ids []int64) (map[int64]DataSetState, error) {
	out := make(map[int64]DataSetState)
	for _, state := range f.dataSets {
		for _, id := range ids {
			if state.LocalDataSetID == id {
				out[id] = state
			}
		}
	}
	return out, nil
}
