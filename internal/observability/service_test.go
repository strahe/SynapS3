package observability

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestServiceMergesDetachedRefreshWithCancellableRefresh(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	checker := &fakeRefreshChecker{
		checkProviders: func(ctx context.Context, _ time.Time, _ []LocalDataSet) ([]ProviderState, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
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
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		_, firstErr = service.RefreshProvidersWithContext(firstCtx, ListOptions{Limit: 10})
	}()
	<-started
	go func() {
		defer wg.Done()
		_, secondErr = service.RefreshProviders(context.Background(), ListOptions{Limit: 10})
	}()

	waitForDetachedRefreshWaiters(t, &service.providerRefresh, 1)
	cancelFirst()
	close(release)
	wg.Wait()

	if !errors.Is(firstErr, context.Canceled) {
		t.Fatalf("RefreshProvidersWithContext error = %v, want context canceled", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("RefreshProviders error = %v", secondErr)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("provider refresh calls = %d, want 1", got)
	}
	if store.providerReplaces != 1 {
		t.Fatalf("provider replaces = %d, want 1", store.providerReplaces)
	}
}

func TestServiceDetachedRefreshKeepsSharedRefreshAliveAfterCancellableJoinCancels(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	checker := &fakeRefreshChecker{
		checkProviders: func(ctx context.Context, _ time.Time, _ []LocalDataSet) ([]ProviderState, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
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

	detachedErr := make(chan error, 1)
	go func() {
		_, err := service.RefreshProviders(context.Background(), ListOptions{Limit: 10})
		detachedErr <- err
	}()
	<-started

	cancellableCtx, cancelCancellable := context.WithCancel(context.Background())
	cancellableErr := make(chan error, 1)
	go func() {
		_, err := service.RefreshProvidersWithContext(cancellableCtx, ListOptions{Limit: 10})
		cancellableErr <- err
	}()
	waitForRefreshWaiters(t, &service.providerRefresh, 1, 1)
	cancelCancellable()

	if err := <-cancellableErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("RefreshProvidersWithContext error = %v, want context canceled", err)
	}
	select {
	case err := <-detachedErr:
		t.Fatalf("detached refresh returned before release: %v", err)
	default:
	}
	close(release)
	if err := <-detachedErr; err != nil {
		t.Fatalf("RefreshProviders error = %v", err)
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
	go func() {
		defer wg.Done()
		_, secondErr = service.RefreshDataSets(ctx, ListOptions{Limit: 10})
	}()
	waitForDetachedRefreshWaiters(t, &service.dataSetRefresh, 2)
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

func TestServiceRefreshSkipsAlreadyCancelledContext(t *testing.T) {
	var calls int32
	checker := &fakeRefreshChecker{
		checkProviders: func(context.Context, time.Time, []LocalDataSet) ([]ProviderState, error) {
			atomic.AddInt32(&calls, 1)
			return []ProviderState{{ProviderID: onChainID(t, "101"), Status: StatusAvailable}}, nil
		},
	}
	service := NewService(ServiceOptions{
		Checker:       checker,
		LocalDataSets: LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) { return nil, nil }),
		Store:         &fakeStateStore{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.RefreshProviders(ctx, ListOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RefreshProviders with cancelled context: %v, want context canceled", err)
	}
	if calls != 0 {
		t.Fatalf("provider refresh calls = %d, want 0", calls)
	}
}

func TestServiceRefreshProvidersWithContextRespectsCancellation(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	releaseRefresh := sync.OnceFunc(func() { close(release) })
	defer releaseRefresh()
	checker := &fakeRefreshChecker{
		checkProviders: func(ctx context.Context, _ time.Time, _ []LocalDataSet) ([]ProviderState, error) {
			close(started)
			<-ctx.Done()
			close(cancelled)
			<-release
			return nil, ctx.Err()
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
		_, err := service.RefreshProvidersWithContext(ctx, ListOptions{})
		errCh <- err
	}()
	<-started
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("refresh did not observe caller cancellation")
	}
	var earlyErr error
	returnedEarly := false
	select {
	case earlyErr = <-errCh:
		returnedEarly = true
	default:
	}
	releaseRefresh()
	if returnedEarly {
		t.Fatalf("RefreshProvidersWithContext returned before its refresh stopped: %v", earlyErr)
	}

	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("RefreshProvidersWithContext after cancellation: %v, want context canceled", err)
	}
}

func TestServiceRefreshAllRespectsCancellation(t *testing.T) {
	started := make(chan struct{})
	checker := &fakeRefreshChecker{
		checkProviders: func(ctx context.Context, _ time.Time, _ []LocalDataSet) ([]ProviderState, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
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
		errCh <- service.RefreshAll(ctx)
	}()
	<-started
	cancel()

	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("RefreshAll after cancellation: %v, want context canceled", err)
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

func TestServiceDataSetObservationSummaryWarnsForIncompleteLocalInventory(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	var countCalls int
	var listCalls int
	service := NewService(ServiceOptions{
		LocalDataSets: LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) {
			listCalls++
			return []LocalDataSet{{ID: 1}, {ID: 2}}, nil
		}),
		LocalDataSetCount: LocalDataSetCountSourceFunc(func(context.Context) (int, error) {
			countCalls++
			return 2, nil
		}),
		Store: &fakeStateStore{
			dataSets: []DataSetState{{LocalDataSetID: 1, Status: StatusAvailable, LastCheckedAt: now}},
		},
		RefreshInterval: time.Minute,
		Now:             func() time.Time { return now },
	})

	page, err := service.ListDataSetObservations(context.Background(), ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListDataSetObservations: %v", err)
	}
	if page.SummarySignal.Level != SignalWarning {
		t.Fatalf("data set summary signal = %s, want warning", page.SummarySignal.Level)
	}
	if page.Summary.Total != 2 || page.Summary.Available != 1 || page.Summary.Unknown != 1 {
		t.Fatalf("data set summary = %#v, want total=2 available=1 unknown=1", page.Summary)
	}
	if countCalls != 1 || listCalls != 0 {
		t.Fatalf("inventory calls = count:%d list:%d, want count only", countCalls, listCalls)
	}
}

func TestServiceDataSetObservationSummaryUsesScopedInventoryForFilteredLists(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	providerID := onChainID(t, "101")
	var listCalls int
	service := NewService(ServiceOptions{
		LocalDataSets: LocalDataSetSourceFunc(func(context.Context) ([]LocalDataSet, error) {
			listCalls++
			return []LocalDataSet{
				{ID: 1, BucketID: 1, ProviderID: providerID},
				{ID: 2, BucketID: 1, ProviderID: providerID},
				{ID: 3, BucketID: 2, ProviderID: onChainID(t, "102")},
			}, nil
		}),
		Store: &fakeStateStore{
			dataSets: []DataSetState{
				{LocalDataSetID: 1, BucketID: 1, ProviderID: providerID, Status: StatusAvailable, LastCheckedAt: now},
				{LocalDataSetID: 3, BucketID: 2, ProviderID: onChainID(t, "102"), Status: StatusAvailable, LastCheckedAt: now},
			},
		},
		RefreshInterval: time.Minute,
		Now:             func() time.Time { return now },
	})

	statusPage, err := service.ListDataSetObservations(context.Background(), ListOptions{Status: StatusAvailable})
	if err != nil {
		t.Fatalf("ListDataSetObservations(status): %v", err)
	}
	if statusPage.SummarySignal.Level != SignalOK {
		t.Fatalf("status summary signal = %s, want ok", statusPage.SummarySignal.Level)
	}
	if listCalls != 0 {
		t.Fatalf("status filter inventory calls = %d, want 0", listCalls)
	}

	for _, opts := range []ListOptions{{BucketID: 1}, {ProviderID: &providerID}} {
		page, err := service.ListDataSetObservations(context.Background(), opts)
		if err != nil {
			t.Fatalf("ListDataSetObservations(%+v): %v", opts, err)
		}
		if page.SummarySignal.Level != SignalWarning {
			t.Fatalf("data set summary signal for opts %+v = %s, want warning", opts, page.SummarySignal.Level)
		}
		if page.Summary.Total != 2 || page.Summary.Available != 1 || page.Summary.Unknown != 1 {
			t.Fatalf("data set summary for opts %+v = %#v, want total=2 available=1 unknown=1", opts, page.Summary)
		}
	}
	if listCalls != 2 {
		t.Fatalf("bucket/provider inventory calls = %d, want 2", listCalls)
	}
}

func TestServiceDataSetObservationsByLocalIDsBatchesLookups(t *testing.T) {
	const sqliteBindParameterLimit = 999
	if dataSetStateLookupBatchSize > sqliteBindParameterLimit {
		t.Fatalf("data set state lookup batch size = %d, want <= %d", dataSetStateLookupBatchSize, sqliteBindParameterLimit)
	}
	const ids = dataSetStateLookupBatchSize + 3
	store := &fakeStateStore{}
	store.dataSets = make([]DataSetState, 0, ids)
	for i := 1; i <= ids; i++ {
		store.dataSets = append(store.dataSets, DataSetState{
			LocalDataSetID: int64(i),
			Status:         StatusAvailable,
			LastCheckedAt:  time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC),
		})
	}
	service := NewService(ServiceOptions{Store: store})

	localIDs := make([]int64, 0, ids)
	for i := 1; i <= ids; i++ {
		localIDs = append(localIDs, int64(i))
	}
	got, err := service.DataSetObservationsByLocalIDs(context.Background(), localIDs)
	if err != nil {
		t.Fatalf("DataSetObservationsByLocalIDs: %v", err)
	}
	if len(got) != ids {
		t.Fatalf("observations len = %d, want %d", len(got), ids)
	}
	if len(store.dataSetStateIDBatches) != 2 {
		t.Fatalf("lookup batches = %#v, want 2 batches", store.dataSetStateIDBatches)
	}
	if len(store.dataSetStateIDBatches[0]) != dataSetStateLookupBatchSize || len(store.dataSetStateIDBatches[1]) != 3 {
		t.Fatalf("lookup batch sizes = %d/%d, want %d/3", len(store.dataSetStateIDBatches[0]), len(store.dataSetStateIDBatches[1]), dataSetStateLookupBatchSize)
	}
}

type fakeRefreshChecker struct {
	checkProviders func(context.Context, time.Time, []LocalDataSet) ([]ProviderState, error)
	checkDataSets  func(context.Context, time.Time, []LocalDataSet) ([]DataSetState, error)
}

func waitForDetachedRefreshWaiters(t *testing.T, group *refreshGroup, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		group.mu.Lock()
		got := 0
		if group.call != nil {
			got = group.call.detachedWaiters
		}
		group.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("detached refresh waiters = %d, want %d", got, want)
		}
		runtime.Gosched()
	}
}

func waitForRefreshWaiters(t *testing.T, group *refreshGroup, wantCancellable, wantDetached int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		group.mu.Lock()
		gotCancellable, gotDetached := 0, 0
		if group.call != nil {
			gotCancellable = group.call.cancellableWaiters
			gotDetached = group.call.detachedWaiters
		}
		group.mu.Unlock()
		if gotCancellable == wantCancellable && gotDetached == wantDetached {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("refresh waiters = cancellable:%d detached:%d, want cancellable:%d detached:%d", gotCancellable, gotDetached, wantCancellable, wantDetached)
		}
		runtime.Gosched()
	}
}

func (f *fakeRefreshChecker) CheckProviders(ctx context.Context, checkedAt time.Time, local []LocalDataSet) ([]ProviderState, error) {
	return f.checkProviders(ctx, checkedAt, local)
}

func (f *fakeRefreshChecker) CheckDataSets(ctx context.Context, checkedAt time.Time, local []LocalDataSet) ([]DataSetState, error) {
	return f.checkDataSets(ctx, checkedAt, local)
}

type fakeStateStore struct {
	providerReplaces      int
	dataSetReplaces       int
	providers             []ProviderState
	dataSets              []DataSetState
	dataSetStateIDBatches [][]int64
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
	dataSets := fakeDataSetStatesForOptions(f.dataSets, opts)
	summary := Summary{Total: len(dataSets)}
	var lastCheckedAt *time.Time
	for _, state := range dataSets {
		switch state.Status {
		case StatusAvailable:
			summary.Available++
		case StatusDegraded:
			summary.Degraded++
		case StatusUnavailable:
			summary.Unavailable++
		case StatusUnknown:
			summary.Unknown++
		}
		if !state.LastCheckedAt.IsZero() && (lastCheckedAt == nil || state.LastCheckedAt.After(*lastCheckedAt)) {
			checkedAt := state.LastCheckedAt
			lastCheckedAt = &checkedAt
		}
	}
	return DataSetStatePage{
		Items:         dataSets,
		Summary:       summary,
		LastCheckedAt: lastCheckedAt,
		Total:         len(dataSets),
		Limit:         opts.Limit,
		Offset:        opts.Offset,
	}, nil
}

func fakeDataSetStatesForOptions(states []DataSetState, opts ListOptions) []DataSetState {
	out := make([]DataSetState, 0, len(states))
	for _, state := range states {
		if opts.Status != "" && state.Status != opts.Status {
			continue
		}
		if opts.BucketID > 0 && state.BucketID != opts.BucketID {
			continue
		}
		if opts.ProviderID != nil && state.ProviderID.String() != opts.ProviderID.String() {
			continue
		}
		out = append(out, state)
	}
	return out
}

func (f *fakeStateStore) GetDataSetStatesByLocalIDs(_ context.Context, ids []int64) (map[int64]DataSetState, error) {
	f.dataSetStateIDBatches = append(f.dataSetStateIDBatches, append([]int64(nil), ids...))
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
