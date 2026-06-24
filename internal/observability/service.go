package observability

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const dataSetStateLookupBatchSize = 900

type RefreshChecker interface {
	CheckProviders(context.Context, time.Time, []LocalDataSet) ([]ProviderState, error)
	CheckDataSets(context.Context, time.Time, []LocalDataSet) ([]DataSetState, error)
}

type LocalDataSetSource interface {
	ListLocalDataSets(context.Context) ([]LocalDataSet, error)
}

type LocalDataSetSourceFunc func(context.Context) ([]LocalDataSet, error)

func (f LocalDataSetSourceFunc) ListLocalDataSets(ctx context.Context) ([]LocalDataSet, error) {
	return f(ctx)
}

type LocalDataSetCountSource interface {
	CountLocalDataSets(context.Context) (int, error)
}

type LocalDataSetCountSourceFunc func(context.Context) (int, error)

func (f LocalDataSetCountSourceFunc) CountLocalDataSets(ctx context.Context) (int, error) {
	return f(ctx)
}

type StateStore interface {
	ReplaceProviderStates(context.Context, time.Time, []ProviderState) error
	ListProviderStates(context.Context, ListOptions) (ProviderStatePage, error)
	ReplaceDataSetStates(context.Context, time.Time, []DataSetState) error
	ListDataSetStates(context.Context, ListOptions) (DataSetStatePage, error)
	GetDataSetStatesByLocalIDs(context.Context, []int64) (map[int64]DataSetState, error)
}

type ServiceOptions struct {
	Checker           RefreshChecker
	LocalDataSets     LocalDataSetSource
	LocalDataSetCount LocalDataSetCountSource
	Store             StateStore
	RefreshInterval   time.Duration
	RefreshTimeout    time.Duration
	Now               func() time.Time
}

type Service struct {
	checker           RefreshChecker
	localDataSets     LocalDataSetSource
	localDataSetCount LocalDataSetCountSource
	store             StateStore
	refreshInterval   time.Duration
	refreshTimeout    time.Duration
	now               func() time.Time
	providerRefresh   refreshGroup
	dataSetRefresh    refreshGroup
}

func NewService(opts ServiceOptions) *Service {
	interval := opts.RefreshInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	timeout := opts.RefreshTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{
		checker:           opts.Checker,
		localDataSets:     opts.LocalDataSets,
		localDataSetCount: opts.LocalDataSetCount,
		store:             opts.Store,
		refreshInterval:   interval,
		refreshTimeout:    timeout,
		now:               now,
	}
}

func (s *Service) RefreshInterval() time.Duration {
	if s == nil || s.refreshInterval <= 0 {
		return 5 * time.Minute
	}
	return s.refreshInterval
}

func (s *Service) RefreshTimeout() time.Duration {
	if s == nil || s.refreshTimeout <= 0 {
		return 10 * time.Minute
	}
	return s.refreshTimeout
}

func (s *Service) RefreshProviders(ctx context.Context, opts ListOptions) (ProviderStatePage, error) {
	return s.refreshProviders(ctx, opts, true)
}

// RefreshProvidersWithContext refreshes provider state using the caller context.
func (s *Service) RefreshProvidersWithContext(ctx context.Context, opts ListOptions) (ProviderStatePage, error) {
	return s.refreshProviders(ctx, opts, false)
}

func (s *Service) refreshProviders(ctx context.Context, opts ListOptions, detach bool) (ProviderStatePage, error) {
	if err := s.providerRefresh.Do(ctx, s.refreshTimeout, detach, func(refreshCtx context.Context) error {
		local, err := s.listLocalDataSets(refreshCtx)
		if err != nil {
			return err
		}
		checkedAt := s.checkedAt()
		states, err := s.checker.CheckProviders(refreshCtx, checkedAt, local)
		if err != nil {
			return err
		}
		return s.store.ReplaceProviderStates(refreshCtx, checkedAt, states)
	}); err != nil {
		return ProviderStatePage{}, err
	}
	return s.ListProviders(ctx, opts)
}

func (s *Service) RefreshDataSets(ctx context.Context, opts ListOptions) (DataSetStatePage, error) {
	return s.refreshDataSets(ctx, opts, true)
}

func (s *Service) refreshDataSets(ctx context.Context, opts ListOptions, detach bool) (DataSetStatePage, error) {
	if err := s.dataSetRefresh.Do(ctx, s.refreshTimeout, detach, func(refreshCtx context.Context) error {
		local, err := s.listLocalDataSets(refreshCtx)
		if err != nil {
			return err
		}
		checkedAt := s.checkedAt()
		states, err := s.checker.CheckDataSets(refreshCtx, checkedAt, local)
		if err != nil {
			return err
		}
		return s.store.ReplaceDataSetStates(refreshCtx, checkedAt, states)
	}); err != nil {
		return DataSetStatePage{}, err
	}
	return s.ListDataSets(ctx, opts)
}

func (s *Service) RefreshAll(ctx context.Context) error {
	var errs []error
	if _, err := s.refreshProviders(ctx, ListOptions{}, false); err != nil {
		errs = append(errs, err)
	}
	if _, err := s.refreshDataSets(ctx, ListOptions{}, false); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *Service) ListProviders(ctx context.Context, opts ListOptions) (ProviderStatePage, error) {
	return s.store.ListProviderStates(ctx, opts)
}

func (s *Service) ListProviderObservations(ctx context.Context, opts ListOptions) (ProviderObservationPage, error) {
	page, err := s.ListProviders(ctx, opts)
	if err != nil {
		return ProviderObservationPage{}, err
	}
	return s.providerObservationPage(page), nil
}

func (s *Service) RefreshProviderObservations(ctx context.Context, opts ListOptions) (ProviderObservationPage, error) {
	page, err := s.RefreshProviders(ctx, opts)
	if err != nil {
		return ProviderObservationPage{}, err
	}
	return s.providerObservationPage(page), nil
}

func (s *Service) ListDataSets(ctx context.Context, opts ListOptions) (DataSetStatePage, error) {
	return s.store.ListDataSetStates(ctx, opts)
}

func (s *Service) ListDataSetObservations(ctx context.Context, opts ListOptions) (DataSetObservationPage, error) {
	page, err := s.ListDataSets(ctx, opts)
	if err != nil {
		return DataSetObservationPage{}, err
	}
	localInventoryTotal, err := s.localDataSetCoverageTotal(ctx, opts, page.Summary.Total)
	if err != nil {
		return DataSetObservationPage{}, err
	}
	return s.dataSetObservationPage(page, localInventoryTotal), nil
}

func (s *Service) RefreshDataSetObservations(ctx context.Context, opts ListOptions) (DataSetObservationPage, error) {
	page, err := s.RefreshDataSets(ctx, opts)
	if err != nil {
		return DataSetObservationPage{}, err
	}
	localInventoryTotal, err := s.localDataSetCoverageTotal(ctx, opts, page.Summary.Total)
	if err != nil {
		return DataSetObservationPage{}, err
	}
	return s.dataSetObservationPage(page, localInventoryTotal), nil
}

func (s *Service) DataSetStatesByLocalIDs(ctx context.Context, localIDs []int64) (map[int64]DataSetState, error) {
	out := make(map[int64]DataSetState)
	for start := 0; start < len(localIDs); start += dataSetStateLookupBatchSize {
		end := min(start+dataSetStateLookupBatchSize, len(localIDs))
		batch, err := s.store.GetDataSetStatesByLocalIDs(ctx, localIDs[start:end])
		if err != nil {
			return nil, err
		}
		for id, state := range batch {
			out[id] = state
		}
	}
	return out, nil
}

func (s *Service) DataSetObservationsByLocalIDs(ctx context.Context, localIDs []int64) (map[int64]DataSetObservation, error) {
	states, err := s.DataSetStatesByLocalIDs(ctx, localIDs)
	if err != nil {
		return nil, err
	}
	out := make(map[int64]DataSetObservation, len(states))
	now := s.checkedAt()
	for id, state := range states {
		out[id] = DataSetObservationFromState(state, s.RefreshInterval(), now)
	}
	return out, nil
}

func (s *Service) providerObservationPage(page ProviderStatePage) ProviderObservationPage {
	items := make([]ProviderObservation, 0, len(page.Items))
	now := s.checkedAt()
	for _, state := range page.Items {
		items = append(items, ProviderObservationFromState(state, s.RefreshInterval(), now))
	}
	return ProviderObservationPage{
		Items:         items,
		Summary:       page.Summary,
		SummarySignal: DefaultAttentionSummarySignal(page.Summary, page.LastCheckedAt, s.RefreshInterval(), now),
		Total:         page.Total,
		Limit:         page.Limit,
		Offset:        page.Offset,
	}
}

func (s *Service) dataSetObservationPage(page DataSetStatePage, localInventoryTotal int) DataSetObservationPage {
	items := make([]DataSetObservation, 0, len(page.Items))
	now := s.checkedAt()
	summary := dataSetSummaryWithCoverage(page.Summary, localInventoryTotal)
	for _, state := range page.Items {
		items = append(items, DataSetObservationFromState(state, s.RefreshInterval(), now))
	}
	return DataSetObservationPage{
		Items:         items,
		Summary:       summary,
		SummarySignal: DefaultAttentionSummarySignal(summary, page.LastCheckedAt, s.RefreshInterval(), now),
		Total:         page.Total,
		Limit:         page.Limit,
		Offset:        page.Offset,
	}
}

func (s *Service) listLocalDataSets(ctx context.Context) ([]LocalDataSet, error) {
	if s.localDataSets == nil {
		return nil, nil
	}
	return s.localDataSets.ListLocalDataSets(ctx)
}

func (s *Service) localDataSetCoverageTotal(ctx context.Context, opts ListOptions, summaryTotal int) (int, error) {
	if opts.Status != "" {
		return summaryTotal, nil
	}
	if opts.BucketID == 0 && opts.ProviderID == nil && s.localDataSetCount != nil {
		return s.localDataSetCount.CountLocalDataSets(ctx)
	}
	local, err := s.listLocalDataSets(ctx)
	if err != nil {
		return 0, err
	}
	if opts.BucketID == 0 && opts.ProviderID == nil {
		return len(local), nil
	}
	count := 0
	for _, dataSet := range local {
		if opts.BucketID > 0 && dataSet.BucketID != opts.BucketID {
			continue
		}
		if opts.ProviderID != nil && dataSet.ProviderID.String() != opts.ProviderID.String() {
			continue
		}
		count++
	}
	return count, nil
}

func dataSetSummaryWithCoverage(summary Summary, localInventoryTotal int) Summary {
	if localInventoryTotal <= summary.Total {
		return summary
	}
	missing := localInventoryTotal - summary.Total
	summary.Total = localInventoryTotal
	summary.Unknown += missing
	return summary
}

func (s *Service) checkedAt() time.Time {
	if s == nil || s.now == nil {
		return time.Now().UTC()
	}
	return s.now().UTC()
}

type refreshGroup struct {
	mu   sync.Mutex
	call *refreshCall
}

type refreshCall struct {
	done               chan struct{}
	cancel             context.CancelFunc
	err                error
	cancellableWaiters int
	detachedWaiters    int
	cancelled          bool
}

func (g *refreshGroup) Do(ctx context.Context, timeout time.Duration, detach bool, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		if !detach {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		g.mu.Lock()
		call := g.call
		if call == nil {
			refreshCtx := context.WithoutCancel(ctx)
			var cancel context.CancelFunc
			if timeout > 0 {
				refreshCtx, cancel = context.WithTimeout(refreshCtx, timeout)
			} else {
				refreshCtx, cancel = context.WithCancel(refreshCtx)
			}
			call = &refreshCall{
				done:   make(chan struct{}),
				cancel: cancel,
			}
			if detach {
				call.detachedWaiters = 1
			} else {
				call.cancellableWaiters = 1
			}
			g.call = call
			g.mu.Unlock()

			go g.run(call, refreshCtx, fn)
			return g.wait(ctx, call, detach)
		}
		if call.cancelled {
			g.mu.Unlock()
			if err := waitForRetiredRefresh(ctx, call, detach); err != nil {
				return err
			}
			continue
		}
		if detach {
			call.detachedWaiters++
		} else {
			call.cancellableWaiters++
		}
		g.mu.Unlock()
		return g.wait(ctx, call, detach)
	}
}

func (g *refreshGroup) wait(ctx context.Context, call *refreshCall, detach bool) error {
	if detach {
		// Detached refreshes ignore caller cancellation but still wait for the
		// shared refresh to finish, so they remain active waiters until done.
		<-call.done
		g.releaseDetached(call)
		return call.err
	}
	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		if g.release(call) {
			<-call.done
		}
		return ctx.Err()
	}
}

func (g *refreshGroup) releaseDetached(call *refreshCall) {
	g.mu.Lock()
	if call.detachedWaiters > 0 {
		call.detachedWaiters--
	}
	g.mu.Unlock()
}

func (g *refreshGroup) release(call *refreshCall) bool {
	g.mu.Lock()
	if g.call != call || call.cancelled {
		g.mu.Unlock()
		return false
	}
	call.cancellableWaiters--
	if call.cancellableWaiters > 0 || call.detachedWaiters > 0 {
		g.mu.Unlock()
		return false
	}
	call.cancelled = true
	cancel := call.cancel
	g.mu.Unlock()
	cancel()
	return true
}

func (g *refreshGroup) run(call *refreshCall, ctx context.Context, fn func(context.Context) error) {
	defer call.cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			call.err = fmt.Errorf("observability refresh panic: %v", recovered)
		}
		g.mu.Lock()
		if g.call == call {
			g.call = nil
		}
		close(call.done)
		g.mu.Unlock()
	}()

	call.err = fn(ctx)
}

func waitForRetiredRefresh(ctx context.Context, call *refreshCall, detach bool) error {
	// The retiring call belongs to earlier callers. Its error is intentionally
	// ignored so Do can join or start a fresh call for the current caller.
	if detach {
		<-call.done
		return nil
	}
	select {
	case <-call.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
