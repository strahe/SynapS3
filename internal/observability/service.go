package observability

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type RefreshChecker interface {
	CheckProviders(context.Context, []LocalDataSet) ([]ProviderState, error)
	CheckDataSets(context.Context, []LocalDataSet) ([]DataSetState, error)
}

type LocalDataSetSource interface {
	ListLocalDataSets(context.Context) ([]LocalDataSet, error)
}

type LocalDataSetSourceFunc func(context.Context) ([]LocalDataSet, error)

func (f LocalDataSetSourceFunc) ListLocalDataSets(ctx context.Context) ([]LocalDataSet, error) {
	return f(ctx)
}

type StateStore interface {
	ReplaceProviderStates(context.Context, []ProviderState) error
	ListProviderStates(context.Context, ListOptions) (ProviderStatePage, error)
	ReplaceDataSetStates(context.Context, []DataSetState) error
	ListDataSetStates(context.Context, ListOptions) (DataSetStatePage, error)
	GetDataSetStatesByLocalIDs(context.Context, []int64) (map[int64]DataSetState, error)
}

type ServiceOptions struct {
	Checker         RefreshChecker
	LocalDataSets   LocalDataSetSource
	Store           StateStore
	RefreshInterval time.Duration
	RefreshTimeout  time.Duration
}

type Service struct {
	checker         RefreshChecker
	localDataSets   LocalDataSetSource
	store           StateStore
	refreshInterval time.Duration
	refreshTimeout  time.Duration
	providerRefresh refreshGroup
	dataSetRefresh  refreshGroup
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
	return &Service{
		checker:         opts.Checker,
		localDataSets:   opts.LocalDataSets,
		store:           opts.Store,
		refreshInterval: interval,
		refreshTimeout:  timeout,
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
	if err := s.providerRefresh.Do(ctx, s.refreshTimeout, func(refreshCtx context.Context) error {
		local, err := s.listLocalDataSets(refreshCtx)
		if err != nil {
			return err
		}
		states, err := s.checker.CheckProviders(refreshCtx, local)
		if err != nil {
			return err
		}
		return s.store.ReplaceProviderStates(refreshCtx, states)
	}); err != nil {
		return ProviderStatePage{}, err
	}
	return s.ListProviders(ctx, opts)
}

func (s *Service) RefreshDataSets(ctx context.Context, opts ListOptions) (DataSetStatePage, error) {
	if err := s.dataSetRefresh.Do(ctx, s.refreshTimeout, func(refreshCtx context.Context) error {
		local, err := s.listLocalDataSets(refreshCtx)
		if err != nil {
			return err
		}
		states, err := s.checker.CheckDataSets(refreshCtx, local)
		if err != nil {
			return err
		}
		return s.store.ReplaceDataSetStates(refreshCtx, states)
	}); err != nil {
		return DataSetStatePage{}, err
	}
	return s.ListDataSets(ctx, opts)
}

func (s *Service) RefreshAll(ctx context.Context) error {
	var errs []error
	if _, err := s.RefreshProviders(ctx, ListOptions{}); err != nil {
		errs = append(errs, err)
	}
	if _, err := s.RefreshDataSets(ctx, ListOptions{}); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *Service) ListProviders(ctx context.Context, opts ListOptions) (ProviderStatePage, error) {
	return s.store.ListProviderStates(ctx, opts)
}

func (s *Service) ListDataSets(ctx context.Context, opts ListOptions) (DataSetStatePage, error) {
	return s.store.ListDataSetStates(ctx, opts)
}

func (s *Service) DataSetStatesByLocalIDs(ctx context.Context, localIDs []int64) (map[int64]DataSetState, error) {
	return s.store.GetDataSetStatesByLocalIDs(ctx, localIDs)
}

func (s *Service) listLocalDataSets(ctx context.Context) ([]LocalDataSet, error) {
	if s.localDataSets == nil {
		return nil, nil
	}
	return s.localDataSets.ListLocalDataSets(ctx)
}

type refreshGroup struct {
	mu   sync.Mutex
	call *refreshCall
}

type refreshCall struct {
	done chan struct{}
	err  error
}

func (g *refreshGroup) Do(ctx context.Context, timeout time.Duration, fn func(context.Context) error) (err error) {
	g.mu.Lock()
	if g.call != nil {
		call := g.call
		g.mu.Unlock()
		select {
		case <-call.done:
			return call.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	call := &refreshCall{done: make(chan struct{})}
	g.call = call
	g.mu.Unlock()

	refreshCtx := context.WithoutCancel(ctx)
	cancel := func() {}
	if timeout > 0 {
		refreshCtx, cancel = context.WithTimeout(refreshCtx, timeout)
	}
	defer cancel()
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
		err = call.err
	}()

	call.err = fn(refreshCtx)
	return call.err
}
