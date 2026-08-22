package admin

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/strahe/synaps3/internal/observability"
)

type stubProviderRegistry struct {
	items []observability.ProviderObservation
	err   error
	opts  observability.ListOptions
	calls []observability.ListOptions
	list  func(observability.ListOptions) observability.ProviderObservationPage
}

func (s *stubProviderRegistry) ListProviderObservations(
	_ context.Context,
	opts observability.ListOptions,
) (observability.ProviderObservationPage, error) {
	s.opts = opts
	s.calls = append(s.calls, opts)
	if s.err != nil {
		return observability.ProviderObservationPage{}, s.err
	}
	if s.list != nil {
		return s.list(opts), nil
	}
	start := min(opts.Offset, len(s.items))
	end := min(start+opts.Limit, len(s.items))
	return observability.ProviderObservationPage{Items: s.items[start:end], Total: len(s.items)}, nil
}

func observedProvider(id string, active, hasPDP bool) observability.ProviderObservation {
	return observability.ProviderObservation{
		Facts: observability.ProviderFacts{
			ProviderID: onChainIDValue(id),
			Active:     &active,
			HasPDP:     &hasPDP,
		},
	}
}

func selectorWithRegistry(items ...observability.ProviderObservation) *StorageProviderSelector {
	return NewStorageProviderSelector(&stubProviderRegistry{items: items})
}

func providerIDStrings(t *testing.T, selector *StorageProviderSelector) []string {
	t.Helper()
	providers, err := selector.ListReplacementProviders(context.Background())
	if err != nil {
		t.Fatalf("ListReplacementProviders: %v", err)
	}
	out := make([]string, 0, len(providers))
	for _, id := range providers {
		out = append(out, id.String())
	}
	return out
}

// A provider without an active PDP offering cannot hold a data set, so offering
// it would only fail later inside the worker.
func TestListReplacementProviders_SkipsProvidersThatCannotHoldADataSet(t *testing.T) {
	got := providerIDStrings(t, selectorWithRegistry(
		observedProvider("101", false, true),
		observedProvider("202", true, false),
		observedProvider("303", true, true),
	))
	if len(got) != 1 || got[0] != "303" {
		t.Fatalf("providers = %v, want only the usable one", got)
	}
}

// Registry order is not guaranteed, so the inventory is sorted. Without it the
// same confirmation could land on a different provider each time it is retried.
func TestListReplacementProviders_IsStableAcrossRegistryOrder(t *testing.T) {
	forward := providerIDStrings(t, selectorWithRegistry(observedProvider("303", true, true), observedProvider("202", true, true)))
	reverse := providerIDStrings(t, selectorWithRegistry(observedProvider("202", true, true), observedProvider("303", true, true)))
	if len(forward) != 2 || forward[0] != "202" || forward[1] != "303" {
		t.Fatalf("providers = %v, want lowest registry ID first", forward)
	}
	if len(reverse) != len(forward) || reverse[0] != forward[0] || reverse[1] != forward[1] {
		t.Fatalf("providers = %v then %v, want the same order regardless of registry order", forward, reverse)
	}
}

// Registry IDs are numbers, not strings: 9 sorts before 23.
func TestListReplacementProviders_SortsNumerically(t *testing.T) {
	got := providerIDStrings(t, selectorWithRegistry(
		observedProvider("23", true, true),
		observedProvider("9", true, true),
		observedProvider("101", true, true),
	))
	want := []string{"9", "23", "101"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatalf("providers = %v, want %v", got, want)
		}
	}
}

func TestListReplacementProviders_ReportsAnUnavailableRegistry(t *testing.T) {
	if _, err := (*StorageProviderSelector)(nil).ListReplacementProviders(context.Background()); err == nil {
		t.Fatal("listing without an inventory returned no error")
	}
	selector := NewStorageProviderSelector(&stubProviderRegistry{err: errors.New("registry unreachable")})
	if _, err := selector.ListReplacementProviders(context.Background()); err == nil {
		t.Fatal("a failing inventory read returned no error")
	}
}

// The storage topology lists providers whose health probe says available. The
// chooser has to ask for exactly that, or it offers providers the operator was
// told are unreachable -- and hides ones they were told are fine.
func TestListReplacementProviders_AsksForTheSameSetTheTopologyShows(t *testing.T) {
	registry := &stubProviderRegistry{items: []observability.ProviderObservation{observedProvider("2", true, true)}}
	if _, err := NewStorageProviderSelector(registry).ListReplacementProviders(context.Background()); err != nil {
		t.Fatalf("ListReplacementProviders: %v", err)
	}
	if registry.opts.Status != observability.StatusAvailable {
		t.Fatalf("requested status = %q, want only the providers reported available", registry.opts.Status)
	}
	if registry.opts.Limit <= 0 {
		t.Fatalf("requested limit = %d, want a bounded page", registry.opts.Limit)
	}
}

func TestListReplacementProviders_ReadsPastTheFirstPage(t *testing.T) {
	items := make([]observability.ProviderObservation, 0, replacementProviderPageSize+1)
	for i := 1; i <= replacementProviderPageSize+1; i++ {
		items = append(items, observedProvider(strconv.Itoa(i), true, i == replacementProviderPageSize+1))
	}
	registry := &stubProviderRegistry{items: items}
	got := providerIDStrings(t, NewStorageProviderSelector(registry))
	if len(got) != 1 || got[0] != strconv.Itoa(replacementProviderPageSize+1) {
		t.Fatalf("providers = %v, want the eligible provider on page two", got)
	}
	if len(registry.calls) != 2 || registry.calls[1].Offset != replacementProviderPageSize {
		t.Fatalf("page requests = %#v, want offsets 0 and %d", registry.calls, replacementProviderPageSize)
	}
}

func TestListReplacementProviders_StopsWhenPaginationDoesNotAdvance(t *testing.T) {
	page := make([]observability.ProviderObservation, 0, replacementProviderPageSize)
	for i := 1; i <= replacementProviderPageSize; i++ {
		page = append(page, observedProvider(strconv.Itoa(i), true, true))
	}
	registry := &stubProviderRegistry{
		list: func(observability.ListOptions) observability.ProviderObservationPage {
			// Simulate a drifting registry that ignores the requested offset and
			// returns the same full page forever.
			return observability.ProviderObservationPage{Items: page, Total: replacementProviderPageSize * 2}
		},
	}
	got := providerIDStrings(t, NewStorageProviderSelector(registry))
	if len(got) != replacementProviderPageSize {
		t.Fatalf("providers = %d, want one de-duplicated page", len(got))
	}
	if len(registry.calls) != 2 {
		t.Fatalf("page requests = %d, want the duplicate page to stop pagination", len(registry.calls))
	}
}
