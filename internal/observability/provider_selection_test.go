package observability

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/types"
	sdktypes "github.com/strahe/synapse-go/types"
)

type selectionTierStore struct {
	*tierTestStore
	snapshots         map[string]*ProviderTierSnapshot
	readErr           error
	reads             int
	firstProviderPage *ProviderStatePage
}

func (s *selectionTierStore) ProviderProfiles(_ context.Context, ids []types.OnChainID) (map[string]ProviderProfile, error) {
	profiles := make(map[string]ProviderProfile, len(ids))
	for _, id := range ids {
		for _, state := range s.providers {
			if state.Profile != nil && state.ProviderID.Equal(id) {
				profiles[id.String()] = *state.Profile
			}
		}
	}
	return profiles, nil
}

func (s *selectionTierStore) ListProviderStates(ctx context.Context, opts ListOptions) (ProviderStatePage, error) {
	if s.firstProviderPage != nil {
		page := *s.firstProviderPage
		s.firstProviderPage = nil
		return page, nil
	}
	return s.fakeStateStore.ListProviderStates(ctx, opts)
}

func (s *selectionTierStore) GetProviderTierSnapshot(_ context.Context, tier string) (*ProviderTierSnapshot, error) {
	s.reads++
	return s.snapshots[tier], s.readErr
}

func (s *selectionTierStore) RecordEndorsedProviders(ctx context.Context, checkedAt time.Time, ids []types.OnChainID) (time.Time, error) {
	at, err := s.tierTestStore.RecordEndorsedProviders(ctx, checkedAt, ids)
	raw := make([]string, 0, len(ids))
	for _, id := range ids {
		raw = append(raw, id.String())
	}
	data, marshalErr := json.Marshal(raw)
	if marshalErr != nil {
		return at, marshalErr
	}
	s.snapshots["endorsed"] = &ProviderTierSnapshot{CheckedAt: at, ProviderIDs: data}
	return at, err
}

func TestSelectionAdmissionFreshnessAndIndependentTiers(t *testing.T) {
	for _, scenario := range []string{"fresh", "missing", "stale", "refresh failure", "none"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			store := &selectionTierStore{tierTestStore: &tierTestStore{fakeStateStore: &fakeStateStore{}}, snapshots: map[string]*ProviderTierSnapshot{
				"approved": {CheckedAt: now, ProviderIDs: json.RawMessage(`[]`)},
				"endorsed": {CheckedAt: now, ProviderIDs: json.RawMessage(`["202"]`)},
			}}
			source := tierEndorsedSource{ids: []sdktypes.BigInt{sdktypes.NewBigInt(202)}}
			tier := providerselect.TierEndorsed
			switch scenario {
			case "missing":
				delete(store.snapshots, "endorsed")
			case "stale", "refresh failure":
				store.snapshots["endorsed"].CheckedAt = now.Add(-3 * time.Minute)
				if scenario == "refresh failure" {
					source.err = errors.New("RPC unavailable")
				}
			case "none":
				tier = providerselect.TierNone
				store.readErr = errors.New("snapshot unavailable")
			}
			service := NewService(ServiceOptions{Store: store, EndorsedProviders: source, RefreshInterval: time.Minute})
			admission, err := service.SelectionAdmission(t.Context(), tier)
			if scenario == "refresh failure" {
				if err == nil {
					t.Fatal("stale membership was used after a failed refresh")
				}
				return
			}
			if err != nil || !admission.Trusted(types.NewOnChainID(202)) {
				t.Fatalf("admission = %#v err=%v", admission, err)
			}
			if scenario == "none" && store.reads != 0 {
				t.Fatalf("none read tier snapshots %d times", store.reads)
			}
			if (scenario == "missing" || scenario == "stale") && store.endorsedCalls != 1 {
				t.Fatalf("refresh writes = %d, want 1", store.endorsedCalls)
			}
		})
	}
}

func TestSelectionInventoryRefreshesEmptyFirstPageWithNonzeroTotal(t *testing.T) {
	now := time.Now().UTC()
	id := types.NewOnChainID(101)
	active, hasPDP := true, true
	url := "https://provider.test"
	states := []ProviderState{{
		ProviderID: id, Status: StatusAvailable, Active: &active, HasPDP: &hasPDP,
		ServiceURL: &url, LastCheckedAt: now,
		Profile: &ProviderProfile{ProviderID: id, Active: true, ServiceURL: url},
	}}
	store := &selectionTierStore{
		tierTestStore: &tierTestStore{fakeStateStore: &fakeStateStore{providers: states}},
		snapshots: map[string]*ProviderTierSnapshot{
			"approved": {CheckedAt: now, ProviderIDs: json.RawMessage(`["101"]`)},
		},
		// The initial rows were read before the first refresh committed, and
		// the total was read afterwards.
		firstProviderPage: &ProviderStatePage{Total: 1},
	}
	checker := &fakeRefreshChecker{checkProviders: func(context.Context, time.Time, []LocalDataSet) ([]ProviderState, error) {
		return states, nil
	}}
	service := NewService(ServiceOptions{Store: store, Checker: checker, RefreshInterval: time.Minute, Now: func() time.Time { return now }})
	in, err := service.SelectionInventory(t.Context(), providerselect.TierApproved)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := providerselect.Select(in, providerselect.StrategyDistribution, 1, nil, false)
	if err != nil || len(selected) != 1 || !selected[0].ID.Equal(id) {
		t.Fatalf("selected providers = %#v, err=%v, want trusted provider %s", selected, err, id.String())
	}
}
