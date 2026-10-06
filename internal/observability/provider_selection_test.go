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
	snapshots map[string]*ProviderTierSnapshot
	readErr   error
	reads     int
}

func (s *selectionTierStore) ProviderProfiles(context.Context, []types.OnChainID) (map[string]ProviderProfile, error) {
	return nil, nil
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
