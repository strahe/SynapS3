package providerselect

import (
	"errors"
	"testing"

	"github.com/strahe/synaps3/internal/types"
)

func selectionCandidate(t *testing.T, id string, load int, speed int64) Candidate {
	t.Helper()
	providerID, err := types.ParseOnChainID("provider_id", id)
	if err != nil {
		t.Fatal(err)
	}
	return Candidate{ID: providerID, Load: load, BytesPerSecond: speed, Active: true, HasPDP: true, HasProfile: true, Healthy: true, Fresh: true, ServiceURL: "https://provider.example", ProfileURL: "https://provider.example"}
}

func TestSelectPrioritiesAndRequiredTier(t *testing.T) {
	a, b, unknown := selectionCandidate(t, "1", 3, 100), selectionCandidate(t, "2", 4, 200), selectionCandidate(t, "3", 0, 0)
	for _, tc := range []struct {
		name       string
		strategy   Strategy
		admission  Admission
		candidates []Candidate
		trusted    bool
		want       string
		wantErr    bool
	}{
		{"distribution", StrategyDistribution, Admission{Tier: TierNone}, []Candidate{a, b}, false, "1", false},
		{"speed", StrategySpeed, Admission{Tier: TierNone}, []Candidate{a, b}, false, "2", false},
		{"measured before unknown", StrategySpeed, Admission{Tier: TierNone}, []Candidate{unknown, a}, false, "1", false},
		{"unknown load", StrategySpeed, Admission{Tier: TierNone}, []Candidate{unknown, selectionCandidate(t, "4", 1, 0)}, false, "3", false},
		{"approved constraint", StrategySpeed, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{a.ID}}, []Candidate{a, b}, false, "1", false},
		{"existing trusted", StrategySpeed, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{a.ID}}, []Candidate{b}, true, "2", false},
		{"independent endorsed", StrategyDistribution, Admission{Tier: TierEndorsed, TrustedIDs: []types.OnChainID{b.ID}}, []Candidate{a, b}, false, "2", false},
		{"no fallback", StrategySpeed, Admission{Tier: TierEndorsed}, []Candidate{a, b}, false, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Select(Inventory{Candidates: tc.candidates, Admission: tc.admission}, tc.strategy, 1, nil, tc.trusted)
			if tc.wantErr {
				if !errors.Is(err, ErrNoTrustedProvider) {
					t.Fatalf("err = %v", err)
				}
				return
			}
			if err != nil || len(got) != 1 || got[0].ID.String() != tc.want {
				t.Fatalf("selection = %#v, err=%v, want=%s", got, err, tc.want)
			}
		})
	}
}

func TestSelectRejectsIneligibleProviders(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func(*Candidate)
	}{
		{"inactive", func(c *Candidate) { c.Active = false }},
		{"no PDP", func(c *Candidate) { c.HasPDP = false }},
		{"missing profile", func(c *Candidate) { c.HasProfile = false }},
		{"unhealthy", func(c *Candidate) { c.Healthy = false }},
		{"stale", func(c *Candidate) { c.Fresh = false }},
		{"changed URL", func(c *Candidate) { c.ProfileURL = "https://old.example" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			c := selectionCandidate(t, "1", 0, 100)
			change.apply(&c)
			got, err := Select(Inventory{Candidates: []Candidate{c}, Admission: Admission{Tier: TierNone}}, StrategySpeed, 1, nil, false)
			if err != nil || len(got) != 0 {
				t.Fatalf("ineligible selection=%#v err=%v", got, err)
			}
		})
	}
}
