package providerselect

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

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
		name    string
		apply   func(*Candidate)
		reason  string
		refresh bool
	}{
		{"inactive", func(c *Candidate) { c.Active = false }, "provider_inactive", false},
		{"no PDP", func(c *Candidate) { c.HasPDP = false }, "provider_missing_pdp", false},
		{"missing profile", func(c *Candidate) { c.HasProfile = false }, "profile_missing", true},
		{"unhealthy", func(c *Candidate) { c.Healthy = false }, "provider_unhealthy", false},
		{"stale", func(c *Candidate) { c.Fresh = false }, "observation_stale", true},
		{"changed URL", func(c *Candidate) { c.ProfileURL = "https://old.example" }, "profile_url_changed", true},
		{"missing URL", func(c *Candidate) { c.ServiceURL = "" }, "service_url_missing", false},
	} {
		t.Run(change.name, func(t *testing.T) {
			c := selectionCandidate(t, "1", 0, 100)
			change.apply(&c)
			got, err := Select(Inventory{Candidates: []Candidate{c}, Admission: Admission{Tier: TierNone}}, StrategySpeed, 1, nil, false)
			var failure *SelectionError
			if !errors.As(err, &failure) || !errors.Is(err, ErrCandidatesTemporarilyUnavailable) || len(got) != 0 {
				t.Fatalf("ineligible selection=%#v err=%v", got, err)
			}
			if len(failure.Diagnostics.Rejections) != 1 || failure.Diagnostics.Rejections[0].Reason != change.reason || failure.Diagnostics.NeedsRefresh() != change.refresh {
				t.Fatalf("diagnostics = %#v, refresh=%v", failure.Diagnostics, failure.Diagnostics.NeedsRefresh())
			}
		})
	}
}

func TestSelectDistinguishesShortageFromTemporaryObstacles(t *testing.T) {
	healthy := selectionCandidate(t, "1", 0, 100)
	unhealthy := selectionCandidate(t, "2", 0, 100)
	unhealthy.Healthy = false
	stale := selectionCandidate(t, "3", 0, 100)
	stale.Fresh = false
	for _, tc := range []struct {
		name       string
		candidates []Candidate
		admission  Admission
		excluded   map[string]ExclusionReason
		trusted    bool
		count      int
		wantCount  int
		wantKind   SelectionFailureKind
		refresh    bool
	}{
		{"empty required tier", []Candidate{healthy}, Admission{Tier: TierApproved}, nil, false, 1, 0, SelectionRequiredTierEmpty, false},
		{"trusted bucket binding", []Candidate{unhealthy}, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{unhealthy.ID}}, map[string]ExclusionReason{"2": ExclusionBucketInUse}, false, 1, 0, SelectionRequiredTierInUse, false},
		{"trusted historical provider", []Candidate{unhealthy}, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{unhealthy.ID}}, map[string]ExclusionReason{"2": ExclusionPreviouslyUsed}, false, 1, 0, SelectionRequiredTierInUse, false},
		{"trusted history without observation", []Candidate{healthy}, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{unhealthy.ID}}, map[string]ExclusionReason{"2": ExclusionPreviouslyUsed}, false, 1, 0, SelectionRequiredTierInUse, false},
		{"trusted context failed", []Candidate{healthy}, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{healthy.ID}}, map[string]ExclusionReason{"1": ExclusionContextOpenFailed}, false, 1, 0, SelectionTemporary, false},
		{"missing trusted observation", []Candidate{healthy}, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{unhealthy.ID}}, nil, false, 1, 0, SelectionTemporary, true},
		{"unhealthy trusted and unrelated stale", []Candidate{unhealthy, stale}, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{unhealthy.ID}}, nil, false, 1, 0, SelectionTemporary, false},
		{"enough healthy despite stale", []Candidate{healthy, stale}, Admission{Tier: TierNone}, nil, false, 1, 1, "", false},
		{"healthy capacity shortage", []Candidate{healthy}, Admission{Tier: TierNone}, nil, false, 2, 1, "", false},
		{"no tier unhealthy shortage", []Candidate{healthy, unhealthy}, Admission{Tier: TierNone}, nil, false, 2, 0, SelectionTemporary, false},
		{"existing trusted stale shortage", []Candidate{healthy, stale}, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{healthy.ID}}, nil, true, 2, 0, SelectionTemporary, true},
		{"bucket occupied shortage", []Candidate{healthy, stale}, Admission{Tier: TierNone}, map[string]ExclusionReason{"3": ExclusionBucketInUse}, false, 2, 1, "", false},
		{"historical provider shortage", []Candidate{healthy, stale}, Admission{Tier: TierNone}, map[string]ExclusionReason{"3": ExclusionPreviouslyUsed}, false, 2, 1, "", false},
		{"trusted satisfied historical shortage", []Candidate{stale}, Admission{Tier: TierApproved, TrustedIDs: []types.OnChainID{healthy.ID}}, map[string]ExclusionReason{"3": ExclusionPreviouslyUsed, "1": ExclusionBucketInUse}, true, 1, 0, "", false},
		{"historical and temporary shortage", []Candidate{healthy, stale}, Admission{Tier: TierNone}, map[string]ExclusionReason{"1": ExclusionPreviouslyUsed}, false, 1, 0, SelectionTemporary, true},
		{"selected provider is not an obstacle", []Candidate{healthy}, Admission{Tier: TierNone}, map[string]ExclusionReason{"1": ExclusionSelected}, true, 1, 0, "", false},
		{"unknown exclusion is conservative", []Candidate{healthy}, Admission{Tier: TierNone}, map[string]ExclusionReason{"1": "unknown"}, false, 1, 0, SelectionTemporary, false},
		{"zero demand", []Candidate{stale}, Admission{Tier: TierApproved}, nil, false, 0, 0, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := Inventory{Candidates: tc.candidates, Admission: tc.admission}
			selected, err := Select(in, StrategyDistribution, tc.count, tc.excluded, tc.trusted)
			if len(selected) != tc.wantCount {
				t.Fatalf("selected = %#v, err=%v", selected, err)
			}
			if tc.wantKind == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var failure *SelectionError
				if !errors.As(err, &failure) || failure.Kind != tc.wantKind {
					t.Fatalf("error = %v, want %s", err, tc.wantKind)
				}
				permanent := tc.wantKind == SelectionRequiredTierEmpty || tc.wantKind == SelectionRequiredTierInUse
				if errors.Is(err, ErrNoTrustedProvider) != permanent || errors.Is(err, ErrCandidatesTemporarilyUnavailable) == permanent {
					t.Fatalf("failure classification = %v", err)
				}
			}
			if diagnostics := Diagnose(in, tc.count, tc.excluded, tc.trusted); diagnostics.NeedsRefresh() != tc.refresh {
				t.Fatalf("refresh = %v, want %v, diagnostics=%#v", diagnostics.NeedsRefresh(), tc.refresh, diagnostics)
			}
		})
	}
}

func TestSelectionDiagnosticsAreBoundedAndKeepDecisionEvidence(t *testing.T) {
	now := time.Now().UTC()
	in := Inventory{Admission: Admission{Tier: TierApproved, CheckedAt: &now}}
	for i := 1; i <= 101; i++ {
		candidate := selectionCandidate(t, strconv.Itoa(i), 0, 0)
		candidate.Healthy = false
		candidate.ObservationCheckedAt = &now
		if i == 101 {
			candidate.Fresh = false
			in.Admission.TrustedIDs = []types.OnChainID{candidate.ID}
		}
		in.Candidates = append(in.Candidates, candidate)
	}
	diagnostics := Diagnose(in, 1, nil, false)
	if diagnostics.RejectedTotal != 101 || diagnostics.RejectedOmitted != 1 || len(diagnostics.Rejections) != 100 || !diagnostics.NeedsRefresh() {
		t.Fatalf("bounded diagnostics = %#v, refresh=%v", diagnostics, diagnostics.NeedsRefresh())
	}
	if diagnostics.Rejections[0].ProviderID.String() != "1" || diagnostics.Rejections[99].ProviderID.String() != "100" || !diagnostics.Rejections[0].ObservationCheckedAt.Equal(now) || !diagnostics.TierCheckedAt.Equal(now) {
		t.Fatalf("selection evidence = %#v", diagnostics)
	}
	raw, err := json.Marshal(diagnostics)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "https://") || strings.Contains(string(raw), "profile_url") || strings.Contains(string(raw), "service_url") {
		t.Fatal("diagnostics included provider URLs")
	}
	cause := errors.New("provider operation failed")
	failure := &SelectionError{Kind: SelectionProviderContext, Cause: cause, Diagnostics: diagnostics}
	if !errors.Is(failure, cause) || errors.Is(failure, ErrCandidatesTemporarilyUnavailable) || errors.Is(failure, ErrNoTrustedProvider) {
		t.Fatal("provider context failure lost its original classification")
	}
}
