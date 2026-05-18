package observability

import (
	"reflect"
	"testing"
	"time"
)

func TestBuildFreshnessClassifiesNoStateFreshAndStale(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	freshCheckedAt := now.Add(-time.Minute)
	staleCheckedAt := now.Add(-11 * time.Minute)

	tests := []struct {
		name          string
		lastCheckedAt *time.Time
		wantStale     bool
		wantWarnings  []FreshnessWarning
	}{
		{name: "no state", wantWarnings: []FreshnessWarning{FreshnessNoStateRecorded}},
		{name: "fresh", lastCheckedAt: &freshCheckedAt, wantWarnings: []FreshnessWarning{}},
		{name: "stale", lastCheckedAt: &staleCheckedAt, wantStale: true, wantWarnings: []FreshnessWarning{FreshnessStaleState}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildFreshness(tt.lastCheckedAt, 5*time.Minute, now)
			if got.Stale != tt.wantStale || !reflect.DeepEqual(got.Warnings, tt.wantWarnings) {
				t.Fatalf("freshness = stale:%v warnings:%#v, want stale:%v warnings:%#v", got.Stale, got.Warnings, tt.wantStale, tt.wantWarnings)
			}
		})
	}
}

func TestBuildSignalMapsStatusAndStale(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	freshCheckedAt := now
	staleCheckedAt := now.Add(-11 * time.Minute)

	tests := []struct {
		name          string
		status        Status
		lastCheckedAt *time.Time
		want          SignalLevel
	}{
		{name: "available", status: StatusAvailable, lastCheckedAt: &freshCheckedAt, want: SignalOK},
		{name: "degraded", status: StatusDegraded, lastCheckedAt: &freshCheckedAt, want: SignalWarning},
		{name: "unknown", status: StatusUnknown, lastCheckedAt: &freshCheckedAt, want: SignalWarning},
		{name: "unavailable", status: StatusUnavailable, lastCheckedAt: &freshCheckedAt, want: SignalBlocking},
		{name: "available stale", status: StatusAvailable, lastCheckedAt: &staleCheckedAt, want: SignalWarning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildSignal(tt.status, nil, nil, tt.lastCheckedAt, 5*time.Minute, now)
			if got.Level != tt.want {
				t.Fatalf("signal level = %s, want %s", got.Level, tt.want)
			}
		})
	}
}

func TestReadinessSummaryPolicies(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	checkedAt := now

	provider := ProviderReadinessSummarySignal(Summary{Total: 3, Degraded: 3}, &checkedAt, time.Minute, now)
	if provider.Level != SignalOK {
		t.Fatalf("provider degraded readiness = %s, want ok", provider.Level)
	}
	provider = ProviderReadinessSummarySignal(Summary{Total: 1, Unknown: 1}, &checkedAt, time.Minute, now)
	if provider.Level != SignalWarning {
		t.Fatalf("provider unknown readiness = %s, want warning", provider.Level)
	}

	dataSet := DataSetReadinessSummarySignal(Summary{Total: 1, Degraded: 1}, &checkedAt, time.Minute, now, 1)
	if dataSet.Level != SignalWarning {
		t.Fatalf("data set degraded readiness = %s, want warning", dataSet.Level)
	}
	dataSet = DataSetReadinessSummarySignal(Summary{}, nil, time.Minute, now, 0)
	if dataSet.Level != SignalOK {
		t.Fatalf("empty inventory readiness = %s, want ok", dataSet.Level)
	}
	dataSet = DataSetReadinessSummarySignal(Summary{Total: 1, Available: 1}, &checkedAt, time.Minute, now, 2)
	if dataSet.Level != SignalWarning {
		t.Fatalf("missing observed local data set readiness = %s, want warning", dataSet.Level)
	}
}

func TestObservationsDoNotExposeEvidence(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	state := ProviderState{
		ProviderID:    onChainID(t, "101"),
		Status:        StatusAvailable,
		ReasonCodes:   []ReasonCode{},
		Active:        boolPtr(true),
		HasPDP:        boolPtr(true),
		ServiceURL:    stringPtr("https://provider.example"),
		HealthStatus:  stringPtr("reachable"),
		LastCheckedAt: now,
		Evidence:      map[string]any{"private": "debug-only"},
	}
	got := ProviderObservationFromState(state, time.Minute, now)
	if got.Facts.ProviderID.String() != "101" || got.Facts.ServiceURL == nil || *got.Facts.ServiceURL != "https://provider.example" {
		t.Fatalf("provider facts = %#v, want typed facts", got.Facts)
	}
}
