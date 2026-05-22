package observability

import (
	"reflect"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/model"
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

func TestCopyHealthFromFactsClassifiesLocalCopyState(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	localDataSetID := int64(11)
	retrievalURL := "https://provider.example/piece"
	healthyObservation := copyHealthDataSetObservation(t, localDataSetID, StatusAvailable, nil, now)

	tests := []struct {
		name        string
		facts       CopyFacts
		observation *DataSetObservation
		wantStatus  Status
		wantReasons []ReasonCode
	}{
		{
			name: "healthy committed copy",
			facts: CopyFacts{
				Status:         model.StorageUploadCopyStatusCommitted,
				ProviderID:     onChainIDPtr(t, "101"),
				LocalDataSetID: &localDataSetID,
				ChainDataSetID: onChainIDPtr(t, "1001"),
				PieceID:        onChainIDPtr(t, "2001"),
				RetrievalURL:   &retrievalURL,
			},
			observation: &healthyObservation,
			wantStatus:  StatusAvailable,
			wantReasons: []ReasonCode{},
		},
		{
			name: "pending copy",
			facts: CopyFacts{
				Status: model.StorageUploadCopyStatusPending,
			},
			wantStatus:  StatusDegraded,
			wantReasons: []ReasonCode{ReasonCopyPending},
		},
		{
			name: "committing copy",
			facts: CopyFacts{
				Status: model.StorageUploadCopyStatusCommitting,
			},
			wantStatus:  StatusDegraded,
			wantReasons: []ReasonCode{ReasonCopyCommitting},
		},
		{
			name: "failed copy",
			facts: CopyFacts{
				Status:    model.StorageUploadCopyStatusFailed,
				LastError: stringPtr("provider rejected piece"),
			},
			wantStatus:  StatusUnavailable,
			wantReasons: []ReasonCode{ReasonCopyFailed},
		},
		{
			name: "committed copy missing evidence",
			facts: CopyFacts{
				Status:         model.StorageUploadCopyStatusCommitted,
				LocalDataSetID: &localDataSetID,
				ChainDataSetID: onChainIDPtr(t, "1001"),
			},
			observation: &healthyObservation,
			wantStatus:  StatusUnknown,
			wantReasons: []ReasonCode{ReasonCopyMissingProvider, ReasonCopyMissingPiece, ReasonCopyMissingRetrievalURL},
		},
		{
			name: "committed copy missing observation",
			facts: CopyFacts{
				Status:         model.StorageUploadCopyStatusCommitted,
				ProviderID:     onChainIDPtr(t, "101"),
				LocalDataSetID: &localDataSetID,
				ChainDataSetID: onChainIDPtr(t, "1001"),
				PieceID:        onChainIDPtr(t, "2001"),
				RetrievalURL:   &retrievalURL,
			},
			wantStatus:  StatusUnknown,
			wantReasons: []ReasonCode{ReasonCopyObservationMissing},
		},
		{
			name: "unavailable data set observation",
			facts: CopyFacts{
				Status:         model.StorageUploadCopyStatusCommitted,
				ProviderID:     onChainIDPtr(t, "101"),
				LocalDataSetID: &localDataSetID,
				ChainDataSetID: onChainIDPtr(t, "1001"),
				PieceID:        onChainIDPtr(t, "2001"),
				RetrievalURL:   &retrievalURL,
			},
			observation: ptr(copyHealthDataSetObservation(t, localDataSetID, StatusUnavailable, []ReasonCode{ReasonChainLookupFailed}, now)),
			wantStatus:  StatusUnknown,
			wantReasons: []ReasonCode{ReasonCopyObservationMissing},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CopyHealthFromFacts(tt.facts, tt.observation, time.Hour, now)
			if got.Status != tt.wantStatus || !reflect.DeepEqual(got.ReasonCodes, tt.wantReasons) {
				t.Fatalf("copy health = status:%s reasons:%#v, want status:%s reasons:%#v", got.Status, got.ReasonCodes, tt.wantStatus, tt.wantReasons)
			}
		})
	}
}

func TestCopyHealthFromFactsTreatsStaleDataSetAsUnknown(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	checkedAt := now.Add(-3 * time.Hour)
	localDataSetID := int64(12)
	retrievalURL := "https://provider.example/piece"

	observation := copyHealthDataSetObservation(t, localDataSetID, StatusAvailable, nil, checkedAt)
	observation.Signal = BuildSignal(StatusAvailable, nil, nil, &checkedAt, time.Hour, now)
	got := CopyHealthFromFacts(CopyFacts{
		Status:         model.StorageUploadCopyStatusCommitted,
		ProviderID:     onChainIDPtr(t, "101"),
		LocalDataSetID: &localDataSetID,
		ChainDataSetID: onChainIDPtr(t, "1001"),
		PieceID:        onChainIDPtr(t, "2001"),
		RetrievalURL:   &retrievalURL,
	}, &observation, time.Hour, now)
	if got.Status != StatusUnknown || !got.Freshness.Stale {
		t.Fatalf("copy health = status:%s stale:%v, want unknown stale", got.Status, got.Freshness.Stale)
	}
}

func copyHealthDataSetObservation(t *testing.T, localID int64, status Status, reasons []ReasonCode, checkedAt time.Time) DataSetObservation {
	t.Helper()
	return DataSetObservation{
		Facts: DataSetFacts{
			LocalDataSetID: localID,
			ProviderID:     onChainID(t, "101"),
			ChainDataSetID: onChainIDPtr(t, "1001"),
			LocalStatus:    model.StorageDataSetStatusReady,
		},
		Signal: BuildSignal(status, reasons, nil, &checkedAt, time.Hour, checkedAt),
	}
}

func ptr[T any](value T) *T {
	return &value
}

func TestDefaultAttentionSummarySignalMapsHealthSummary(t *testing.T) {
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	checkedAt := now

	healthy := DefaultAttentionSummarySignal(Summary{Total: 3, Available: 3}, &checkedAt, time.Minute, now)
	if healthy.Level != SignalOK {
		t.Fatalf("healthy summary signal = %s, want ok", healthy.Level)
	}
	for _, summary := range []Summary{
		{Total: 1, Degraded: 1},
		{Total: 1, Unknown: 1},
	} {
		got := DefaultAttentionSummarySignal(summary, &checkedAt, time.Minute, now)
		if got.Level != SignalWarning {
			t.Fatalf("attention summary signal = %s, want warning for %#v", got.Level, summary)
		}
	}
	unavailable := DefaultAttentionSummarySignal(Summary{Total: 1, Unavailable: 1}, &checkedAt, time.Minute, now)
	if unavailable.Level != SignalBlocking {
		t.Fatalf("unavailable summary signal = %s, want blocking", unavailable.Level)
	}

	noState := DefaultAttentionSummarySignal(Summary{}, nil, time.Minute, now)
	if noState.Level != SignalWarning {
		t.Fatalf("no-state summary signal = %s, want warning", noState.Level)
	}
}

func TestWorstSignalLevelUsesSignalSeverity(t *testing.T) {
	tests := []struct {
		name      string
		current   SignalLevel
		candidate SignalLevel
		want      SignalLevel
	}{
		{name: "warning beats ok", current: SignalOK, candidate: SignalWarning, want: SignalWarning},
		{name: "blocking beats warning", current: SignalWarning, candidate: SignalBlocking, want: SignalBlocking},
		{name: "warning does not beat blocking", current: SignalBlocking, candidate: SignalWarning, want: SignalBlocking},
		{name: "ok does not beat warning", current: SignalWarning, candidate: SignalOK, want: SignalWarning},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WorstSignalLevel(tt.current, tt.candidate); got != tt.want {
				t.Fatalf("WorstSignalLevel(%s, %s) = %s, want %s", tt.current, tt.candidate, got, tt.want)
			}
		})
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
