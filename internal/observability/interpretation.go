package observability

import "time"

func BuildFreshness(lastCheckedAt *time.Time, interval time.Duration, now time.Time) Freshness {
	warnings := make([]FreshnessWarning, 0, 1)
	freshness := Freshness{
		LastCheckedAt: lastCheckedAt,
		Warnings:      warnings,
	}
	if lastCheckedAt == nil || lastCheckedAt.IsZero() {
		freshness.Warnings = append(freshness.Warnings, FreshnessNoStateRecorded)
		return freshness
	}
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if now.UTC().Sub(lastCheckedAt.UTC()) > interval*2 {
		freshness.Stale = true
		freshness.Warnings = append(freshness.Warnings, FreshnessStaleState)
	}
	return freshness
}

func BuildSignal(status Status, reasons []ReasonCode, lastError *string, lastCheckedAt *time.Time, interval time.Duration, now time.Time) Signal {
	freshness := BuildFreshness(lastCheckedAt, interval, now)
	level := signalLevelForStatus(status)
	if freshness.Stale && level == SignalOK {
		level = SignalWarning
	}
	outReasons := make([]ReasonCode, 0, len(reasons))
	outReasons = append(outReasons, reasons...)
	return Signal{
		Status:      status,
		Level:       level,
		ReasonCodes: outReasons,
		LastError:   lastError,
		Freshness:   freshness,
	}
}

func DefaultAttentionSummarySignal(summary Summary, lastCheckedAt *time.Time, interval time.Duration, now time.Time) SummarySignal {
	freshness := BuildFreshness(lastCheckedAt, interval, now)
	level := SignalOK
	if len(freshness.Warnings) > 0 || summary.Degraded > 0 || summary.Unknown > 0 {
		level = WorstSignalLevel(level, SignalWarning)
	}
	if summary.Unavailable > 0 {
		level = WorstSignalLevel(level, SignalBlocking)
	}
	return SummarySignal{Level: level, Freshness: freshness}
}

func WorstSignalLevel(current SignalLevel, candidate SignalLevel) SignalLevel {
	if signalLevelRank(candidate) > signalLevelRank(current) {
		return candidate
	}
	return current
}

func signalLevelRank(level SignalLevel) int {
	switch level {
	case SignalBlocking:
		return 2
	case SignalWarning:
		return 1
	default:
		return 0
	}
}

func ProviderObservationFromState(state ProviderState, interval time.Duration, now time.Time) ProviderObservation {
	checkedAt := timePtrIfSet(state.LastCheckedAt)
	return ProviderObservation{
		Facts: ProviderFacts{
			ProviderID:   state.ProviderID,
			Active:       state.Active,
			HasPDP:       state.HasPDP,
			ServiceURL:   state.ServiceURL,
			HealthStatus: state.HealthStatus,
		},
		Signal: BuildSignal(state.Status, state.ReasonCodes, state.LastError, checkedAt, interval, now),
	}
}

func DataSetObservationFromState(state DataSetState, interval time.Duration, now time.Time) DataSetObservation {
	checkedAt := timePtrIfSet(state.LastCheckedAt)
	return DataSetObservation{
		Facts: DataSetFacts{
			LocalDataSetID:   state.LocalDataSetID,
			BucketID:         state.BucketID,
			BucketName:       state.BucketName,
			CopyIndex:        state.CopyIndex,
			ProviderID:       state.ProviderID,
			ChainDataSetID:   state.ChainDataSetID,
			ClientDataSetID:  state.ClientDataSetID,
			LocalStatus:      state.LocalStatus,
			ActivePieceCount: state.ActivePieceCount,
		},
		Signal: BuildSignal(state.Status, state.ReasonCodes, state.LastError, checkedAt, interval, now),
	}
}

func signalLevelForStatus(status Status) SignalLevel {
	switch status {
	case StatusUnavailable:
		return SignalBlocking
	case StatusDegraded, StatusUnknown:
		return SignalWarning
	default:
		return SignalOK
	}
}

func timePtrIfSet(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	out := value
	return &out
}
