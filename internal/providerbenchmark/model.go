package providerbenchmark

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

const (
	SampleBytes          int64 = 32 << 20
	MinimumSampleBytes   int64 = 8 << 20
	MeasurementWindow          = 30 * time.Second
	SessionCreateTimeout       = 15 * time.Second
)

type Measurement struct {
	Bytes    int64
	Duration time.Duration
}

// SuccessfulMeasurement is a historical reference, independent of current admission.
type SuccessfulMeasurement struct {
	SampleBytes    int64     `json:"sample_bytes"`
	DurationMS     int64     `json:"duration_ms"`
	BytesPerSecond int64     `json:"bytes_per_second"`
	TestedAt       time.Time `json:"tested_at"`
}

// UploadProbe measures one upload sample against a provider service URL.
type UploadProbe interface {
	Probe(context.Context, string) (Measurement, error)
}

type State string

const (
	StateTesting   State = "testing"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
)

type Result struct {
	bun.BaseModel `bun:"table:provider_upload_speed_tests"`

	ProviderID     string     `bun:"provider_id,pk,type:text" json:"-"`
	State          State      `bun:"state,type:text,notnull" json:"state"`
	ServiceURLHash string     `bun:"service_url_hash,type:text,notnull" json:"-"`
	SampleBytes    int64      `bun:"sample_bytes,notnull" json:"sample_bytes"`
	DurationMS     *int64     `bun:"duration_ms" json:"duration_ms,omitempty"`
	BytesPerSecond *int64     `bun:"bytes_per_second" json:"bytes_per_second,omitempty"`
	TestedAt       *time.Time `bun:"tested_at" json:"tested_at,omitempty"`
	FailureCode    *string    `bun:"failure_code,type:text" json:"failure_code,omitempty"`
	ActiveTaskID   *int64     `bun:"active_task_id" json:"-"`
	CreatedAt      time.Time  `bun:"created_at,notnull" json:"-"`
	UpdatedAt      time.Time  `bun:"updated_at,notnull" json:"-"`
}

// Outcome reads the last settled result, including while a new test is running.
func (r Result) Outcome(serviceURL string) (bytesPerSecond int64, settled bool) {
	if serviceURL == "" || r.ServiceURLHash != URLHash(serviceURL) || r.TestedAt == nil {
		return 0, false
	}
	if r.FailureCode != nil && *r.FailureCode != "" && r.DurationMS == nil && r.BytesPerSecond == nil {
		return 0, true
	}
	if r.FailureCode == nil && r.SampleBytes > 0 && r.DurationMS != nil && *r.DurationMS > 0 && r.BytesPerSecond != nil && *r.BytesPerSecond > 0 {
		return *r.BytesPerSecond, true
	}
	return 0, false
}

type failureCode string

const (
	failureUploadFailed            failureCode = "upload_failed"
	failureTimeout                 failureCode = "timeout"
	failureUnavailable             failureCode = "unavailable"
	failureInvalidInput            failureCode = "invalid_input"
	failureProviderChanged         failureCode = "provider_changed"
	failureRecordFailed            failureCode = "record_failed"
	failureInvalidCheckpoint       failureCode = "invalid_checkpoint"
	failureInterrupted             failureCode = "interrupted"
	failureHandlerUnavailable      failureCode = "handler_unavailable"
	failureInputVersionUnsupported failureCode = "input_version_unsupported"
	failureInputCodecPanic         failureCode = "input_codec_panic"
	failureHandlerPanic            failureCode = "handler_panic"
	failureInvalidResult           failureCode = "invalid_result"
)

// MeasuredFailure treats unknown failures as completed measurements.
func MeasuredFailure(code string) bool {
	//exhaustive:enforce
	switch failureCode(code) {
	case failureUnavailable, failureInvalidInput, failureProviderChanged, failureRecordFailed, failureInvalidCheckpoint, failureInterrupted,
		failureHandlerUnavailable, failureInputVersionUnsupported, failureInputCodecPanic, failureHandlerPanic, failureInvalidResult:
		return false
	case failureUploadFailed, failureTimeout:
		return true
	default:
		return true
	}
}

// NonMeasuredFailureCodes identifies failures eligible for automatic remeasurement.
func NonMeasuredFailureCodes() []string {
	var codes []string
	for _, code := range []failureCode{
		failureUploadFailed, failureTimeout, failureUnavailable, failureInvalidInput, failureProviderChanged, failureRecordFailed,
		failureInvalidCheckpoint, failureInterrupted, failureHandlerUnavailable, failureInputVersionUnsupported,
		failureInputCodecPanic, failureHandlerPanic, failureInvalidResult,
	} {
		if !MeasuredFailure(string(code)) {
			codes = append(codes, string(code))
		}
	}
	return codes
}

// Priority orders measurements by provider membership and accepted bindings.
func Priority(endorsed, approved, bound bool) int {
	switch {
	case endorsed:
		return 0
	case approved:
		return 1
	case bound:
		return 2
	default:
		return 3
	}
}

// Due reports whether a provider can start another automatic measurement.
func Due(row *Result, currentHash string, retestInterval time.Duration, now time.Time) bool {
	if row == nil {
		return true
	}
	//exhaustive:enforce
	switch row.State {
	case StateTesting:
		return false
	case StateSucceeded, StateFailed:
	default:
		return false
	}
	if row.ServiceURLHash != currentHash || row.TestedAt == nil {
		return true
	}
	if row.FailureCode != nil && !MeasuredFailure(*row.FailureCode) {
		return true
	}
	return retestInterval > 0 && !row.TestedAt.After(now.Add(-retestInterval))
}

type Input struct {
	ProviderID     string `json:"provider_id"`
	ServiceURLHash string `json:"service_url_hash"`
}

type Checkpoint struct {
	Attempted      bool  `json:"attempted"`
	SampleBytes    int64 `json:"sample_bytes,omitempty"`
	DurationMS     int64 `json:"duration_ms,omitempty"`
	BytesPerSecond int64 `json:"bytes_per_second,omitempty"`
}

func URLHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func CurrentServiceURL(ctx context.Context, service *observability.Service, providerID types.OnChainID) (string, bool, error) {
	page, err := service.ListProviderObservations(ctx, observability.ListOptions{ProviderID: &providerID, Limit: 1})
	if err != nil {
		return "", false, err
	}
	if len(page.Items) == 0 {
		return "", false, nil
	}
	item := page.Items[0]
	if _, reason := Eligibility(&item, nil); reason != "" {
		return "", false, nil
	}
	profile, err := service.ProviderProfile(ctx, providerID)
	if err != nil {
		return "", false, err
	}
	serviceURL, reason := Eligibility(&item, profile)
	return serviceURL, reason == "", nil
}

// Eligibility uses the same provider evidence for admission and action views.
func Eligibility(item *observability.ProviderObservation, profile *observability.ProviderProfile) (string, string) {
	switch {
	case item == nil:
		return "", "Refresh this provider to check its availability."
	case item.Signal.Freshness.Stale:
		return "", "Refresh this provider's health information."
	case item.Facts.Active == nil || !*item.Facts.Active:
		return "", "Provider is not active."
	case item.Facts.HasPDP == nil || !*item.Facts.HasPDP:
		return "", "Provider does not offer PDP storage."
	case item.Facts.ServiceURL == nil || *item.Facts.ServiceURL == "":
		return "", "Provider has no service URL."
	case item.Signal.Status != observability.StatusAvailable:
		return "", "Provider is currently unavailable."
	case profile != nil && profile.ServiceURL != *item.Facts.ServiceURL:
		return "", "Refresh this provider after its service URL changed."
	default:
		return *item.Facts.ServiceURL, ""
	}
}
