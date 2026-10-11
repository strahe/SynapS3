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

const SampleBytes int64 = 32 << 20

// UploadProbe measures one upload sample against a provider service URL.
type UploadProbe interface {
	Probe(context.Context, string) (time.Duration, error)
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

type Input struct {
	ProviderID     string `json:"provider_id"`
	ServiceURLHash string `json:"service_url_hash"`
}

type Checkpoint struct {
	Attempted      bool  `json:"attempted"`
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
