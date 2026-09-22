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
	if item.Signal.Status != observability.StatusAvailable || item.Signal.Freshness.Stale ||
		item.Facts.Active == nil || !*item.Facts.Active || item.Facts.HasPDP == nil || !*item.Facts.HasPDP ||
		item.Facts.ServiceURL == nil || *item.Facts.ServiceURL == "" {
		return "", false, nil
	}
	return *item.Facts.ServiceURL, true, nil
}
