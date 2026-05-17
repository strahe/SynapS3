package availability

import (
	"errors"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type Status string

const (
	StatusAvailable   Status = "available"
	StatusDegraded    Status = "degraded"
	StatusUnavailable Status = "unavailable"
	StatusUnknown     Status = "unknown"
)

type ReasonCode string

const (
	ReasonRegistryLookupFailed    ReasonCode = "registry_lookup_failed"
	ReasonProviderInactive        ReasonCode = "provider_inactive"
	ReasonProviderHTTPUnreachable ReasonCode = "provider_http_unreachable"
	ReasonChainLookupFailed       ReasonCode = "chain_lookup_failed"
	ReasonChainDataSetMissing     ReasonCode = "chain_data_set_missing"
	ReasonChainDataSetInactive    ReasonCode = "chain_data_set_inactive"
	ReasonChainDataSetUnmanaged   ReasonCode = "chain_data_set_unmanaged"
	ReasonLocalStatusNotReady     ReasonCode = "local_status_not_ready"
	ReasonProviderMismatch        ReasonCode = "provider_mismatch"
	ReasonMetadataMismatch        ReasonCode = "metadata_mismatch"
	ReasonStaleSnapshot           ReasonCode = "stale_snapshot"
)

var ErrProviderNotFound = errors.New("provider not found")

type Provider struct {
	ID           types.OnChainID
	Active       bool
	HasPDP       bool
	ServiceURL   string
	HealthStatus string
}

type LocalDataSet struct {
	ID              int64
	BucketID        int64
	BucketName      string
	CopyIndex       int
	ProviderID      types.OnChainID
	DataSetID       *types.OnChainID
	ClientDataSetID *types.OnChainID
	Status          model.StorageDataSetStatus
}

type ChainDataSet struct {
	DataSetID        types.OnChainID
	ClientDataSetID  *types.OnChainID
	ProviderID       types.OnChainID
	IsLive           bool
	IsManaged        bool
	ActivePieceCount *int64
	Metadata         map[string]string
}

type ProviderSnapshot struct {
	bun.BaseModel `bun:"table:availability_provider_snapshots"`

	ProviderID    types.OnChainID `bun:"provider_id,pk,type:text" json:"provider_id"`
	Status        Status          `bun:"status,notnull" json:"status"`
	ReasonCodes   []ReasonCode    `bun:"reason_codes,type:jsonb,notnull" json:"reason_codes"`
	Active        bool            `bun:"active,notnull" json:"active"`
	HealthStatus  string          `bun:"health_status,notnull" json:"health_status"`
	LastCheckedAt time.Time       `bun:"last_checked_at,nullzero,notnull" json:"last_checked_at"`
	LastError     *string         `bun:"last_error,nullzero" json:"last_error,omitempty"`
	Evidence      map[string]any  `bun:"evidence_json,type:jsonb,notnull" json:"evidence"`
	CreatedAt     time.Time       `bun:"created_at,nullzero,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt     time.Time       `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

type DataSetSnapshot struct {
	bun.BaseModel `bun:"table:availability_data_set_snapshots"`

	LocalDataSetID   int64                      `bun:"local_data_set_id,pk" json:"local_data_set_id"`
	BucketID         int64                      `bun:"bucket_id,notnull" json:"bucket_id"`
	BucketName       string                     `bun:"bucket_name,notnull" json:"bucket_name"`
	ProviderID       types.OnChainID            `bun:"provider_id,type:text,notnull" json:"provider_id"`
	ChainDataSetID   *types.OnChainID           `bun:"chain_data_set_id,type:text" json:"chain_data_set_id,omitempty"`
	ClientDataSetID  *types.OnChainID           `bun:"client_data_set_id,type:text" json:"client_data_set_id,omitempty"`
	LocalStatus      model.StorageDataSetStatus `bun:"local_status,notnull" json:"local_status"`
	Status           Status                     `bun:"status,notnull" json:"status"`
	ReasonCodes      []ReasonCode               `bun:"reason_codes,type:jsonb,notnull" json:"reason_codes"`
	ActivePieceCount *int64                     `bun:"active_piece_count,nullzero" json:"active_piece_count,omitempty"`
	LastCheckedAt    time.Time                  `bun:"last_checked_at,nullzero,notnull" json:"last_checked_at"`
	LastError        *string                    `bun:"last_error,nullzero" json:"last_error,omitempty"`
	Evidence         map[string]any             `bun:"evidence_json,type:jsonb,notnull" json:"evidence"`
	CreatedAt        time.Time                  `bun:"created_at,nullzero,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt        time.Time                  `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}

type Summary struct {
	Total       int `json:"total"`
	Available   int `json:"available"`
	Degraded    int `json:"degraded"`
	Unavailable int `json:"unavailable"`
	Unknown     int `json:"unknown"`
}

type ListOptions struct {
	Limit      int
	Offset     int
	Status     Status
	BucketID   int64
	ProviderID *types.OnChainID
}

type ProviderSnapshotPage struct {
	Items         []ProviderSnapshot `json:"items"`
	Summary       Summary            `json:"summary"`
	LastCheckedAt *time.Time         `json:"last_checked_at,omitempty"`
	Total         int                `json:"total"`
	Limit         int                `json:"limit"`
	Offset        int                `json:"offset"`
}

type DataSetSnapshotPage struct {
	Items         []DataSetSnapshot `json:"items"`
	Summary       Summary           `json:"summary"`
	LastCheckedAt *time.Time        `json:"last_checked_at,omitempty"`
	Total         int               `json:"total"`
	Limit         int               `json:"limit"`
	Offset        int               `json:"offset"`
}
