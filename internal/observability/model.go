package observability

import (
	"context"
	"encoding/json"
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
	ReasonProviderMissingPDP      ReasonCode = "provider_missing_pdp"
	ReasonProviderHTTPUnreachable ReasonCode = "provider_http_unreachable"
	ReasonChainLookupFailed       ReasonCode = "chain_lookup_failed"
	ReasonChainDataSetMissing     ReasonCode = "chain_data_set_missing"
	ReasonChainDataSetInactive    ReasonCode = "chain_data_set_inactive"
	ReasonChainDataSetUnmanaged   ReasonCode = "chain_data_set_unmanaged"
	ReasonLocalStatusNotReady     ReasonCode = "local_status_not_ready"
	ReasonProviderMismatch        ReasonCode = "provider_mismatch"
	ReasonMetadataMismatch        ReasonCode = "metadata_mismatch"
	ReasonCopyUnderReplicated     ReasonCode = "copy_under_replicated"
	ReasonCopyPending             ReasonCode = "copy_pending"
	ReasonCopyCommitting          ReasonCode = "copy_committing"
	ReasonCopyFailed              ReasonCode = "copy_failed"
	ReasonCopyMissingProvider     ReasonCode = "copy_missing_provider"
	ReasonCopyMissingDataSet      ReasonCode = "copy_missing_data_set"
	ReasonCopyMissingPiece        ReasonCode = "copy_missing_piece"
	ReasonCopyMissingRetrievalURL ReasonCode = "copy_missing_retrieval_url"
	ReasonCopyObservationMissing  ReasonCode = "copy_observation_missing"
)

var ErrProviderNotFound = errors.New("provider not found")

type CollectionType string

const (
	CollectionProviders CollectionType = "providers"
	CollectionDataSets  CollectionType = "data_sets"
)

type SignalLevel string

const (
	SignalOK       SignalLevel = "ok"
	SignalWarning  SignalLevel = "warning"
	SignalBlocking SignalLevel = "blocking"
)

type FreshnessWarning string

const (
	FreshnessNoStateRecorded FreshnessWarning = "no_state_recorded"
	FreshnessStaleState      FreshnessWarning = "stale_state"
)

type Provider struct {
	ID           types.OnChainID
	Active       bool
	HasPDP       bool
	ServiceURL   string
	HealthStatus string
	Profile      *ProviderProfile
}

// ProviderProfile is the last successful Registry read for one provider.
type ProviderProfile struct {
	bun.BaseModel          `bun:"table:provider_profiles"`
	ProviderID             types.OnChainID `bun:"provider_id,pk,type:text" json:"provider_id"`
	Name                   string          `bun:"name,type:text,notnull" json:"name"`
	Description            string          `bun:"description,type:text,notnull" json:"description"`
	ServiceProviderAddress string          `bun:"service_provider_address,type:text,notnull" json:"service_provider_address"`
	PayeeAddress           string          `bun:"payee_address,type:text,notnull" json:"payee_address"`
	Active                 bool            `bun:"active,notnull" json:"active"`
	ServiceURL             string          `bun:"service_url,type:text,notnull" json:"service_url"`
	RegistrySnapshot       json.RawMessage `bun:"registry_snapshot_json,type:jsonb,notnull" json:"registry_snapshot"`
	LastSuccessAt          time.Time       `bun:"last_success_at,notnull" json:"last_success_at"`
	Approved               bool            `bun:"-" json:"approved"`
	ApprovedCheckedAt      *time.Time      `bun:"-" json:"approved_checked_at"`
	Endorsed               bool            `bun:"-" json:"endorsed"`
	EndorsedCheckedAt      *time.Time      `bun:"-" json:"endorsed_checked_at"`
}

type ProviderTierSnapshot struct {
	bun.BaseModel `bun:"table:provider_tier_snapshots"`
	Tier          string          `bun:"tier,pk,type:text"`
	ProviderIDs   json.RawMessage `bun:"provider_ids_json,type:jsonb,notnull"`
	CheckedAt     time.Time       `bun:"checked_at,notnull"`
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
	HasActivePieces  *bool
	Metadata         map[string]string
}

type ProviderState struct {
	bun.BaseModel `bun:"table:observability_provider_states"`
	Profile       *ProviderProfile `bun:"-" json:"-"`

	ProviderID    types.OnChainID `bun:"provider_id,pk,type:text" json:"provider_id"`
	Status        Status          `bun:"status,type:text,notnull" json:"status"`
	ReasonCodes   []ReasonCode    `bun:"reason_codes,type:jsonb,notnull" json:"reason_codes"`
	Active        *bool           `bun:"active" json:"active,omitempty"`
	HasPDP        *bool           `bun:"has_pdp" json:"has_pdp,omitempty"`
	ServiceURL    *string         `bun:"service_url,type:text" json:"service_url,omitempty"`
	HealthStatus  *string         `bun:"health_status,type:text" json:"health_status,omitempty"`
	LastCheckedAt time.Time       `bun:"last_checked_at,nullzero,notnull" json:"last_checked_at"`
	LastAttemptAt time.Time       `bun:"last_attempt_at,nullzero,notnull" json:"last_attempt_at"`
	LastError     *string         `bun:"last_error,type:text,nullzero" json:"last_error,omitempty"`
	Evidence      map[string]any  `bun:"evidence_json,type:jsonb,notnull" json:"evidence"`
}

type DataSetState struct {
	bun.BaseModel `bun:"table:observability_data_set_states,alias:observability_data_set_state"`

	LocalDataSetID  int64            `bun:"local_data_set_id,pk" json:"local_data_set_id"`
	BucketID        int64            `bun:"bucket_id,notnull" json:"bucket_id"`
	CopyIndex       int              `bun:"copy_index,type:integer,notnull" json:"copy_index"`
	ProviderID      types.OnChainID  `bun:"provider_id,type:text,notnull" json:"provider_id"`
	ChainDataSetID  *types.OnChainID `bun:"chain_data_set_id,type:text" json:"chain_data_set_id,omitempty"`
	ClientDataSetID *types.OnChainID `bun:"client_data_set_id,type:text" json:"client_data_set_id,omitempty"`
	// BucketName and LocalStatus are projected from the rows that own them.
	// Observations are rebuilt wholesale on every refresh, so a copy here would
	// only add a second, staler answer to the same question.
	BucketName       string                     `bun:",scanonly" json:"bucket_name"`
	LocalStatus      model.StorageDataSetStatus `bun:",scanonly" json:"local_status"`
	Status           Status                     `bun:"status,type:text,notnull" json:"status"`
	ReasonCodes      []ReasonCode               `bun:"reason_codes,type:jsonb,notnull" json:"reason_codes"`
	ActivePieceCount *int64                     `bun:"active_piece_count,nullzero" json:"active_piece_count,omitempty"`
	LastCheckedAt    time.Time                  `bun:"last_checked_at,nullzero,notnull" json:"last_checked_at"`
	LastError        *string                    `bun:"last_error,type:text,nullzero" json:"last_error,omitempty"`
	Evidence         map[string]any             `bun:"evidence_json,type:jsonb,notnull" json:"evidence"`
}

type CollectionState struct {
	bun.BaseModel `bun:"table:observability_collection_states"`

	CollectionType CollectionType `bun:"collection_type,pk,type:text" json:"collection_type"`
	LastCheckedAt  time.Time      `bun:"last_checked_at,nullzero,notnull" json:"last_checked_at"`
	CreatedAt      time.Time      `bun:"created_at,nullzero,notnull" json:"created_at"`
	UpdatedAt      time.Time      `bun:"updated_at,nullzero,notnull" json:"updated_at"`
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

type ProviderStatePage struct {
	Items         []ProviderState `json:"items"`
	Summary       Summary         `json:"summary"`
	LastCheckedAt *time.Time      `json:"last_checked_at,omitempty"`
	Total         int             `json:"total"`
	Limit         int             `json:"limit"`
	Offset        int             `json:"offset"`
}

type ProviderFacts struct {
	ProviderID   types.OnChainID `json:"provider_id"`
	Active       *bool           `json:"active,omitempty"`
	HasPDP       *bool           `json:"has_pdp,omitempty"`
	ServiceURL   *string         `json:"service_url,omitempty"`
	HealthStatus *string         `json:"health_status,omitempty"`
}

type DataSetFacts struct {
	LocalDataSetID   int64                      `json:"local_data_set_id"`
	BucketID         int64                      `json:"bucket_id"`
	BucketName       string                     `json:"bucket_name"`
	CopyIndex        int                        `json:"copy_index"`
	ProviderID       types.OnChainID            `json:"provider_id"`
	ChainDataSetID   *types.OnChainID           `json:"chain_data_set_id,omitempty"`
	ClientDataSetID  *types.OnChainID           `json:"client_data_set_id,omitempty"`
	LocalStatus      model.StorageDataSetStatus `json:"local_status"`
	ActivePieceCount *int64                     `json:"active_piece_count,omitempty"`
	HasActivePieces  *bool                      `json:"has_active_pieces,omitempty"`
}

type CopyFacts struct {
	Status         model.StorageCopyStatus `json:"status"`
	ProviderID     *types.OnChainID        `json:"provider_id,omitempty"`
	LocalDataSetID *int64                  `json:"local_data_set_id,omitempty"`
	ChainDataSetID *types.OnChainID        `json:"chain_data_set_id,omitempty"`
	PieceID        *types.OnChainID        `json:"piece_id,omitempty"`
	RetrievalURL   *string                 `json:"retrieval_url,omitempty"`
	LastError      *string                 `json:"last_error,omitempty"`
}

type Freshness struct {
	LastCheckedAt *time.Time         `json:"last_checked_at,omitempty"`
	Stale         bool               `json:"stale"`
	Warnings      []FreshnessWarning `json:"warnings"`
}

type Signal struct {
	Status      Status       `json:"status"`
	Level       SignalLevel  `json:"level"`
	ReasonCodes []ReasonCode `json:"reason_codes"`
	LastError   *string      `json:"last_error,omitempty"`
	Freshness   Freshness    `json:"freshness"`
}

type SummarySignal struct {
	Level     SignalLevel `json:"level"`
	Freshness Freshness   `json:"freshness"`
}

type ProviderObservation struct {
	Facts  ProviderFacts `json:"facts"`
	Signal Signal        `json:"signal"`
}

type DataSetObservation struct {
	Facts  DataSetFacts `json:"facts"`
	Signal Signal       `json:"signal"`
}

type ProviderObservationPage struct {
	Items         []ProviderObservation `json:"items"`
	Summary       Summary               `json:"summary"`
	SummarySignal SummarySignal         `json:"summary_signal"`
	Total         int                   `json:"total"`
	Limit         int                   `json:"limit"`
	Offset        int                   `json:"offset"`
}

type DataSetObservationPage struct {
	Items         []DataSetObservation `json:"items"`
	Summary       Summary              `json:"summary"`
	SummarySignal SummarySignal        `json:"summary_signal"`
	Total         int                  `json:"total"`
	Limit         int                  `json:"limit"`
	Offset        int                  `json:"offset"`
}

type DataSetStatePage struct {
	Items         []DataSetState `json:"items"`
	Summary       Summary        `json:"summary"`
	LastCheckedAt *time.Time     `json:"last_checked_at,omitempty"`
	Total         int            `json:"total"`
	Limit         int            `json:"limit"`
	Offset        int            `json:"offset"`
}

var _ bun.BeforeAppendModelHook = (*CollectionState)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (c *CollectionState) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if c.CreatedAt.IsZero() {
		c.CreatedAt = now
	}
	if c.UpdatedAt.IsZero() {
		c.UpdatedAt = now
	}
	return nil
}
