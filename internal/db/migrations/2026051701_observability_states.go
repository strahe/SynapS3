package migrations

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

func init() {
	Migrations.MustRegister(up2026051701ObservabilityStates, down2026051701ObservabilityStates)
}

type observabilityProviderState2026051701 struct {
	bun.BaseModel `bun:"table:observability_provider_states"`

	ProviderID    string         `bun:"provider_id,pk,type:text"`
	Status        string         `bun:"status,notnull"`
	ReasonCodes   []string       `bun:"reason_codes,type:jsonb,notnull"`
	Active        bool           `bun:"active,notnull"`
	HealthStatus  string         `bun:"health_status,notnull"`
	LastCheckedAt time.Time      `bun:"last_checked_at,nullzero,notnull"`
	LastError     *string        `bun:"last_error,nullzero"`
	Evidence      map[string]any `bun:"evidence_json,type:jsonb,notnull"`
	CreatedAt     time.Time      `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	UpdatedAt     time.Time      `bun:"updated_at,nullzero,notnull,default:current_timestamp"`
}

type observabilityDataSetState2026051701 struct {
	bun.BaseModel `bun:"table:observability_data_set_states"`

	LocalDataSetID   int64          `bun:"local_data_set_id,pk"`
	BucketID         int64          `bun:"bucket_id,notnull"`
	BucketName       string         `bun:"bucket_name,notnull"`
	ProviderID       string         `bun:"provider_id,type:text,notnull"`
	ChainDataSetID   *string        `bun:"chain_data_set_id,type:text"`
	ClientDataSetID  *string        `bun:"client_data_set_id,type:text"`
	LocalStatus      string         `bun:"local_status,notnull"`
	Status           string         `bun:"status,notnull"`
	ReasonCodes      []string       `bun:"reason_codes,type:jsonb,notnull"`
	ActivePieceCount *int64         `bun:"active_piece_count,nullzero"`
	LastCheckedAt    time.Time      `bun:"last_checked_at,nullzero,notnull"`
	LastError        *string        `bun:"last_error,nullzero"`
	Evidence         map[string]any `bun:"evidence_json,type:jsonb,notnull"`
	CreatedAt        time.Time      `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	UpdatedAt        time.Time      `bun:"updated_at,nullzero,notnull,default:current_timestamp"`
}

func up2026051701ObservabilityStates(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewCreateTable().
		Model((*observabilityProviderState2026051701)(nil)).
		IfNotExists().
		ColumnExpr("CONSTRAINT chk_observability_provider_status CHECK (status IN ('available', 'degraded', 'unavailable', 'unknown'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating observability_provider_states table: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*observabilityProviderState2026051701)(nil)).
		Index("idx_observability_provider_states_status").
		Column("status", "last_checked_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating observability provider status index: %w", err)
	}

	if _, err := db.NewCreateTable().
		Model((*observabilityDataSetState2026051701)(nil)).
		IfNotExists().
		ForeignKey("(local_data_set_id) REFERENCES storage_data_sets(id) ON UPDATE CASCADE ON DELETE CASCADE").
		ColumnExpr("CONSTRAINT chk_observability_data_set_status CHECK (status IN ('available', 'degraded', 'unavailable', 'unknown'))").
		Exec(ctx); err != nil {
		return fmt.Errorf("creating observability_data_set_states table: %w", err)
	}

	if _, err := db.NewCreateIndex().
		Model((*observabilityDataSetState2026051701)(nil)).
		Index("idx_observability_data_set_states_bucket_status").
		Column("bucket_id", "status", "last_checked_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating observability data set bucket status index: %w", err)
	}
	if _, err := db.NewCreateIndex().
		Model((*observabilityDataSetState2026051701)(nil)).
		Index("idx_observability_data_set_states_provider_status").
		Column("provider_id", "status", "last_checked_at").
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("creating observability data set provider status index: %w", err)
	}

	return nil
}

func down2026051701ObservabilityStates(ctx context.Context, db *bun.DB) error {
	if _, err := db.NewDropTable().
		Model((*observabilityDataSetState2026051701)(nil)).
		IfExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("dropping observability_data_set_states table: %w", err)
	}
	if _, err := db.NewDropTable().
		Model((*observabilityProviderState2026051701)(nil)).
		IfExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("dropping observability_provider_states table: %w", err)
	}
	return nil
}
