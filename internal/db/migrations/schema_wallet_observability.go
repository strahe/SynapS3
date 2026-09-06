package migrations

import (
	"context"
	"encoding/json"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type walletOperation2026090101 struct {
	bun.BaseModel `bun:"table:wallet_operations"`

	ID                   int64   `bun:",pk,autoincrement,identity"`
	Type                 string  `bun:"type:text,notnull"`
	ClientRequestID      string  `bun:"type:text,notnull"`
	Amount               string  `bun:"type:text,notnull"`
	Status               string  `bun:"type:text,notnull,default:'pending'"`
	TxHash               *string `bun:"type:text"`
	LastError            *string `bun:"type:text"`
	BroadcastAttemptedAt *time.Time
	TaskID               *int64
	StartedAt            *time.Time
	SubmittedAt          *time.Time
	CompletedAt          *time.Time
	CreatedAt            time.Time `bun:",notnull"`
	UpdatedAt            time.Time `bun:",notnull"`
}

func createWalletSchema(ctx context.Context, db bun.IDB) error {
	amountCheck := `((type = 'approve' AND amount = '0') OR (type IN ('fund', 'withdraw') AND amount GLOB '[1-9]*' AND amount NOT GLOB '*[^0-9]*'))`
	if db.Dialect().Name() == dialect.PG {
		amountCheck = `((type = 'approve' AND amount = '0') OR (type IN ('fund', 'withdraw') AND amount ~ '^[1-9][0-9]*$'))`
	}
	if err := createInitialTable(ctx, db, initialTableSpec{
		name:  "wallet_operations",
		model: (*walletOperation2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_wallet_operations_identity CHECK (client_request_id <> '' AND amount <> '' AND (tx_hash IS NULL OR tx_hash <> ''))",
			"CONSTRAINT chk_wallet_operations_type CHECK (type IN ('fund', 'withdraw', 'approve'))",
			"CONSTRAINT chk_wallet_operations_status CHECK (status IN ('pending', 'submitted', 'confirmed', 'failed', 'unknown'))",
			"CONSTRAINT chk_wallet_operations_submitted_shape CHECK (status <> 'submitted' OR (tx_hash IS NOT NULL AND submitted_at IS NOT NULL))",
			"CONSTRAINT chk_wallet_operations_amount CHECK (" + amountCheck + ")",
			// A uint256 in base 10 is at most 78 digits. The column stays text
			// because the value is returned and compared verbatim.
			"CONSTRAINT chk_wallet_operations_amount_length CHECK (length(amount) BETWEEN 1 AND 78)",
		},
		foreignKeys: []string{
			"(task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}); err != nil {
		return err
	}
	return createInitialIndexes(ctx, db,
		initialIndexSpec{name: "idx_wallet_operations_request", table: "wallet_operations", columns: []string{"type", "client_request_id"}, unique: true},
		initialIndexSpec{name: "idx_wallet_operations_status_created", table: "wallet_operations", columns: []string{"status", "created_at", "id"}},
		initialIndexSpec{name: "idx_wallet_operations_recent", table: "wallet_operations", columns: []string{"created_at DESC", "id DESC"}},
		initialIndexSpec{name: "idx_wallet_operations_task", table: "wallet_operations", columns: []string{"task_id"}, where: "task_id IS NOT NULL", unique: true},
	)
}

type observabilityCollectionState2026090101 struct {
	bun.BaseModel `bun:"table:observability_collection_states"`

	CollectionType string    `bun:"type:text,pk"`
	LastCheckedAt  time.Time `bun:",notnull"`
	CreatedAt      time.Time `bun:",notnull"`
	UpdatedAt      time.Time `bun:",notnull"`
}

type observabilityProviderState2026090101 struct {
	bun.BaseModel `bun:"table:observability_provider_states"`

	ProviderID    string          `bun:"type:text,pk"`
	Status        string          `bun:"type:text,notnull"`
	ReasonCodes   json.RawMessage `bun:"type:jsonb,notnull"`
	Active        *bool
	HasPDP        *bool
	ServiceURL    *string         `bun:"type:text"`
	HealthStatus  *string         `bun:"type:text"`
	LastCheckedAt time.Time       `bun:",notnull"`
	LastError     *string         `bun:"type:text"`
	Evidence      json.RawMessage `bun:"evidence_json,type:jsonb,notnull"`
}

type observabilityDataSetState2026090101 struct {
	bun.BaseModel `bun:"table:observability_data_set_states"`

	LocalDataSetID  int64   `bun:",pk"`
	BucketID        int64   `bun:",notnull"`
	CopyIndex       int     `bun:"type:integer,notnull"`
	ProviderID      string  `bun:"type:text,notnull"`
	ChainDataSetID  *string `bun:"type:text"`
	ClientDataSetID *string `bun:"type:text"`
	// The bucket's name and the data set's own status are a join away and can
	// disagree with the authorities that own them, so neither is copied here.
	Status           string          `bun:"type:text,notnull"`
	ReasonCodes      json.RawMessage `bun:"type:jsonb,notnull"`
	ActivePieceCount *int64
	LastCheckedAt    time.Time       `bun:",notnull"`
	LastError        *string         `bun:"type:text"`
	Evidence         json.RawMessage `bun:"evidence_json,type:jsonb,notnull"`
}

func createObservabilitySchema(ctx context.Context, db bun.IDB) error {
	tables := []initialTableSpec{
		{
			name:  "observability_collection_states",
			model: (*observabilityCollectionState2026090101)(nil),
			constraints: []string{
				"CONSTRAINT chk_observability_collection_type CHECK (collection_type IN ('providers', 'data_sets'))",
			},
		},
		{
			name:        "observability_provider_states",
			model:       (*observabilityProviderState2026090101)(nil),
			jsonColumns: initialJSONColumns("observability_provider_states"),
			constraints: []string{
				"CONSTRAINT chk_observability_provider_identity CHECK (provider_id <> '')",
				"CONSTRAINT chk_observability_provider_status CHECK (status IN ('available', 'degraded', 'unavailable', 'unknown'))",
			},
		},
		{
			name:        "observability_data_set_states",
			model:       (*observabilityDataSetState2026090101)(nil),
			jsonColumns: initialJSONColumns("observability_data_set_states"),
			constraints: []string{
				"CONSTRAINT chk_observability_data_set_identity CHECK (provider_id <> '' AND (chain_data_set_id IS NULL OR chain_data_set_id <> '') AND (client_data_set_id IS NULL OR client_data_set_id <> ''))",
				"CONSTRAINT fk_observability_data_set_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"CONSTRAINT chk_observability_data_set_status CHECK (status IN ('available', 'degraded', 'unavailable', 'unknown'))",
			},
			foreignKeys: []string{
				"(local_data_set_id, bucket_id, copy_index, provider_id) REFERENCES storage_data_sets (id, bucket_id, copy_index, provider_id) ON UPDATE RESTRICT ON DELETE CASCADE",
			},
		},
	}
	for _, table := range tables {
		if err := createInitialTable(ctx, db, table); err != nil {
			return err
		}
	}
	return createInitialIndexes(ctx, db,
		initialIndexSpec{name: "idx_observability_provider_states_status", table: "observability_provider_states", columns: []string{"status", "last_checked_at"}},
		initialIndexSpec{name: "idx_observability_data_set_states_bucket_status", table: "observability_data_set_states", columns: []string{"bucket_id", "status", "last_checked_at"}},
		initialIndexSpec{name: "idx_observability_data_set_states_provider_status", table: "observability_data_set_states", columns: []string{"provider_id", "status", "last_checked_at"}},
	)
}
