package migrations

import (
	"context"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// storageContent2026090101 is the identity of one bucket-scoped byte payload.
// Durability, pipeline state and ingress progress are deliberately absent: the
// first two are functions of the copy rows and stay queries, while progress
// belongs to the concrete ingress transfer that produced it.
type storageContent2026090101 struct {
	bun.BaseModel `bun:"table:storage_contents"`

	ID                int64   `bun:",pk,autoincrement,identity"`
	BucketID          int64   `bun:",notnull"`
	Checksum          string  `bun:"type:text,notnull"`
	ContentSize       int64   `bun:",notnull"`
	PieceCID          *string `bun:"type:text"`
	RequestedCopies   int     `bun:"type:integer,notnull"`
	ErrorMessage      *string `bun:"type:text"`
	AcceptedAt        *time.Time
	CleanupGeneration int64 `bun:",notnull,default:0"`
	CleanupTaskID     *int64
	CreatedAt         time.Time `bun:",notnull"`
	UpdatedAt         time.Time `bun:",notnull"`
}

type storageDataSet2026090101 struct {
	bun.BaseModel `bun:"table:storage_data_sets"`

	ID                   int64   `bun:",pk,autoincrement,identity"`
	BucketID             int64   `bun:",notnull"`
	ProviderID           string  `bun:"type:text,notnull"`
	CopyIndex            int     `bun:"type:integer,notnull"`
	Generation           int64   `bun:",notnull,default:1"`
	IsCurrent            bool    `bun:",notnull"`
	DataSetID            *string `bun:"type:text"`
	ClientDataSetID      *string `bun:"type:text"`
	Status               string  `bun:"type:text,notnull,default:'pending'"`
	CreateTransactionID  *string `bun:"type:text"`
	CreateStatusURL      *string `bun:"type:text"`
	CreatedByContentID   *int64
	LastUsedContentID    *int64
	LastError            *string `bun:"type:text"`
	EnsureTaskID         *int64
	RetirementGeneration int64 `bun:",notnull,default:0"`
	RetirementTaskID     *int64
	CreatedAt            time.Time `bun:",notnull"`
	UpdatedAt            time.Time `bun:",notnull"`
}

type storageCopy2026090101 struct {
	bun.BaseModel `bun:"table:storage_copies"`

	ID        int64 `bun:",pk,autoincrement,identity"`
	ContentID int64 `bun:",notnull"`
	BucketID  int64 `bun:",notnull"`
	// ContentSize repeats storage_contents.content_size so the ingress bound
	// stays a local CHECK. A composite foreign key keeps the copy from
	// drifting away from the content it transfers.
	ContentSize        int64   `bun:",notnull"`
	StorageDataSetID   int64   `bun:",notnull"`
	CopyIndex          int     `bun:"type:integer,notnull"`
	ProviderID         string  `bun:"type:text,notnull"`
	PieceID            *string `bun:"type:text"`
	TransferMethod     string  `bun:"type:text,notnull"`
	Status             string  `bun:"type:text,notnull,default:'pending'"`
	RetrievalURL       *string `bun:"type:text"`
	CommitExtraDataHex *string `bun:"type:text"`
	CommitReadyAt      *time.Time
	// The confirmed commit attempt this copy projects. Its status is repeated so
	// a composite foreign key can require the referenced attempt to be
	// confirmed, which is what keeps the projection from drifting.
	ConfirmedAttemptID      *string `bun:"type:text"`
	ConfirmedAttemptStatus  *string `bun:"type:text"`
	IngressBytesTransferred int64   `bun:",notnull,default:0"`
	IngressStoreAttempt     int     `bun:"type:integer,notnull,default:0"`
	ProgressUpdatedAt       *time.Time
	WorkGeneration          int64 `bun:",notnull,default:0"`
	ActiveTaskID            *int64
	LastError               *string   `bun:"type:text"`
	CreatedAt               time.Time `bun:",notnull"`
	UpdatedAt               time.Time `bun:",notnull"`
}

type storageCommitAttempt2026090101 struct {
	bun.BaseModel `bun:"table:storage_commit_attempts"`

	AttemptID              string  `bun:"type:text,pk"`
	ContentID              int64   `bun:",notnull"`
	StorageDataSetID       int64   `bun:",notnull"`
	Status                 string  `bun:"type:text,notnull,default:'reserved'"`
	ExtraDataHex           *string `bun:"type:text"`
	TransactionID          *string `bun:"type:text"`
	SubmissionJSON         *string `bun:"type:text"`
	ConfirmedTransactionID *string `bun:"type:text"`
	AttentionCode          *string `bun:"type:text"`
	AttentionAt            *time.Time
	ReleaseReason          *string `bun:"type:text"`
	LastError              *string `bun:"type:text"`
	AttemptedAt            *time.Time
	ResolvedAt             *time.Time
	CreatedAt              time.Time `bun:",notnull"`
	UpdatedAt              time.Time `bun:",notnull"`
}

// storagePullAttempt2026090101 is the ledger of provider-side copy requests.
// Every source column is NOT NULL, which replaces the all-or-nothing check that
// used to guard five nullable columns on the copy.
type storagePullAttempt2026090101 struct {
	bun.BaseModel `bun:"table:storage_pull_attempts"`

	AttemptID          string    `bun:"type:text,pk"`
	ContentID          int64     `bun:",notnull"`
	StorageDataSetID   int64     `bun:",notnull"`
	Status             string    `bun:"type:text,notnull"`
	SourceProviderID   string    `bun:"type:text,notnull"`
	SourceDataSetID    string    `bun:"type:text,notnull"`
	SourcePieceID      string    `bun:"type:text,notnull"`
	SourcePieceCID     string    `bun:"type:text,notnull"`
	SourceRetrievalURL string    `bun:"type:text,notnull"`
	LastError          *string   `bun:"type:text"`
	AttemptedAt        time.Time `bun:",notnull"`
	ResolvedAt         *time.Time
	CreatedAt          time.Time `bun:",notnull"`
	UpdatedAt          time.Time `bun:",notnull"`
}

type storageReplacement2026090101 struct {
	bun.BaseModel `bun:"table:storage_replacements"`

	ID                  int64   `bun:",pk,autoincrement,identity"`
	BucketID            int64   `bun:",notnull"`
	CopyIndex           int     `bun:"type:integer,notnull"`
	SourceDataSetID     int64   `bun:",notnull"`
	TargetDataSetID     int64   `bun:",notnull"`
	SelectionMode       string  `bun:"type:text,notnull"`
	RequestedProviderID *string `bun:"type:text"`
	ClientRequestID     string  `bun:"type:text,notnull"`
	Status              string  `bun:"type:text,notnull"`
	WaitReason          *string `bun:"type:text"`
	FailureReason       *string `bun:"type:text"`
	LastError           *string `bun:"type:text"`
	ItemsTotal          int     `bun:"type:integer,notnull,default:0"`
	ItemsCopied         int     `bun:"type:integer,notnull,default:0"`
	SeedCursorContentID int64   `bun:",notnull,default:0"`
	SeedingComplete     bool    `bun:",notnull,default:false"`
	TaskGeneration      int64   `bun:",notnull,default:1"`
	TaskID              *int64
	SupersededByID      *int64
	CreatedAt           time.Time `bun:",notnull"`
	UpdatedAt           time.Time `bun:",notnull"`
}

// storageDataSetTermination2026090101 records one data set's end of term. A
// replacement terminates the source it replaces and, once superseded, the
// target it abandoned; a third kind of termination adds a row here instead of
// another repeated column group on storage_replacements.
type storageDataSetTermination2026090101 struct {
	bun.BaseModel `bun:"table:storage_data_set_terminations"`

	ID            int64  `bun:",pk,autoincrement,identity"`
	ReplacementID int64  `bun:",notnull"`
	Role          string `bun:"type:text,notnull"`
	// Exactly one of the data set columns is set, chosen by role. Each carries a
	// composite foreign key back to the matching column on the replacement, so a
	// row cannot name a data set the replacement never held in that role.
	SourceDataSetID          *int64
	AbandonedTargetDataSetID *int64
	TxHash                   *string   `bun:"type:text"`
	Epoch                    int64     `bun:",notnull"`
	CreatedAt                time.Time `bun:",notnull"`
	UpdatedAt                time.Time `bun:",notnull"`
}

func storageDataSetTerminationTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_data_set_terminations",
		model: (*storageDataSetTermination2026090101)(nil),
		constraints: []string{
			"CONSTRAINT uq_storage_data_set_terminations_role UNIQUE (replacement_id, role)",
			"CONSTRAINT chk_storage_data_set_terminations_role CHECK (role IN ('source', 'abandoned_target'))",
			"CONSTRAINT chk_storage_data_set_terminations_subject CHECK ((role = 'source') = (source_data_set_id IS NOT NULL) AND (role = 'abandoned_target') = (abandoned_target_data_set_id IS NOT NULL))",
			"CONSTRAINT chk_storage_data_set_terminations_epoch CHECK (epoch >= 0)",
			"CONSTRAINT chk_storage_data_set_terminations_tx_hash CHECK (tx_hash IS NULL OR tx_hash <> '')",
		},
		foreignKeys: []string{
			"(replacement_id) REFERENCES storage_replacements (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(replacement_id, source_data_set_id) REFERENCES storage_replacements (id, source_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(replacement_id, abandoned_target_data_set_id) REFERENCES storage_replacements (id, target_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

type storageReplacementItem2026090101 struct {
	bun.BaseModel `bun:"table:storage_replacement_items"`

	ID            int64 `bun:",pk,autoincrement,identity"`
	ReplacementID int64 `bun:",notnull"`
	ContentID     int64 `bun:",notnull"`
	// TargetDataSetID is written when the item is seeded, together with the
	// pending copy it names. A nullable column would make the composite foreign
	// key below skip validation entirely whenever it was unset.
	TargetDataSetID int64     `bun:",notnull"`
	Status          string    `bun:"type:text,notnull,default:'pending'"`
	LastError       *string   `bun:"type:text"`
	CreatedAt       time.Time `bun:",notnull"`
	UpdatedAt       time.Time `bun:",notnull"`
}

type storageCleanupCopy2026090101 struct {
	bun.BaseModel `bun:"table:storage_cleanup_copies"`

	ID               int64   `bun:",pk,autoincrement,identity"`
	ContentID        int64   `bun:",notnull"`
	BucketID         int64   `bun:",notnull"`
	CopyIndex        int     `bun:"type:integer,notnull"`
	ProviderID       string  `bun:"type:text,notnull"`
	StorageDataSetID int64   `bun:",notnull"`
	DataSetID        *string `bun:"type:text"`
	ClientDataSetID  *string `bun:"type:text"`
	PieceID          string  `bun:"type:text,notnull"`
	PieceCID         string  `bun:"type:text,notnull"`
	RetrievalURL     *string `bun:"type:text"`
	Status           string  `bun:"type:text,notnull,default:'pending'"`
	DeleteTxHash     *string `bun:"type:text"`
	LastError        *string `bun:"type:text"`
	ScheduledAt      *time.Time
	RemovedAt        *time.Time
	CreatedAt        time.Time `bun:",notnull"`
	UpdatedAt        time.Time `bun:",notnull"`
}

func createStorageSchema(ctx context.Context, db bun.IDB) error {
	checksumCheck := "length(checksum) = 64 AND checksum NOT GLOB '*[^0-9a-f]*'"
	if db.Dialect().Name() == dialect.PG {
		checksumCheck = "checksum ~ '^[0-9a-f]{64}$'"
	}
	tables := []initialTableSpec{
		{
			name:  "storage_contents",
			model: (*storageContent2026090101)(nil),
			constraints: []string{
				// Content dedup is a unique-key lookup, not an index scan.
				"CONSTRAINT uq_storage_contents_bytes UNIQUE (bucket_id, checksum, content_size)",
				// Candidate keys for the composite foreign keys that pin
				// denormalized identity on object_versions and storage_copies.
				"CONSTRAINT uq_storage_contents_id_bucket UNIQUE (id, bucket_id)",
				"CONSTRAINT uq_storage_contents_addr UNIQUE (id, bucket_id, content_size)",
				"CONSTRAINT chk_storage_contents_identity CHECK ((" + checksumCheck + ") AND (piece_cid IS NULL OR piece_cid <> ''))",
				"CONSTRAINT chk_storage_contents_content_size CHECK (content_size >= 0)",
				"CONSTRAINT chk_storage_contents_requested_copies CHECK (requested_copies BETWEEN 1 AND 8)",
				"CONSTRAINT chk_storage_contents_cleanup_generation CHECK (cleanup_generation >= 0)",
			},
			foreignKeys: []string{
				"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(cleanup_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
		},
		{
			name:  "storage_data_sets",
			model: (*storageDataSet2026090101)(nil),
			constraints: []string{
				"CONSTRAINT uq_storage_data_sets_id_bucket_slot UNIQUE (id, bucket_id, copy_index)",
				"CONSTRAINT uq_storage_data_sets_identity UNIQUE (id, bucket_id, copy_index, provider_id)",
				"CONSTRAINT chk_storage_data_sets_identity CHECK (provider_id <> '' AND (data_set_id IS NULL OR data_set_id <> '') AND (client_data_set_id IS NULL OR client_data_set_id <> '') AND (create_transaction_id IS NULL OR create_transaction_id <> '') AND (create_status_url IS NULL OR create_status_url <> ''))",
				// The replica slot is a row, so the index is a foreign key rather than a range check.
				"CONSTRAINT fk_storage_data_sets_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"CONSTRAINT chk_storage_data_sets_generation CHECK (generation >= 1 AND retirement_generation >= 0)",
				"CONSTRAINT chk_storage_data_sets_status CHECK (status IN ('pending', 'creating', 'ready', 'failed', 'draining', 'retired'))",
				"CONSTRAINT chk_storage_data_sets_ready_identity CHECK (status <> 'ready' OR data_set_id IS NOT NULL)",
				"CONSTRAINT chk_storage_data_sets_current_shape CHECK (status NOT IN ('failed', 'draining', 'retired') OR is_current = FALSE)",
			},
			foreignKeys: []string{
				"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(created_by_content_id, bucket_id) REFERENCES storage_contents (id, bucket_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(last_used_content_id, bucket_id) REFERENCES storage_contents (id, bucket_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(ensure_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(retirement_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
		},
		{
			name:  "storage_copies",
			model: (*storageCopy2026090101)(nil),
			constraints: []string{
				"CONSTRAINT uq_storage_copies_content_data_set UNIQUE (content_id, storage_data_set_id)",
				// The replica slot is a row, so the index is a foreign key rather than a range check.
				"CONSTRAINT fk_storage_copies_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"CONSTRAINT chk_storage_copies_work_generation CHECK (work_generation >= 0)",
				"CONSTRAINT chk_storage_copies_status CHECK (status IN ('pending', 'piece_ready', 'committing', 'committed', 'failed'))",
				// The projected status is pinned to a literal so the composite
				// foreign key below can only ever reach a confirmed attempt.
				"CONSTRAINT chk_storage_copies_confirmed_attempt_status CHECK (confirmed_attempt_status IS NULL OR confirmed_attempt_status = 'confirmed')",
				// Committed and "has confirmed evidence" are the same fact. The
				// foreign key alone would still allow a committed copy with no
				// evidence at all, so both directions are stated here.
				"CONSTRAINT chk_storage_copies_committed_evidence CHECK ((status = 'committed') = (confirmed_attempt_id IS NOT NULL))",
				"CONSTRAINT chk_storage_copies_confirmed_attempt_shape CHECK ((confirmed_attempt_id IS NULL AND confirmed_attempt_status IS NULL) OR (confirmed_attempt_id IS NOT NULL AND confirmed_attempt_id <> '' AND confirmed_attempt_status IS NOT NULL))",
				"CONSTRAINT chk_storage_copies_transfer_method CHECK (transfer_method IN ('ingress', 'peer_pull'))",
				"CONSTRAINT chk_storage_copies_optional_identity CHECK (provider_id <> '' AND (piece_id IS NULL OR piece_id <> '') AND (retrieval_url IS NULL OR retrieval_url <> '') AND (commit_extra_data_hex IS NULL OR commit_extra_data_hex <> ''))",
				"CONSTRAINT chk_storage_copies_committed_shape CHECK (status <> 'committed' OR (piece_id IS NOT NULL AND piece_id <> '' AND retrieval_url IS NOT NULL AND retrieval_url <> ''))",
				"CONSTRAINT chk_storage_copies_commit_ready CHECK (commit_ready_at IS NULL OR status IN ('piece_ready', 'committing', 'committed'))",
				"CONSTRAINT chk_storage_copies_content_size CHECK (content_size >= 0)",
				// Ingress progress belongs to the transfer that produces it, so
				// only an ingress copy may carry it.
				"CONSTRAINT chk_storage_copies_ingress_progress CHECK (transfer_method = 'ingress' OR (ingress_bytes_transferred = 0 AND ingress_store_attempt = 0 AND progress_updated_at IS NULL))",
				"CONSTRAINT chk_storage_copies_ingress_bytes CHECK (ingress_bytes_transferred >= 0 AND ingress_bytes_transferred <= content_size)",
				"CONSTRAINT chk_storage_copies_ingress_attempt CHECK (ingress_store_attempt >= 0)",
			},
			foreignKeys: []string{
				"(content_id, bucket_id, content_size) REFERENCES storage_contents (id, bucket_id, content_size) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(storage_data_set_id, bucket_id, copy_index, provider_id) REFERENCES storage_data_sets (id, bucket_id, copy_index, provider_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
				"(active_task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			},
			forwardForeignKeys: []initialForwardForeignKey{storageCopyConfirmedAttemptForeignKey2026090101()},
		},
		storageCommitAttemptTable2026090101(),
		storagePullAttemptTable2026090101(),
		storageReplacementTable2026090101(),
		storageDataSetTerminationTable2026090101(),
		storageReplacementItemTable2026090101(),
		storageCleanupCopyTable2026090101(),
	}
	for _, table := range tables {
		if err := createInitialTable(ctx, db, table); err != nil {
			return err
		}
	}
	if err := createInitialIndexes(ctx, db, storageIndexes2026090101()...); err != nil {
		return err
	}
	// storage_copies was created before storage_commit_attempts existed, so
	// PostgreSQL takes the projection constraint here.
	return addForwardForeignKey(ctx, db, "storage_copies", storageCopyConfirmedAttemptForeignKey2026090101())
}

// storageCopyConfirmedAttemptForeignKey2026090101 welds the copy's committed
// state to the ledger row that proves it. storage_copies is created before
// storage_commit_attempts, so the constraint is forward-declared.
func storageCopyConfirmedAttemptForeignKey2026090101() initialForwardForeignKey {
	return initialForwardForeignKey{
		name:       "fk_storage_copies_confirmed_attempt",
		definition: "(confirmed_attempt_id, confirmed_attempt_status) REFERENCES storage_commit_attempts (attempt_id, status) ON UPDATE RESTRICT ON DELETE RESTRICT",
	}
}

func storageCommitAttemptTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:        "storage_commit_attempts",
		jsonColumns: initialJSONColumns("storage_commit_attempts"),
		model:       (*storageCommitAttempt2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_storage_commit_attempts_identity CHECK (attempt_id <> '' AND (extra_data_hex IS NULL OR extra_data_hex <> '') AND (transaction_id IS NULL OR transaction_id <> '') AND (submission_json IS NULL OR submission_json <> '') AND (confirmed_transaction_id IS NULL OR confirmed_transaction_id <> '') AND (attention_code IS NULL OR attention_code <> '') AND (release_reason IS NULL OR release_reason <> ''))",
			"CONSTRAINT chk_storage_commit_attempts_status CHECK (status IN ('reserved', 'attempted', 'confirmed', 'released', 'rejected'))",
			// Candidate key for the copy's confirmed-attempt projection.
			"CONSTRAINT uq_storage_commit_attempts_status UNIQUE (attempt_id, status)",
			"CONSTRAINT chk_storage_commit_attempts_resolution CHECK ((status IN ('reserved', 'attempted') AND resolved_at IS NULL) OR (status IN ('confirmed', 'released', 'rejected') AND resolved_at IS NOT NULL))",
			`CONSTRAINT chk_storage_commit_attempts_evidence_shape CHECK (
				(status = 'reserved' AND attempted_at IS NULL AND extra_data_hex IS NULL AND transaction_id IS NULL AND submission_json IS NULL AND confirmed_transaction_id IS NULL AND attention_code IS NULL AND attention_at IS NULL AND last_error IS NULL)
				OR (status = 'attempted' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND confirmed_transaction_id IS NULL AND last_error IS NULL)
				OR (status = 'confirmed' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND transaction_id IS NOT NULL AND confirmed_transaction_id IS NOT NULL AND last_error IS NULL)
				OR (status = 'released' AND confirmed_transaction_id IS NULL AND last_error IS NULL AND ((attempted_at IS NULL AND extra_data_hex IS NULL AND transaction_id IS NULL AND submission_json IS NULL AND attention_code IS NULL AND attention_at IS NULL) OR (attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL)))
				OR (status = 'rejected' AND attempted_at IS NOT NULL AND extra_data_hex IS NOT NULL AND confirmed_transaction_id IS NULL AND last_error IS NOT NULL AND last_error <> '')
			)`,
			"CONSTRAINT chk_storage_commit_attempts_submission CHECK (submission_json IS NULL OR transaction_id IS NOT NULL)",
			"CONSTRAINT chk_storage_commit_attempts_attention CHECK ((attention_code IS NULL AND attention_at IS NULL) OR (attention_code IS NOT NULL AND attention_at IS NOT NULL AND attempted_at IS NOT NULL))",
			"CONSTRAINT chk_storage_commit_attempts_release CHECK ((status = 'released' AND release_reason IS NOT NULL) OR (status <> 'released' AND release_reason IS NULL))",
		},
		foreignKeys: []string{
			"(content_id, storage_data_set_id) REFERENCES storage_copies (content_id, storage_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

func storagePullAttemptTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_pull_attempts",
		model: (*storagePullAttempt2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_storage_pull_attempts_identity CHECK (attempt_id <> '' AND source_provider_id <> '' AND source_data_set_id <> '' AND source_piece_id <> '' AND source_piece_cid <> '' AND source_retrieval_url <> '')",
			"CONSTRAINT chk_storage_pull_attempts_status CHECK (status IN ('attempted', 'abandoned'))",
			// An error only makes sense on a request nobody will observe again;
			// reopening a copy carries no error string.
			"CONSTRAINT chk_storage_pull_attempts_error CHECK (last_error IS NULL OR (status = 'abandoned' AND last_error <> ''))",
			"CONSTRAINT chk_storage_pull_attempts_abandoned CHECK (status <> 'abandoned' OR resolved_at IS NOT NULL)",
		},
		foreignKeys: []string{
			"(content_id, storage_data_set_id) REFERENCES storage_copies (content_id, storage_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

func storageReplacementTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_replacements",
		model: (*storageReplacement2026090101)(nil),
		constraints: []string{
			"CONSTRAINT uq_storage_replacements_id_source UNIQUE (id, source_data_set_id)",
			"CONSTRAINT uq_storage_replacements_id_target UNIQUE (id, target_data_set_id)",
			"CONSTRAINT chk_storage_replacements_identity CHECK (client_request_id <> '' AND (requested_provider_id IS NULL OR requested_provider_id <> ''))",
			// The replica slot is a row, so the index is a foreign key rather than a range check.
			"CONSTRAINT fk_storage_replacements_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"CONSTRAINT chk_storage_replacements_selection_mode CHECK (selection_mode IN ('automatic', 'manual'))",
			"CONSTRAINT chk_storage_replacements_status CHECK (status IN ('preparing_target', 'migrating', 'waiting', 'retiring', 'cleanup_attention', 'failed', 'completed', 'superseded'))",
			"CONSTRAINT chk_storage_replacements_wait_reason CHECK (wait_reason IS NULL OR wait_reason <> '')",
			"CONSTRAINT chk_storage_replacements_failure_reason CHECK (failure_reason IS NULL OR failure_reason <> '')",
			"CONSTRAINT chk_storage_replacements_client_request_id CHECK (length(client_request_id) BETWEEN 1 AND 128)",
			"CONSTRAINT chk_storage_replacements_distinct_data_sets CHECK (source_data_set_id <> target_data_set_id)",
			"CONSTRAINT chk_storage_replacements_items CHECK (items_total >= 0 AND items_copied >= 0 AND items_copied <= items_total)",
			"CONSTRAINT chk_storage_replacements_generation CHECK (task_generation >= 1)",
		},
		foreignKeys: []string{
			"(bucket_id) REFERENCES buckets (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(source_data_set_id, bucket_id, copy_index) REFERENCES storage_data_sets (id, bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(target_data_set_id, bucket_id, copy_index) REFERENCES storage_data_sets (id, bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(task_id) REFERENCES tasks (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(superseded_by_id) REFERENCES storage_replacements (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

func storageReplacementItemTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_replacement_items",
		model: (*storageReplacementItem2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_storage_replacement_items_status CHECK (status IN ('pending', 'copied', 'cancelled', 'attention'))",
			"CONSTRAINT uq_storage_replacement_items_content UNIQUE (replacement_id, content_id)",
		},
		foreignKeys: []string{
			"(replacement_id) REFERENCES storage_replacements (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(content_id) REFERENCES storage_contents (id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(replacement_id, target_data_set_id) REFERENCES storage_replacements (id, target_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(content_id, target_data_set_id) REFERENCES storage_copies (content_id, storage_data_set_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

func storageCleanupCopyTable2026090101() initialTableSpec {
	return initialTableSpec{
		name:  "storage_cleanup_copies",
		model: (*storageCleanupCopy2026090101)(nil),
		constraints: []string{
			"CONSTRAINT chk_storage_cleanup_copies_identity CHECK (provider_id <> '' AND piece_id <> '' AND piece_cid <> '' AND (data_set_id IS NULL OR data_set_id <> '') AND (client_data_set_id IS NULL OR client_data_set_id <> '') AND (delete_tx_hash IS NULL OR delete_tx_hash <> ''))",
			// The replica slot is a row, so the index is a foreign key rather than a range check.
			"CONSTRAINT fk_storage_cleanup_copies_replica_slot FOREIGN KEY (bucket_id, copy_index) REFERENCES bucket_replica_slots (bucket_id, copy_index) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"CONSTRAINT chk_storage_cleanup_copies_status CHECK (status IN ('pending', 'delete_scheduled', 'removed', 'failed', 'unsupported'))",
			"CONSTRAINT chk_storage_cleanup_copies_delete_scheduled CHECK (status <> 'delete_scheduled' OR (delete_tx_hash IS NOT NULL AND scheduled_at IS NOT NULL))",
			"CONSTRAINT uq_storage_cleanup_copies_physical UNIQUE (content_id, storage_data_set_id, piece_id)",
		},
		foreignKeys: []string{
			"(content_id, bucket_id) REFERENCES storage_contents (id, bucket_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
			"(storage_data_set_id, bucket_id, copy_index, provider_id) REFERENCES storage_data_sets (id, bucket_id, copy_index, provider_id) ON UPDATE RESTRICT ON DELETE RESTRICT",
		},
	}
}

func storageIndexes2026090101() []initialIndexSpec {
	return []initialIndexSpec{
		{name: "idx_storage_contents_bucket_id", table: "storage_contents", columns: []string{"bucket_id", "id"}},
		{name: "idx_storage_contents_cleanup_task", table: "storage_contents", columns: []string{"cleanup_task_id"}, where: "cleanup_task_id IS NOT NULL", unique: true},
		{name: "idx_storage_data_sets_provider_data_set", table: "storage_data_sets", columns: []string{"provider_id", "data_set_id"}, where: "data_set_id IS NOT NULL", unique: true},
		{name: "idx_storage_data_sets_bucket_copy_current", table: "storage_data_sets", columns: []string{"bucket_id", "copy_index"}, where: "is_current = TRUE", unique: true},
		{name: "idx_storage_data_sets_bucket_copy_generation", table: "storage_data_sets", columns: []string{"bucket_id", "copy_index", "generation"}, unique: true},
		{name: "idx_storage_data_sets_bucket_provider_active", table: "storage_data_sets", columns: []string{"bucket_id", "provider_id"}, where: "status <> 'retired'", unique: true},
		{name: "idx_storage_data_sets_created_by_content", table: "storage_data_sets", columns: []string{"created_by_content_id"}},
		{name: "idx_storage_data_sets_last_used_content", table: "storage_data_sets", columns: []string{"last_used_content_id"}},
		{name: "idx_storage_data_sets_ensure_task", table: "storage_data_sets", columns: []string{"ensure_task_id"}, where: "ensure_task_id IS NOT NULL", unique: true},
		{name: "idx_storage_data_sets_retirement_task", table: "storage_data_sets", columns: []string{"retirement_task_id"}, where: "retirement_task_id IS NOT NULL", unique: true},
		{name: "idx_storage_copies_content_slot", table: "storage_copies", columns: []string{"content_id", "copy_index"}},
		{name: "idx_storage_copies_data_set_identity", table: "storage_copies", columns: []string{"storage_data_set_id", "bucket_id", "copy_index", "provider_id"}},
		{name: "idx_storage_copies_content_transfer_method_index", table: "storage_copies", columns: []string{"content_id", "transfer_method", "copy_index"}},
		{name: "idx_storage_copies_ingress_content", table: "storage_copies", columns: []string{"content_id"}, where: "transfer_method = 'ingress'", unique: true},
		{name: "idx_storage_copies_status_data_set_content", table: "storage_copies", columns: []string{"status", "storage_data_set_id", "content_id"}},
		{name: "idx_storage_copies_status_piece_identity_content", table: "storage_copies", columns: []string{"status", "provider_id", "piece_id", "content_id"}},
		{name: "idx_storage_copies_commit_ready", table: "storage_copies", columns: []string{"storage_data_set_id", "commit_ready_at", "id"}, where: "status = 'piece_ready' AND commit_ready_at IS NOT NULL"},
		{name: "idx_storage_copies_active_task", table: "storage_copies", columns: []string{"active_task_id"}, where: "active_task_id IS NOT NULL", unique: true},
		{name: "idx_storage_commit_attempts_unresolved_copy", table: "storage_commit_attempts", columns: []string{"content_id", "storage_data_set_id"}, where: "resolved_at IS NULL", unique: true},
		{name: "idx_storage_commit_attempts_unresolved_data_set", table: "storage_commit_attempts", columns: []string{"storage_data_set_id", "created_at", "attempt_id"}, where: "resolved_at IS NULL"},
		{name: "idx_storage_commit_attempts_copy_history", table: "storage_commit_attempts", columns: []string{"content_id", "storage_data_set_id", "created_at DESC", "attempt_id"}},
		{name: "idx_storage_cleanup_copies_replica_slot", table: "storage_cleanup_copies", columns: []string{"bucket_id", "copy_index"}},
		{name: "idx_storage_copies_confirmed_attempt", table: "storage_copies", columns: []string{"confirmed_attempt_id", "confirmed_attempt_status"}, where: "confirmed_attempt_id IS NOT NULL"},
		{name: "idx_storage_copies_replica_slot", table: "storage_copies", columns: []string{"bucket_id", "copy_index"}},
		// One unresolved attempt per copy: a second source can only be tried
		// after the first is abandoned.
		{name: "idx_storage_pull_attempts_unresolved_copy", table: "storage_pull_attempts", columns: []string{"content_id", "storage_data_set_id"}, where: "resolved_at IS NULL", unique: true},
		{name: "idx_storage_pull_attempts_copy_history", table: "storage_pull_attempts", columns: []string{"content_id", "storage_data_set_id", "created_at DESC", "attempt_id"}},
		{name: "idx_storage_replacements_active_source", table: "storage_replacements", columns: []string{"source_data_set_id"}, where: "status NOT IN ('completed', 'superseded')", unique: true},
		{name: "idx_storage_replacements_active_target", table: "storage_replacements", columns: []string{"target_data_set_id"}, where: "status NOT IN ('completed', 'superseded')", unique: true},
		{name: "idx_storage_replacements_active_bucket_slot", table: "storage_replacements", columns: []string{"bucket_id", "copy_index"}, where: "status NOT IN ('completed', 'superseded')", unique: true},
		{name: "idx_storage_replacements_source_identity", table: "storage_replacements", columns: []string{"source_data_set_id", "bucket_id", "copy_index"}},
		{name: "idx_storage_replacements_target_identity", table: "storage_replacements", columns: []string{"target_data_set_id", "bucket_id", "copy_index"}},
		{name: "idx_storage_replacements_superseded_by", table: "storage_replacements", columns: []string{"superseded_by_id"}, where: "superseded_by_id IS NOT NULL"},
		{name: "idx_storage_replacements_bucket_slot", table: "storage_replacements", columns: []string{"bucket_id", "copy_index", "id"}},
		{name: "idx_storage_replacements_bucket_request", table: "storage_replacements", columns: []string{"bucket_id", "client_request_id"}, unique: true},
		{name: "idx_storage_replacements_task", table: "storage_replacements", columns: []string{"task_id"}, where: "task_id IS NOT NULL", unique: true},
		{name: "idx_storage_replacement_items_state", table: "storage_replacement_items", columns: []string{"replacement_id", "status", "id"}},
		{name: "idx_storage_replacement_items_content_id", table: "storage_replacement_items", columns: []string{"content_id"}},
		{name: "idx_storage_replacement_items_target_copy", table: "storage_replacement_items", columns: []string{"content_id", "target_data_set_id"}},
		{name: "idx_storage_cleanup_copies_content_status", table: "storage_cleanup_copies", columns: []string{"content_id", "status", "id"}},
		{name: "idx_storage_cleanup_copies_data_set_identity", table: "storage_cleanup_copies", columns: []string{"storage_data_set_id", "bucket_id", "copy_index", "provider_id"}},
		{name: "idx_storage_cleanup_copies_status_scheduled", table: "storage_cleanup_copies", columns: []string{"status", "scheduled_at", "id"}},
	}
}
