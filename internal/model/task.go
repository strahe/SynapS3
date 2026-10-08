package model

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/uptrace/bun"
)

// TaskType is the only persistent classification of asynchronous work.
type TaskType string

const (
	TaskTypeBucketProvision               TaskType = "bucket_provision"
	TaskTypeUploadPlan                    TaskType = "upload_plan"
	TaskTypeStorageDataSetEnsure          TaskType = "storage_dataset_ensure"
	TaskTypeStorageTransferPlan           TaskType = "storage_transfer_plan"
	TaskTypeStorageStore                  TaskType = "storage_store"
	TaskTypeStoragePull                   TaskType = "storage_pull"
	TaskTypeStorageCommit                 TaskType = "storage_commit"
	TaskTypeProviderReplacementCoordinate TaskType = "provider_replacement_coordinate"
	TaskTypeCacheCapacityReconcile        TaskType = "cache_capacity_reconcile"
	TaskTypeCacheEvict                    TaskType = "cache_evict"
	TaskTypeCacheReconcileDurability      TaskType = "cache_reconcile_durability"
	TaskTypeStorageCleanup                TaskType = "storage_cleanup"
	TaskTypeStorageDataSetRetire          TaskType = "storage_dataset_retire"
	TaskTypeWalletOperation               TaskType = "wallet_operation"
	TaskTypeObservabilityRefresh          TaskType = "observability_refresh"
	TaskTypeApprovedProviderRefresh       TaskType = "approved_provider_refresh"
	TaskTypeEndorsedProviderRefresh       TaskType = "endorsed_provider_refresh"
	TaskTypeProviderUploadSpeedTest       TaskType = "provider_upload_speed_test"
	TaskTypeGC                            TaskType = "task_gc"
)

// Task subjects that repository logic finds tasks by. Definitions of tasks
// with these subjects derive them from the task input, so the subject always
// names the row the input names.
const (
	TaskSubjectStorageContent       = "storage_content"
	TaskSubjectStorageCopy          = "storage_copy"
	TaskSubjectStorageCommitRequest = "storage_commit_request"
)

// RecurringSystemTaskTypes returns the scheduled maintenance task types.
func RecurringSystemTaskTypes() []TaskType {
	return []TaskType{TaskTypeCacheCapacityReconcile, TaskTypeObservabilityRefresh, TaskTypeApprovedProviderRefresh, TaskTypeEndorsedProviderRefresh}
}

// IsRecurringSystem reports whether the task is a perpetual maintenance loop
// rather than a finite user or domain operation.
func (t TaskType) IsRecurringSystem() bool {
	return slices.Contains(RecurringSystemTaskTypes(), t)
}

// TaskStatus is the complete task lifecycle.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TaskResumeMode controls whether a handler may initiate an external effect.
type TaskResumeMode string

const (
	TaskResumeModeExecute TaskResumeMode = "execute"
	TaskResumeModeRecover TaskResumeMode = "recover"
)

// Task is a permanently retained execution round. Domain tables own external
// effect safety independently of its execution history.
type Task struct {
	bun.BaseModel `bun:"table:tasks"`

	ID             int64      `bun:",pk,autoincrement,identity"`
	Type           TaskType   `bun:"type:text,notnull"`
	IdempotencyKey string     `bun:"type:text,notnull"`
	InputVersion   int        `bun:"type:integer,notnull"`
	InputHash      string     `bun:"type:text,notnull"`
	SubjectType    *string    `bun:"type:text,nullzero"`
	SubjectKey     *string    `bun:"type:text,nullzero"`
	RetryOfTaskID  *int64     `bun:",nullzero"`
	RetryGroupKey  string     `bun:"type:text,notnull"`
	SupersededAt   *time.Time `bun:",nullzero"`

	// Input and Checkpoint live in task_payloads and are projected on read, so
	// renewing a lease never rewrites the JSON a task carries.
	Input      json.RawMessage `bun:",scanonly"`
	Checkpoint json.RawMessage `bun:",scanonly"`
	Policy     json.RawMessage `bun:",scanonly"`
	Runtime    json.RawMessage `bun:",scanonly"`

	Status        TaskStatus     `bun:"type:text,notnull,default:'pending'"`
	ResumeMode    TaskResumeMode `bun:"type:text,notnull,default:'execute'"`
	AvailableAt   time.Time      `bun:",nullzero,notnull"`
	WaitReason    *string        `bun:"type:text,nullzero"`
	RetryCount    int            `bun:"type:integer,notnull,default:0"`
	RetryLimit    *int           `bun:"type:integer,nullzero"`
	FailureReason *string        `bun:"type:text,nullzero"`
	LastError     *string        `bun:"type:text,nullzero"`
	StatusMessage *string        `bun:"type:text,nullzero"`

	CancellationRequestedAt *time.Time `bun:",nullzero"`
	CancellationReason      *string    `bun:"type:text,nullzero"`
	ClaimGeneration         int64      `bun:",notnull,default:0"`
	ClaimedAt               *time.Time `bun:",nullzero"`
	LeaseUntil              *time.Time `bun:",nullzero"`
	StartedAt               *time.Time `bun:",nullzero"`
	FinishedAt              *time.Time `bun:",nullzero"`
	AcknowledgedAt          *time.Time `bun:",nullzero"`
	CreatedAt               time.Time  `bun:",nullzero,notnull"`
	UpdatedAt               time.Time  `bun:",nullzero,notnull"`
	WorkStartedAt           *time.Time `bun:",nullzero"`
}

// TaskPayload carries a task's input and checkpoint JSON. It is a separate row
// because tasks.lease_until is indexed and renewed on every heartbeat, which on
// PostgreSQL rewrites the whole row.
type TaskPayload struct {
	bun.BaseModel `bun:"table:task_payloads"`

	TaskID     int64           `bun:",pk"`
	Input      json.RawMessage `bun:"input_json,type:jsonb,notnull"`
	Checkpoint json.RawMessage `bun:"checkpoint_json,type:jsonb,nullzero"`
	Policy     json.RawMessage `bun:"policy_json,type:jsonb,notnull"`
	Runtime    json.RawMessage `bun:"runtime_json,type:jsonb,notnull"`
}

// TaskRuntime is durable admission evidence, independent of diagnostic events.
type TaskRuntime struct {
	OperationKey        string     `json:"operation_key,omitempty"`
	OperationStartedAt  *time.Time `json:"operation_started_at,omitempty"`
	LastAdmittedAttempt int        `json:"last_admitted_attempt,omitempty"`
}

type TaskEvent struct {
	bun.BaseModel `bun:"table:task_events"`
	TaskID        int64           `bun:",pk"`
	Sequence      int64           `bun:",pk"`
	Type          string          `bun:"type:text,notnull"`
	CreatedAt     time.Time       `bun:",notnull"`
	Details       json.RawMessage `bun:"details_json,type:jsonb,notnull"`
}

type TaskSchedule struct {
	bun.BaseModel `bun:"table:task_schedules"`
	Key           string    `bun:"type:text,pk"`
	NextRunAt     time.Time `bun:",notnull"`
	LatestTaskID  *int64    `bun:",nullzero"`
	Generation    int64     `bun:",notnull"`
}

func (t *Task) CancellationRequested() bool {
	return t != nil && t.CancellationRequestedAt != nil
}

var _ bun.BeforeAppendModelHook = (*Task)(nil)

// BeforeAppendModel stamps the audit columns on insert. The database has no
// timestamp default, so every row is written with one encoding instead of two
// that sort against each other inside the same second.
func (t *Task) BeforeAppendModel(_ context.Context, query bun.Query) error {
	if _, ok := query.(*bun.InsertQuery); !ok {
		return nil
	}
	now := time.Now().UTC()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = now
	}
	return nil
}
