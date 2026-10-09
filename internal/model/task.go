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

// Task is a live execution round. Terminal snapshots live in TaskHistory.
// Domain tables own external-effect safety independently of task retention.
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
	SupersededAt   *time.Time `bun:",scanonly"`

	Input      json.RawMessage `bun:"input_json,type:jsonb,notnull"`
	Checkpoint json.RawMessage `bun:"checkpoint_json,type:jsonb,nullzero"`
	Policy     json.RawMessage `bun:"policy_json,type:jsonb,notnull"`
	Runtime    json.RawMessage `bun:"runtime_json,type:jsonb,notnull"`
	Events     json.RawMessage `bun:"events_json,type:jsonb,notnull"`

	Status        TaskStatus     `bun:"type:text,notnull,default:'pending'"`
	ResumeMode    TaskResumeMode `bun:"type:text,notnull,default:'execute'"`
	AvailableAt   time.Time      `bun:",nullzero,notnull"`
	WaitReason    *string        `bun:"type:text,nullzero"`
	RetryCount    int            `bun:"type:integer,notnull,default:0"`
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
	AcknowledgedAt          *time.Time `bun:",scanonly"`
	CreatedAt               time.Time  `bun:",nullzero,notnull"`
	UpdatedAt               time.Time  `bun:",nullzero,notnull"`
	WorkStartedAt           *time.Time `bun:",nullzero"`
}

// TaskHistory preserves one archived execution round under its original task ID.
type TaskHistory struct {
	bun.BaseModel `bun:"table:task_history"`

	TaskID         int64      `bun:",pk"`
	Type           TaskType   `bun:"type:text,notnull"`
	IdempotencyKey string     `bun:"type:text,notnull"`
	InputVersion   int        `bun:"type:integer,notnull"`
	InputHash      string     `bun:"type:text,notnull"`
	SubjectType    *string    `bun:"type:text,nullzero"`
	SubjectKey     *string    `bun:"type:text,nullzero"`
	RetryOfTaskID  *int64     `bun:",nullzero"`
	SupersededAt   *time.Time `bun:",nullzero"`

	Input      json.RawMessage `bun:"input_json,type:jsonb,notnull"`
	Checkpoint json.RawMessage `bun:"checkpoint_json,type:jsonb,nullzero"`
	Policy     json.RawMessage `bun:"policy_json,type:jsonb,notnull"`
	Runtime    json.RawMessage `bun:"runtime_json,type:jsonb,notnull"`
	Events     json.RawMessage `bun:"events_json,type:jsonb,notnull"`

	Status        TaskStatus     `bun:"type:text,notnull"`
	ResumeMode    TaskResumeMode `bun:"type:text,notnull,default:'execute'"`
	AvailableAt   time.Time      `bun:",nullzero,notnull"`
	WaitReason    *string        `bun:"type:text,nullzero"`
	RetryCount    int            `bun:"type:integer,notnull,default:0"`
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

// TaskHistoryFromTask copies the locked round without changing execution evidence.
func TaskHistoryFromTask(task *Task) *TaskHistory {
	if task == nil {
		return nil
	}
	return &TaskHistory{
		TaskID:                  task.ID,
		Type:                    task.Type,
		IdempotencyKey:          task.IdempotencyKey,
		InputVersion:            task.InputVersion,
		InputHash:               task.InputHash,
		SubjectType:             task.SubjectType,
		SubjectKey:              task.SubjectKey,
		RetryOfTaskID:           task.RetryOfTaskID,
		SupersededAt:            task.SupersededAt,
		Input:                   task.Input,
		Checkpoint:              task.Checkpoint,
		Policy:                  task.Policy,
		Runtime:                 task.Runtime,
		Events:                  task.Events,
		Status:                  task.Status,
		ResumeMode:              task.ResumeMode,
		AvailableAt:             task.AvailableAt,
		WaitReason:              task.WaitReason,
		RetryCount:              task.RetryCount,
		FailureReason:           task.FailureReason,
		LastError:               task.LastError,
		StatusMessage:           task.StatusMessage,
		CancellationRequestedAt: task.CancellationRequestedAt,
		CancellationReason:      task.CancellationReason,
		ClaimGeneration:         task.ClaimGeneration,
		ClaimedAt:               task.ClaimedAt,
		LeaseUntil:              task.LeaseUntil,
		StartedAt:               task.StartedAt,
		FinishedAt:              task.FinishedAt,
		AcknowledgedAt:          task.AcknowledgedAt,
		CreatedAt:               task.CreatedAt,
		UpdatedAt:               task.UpdatedAt,
		WorkStartedAt:           task.WorkStartedAt,
	}
}

// TaskRuntime is durable admission evidence, independent of diagnostic events.
type TaskRuntime struct {
	OperationKey        string     `json:"operation_key,omitempty"`
	OperationStartedAt  *time.Time `json:"operation_started_at,omitempty"`
	LastAdmittedAttempt int        `json:"last_admitted_attempt,omitempty"`
}

// TaskEvent is a bounded diagnostic entry owned by its containing task.
type TaskEvent struct {
	TaskID    int64           `json:"-"`
	Sequence  int64           `json:"sequence"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Details   json.RawMessage `json:"details"`
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
