package model

import (
	"time"

	"github.com/uptrace/bun"
)

// TaskType represents the kind of async operation.
type TaskType string

const (
	TaskTypeUpload     TaskType = "upload"
	TaskTypeEvictCache TaskType = "evict_cache"
)

// TaskStatus represents the processing state of a task.
type TaskStatus string

const (
	TaskStatusQueued    TaskStatus = "queued"
	TaskStatusScheduled TaskStatus = "scheduled"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusWaiting   TaskStatus = "waiting"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusExhausted TaskStatus = "exhausted"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TaskWaitReason identifies why a task is waiting without treating the wait as
// an error.
type TaskWaitReason string

const (
	TaskWaitReasonDependency           TaskWaitReason = "dependency"
	TaskWaitReasonExternalConfirmation TaskWaitReason = "external_confirmation"
)

// Task represents an async job in the DB-backed queue with lease semantics.
type Task struct {
	bun.BaseModel `bun:"table:tasks"`

	ID             int64                  `bun:",pk,autoincrement"`
	Type           TaskType               `bun:",notnull"`
	Stage          *string                `bun:",nullzero"`
	RefType        string                 `bun:",notnull"` // "object" or "bucket"
	RefID          int64                  `bun:",notnull"`
	RefVersionID   string                 `bun:",notnull"`
	IdempotencyKey string                 `bun:",unique,notnull"`
	Payload        map[string]interface{} `bun:"type:jsonb"`
	Status         TaskStatus             `bun:",notnull,default:'queued'"`
	RetryCount     int                    `bun:",notnull,default:0"`
	MaxRetries     int                    `bun:",notnull,default:5"`
	LastError      *string                `bun:",nullzero"`
	StatusMessage  *string                `bun:",nullzero"`
	WaitReason     *TaskWaitReason        `bun:",nullzero"`
	ScheduledAt    time.Time              `bun:",nullzero,notnull,default:current_timestamp"`
	ClaimedAt      *time.Time             `bun:",nullzero"`
	LeaseUntil     *time.Time             `bun:",nullzero"`
	StartedAt      *time.Time             `bun:",nullzero"`
	CompletedAt    *time.Time             `bun:",nullzero"`
}
