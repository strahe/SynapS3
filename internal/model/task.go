package model

import (
	"time"

	"github.com/uptrace/bun"
)

// TaskType represents the kind of async operation.
type TaskType string

const (
	TaskTypeUploadToSP     TaskType = "upload_to_sp"
	TaskTypeCreateProofSet TaskType = "create_proof_set"
	TaskTypeAddRoots       TaskType = "add_roots"
	TaskTypeEvictCache     TaskType = "evict_cache"
	TaskTypeDeleteProofSet TaskType = "delete_proof_set"
)

// TaskStatus represents the processing state of a task.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// Task represents an async job in the DB-backed queue with lease semantics.
type Task struct {
	bun.BaseModel `bun:"table:tasks"`

	ID             int64                  `bun:",pk,autoincrement"`
	Type           TaskType               `bun:",notnull"`
	RefType        string                 `bun:",notnull"` // "object" or "bucket"
	RefID          int64                  `bun:",notnull"`
	RefGeneration  int64                  `bun:",notnull"`
	IdempotencyKey string                 `bun:",unique,notnull"`
	Payload        map[string]interface{} `bun:"type:jsonb"`
	Status         TaskStatus             `bun:",notnull,default:'pending'"`
	RetryCount     int                    `bun:",notnull,default:0"`
	MaxRetries     int                    `bun:",notnull,default:5"`
	LastError      *string                `bun:",nullzero"`
	ScheduledAt    time.Time              `bun:",nullzero,notnull,default:current_timestamp"`
	ClaimedAt      *time.Time             `bun:",nullzero"`
	LeaseUntil     *time.Time             `bun:",nullzero"`
	StartedAt      *time.Time             `bun:",nullzero"`
	CompletedAt    *time.Time             `bun:",nullzero"`
}
