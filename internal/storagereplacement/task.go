package storagereplacement

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

const (
	// StageMigrate advances the replacement control plane through the uploader;
	// item transfers run on the dedicated replacement worker.
	StageMigrate = "replace_provider"
	// StageRetire runs the retirement safety gate through the storage cleanup
	// worker.
	StageRetire = "retire_data_set"
	// StageRetireAbandonedTarget ends the service of a target that a later
	// confirmation replaced before it ever took over the slot.
	StageRetireAbandonedTarget = "retire_abandoned_target"

	replacementIDPayloadKey = "replacement_id"
	migrateTaskKeyPrefix    = "upload:storage-replacement:"
	retireTaskKeyPrefix     = "storage_cleanup:storage-replacement:"
)

// MigrateTaskKey identifies the single migration coordinator for one
// replacement. Item concurrency is owned by the durable replacement queue,
// not by this control-plane task.
func MigrateTaskKey(replacementID int64) string {
	return fmt.Sprintf("%s%d:migrate", migrateTaskKeyPrefix, replacementID)
}

// RetireTaskKey identifies the single retirement coordinator for one
// replacement. It retires the generation being replaced.
func RetireTaskKey(replacementID int64) string {
	return fmt.Sprintf("%s%d:cleanup", retireTaskKeyPrefix, replacementID)
}

// AbandonedTargetTaskKey identifies the cleanup of a target a later
// confirmation abandoned. It is a separate coordinator because it retires the
// opposite generation and answers a different safety question.
func AbandonedTargetTaskKey(replacementID int64) string {
	return fmt.Sprintf("%s%d:abandoned-target", retireTaskKeyPrefix, replacementID)
}

// NewMigrateTask builds the singleton migration coordinator for one
// replacement. It prepares the target, seeds durable items, observes their
// bounded execution state, and hands completed migration to retirement.
func NewMigrateTask(replacementID, bucketID int64, versionID string, maxRetries int, scheduledAt time.Time) *model.Task {
	stage := StageMigrate
	return &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "bucket",
		RefID:          bucketID,
		RefVersionID:   versionID,
		IdempotencyKey: MigrateTaskKey(replacementID),
		Payload:        NewMigratePayload(replacementID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    scheduledAt,
	}
}

// NewRetireTask builds the singleton retirement coordinator for one
// replacement. It runs on the storage cleanup worker because terminating a
// service is a destructive lifecycle action, not upload work.
func NewRetireTask(replacementID, bucketID int64, maxRetries int, scheduledAt time.Time) *model.Task {
	stage := StageRetire
	return &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		Stage:          &stage,
		RefType:        "bucket",
		RefID:          bucketID,
		IdempotencyKey: RetireTaskKey(replacementID),
		Payload:        NewRetirePayload(replacementID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    scheduledAt,
	}
}

// MigratePayload is the persisted state of the migration coordinator. The
// durable item queue owns all transfer state.
type MigratePayload struct {
	ReplacementID int64
}

// NewMigratePayload builds the coordinator payload.
func NewMigratePayload(replacementID int64) map[string]any {
	return map[string]any{replacementIDPayloadKey: replacementID}
}

// ParseMigratePayload decodes a migration coordinator payload. These tasks only
// ever exist after the upgrade that introduced them, so the replacement ID is
// required rather than inferred.
func ParseMigratePayload(task *model.Task) (MigratePayload, error) {
	if task == nil {
		return MigratePayload{}, errors.New("nil replacement migration task")
	}
	replacementID, err := payloadInt64(task.Payload, replacementIDPayloadKey)
	if err != nil {
		return MigratePayload{}, err
	}
	if replacementID <= 0 {
		return MigratePayload{}, fmt.Errorf("replacement migration task %s must be positive", replacementIDPayloadKey)
	}
	return MigratePayload{ReplacementID: replacementID}, nil
}

// NewRetirePayload builds the retirement coordinator payload.
func NewRetirePayload(replacementID int64) map[string]any {
	return map[string]any{replacementIDPayloadKey: replacementID}
}

// NewAbandonedTargetTask builds the coordinator that ends the paid service of a
// target no confirmation uses any more. Without it the abandoned service keeps
// costing money after a later confirmation takes over.
func NewAbandonedTargetTask(replacementID, bucketID int64, maxRetries int, scheduledAt time.Time) *model.Task {
	stage := StageRetireAbandonedTarget
	return &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		Stage:          &stage,
		RefType:        "bucket",
		RefID:          bucketID,
		IdempotencyKey: AbandonedTargetTaskKey(replacementID),
		Payload:        NewRetirePayload(replacementID),
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetries,
		ScheduledAt:    scheduledAt,
	}
}

// ParseRetirePayload decodes a retirement coordinator payload.
func ParseRetirePayload(task *model.Task) (int64, error) {
	if task == nil {
		return 0, errors.New("nil replacement retirement task")
	}
	replacementID, err := payloadInt64(task.Payload, replacementIDPayloadKey)
	if err != nil {
		return 0, err
	}
	if replacementID <= 0 {
		return 0, fmt.Errorf("replacement retirement task %s must be positive", replacementIDPayloadKey)
	}
	return replacementID, nil
}

// IsCoordinatorTask reports whether a task belongs to a replacement. Generic
// exhausted-task retry uses it to refuse work that must resume through the
// dedicated replacement action.
func IsCoordinatorTask(taskType model.TaskType, stage *string) bool {
	if stage == nil {
		return false
	}
	switch {
	case taskType == model.TaskTypeUpload && *stage == StageMigrate:
		return true
	case taskType == model.TaskTypeStorageCleanup && *stage == StageRetire:
		return true
	case taskType == model.TaskTypeStorageCleanup && *stage == StageRetireAbandonedTarget:
		return true
	default:
		return false
	}
}

// Payload values survive a JSON round trip through the task table, so an
// integer can come back as float64 or json.Number depending on the driver.
func payloadInt64(payload map[string]any, key string) (int64, error) {
	if payload == nil {
		return 0, fmt.Errorf("replacement task payload is missing %s", key)
	}
	raw, ok := payload[key]
	if !ok {
		return 0, fmt.Errorf("replacement task payload is missing %s", key)
	}
	switch value := raw.(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case float64:
		return int64(value), nil
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, fmt.Errorf("replacement task payload %s is not an integer: %w", key, err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("replacement task payload %s has type %T, want an integer", key, raw)
	}
}
