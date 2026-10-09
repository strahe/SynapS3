package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

type Service struct {
	registry *Registry
	repos    *repository.Repositories
}

type EnqueueRequest struct {
	Type           model.TaskType
	IdempotencyKey string
	Input          any
	SubjectType    string
	SubjectKey     string
	AvailableAt    time.Time
}

func NewService(registry *Registry, repos *repository.Repositories) (*Service, error) {
	if registry == nil || repos == nil || repos.Tasks == nil {
		return nil, errors.New("task service requires registry and repository")
	}
	return &Service{registry: registry, repos: repos}, nil
}

func (s *Service) Enqueue(ctx context.Context, request EnqueueRequest) (*model.Task, bool, error) {
	task, err := s.prepare(request)
	if err != nil {
		return nil, false, err
	}
	return s.enqueuePrepared(ctx, s.repos.Tasks, task)
}

// EnqueueInTransaction uses a transaction-scoped repository set supplied by
// the caller. It exists for domain mutations that must bind the task in the
// same transaction; callers still cannot bypass registry validation.
func (s *Service) EnqueueInTransaction(
	ctx context.Context,
	txRepos *repository.Repositories,
	request EnqueueRequest,
) (*model.Task, bool, error) {
	if txRepos == nil || txRepos.Tasks == nil {
		return nil, false, errors.New("transaction task repository is required")
	}
	prepared, err := s.prepare(request)
	if err != nil {
		return nil, false, err
	}
	return s.enqueuePrepared(ctx, txRepos.Tasks, prepared)
}

// EnqueueOrReplaceTerminalInTransaction is the cached-content upload-plan
// entrypoint. A new live reference creates a successor to terminal work.
func (s *Service) EnqueueOrReplaceTerminalInTransaction(
	ctx context.Context,
	txRepos *repository.Repositories,
	request EnqueueRequest,
) (*model.Task, bool, error) {
	if txRepos == nil || txRepos.Tasks == nil {
		return nil, false, errors.New("transaction task repository is required")
	}
	if request.Type != model.TaskTypeUploadPlan {
		return nil, false, fmt.Errorf("terminal replacement is limited to upload plans: %w", repository.ErrInvalidInput)
	}
	prepared, err := s.prepare(request)
	if err != nil {
		return nil, false, err
	}
	stored, created, err := s.enqueuePrepared(ctx, txRepos.Tasks, prepared)
	if err != nil || created {
		return stored, created, err
	}
	switch stored.Status {
	case model.TaskStatusPending, model.TaskStatusRunning:
		return stored, false, nil
	case model.TaskStatusFailed, model.TaskStatusCancelled:
		if err := txRepos.Tasks.SupersedeTerminal(ctx, stored.ID); err != nil {
			return nil, false, err
		}
		prepared.RetryOfTaskID = &stored.ID
		prepared.ID = 0
		return s.enqueuePrepared(ctx, txRepos.Tasks, prepared)
	case model.TaskStatusCompleted:
		return nil, false, fmt.Errorf("completed task conflicts with cached content: %w", repository.ErrConflict)
	default:
		return nil, false, fmt.Errorf("task has invalid status %q: %w", stored.Status, repository.ErrConflict)
	}
}

// EnqueueTx creates a task and binds its domain owner in one transaction.
// bind must perform database work only and tolerate a transaction retry.
func (s *Service) EnqueueTx(
	ctx context.Context,
	request EnqueueRequest,
	bind func(context.Context, *repository.Repositories, *model.Task, bool) error,
) (*model.Task, bool, error) {
	prepared, err := s.prepare(request)
	if err != nil {
		return nil, false, err
	}
	var stored *model.Task
	var created bool
	err = s.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		var enqueueErr error
		stored, created, enqueueErr = s.enqueuePrepared(ctx, txRepos.Tasks, prepared)
		if enqueueErr != nil {
			return enqueueErr
		}
		if bind != nil {
			return bind(ctx, txRepos, stored, created)
		}
		return nil
	})
	return stored, created, err
}

func (s *Service) prepare(request EnqueueRequest) (*model.Task, error) {
	definition, ok := s.registry.Definition(request.Type)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownType, request.Type)
	}
	if request.IdempotencyKey == "" || (request.SubjectType == "") != (request.SubjectKey == "") {
		return nil, fmt.Errorf("task identity is incomplete: %w", repository.ErrInvalidInput)
	}
	raw, err := json.Marshal(request.Input)
	if err != nil {
		return nil, fmt.Errorf("encoding task input: %w", err)
	}
	canonical, err := canonicalizeInput(definition.Codec, raw)
	if err != nil {
		return nil, fmt.Errorf("validating %s input: %w", request.Type, err)
	}
	subjectType, subjectKey := request.SubjectType, request.SubjectKey
	if definition.Subject != nil {
		subject, err := definition.Subject(canonical)
		if err != nil {
			return nil, fmt.Errorf("deriving %s subject: %w", request.Type, err)
		}
		if subjectType != "" && (subjectType != subject.Type || subjectKey != subject.Key) {
			return nil, fmt.Errorf("%s subject %s/%s does not name its input: %w", request.Type, subjectType, subjectKey, repository.ErrInvalidInput)
		}
		subjectType, subjectKey = subject.Type, subject.Key
	}
	sum := sha256.Sum256(canonical)
	availableAt := request.AvailableAt
	if availableAt.IsZero() {
		availableAt = time.Now()
	}
	task := &model.Task{
		Type: request.Type, IdempotencyKey: request.IdempotencyKey,
		InputVersion: definition.InputVersion, Input: canonical,
		InputHash: hex.EncodeToString(sum[:]),
		Status:    model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute,
		AvailableAt: availableAt, Runtime: json.RawMessage(`{}`), Events: json.RawMessage(`[]`),
	}
	task.Policy, err = encodePolicy(definition.Policy)
	if err != nil {
		return nil, err
	}
	if subjectType != "" {
		task.SubjectType = &subjectType
		task.SubjectKey = &subjectKey
	}
	return task, nil
}

func (s *Service) enqueuePrepared(ctx context.Context, tasks repository.TaskRepository, task *model.Task) (*model.Task, bool, error) {
	stored, created, err := tasks.Enqueue(ctx, task)
	if err != nil {
		return nil, false, err
	}
	if stored.InputVersion != task.InputVersion || stored.InputHash != task.InputHash {
		return nil, false, fmt.Errorf("%w for %s/%s", ErrInputConflict, task.Type, task.IdempotencyKey)
	}
	return stored, created, nil
}

func (s *Service) Get(ctx context.Context, id int64) (*model.Task, error) {
	return s.repos.Tasks.GetByID(ctx, id)
}

func (s *Service) Acknowledge(ctx context.Context, id int64) error {
	return s.repos.Tasks.AcknowledgeFailed(ctx, id)
}

// AcknowledgeMatching acknowledges a backlog of failures in one step and reports
// how many it acknowledged.
func (s *Service) AcknowledgeMatching(ctx context.Context, filter repository.TaskAcknowledgeFilter) (int, error) {
	return s.repos.Tasks.AcknowledgeFailedMatching(ctx, filter)
}

// CountAcknowledgeable reports how many failures AcknowledgeMatching would
// acknowledge under the same filter.
func (s *Service) CountAcknowledgeable(ctx context.Context, filter repository.TaskAcknowledgeFilter) (int, error) {
	return s.repos.Tasks.CountFailedMatching(ctx, filter)
}

func (s *Service) WakeInTransaction(ctx context.Context, txRepos *repository.Repositories, ids []int64) (int, error) {
	if txRepos == nil || txRepos.Tasks == nil {
		return 0, errors.New("transaction task repository is required")
	}
	return txRepos.Tasks.WakePending(ctx, ids)
}

func (s *Service) Retryable(task *model.Task) bool {
	if task == nil || task.Status != model.TaskStatusFailed || task.SupersededAt != nil {
		return false
	}
	definition, ok := s.registry.Definition(task.Type)
	return ok && definition.manualRetryAllowed(task)
}

func (s *Service) Acknowledgeable(task *model.Task) bool {
	return task != nil && task.Status == model.TaskStatusFailed && task.AcknowledgedAt == nil
}
