package task

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
	registry  *Registry
	repos     *repository.Repositories
	retention time.Duration
}

type EnqueueRequest struct {
	Type           model.TaskType
	IdempotencyKey string
	Input          any
	SubjectType    string
	SubjectKey     string
	AvailableAt    time.Time
}

func NewService(registry *Registry, repos *repository.Repositories, retention time.Duration) (*Service, error) {
	if registry == nil || repos == nil || repos.Tasks == nil || retention <= 0 {
		return nil, errors.New("task service requires registry, repository, and positive retention")
	}
	return &Service{registry: registry, repos: repos, retention: retention}, nil
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
		AvailableAt: availableAt, RetryLimit: cloneInt(definition.RetryLimit),
	}
	if request.SubjectType != "" {
		task.SubjectType = &request.SubjectType
		task.SubjectKey = &request.SubjectKey
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

func (s *Service) Retry(ctx context.Context, id int64) error {
	task, err := s.repos.Tasks.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if task == nil || task.Status != model.TaskStatusFailed {
		return repository.ErrNotFound
	}
	definition, ok := s.registry.Definition(task.Type)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownType, task.Type)
	}
	if !definition.manualRetryAllowed(task) {
		return ErrRetryUnsupported
	}
	return s.repos.Tasks.RetryFailed(ctx, id)
}

func (s *Service) Acknowledge(ctx context.Context, id int64) error {
	return s.repos.Tasks.AcknowledgeFailed(ctx, id, s.retention)
}

func (s *Service) WakeInTransaction(ctx context.Context, txRepos *repository.Repositories, ids []int64) (int, error) {
	if txRepos == nil || txRepos.Tasks == nil {
		return 0, errors.New("transaction task repository is required")
	}
	return txRepos.Tasks.WakePending(ctx, ids)
}

func (s *Service) Retryable(task *model.Task) bool {
	if task == nil || task.Status != model.TaskStatusFailed {
		return false
	}
	definition, ok := s.registry.Definition(task.Type)
	return ok && definition.manualRetryAllowed(task)
}

func (s *Service) Acknowledgeable(task *model.Task) bool {
	return task != nil && task.Status == model.TaskStatusFailed && task.AcknowledgedAt == nil
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
