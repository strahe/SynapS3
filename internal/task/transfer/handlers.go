package transfer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

const (
	storageDependencyWait     = time.Minute
	storagePollInterval       = 5 * time.Second
	storageSourcePollInterval = 30 * time.Second
	storeAttentionAfter       = 30 * time.Minute
	pullSubmitTimeout         = 30 * time.Second
	pullStatusTimeout         = 4 * time.Second
)

type ReadyDataSetResolver interface {
	OpenReadyDataSet(context.Context, *model.StorageDataSet) (synapse.DataSetTarget, error)
}

type EventPublisher interface {
	Publish(string, map[string]any)
}

type CoordinatorDependencies struct {
	Repositories *repository.Repositories
	CacheGate    *cacheaccess.Gate
	Scheduler    *taskengine.Scheduler
	Messenger    *taskengine.Messenger
}

type CopyCoordinator struct {
	deps CoordinatorDependencies
}

func NewCopyCoordinator(deps CoordinatorDependencies) (*CopyCoordinator, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil || deps.Scheduler == nil || deps.Messenger == nil {
		return nil, errors.New("copy coordinator requires repositories, scheduler and messenger")
	}
	return &CopyCoordinator{deps: deps}, nil
}

type PlanDependencies struct {
	Coordinator *CopyCoordinator
	Cache       cache.Cache
}

type PlanHandler struct {
	*taskengine.FuncHandler
	*CopyCoordinator
	deps PlanDependencies
}

func NewPlanHandler(deps PlanDependencies) (*PlanHandler, error) {
	if deps.Coordinator == nil {
		return nil, errors.New("transfer plan requires a copy coordinator")
	}
	h := &PlanHandler{CopyCoordinator: deps.Coordinator, deps: deps}
	h.FuncHandler = h.transferPlanHandler()
	return h, nil
}

type StoreDependencies struct {
	Coordinator       *CopyCoordinator
	Resolver          ReadyDataSetResolver
	Cache             cache.Cache
	CacheGate         *cacheaccess.Gate
	Events            EventPublisher
	ParkedPieces      synapse.ParkedPieceChecker
	CommitMaxBacklog  int
	UploadConcurrency int
	Logger            *slog.Logger
}

type StoreHandler struct {
	*taskengine.FuncHandler
	*CopyCoordinator
	deps StoreDependencies
}

func NewStoreHandler(deps StoreDependencies) (*StoreHandler, error) {
	if deps.Coordinator == nil {
		return nil, errors.New("store requires a copy coordinator")
	}
	if deps.UploadConcurrency < 0 {
		return nil, errors.New("upload concurrency cannot be negative")
	}
	if deps.UploadConcurrency == 0 {
		deps.UploadConcurrency = config.DefaultUploadConcurrency
	}
	if deps.CommitMaxBacklog == 0 {
		deps.CommitMaxBacklog = config.DefaultCommitMaxBacklog
	}
	if deps.CommitMaxBacklog < 1 {
		return nil, errors.New("batch backlog must be positive")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	h := &StoreHandler{CopyCoordinator: deps.Coordinator, deps: deps}
	h.FuncHandler = h.storeHandler()
	return h, nil
}

type PullDependencies struct {
	Coordinator      *CopyCoordinator
	Resolver         ReadyDataSetResolver
	Cache            cache.Cache
	ParkedPieces     synapse.ParkedPieceChecker
	CommitMaxBacklog int
}

type PullHandler struct {
	*taskengine.FuncHandler
	*CopyCoordinator
	deps PullDependencies
}

func NewPullHandler(deps PullDependencies) (*PullHandler, error) {
	if deps.Coordinator == nil {
		return nil, errors.New("pull requires a copy coordinator")
	}
	if deps.CommitMaxBacklog == 0 {
		deps.CommitMaxBacklog = config.DefaultCommitMaxBacklog
	}
	if deps.CommitMaxBacklog < 1 {
		return nil, errors.New("batch backlog must be positive")
	}
	h := &PullHandler{CopyCoordinator: deps.Coordinator, deps: deps}
	h.FuncHandler = h.pullHandler()
	return h, nil
}

type storeCheckpoint struct {
	AttemptedAt        time.Time `json:"attempted_at"`
	IntendedPieceCID   string    `json:"intended_piece_cid"`
	ProviderServiceURL string    `json:"provider_service_url"`
	IngressAttempt     int       `json:"ingress_attempt,omitempty"`
}

type pullCheckpoint struct {
	AttemptID     string    `json:"attempt_id"`
	Accepted      bool      `json:"accepted,omitempty"`
	NextRequestAt time.Time `json:"next_request_at,omitzero"`
}

func (h *CopyCoordinator) wake(ctx context.Context, tx *repository.Repositories, ids []int64) (int, error) {
	count, err := h.deps.Scheduler.WakeInTransaction(ctx, tx, ids, taskengine.WakePendingFilter{
		Types: []model.TaskType{model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore},
	})
	if err != nil {
		return 0, err
	}
	pulls, err := h.deps.Scheduler.WakeInTransaction(ctx, tx, ids, taskengine.WakePendingFilter{
		Types:           []model.TaskType{model.TaskTypeStoragePull},
		SkipWaitReasons: []string{storagepull.WaitQueueFull},
	})
	return count + pulls, err
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
