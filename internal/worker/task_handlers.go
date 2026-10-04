package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
)

// TaskHandlerDependencies are the domain services used by registered task
// handlers. The task engine remains the only owner of polling and leases.
type TaskHandlerDependencies struct {
	Repositories           *repository.Repositories
	Events                 EventPublisher
	Cache                  cache.Cache
	CacheGate              *cacheaccess.Gate
	CacheTracker           *cacheaccess.Tracker
	Storage                synapse.StorageClient
	Wallet                 synapse.WalletOperator
	Receipts               WalletReceiptChecker
	WalletBroadcastTimeout time.Duration
	WalletReceiptTimeout   time.Duration
	Terminator             synapse.ServiceTerminator
	Epochs                 synapse.ChainEpochReader
	Observability          *observability.Service
	UploadSpeedProbe       providerbenchmark.UploadProbe
	ParkedPieces           synapse.ParkedPieceChecker
	CommitNonces           synapse.CommitNonceReader
	EvictionPolicy         cache.EvictionPolicy
	MaxCacheBytes          int64
	MaxWriteBytes          int64 // largest single cache write; 0 means no room is kept for it
	LRUHighPercent         int
	LRULowPercent          int
	DefaultCopies          int
	MaxRetries             int
	// CommitMaxPieces, CommitMaxWait and CommitMaxBacklog shape how transferred
	// copies of one data set are registered together.
	CommitMaxPieces           int
	CommitMaxWait             time.Duration
	CommitMaxBacklog          int
	CommitSealOnCachePressure bool
	// LegacyPieceStorageIDLimit is the first data set ID of the deployment's
	// compact piece layout; zero applies no legacy piece limit.
	LegacyPieceStorageIDLimit uint64
	Logger                    *slog.Logger
}

type EventPublisher interface {
	Publish(topic string, payload map[string]any)
}

type TaskHandlers struct {
	deps          TaskHandlerDependencies
	taskService   *taskengine.Service
	cachePressure atomic.Pointer[cachePressureState]

	lruCapacityMu      sync.Mutex
	lruProjectedBytes  int64
	lruInFlightDeletes int
}

func NewTaskHandlers(deps TaskHandlerDependencies) (*TaskHandlers, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("task handlers require repositories")
	}
	if deps.MaxRetries < 0 {
		return nil, errors.New("task retry limit cannot be negative")
	}
	if deps.WalletBroadcastTimeout < 0 || deps.WalletReceiptTimeout < 0 {
		return nil, errors.New("wallet timeouts cannot be negative")
	}
	if deps.WalletBroadcastTimeout == 0 {
		deps.WalletBroadcastTimeout = 2 * time.Minute
	}
	if deps.WalletReceiptTimeout == 0 {
		deps.WalletReceiptTimeout = 15 * time.Second
	}
	if deps.EvictionPolicy == cache.EvictionPolicyLRU &&
		(deps.MaxCacheBytes <= 0 || deps.LRULowPercent < 0 || deps.LRULowPercent > 100 ||
			deps.LRUHighPercent < 0 || deps.LRUHighPercent > 100 || deps.LRUHighPercent <= deps.LRULowPercent) {
		return nil, errors.New("LRU cache capacity requires valid size and watermarks")
	}
	if deps.CommitMaxPieces == 0 {
		deps.CommitMaxPieces = config.DefaultCommitMaxPieces
	}
	if deps.CommitMaxBacklog == 0 {
		deps.CommitMaxBacklog = config.DefaultCommitMaxBacklog
	}
	if deps.CommitMaxPieces < 1 || deps.CommitMaxWait < 0 || deps.CommitMaxBacklog < deps.CommitMaxPieces {
		return nil, errors.New("storage registration limits are invalid")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &TaskHandlers{deps: deps}, nil
}

func (h *TaskHandlers) SetTaskService(service *taskengine.Service) {
	h.taskService = service
}

func (h *TaskHandlers) RegisterCore(registry *taskengine.Registry) error {
	if registry == nil {
		return errors.New("task registry is required")
	}
	for _, handler := range []taskengine.Handler{
		h.cacheCapacityHandler(),
		h.cacheEvictHandler(),
		h.cacheDurabilityHandler(),
		h.storageCleanupHandler(),
		h.walletHandler(),
		h.observabilityHandler(),
		h.providerTierHandler(model.TaskTypeApprovedProviderRefresh, "approved"),
		h.providerTierHandler(model.TaskTypeEndorsedProviderRefresh, "endorsed"),
		h.providerUploadSpeedHandler(),
		h.gcHandler(),
	} {
		if err := registry.Register(handler); err != nil {
			return err
		}
	}
	return nil
}

// RegisterStorage installs the workflow-neutral storage pipeline. Business
// coordinators may create copy work, but only these handlers mutate a copy's
// provider-side storage state.
func (h *TaskHandlers) RegisterStorage(registry *taskengine.Registry) error {
	if registry == nil {
		return errors.New("task registry is required")
	}
	for _, handler := range []taskengine.Handler{
		h.bucketProvisionHandler(),
		h.uploadPlanHandler(),
		h.dataSetEnsureHandler(),
		h.transferPlanHandler(),
		h.storeHandler(),
		h.pullHandler(),
		h.commitHandler(),
	} {
		if err := registry.Register(handler); err != nil {
			return err
		}
	}
	return nil
}

// RegisterReplacement installs the business coordinator and the destructive
// retirement task. Copy movement remains owned by RegisterStorage handlers.
func (h *TaskHandlers) RegisterReplacement(registry *taskengine.Registry) error {
	if registry == nil {
		return errors.New("task registry is required")
	}
	for _, handler := range []taskengine.Handler{
		h.replacementCoordinateHandler(),
		h.dataSetRetireHandler(),
	} {
		if err := registry.Register(handler); err != nil {
			return err
		}
	}
	return nil
}

type taskHandler struct {
	definition taskengine.Definition
	execute    func(context.Context, taskengine.Execution) taskengine.Result
	recover    func(context.Context, taskengine.Execution) taskengine.Result
}

func (h taskHandler) Definition() taskengine.Definition { return h.definition }

func (h taskHandler) Execute(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	if h.execute == nil {
		return taskengine.Fail(errors.New("execute handler is unavailable"), "handler_unavailable", nil)
	}
	return h.execute(ctx, execution)
}

func (h taskHandler) Recover(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	if h.recover == nil {
		return taskengine.Fail(errors.New("recover handler is unavailable"), "handler_unavailable", nil)
	}
	return h.recover(ctx, execution)
}

func (h *TaskHandlers) retryLimit() *int {
	value := h.deps.MaxRetries
	return &value
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
