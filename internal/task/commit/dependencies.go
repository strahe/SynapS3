package commit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

const (
	storageDependencyWait = time.Minute
	storagePollInterval   = 5 * time.Second
)

type ReadyDataSetResolver interface {
	OpenReadyDataSet(context.Context, *model.StorageDataSet) (synapse.DataSetTarget, error)
}

type PressureReader interface {
	TargetBytes() (int64, bool)
}

type Dependencies struct {
	Repositories *repository.Repositories
	Scheduler    *taskengine.Scheduler
	RetryPolicy  interface {
		RetryableContext(context.Context, *model.Task) (bool, error)
	}
	Messenger                 *taskengine.Messenger
	Resolver                  ReadyDataSetResolver
	CommitNonces              synapse.CommitNonceReader
	ParkedPieces              synapse.ParkedPieceChecker
	Cache                     cache.Cache
	EvictionPolicy            cache.EvictionPolicy
	Pressure                  PressureReader
	CommitSealOnCachePressure bool
	CommitMaxPieces           int
	CommitMaxWait             time.Duration
	LegacyPieceStorageIDLimit uint64
	Logger                    *slog.Logger
}

type Handler struct {
	*taskengine.FuncHandler
	deps Dependencies
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil || deps.Scheduler == nil || deps.Messenger == nil {
		return nil, errors.New("storage commit requires repositories, scheduler and messenger")
	}
	if deps.CommitMaxPieces == 0 {
		deps.CommitMaxPieces = config.DefaultCommitMaxPieces
	}
	if deps.CommitMaxPieces < 1 || deps.CommitMaxWait < 0 {
		return nil, errors.New("batch limits are invalid")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	h := &Handler{deps: deps}
	h.FuncHandler = h.commitHandler()
	return h, nil
}

func (h *Handler) OnCachePressure(ctx context.Context) error {
	return h.deps.Repositories.Contents.WakeCacheDependentCommitTasks(ctx)
}

func (h *Handler) commitCachePressure(ctx context.Context, requestID string) (bool, error) {
	if !h.deps.CommitSealOnCachePressure || h.deps.EvictionPolicy == cache.EvictionPolicyNone || h.deps.Pressure == nil || h.deps.Cache == nil {
		return false, nil
	}
	target, active := h.deps.Pressure.TargetBytes()
	if !active || h.deps.Cache.CapacitySnapshot().OccupiedBytes() <= target {
		return false, nil
	}
	return h.deps.Repositories.Contents.HasCacheDependentCommitMembers(ctx, requestID)
}

func pressureActive(reader PressureReader) bool {
	if reader == nil {
		return false
	}
	_, active := reader.TargetBytes()
	return active
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
