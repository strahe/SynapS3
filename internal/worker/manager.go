package worker

import (
	"context"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	taskengine "github.com/strahe/synaps3/internal/task"
)

// WalletReceiptChecker observes a previously broadcast wallet transaction.
type WalletReceiptChecker interface {
	TransactionReceipt(context.Context, common.Hash) (*ethtypes.Receipt, error)
}

// Manager exposes the single task engine through the application's worker
// lifecycle and health contracts.
type Manager struct {
	engine *taskengine.Engine
	logger *slog.Logger
}

func NewManager(engine *taskengine.Engine, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{engine: engine, logger: logger}
}

// Start runs the universal task engine until ctx is cancelled.
func (m *Manager) Start(ctx context.Context) {
	if m == nil || m.engine == nil {
		return
	}
	m.logger.Info("starting task engine")
	if err := m.engine.Run(ctx); err != nil && ctx.Err() == nil {
		m.logger.Error("task engine exited", "error", err)
	}
}

func (m *Manager) WorkerHealth() map[string]bool {
	return map[string]bool{"tasks": m != nil && m.engine != nil && m.engine.Healthy()}
}
