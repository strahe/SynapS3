package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/uptrace/bun"
)

// Evictor periodically removes cached files for objects that have been
// successfully uploaded to the Storage Provider.
type Evictor struct {
	db       *bun.DB
	cache    cache.Cache
	interval time.Duration
	logger   *slog.Logger
}

// NewEvictor creates a new cache evictor worker.
func NewEvictor(db *bun.DB, c cache.Cache, interval time.Duration, logger *slog.Logger) *Evictor {
	return &Evictor{
		db:       db,
		cache:    c,
		interval: interval,
		logger:   logger,
	}
}

func (e *Evictor) Name() string { return "evictor" }

func (e *Evictor) Run(ctx context.Context) error {
	return pollLoop(ctx, e.interval, e.tick)
}

func (e *Evictor) tick(ctx context.Context) error {
	// TODO: find objects eligible for cache eviction (state in uploaded/onchaining/onchained
	// AND piece_cid IS NOT NULL), remove cache files.
	// State transitions: uploaded/onchaining/onchained → cache_evicted.
	// Use state.TransitionState for all changes.
	e.logger.Debug("evictor tick")
	return nil
}
