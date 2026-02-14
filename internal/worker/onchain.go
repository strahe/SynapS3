package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/uptrace/bun"
)

// OnChain polls for uploaded objects and submits them to the Filecoin chain
// by adding roots to their bucket's ProofSet.
type OnChain struct {
	db           *bun.DB
	concurrency  int
	pollInterval time.Duration
	logger       *slog.Logger
}

// NewOnChain creates a new on-chain worker.
func NewOnChain(db *bun.DB, concurrency int, pollInterval time.Duration, logger *slog.Logger) *OnChain {
	return &OnChain{
		db:           db,
		concurrency:  concurrency,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

func (o *OnChain) Name() string { return "onchain" }

func (o *OnChain) Run(ctx context.Context) error {
	return pollLoop(ctx, o.pollInterval, o.tick)
}

func (o *OnChain) tick(ctx context.Context) error {
	// TODO: find objects in "uploaded" state, verify ProofSet exists for bucket,
	// call AddRoots via go-synapse SDK.
	// State transitions: uploaded → onchaining (claim), then onchaining → onchained (success)
	// or onchaining → failed (error). Use state.TransitionState for all changes.
	o.logger.Debug("onchain tick")
	return nil
}
