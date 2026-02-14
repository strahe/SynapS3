package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/uptrace/bun"
)

// Uploader polls the task queue for upload_to_sp tasks and processes them.
type Uploader struct {
	db           *bun.DB
	concurrency  int
	pollInterval time.Duration
	logger       *slog.Logger
}

// NewUploader creates a new SP upload worker.
func NewUploader(db *bun.DB, concurrency int, pollInterval time.Duration, logger *slog.Logger) *Uploader {
	return &Uploader{
		db:           db,
		concurrency:  concurrency,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

func (u *Uploader) Name() string { return "uploader" }

func (u *Uploader) Run(ctx context.Context) error {
	return pollLoop(ctx, u.pollInterval, u.tick)
}

func (u *Uploader) tick(ctx context.Context) error {
	// TODO: claim pending upload_to_sp tasks with lease semantics,
	// verify object generation, upload to SP via go-synapse SDK.
	// State transitions: cached → uploading (claim), then uploading → uploaded (success)
	// or uploading → failed (error). Use state.TransitionState for all changes.
	u.logger.Debug("uploader tick")
	return nil
}
