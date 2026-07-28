package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/cache"
)

func (e *Evictor) runLRUPlanner(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		e.recordTick()
		if err := e.planLRUEvictions(ctx); err != nil && ctx.Err() == nil {
			e.logger.Error("planning LRU cache evictions", "error", err)
		}
		if !sleepUntilNextWorkerPoll(ctx, e.pollInterval) {
			return
		}
	}
}

func (e *Evictor) planLRUEvictions(ctx context.Context) error {
	if e.policy != cache.EvictionPolicyLRU || e.maxCacheBytes <= 0 {
		return nil
	}
	usedBytes := e.cache.UsedBytes()
	highBytes := watermarkBytes(e.maxCacheBytes, e.highWatermarkPercent)
	if usedBytes < highBytes {
		return nil
	}
	lowBytes := watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent)
	activeBytes, err := e.repos.CacheEvictions.ActiveLRUBytes(ctx)
	if err != nil {
		return err
	}
	bytesToPlan := usedBytes - lowBytes - activeBytes
	if bytesToPlan <= 0 {
		return nil
	}

	var plannedBytes int64
	plannedTasks := 0
	for bytesToPlan > 0 {
		createdThisBatch := 0
		terminalSince := time.Now().Add(-lruTerminalRetryDelay)
		candidates, err := e.repos.CacheEvictions.ListLRUCandidates(
			ctx,
			terminalSince,
			lruCandidateBatchSize,
		)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			activated, err := e.repos.CacheEvictions.PlanLRU(
				ctx,
				candidate,
				e.maxRetries,
				terminalSince,
			)
			if err != nil {
				return fmt.Errorf("creating LRU eviction task for version %s: %w", candidate.VersionID, err)
			}
			if !activated {
				continue
			}
			plannedTasks++
			createdThisBatch++
			plannedBytes += candidate.Size
			bytesToPlan -= candidate.Size
			if bytesToPlan <= 0 {
				break
			}
		}
		if len(candidates) < lruCandidateBatchSize || createdThisBatch == 0 {
			break
		}
	}

	if plannedTasks == 0 {
		e.logger.Warn(
			"cache is above the LRU high watermark but no remotely safe entries can be evicted",
			"usedBytes",
			usedBytes,
			"highBytes",
			highBytes,
			"lowBytes",
			lowBytes,
		)
		return nil
	}
	e.logger.Info(
		"planned LRU cache evictions",
		"tasks",
		plannedTasks,
		"plannedBytes",
		plannedBytes,
		"usedBytes",
		usedBytes,
		"targetBytes",
		lowBytes,
	)
	return nil
}

func watermarkBytes(maxBytes int64, percent int) int64 {
	if maxBytes <= 0 || percent <= 0 {
		return 0
	}
	if percent >= 100 {
		return maxBytes
	}
	return (maxBytes/100)*int64(percent) + (maxBytes%100)*int64(percent)/100
}
